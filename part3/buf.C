// CS564: Buffer Manager
// Student name: Jiaxi Xu
// Student ID: 9083446352
// Student name: Xueheng Wang
// Student ID: 9081571748
// Student name: John Che
// Student ID: 908 166 6506
// File purpose: implementations of the buffer manager methods

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

// Purpose: Allocates a free frame using the clock algorithm
// parameters: int & frame: frameNo of the allocated
// return: OK if success
// BUFFEREXCEEDED if all buffer frames are pinned, 
// UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk

const Status BufMgr::allocBuf(int & frame) 
{
	 //Iterate through frames in buf table
	 Status status;
	 int frame_count = 1;
	 while (frame_count <= (2*numBufs)) {
		 BufDesc* currbuf = &bufTable[clockHand];
		 
		 if (currbuf->valid == false) { //no valid pages, directly use
		 	 frame = clockHand;
		 	 return OK;
		 } else { // valid page there
	 		 if (currbuf -> pinCnt > 0) { 
	 		 	advanceClock();
	 		 	frame_count ++;
	 		 	continue;
	 		 } else { //pinCnt = 0
	 		 	if (currbuf->refbit==true) { //if hot page, clear refbit and continue
	 		 		currbuf->refbit=false; 
	 		 		advanceClock(); 
	 		 		frame_count ++;
	 		 		continue;
	 		 	} else { // valid page there, but we can use this frame
	 		 		if (currbuf->dirty==false) { // won't flush, just clear and use
	 		 			status = hashTable->remove(currbuf->file,currbuf->pageNo);
	 		 			// if (status != OK) {return status;}
	 		 			currbuf->Clear();
						frame = clockHand;
		 	 			return OK;
	 		 		} else { // need to flush to disk
	 		 			//#ifdef DEBUGBUF
						//	cout << "flushing page " << currbuf->pageNo
						//			 << " from frame " << clockHand << endl;
						//#endif
						status = currbuf->file->writePage(currbuf->pageNo, &(bufPool[clockHand]));
						if (status != OK) {return UNIXERR;}
						currbuf->dirty = false;
						status = hashTable->remove(currbuf->file,currbuf->pageNo);
						//if (status != OK) {return status;}
						currbuf->Clear();
						frame = clockHand;
		 	 			return OK;
	 		 		}
	 		 	}
	 		 }
		 }
	}
	return BUFFEREXCEEDED; //all frames are pinned
}
		 
// Purpose: read a page
// parameters File* file: a pointer to the file
// const int PageNo: page number that we want to read
// Page*& page: a pointer to the reference to the file we want to read
// return: OK if no errors occurred
// UNIXERR if a Unix error occurred
// BUFFEREXCEEDED if all buffer frames are pinned
// HASHTBLERROR if a hash table error occurred

const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status ret_stat = OK;
    int frameNo = -1;
    // look up the page in the hash table, get the corresponding frameNo if exists
    ret_stat = hashTable->lookup(file, PageNo, frameNo);
    if (ret_stat==HASHTBLERROR){
    	return ret_stat;
    }
    // Case: Page is in the buffer pool
    if (ret_stat == OK && frameNo >= 0) {
    	BufDesc* entry = &bufTable[frameNo];
    	entry->pinCnt++;
    	entry->refbit = true;
    	page = & (bufPool[frameNo]);
        //printf("find\n");
    // Case: Page is not in the buffer pool
    } else {	
	        ret_stat = allocBuf(frameNo);
		//printf("%s\n", frameNo.c_str());

		if (ret_stat == BUFFEREXCEEDED) { // all buffer frames are pinned or the call to the I/O layer returned an error
			return BUFFEREXCEEDED;
		} else if (ret_stat == UNIXERR){
			return UNIXERR;
		}else { // frame allocated
			BufDesc* entry = &bufTable[frameNo];
                        entry->Set(file, PageNo);
			ret_stat = file->readPage(PageNo,&(bufPool[frameNo]));
			if (ret_stat == UNIXERR) {
				entry->Clear();
				return UNIXERR;
			}
			// insert to hash table
			ret_stat = hashTable->insert(file, PageNo, frameNo);
			if (ret_stat == HASHTBLERROR) {
				entry->Clear();
				return ret_stat;
			}
			// set buffer frame
		        entry->refbit = true;
			page = &(bufPool[frameNo]);
		}	
	}
	return OK;
}

// Purpose: Unpin a page
// parameters: File* file the file that needs to be unpinned
// const int PageNo the page number that needs to be unpinned
// const bool dirty whether the page is dirty or not 
// return: OK if success
// HASHNOTFOUND if the page is not in the buffer pool hash table
// PAGENOTPINNED if the pin count is already 0

const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo = -1;
    // init stage
    Status status = OK;
    status = hashTable->lookup(file, PageNo, frameNo);
    // HASHNOTFOUND if the page is not in the buffer pool hash table
    if (status != OK) return HASHNOTFOUND;
    BufDesc* entry = &bufTable[frameNo];
    // PAGENOTPINNED if the pin count is already 0
    if (entry->pinCnt == 0) {
        return PAGENOTPINNED;
    }
    if (dirty==true) {
        entry->dirty = true; // if dirty == true, sets the dirty bit
    }
    entry->pinCnt-- ; // Decrements the pinCnt of the frame containing (file, PageNo)
    return OK;
}

// Purpose: allocate a page from a file 
// params: File* file: the file to be allocated a page
// int& PageNo: the page number
// Page*& page: the pointer to the newly allocated buffer frame with page
// return: OK if no errors occurred
// UNIXERR if a Unix error occurred
// BUFFEREXCEEDED if all buffer frames are pinned
// HASHTBLERROR if a hash table error occurred

const Status BufMgr::allocPage(File* file, int& PageNo, Page*& page)
{
        Status status;
        int frameNo = -1;
        int pagenumber = -1;
        // allocate a page from file
        status=file->allocatePage(pagenumber);
        if (status != OK) {
		return status;
	}
        status = allocBuf(frameNo);

		if (status != OK) {return status;}
        
        // insert to hash table  
        status = hashTable->insert(file, pagenumber, frameNo);
        if (status != OK) return status;
        
        // set buffer frame
        BufDesc* entry = &bufTable[frameNo];
        entry->Set(file, pagenumber);
        PageNo = pagenumber;
        // a pointer to the buffer frame allocated for the page via the page parameter
        page = &(bufPool[frameNo]);
        return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
