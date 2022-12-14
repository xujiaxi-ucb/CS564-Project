// CS564: Heapfile
// Student name: Jiaxi Xu
// Student ID: 9083446352
// Student name: Xueheng Wang
// Student ID: 9081571748
// Student name: John Che
// Student ID: 908 166 6506
// File purpose: implementations of the buffer manager methods

#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
     {
	// file doesn't exist. First create it and allocate
	// an empty header page and data page.

	db.createFile(fileName);
	bufMgr->allocPage(file , newPageNo, newPage);
	hdrPage = (FileHdrPage*) newPage;
		
    //initiallize value in headerpage
	strcpy(hdrPage-> fileName, fileName.c_str());   // name of file
	hdrPage->firstPage = 0;
    hdrPage->lastPage = 0;
	hdrPage-> pageCnt=0;	// number of pages
	hdrPage-> recCnt=0;		// record count

	bufMgr->allocPage(file, newPageNo, newPage) ; //reassignment
	newPage->init(newPageNo);
		
	hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

       //unpin both pages
       bufMgr->unPinPage(file, newPageNo, true) ;
       bufMgr->unPinPage(file, newPageNo, true) ;
	
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        File* file = filePtr; // save the File* returned
        
        int pageNo = -1;
        // get the header page number 
		status = file->getFirstPage(pageNo);
        
        // read header page to buffer
        status = bufMgr->readPage(file, pageNo, pagePtr);
        // initialize fields of header page
        headerPage = (FileHdrPage*)pagePtr;
        headerPageNo = pageNo;
        hdrDirtyFlag = false;

        // read first data page
        int firstPageNo = headerPage->firstPage;

        // initialize fields of current page
        status = bufMgr->readPage(file, firstPageNo, pagePtr);
        curPage = pagePtr;
        curPageNo = firstPageNo;
        curDirtyFlag = false;
        curRec = NULLRID;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}


// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

//  Returns (via the rec parameter) the record whose rid is stored in curRec

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (curPage != NULL) {
        //if record is not on the currently pinned page, the current page is unpinned
        if (curPageNo != rid.pageNo) {
            status = bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
            if (status != OK) {
                curPage = NULL;
                curPageNo = 0;
                curDirtyFlag = false;
                return status;
            }
        } else {
            // the required page is correctly pinned
            status = curPage->getRecord(rid,rec);
            curRec = rid;
            return status;
        }
    }

    // if page is not in buffer, we will load it
    if ((status = bufMgr->readPage(filePtr, rid.pageNo, curPage)) != OK) {
        return status;
    }
    curDirtyFlag = false;
    curPageNo = rid.pageNo;
    curRec = rid;

    return curPage->getRecord(rid, rec);
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


/* Find next record that satisfies the predicate */
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    Page*	pagePtr;

    // if end of the file
    if (curPageNo == -1) {
        return FILEEOF;
    }

    if (curPage == NULL) { // need to load the first page
        // read first data page
        int firstPageNo = headerPage->firstPage;
        // initialize fields of current page
        status = bufMgr->readPage(filePtr, firstPageNo, pagePtr);
        if (status!=OK) {return status;}
        curPage = pagePtr;
        curPageNo = firstPageNo;
        curDirtyFlag = false;
        // get the first record of current page
        status = curPage->firstRecord(tmpRid);
        if (status == NORECORDS){ // unpin page and return FILEEOF
            bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
            curPage = NULL;
            curPageNo = -1;
            return FILEEOF;
        }else {
            curRec = tmpRid;
            status = getRecord(rec);
            if (status!=OK) {return status;}
            if (matchRec(rec)){ // determine if the first record satisfies the filter
                outRid = curRec; // return curRec
                return OK;
            }
        }
    }
    
    while (getNextRID(nextRid)== OK) { // traverse through every record
        // update current record to next record found
        curRec = nextRid;
        status = getRecord(rec);
        if (status!=OK) {return status;}
        if (matchRec(rec)){ // determine if matches
            outRid = curRec; // return curRec
            return OK;
        } else {
            continue;
        }
    }

    return status;
}

/* helper method to get the RID of next record in the file, read a new page if reaching the end of current page */
const Status HeapFileScan::getNextRID(RID& nextRid){
    Status 	status = OK;
    RID	tmpRid;
    int nextPageNo;
    status = curPage->nextRecord(curRec, tmpRid);
    while (status == ENDOFPAGE || status == NORECORDS){
        if (curPageNo == headerPage->lastPage) {
            bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
            curPage = NULL;
            return FILEEOF;
        }
        status = curPage->getNextPage(nextPageNo);
        if (status!=OK) {return status;}
        // unpin current page
        bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
        // load next page
        curPageNo = nextPageNo;
        curDirtyFlag = false;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status!=OK) {return status;}
        //read first record
        status = curPage->firstRecord(tmpRid);
    }

    if (status == OK) {
        // return next rid
        nextRid = tmpRid;
        return OK;
    } else {
        bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
        curPage = NULL;
        return status;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) 
    {
        // Read the last page and make it the current page
        curPageNo = headerPage->lastPage;
    	status = bufMgr->readPage(filePtr, curPageNo, curPage);
    	if (status != OK) 
        {
            return status;
        } 
    } else if ((status = curPage->insertRecord(rec,rid)) != OK) // If curPage is full
    {
        // Allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) 
        {
            return status;
        } 
        // Initialize the empty page
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1);
        if (status != OK) 
        {
            return status;
        } 
        // Modify header page contents properly
        headerPage -> lastPage = newPageNo;
        headerPage -> pageCnt++;
        hdrDirtyFlag = true;
        // Link up new page appropriately
        status = curPage->setNextPage(newPageNo);
        if (status != OK) 
        {
            return status;
        } 
        // Make current page the newly allocated page
        // Unpin curPage
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (status != OK)
        {
            curDirtyFlag = false;
            curPageNo = -1;
            curPage = NULL;
            status = bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }
        // Set curPage and curPageNo
        curPage = newPage;
	    curPageNo = newPageNo;
        // Insert record
        status = curPage->insertRecord(rec, rid);
        outRid = rid;
        headerPage -> pageCnt++;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
        return status;
    } else // if nothing is wrong
    {
        status = curPage->insertRecord(rec, rid);
        outRid = rid;
        headerPage -> pageCnt++;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
        return status;
    }  
}
