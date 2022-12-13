#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, //total number of attributes to be projected
		       const attrInfo projNames[], //list of all attributes to be proejcted
		       const attrInfo *attr, //pointer which contains relname, attrName and more!
		       const Operator op, 
		       const char *attrValue //search value
			   )
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    Status status;
    AttrDesc attrDesc;
    	//check if attr is null
    		//if null ,  call scanselect  and return okay
        	//else use the get info function for attrCat in catalog.h
        	//call scanselect then return okay
    if (attr != NULL){
    	Status status=attrCat->getInfo(string(attr->relName),string(attr->attrName) , attrDesc);
    	if (status != OK){  return status; }
    }

    int recLen=attr->attrLen; //length of output tuple
    status = ScanSelect(result, projCnt, projNames[], &attrDesc, op, attrValue, recLen);
    return status;
}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
//TODO
//check status. Check status whenever you use it
 //create temp rec
 char tempRecData[reclen];
 Record tempRec;
 tempRec.data = (void *) tempRecData;
 tempRec.length = reclen;

 Status status;
 InsertFileScan resultRel(result,status);
 if (status != OK){	return status;}
 //open table to be scanned as heapfileobject
 HeapFileScan heapfileobj(string(projNames[0].relName), status);
 if (status != OK){ return status;	 }
 //check if unconditional scan is required
  if(attrDesc==NULL){
 	 //How do you perform unconditional scan...
	  filter=NULL;
  }
  //check the attrType and convert filter accordingly
  // if (attrDesc->attrType == STRING){ atoi(filter)
  //  } No need to handle conversion. See Heapfile.c from line 429

 //start scan
 status= heapfileobj.startScan(attrDesc->attrOffset,attrDesc->attrLen,(Datatype)attrDesc->attrType, filter,op);
 if (status != OK){ return status;	 }

 // scan
 RID currRID;
 Record currRec;
 int resultTupCnt = 0;
 while (heapfileobj.scanNext(currRID) == OK) {
     status = heapfileobj.getRecord(currRec);
     if (status == OK){ //if we find a record, copy to tempRec
    	int tempRecOffset = 0;
		 for (int i = 0; i < projCnt; i++){
			 	 memcpy(tempRecData + tempRecOffset,
    			 (char *)currRec.data + projNames[i].attrOffset,
				 projNames[i].attrLen); //COPY TO TempRec

			 	tempRecOffset += projNames[i].attrLen; //increment offset for next attribute reading
		 }
     }else {
    	 return status;
     }
     //insert into output table
     RID newRID;
     status = resultRel.insertRecord(tempRec, newRID); //remember that newRID is output!
     ASSERT(status == OK);
     resultTupCnt++;
 }
 printf("tuple nested join produced %d result tuples \n", resultTupCnt);
 return OK;

}
