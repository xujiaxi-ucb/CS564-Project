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
    int reclen = 0; // length of record: summed length of all outputs
    
    AttrDesc projNamesDesc[projCnt]; // stores all attributes to be projected
    //convert projNames to ProjectNamesDesc 
    for (int i = 0; i < projCnt; i++){
    	status=attrCat->getInfo(string(projNames[i].relName),string(projNames[i].attrName), projNamesDesc[i]);
    	if (status != OK){  return status; }
    	reclen += projNamesDesc[i].attrLen;
    }
    
//   open table to be scanned as heapfileobject
//     HeapFileScan heapfileobj(string(projNames[0].relName), status);
//     if (status != OK){ return status;	 }
    
    		//check if attr is null
    		//if null ,call scanselect and return okay
        	//else use the get info function for attrCat in catalog.h
        	//call scanselect then return okay
    const char* filter;
    Operator op1;
    if (attr != NULL){
    	status=attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
    	if (status != OK){  return status; }
    	//char* temp1;
    	int temp2;
    	float temp3;
    	// convert to proper type
    	switch (attr->attrType) {
    		case STRING:
    			filter = attrValue;
    			break;
            case INTEGER:
            	temp2 = atoi(attrValue);
                filter = (char*)&temp2;
                break;
            case FLOAT:
            	temp3 = atof(attrValue);
                filter = (char*)&temp3;
                break;           
        }	
    } else{ // if NULL, unconditional scan, set filter = NULL, do not care other fields 
    	attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
    	attrDesc.attrType = STRING;
    	filter = NULL;
    }
	
	// call ScanSelect after all arguments are constructed
	status = ScanSelect(result, projCnt, projNamesDesc, &attrDesc, op, filter, reclen);
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
//cout << "filter: " << endl;
//cout << *filter << endl;
 //TODO
 //check status. Check status whenever you use it
 //create temp rec
 //cout << reclen << endl;
 char tempRecData[reclen];
 Record tempRec;
 tempRec.data = (void *) tempRecData;
 tempRec.length = reclen;
 Status status;
 
 InsertFileScan resultRel(result,status);
 if (status != OK){	return status;}
 
 //open table to be scanned as heapfileobject
 HeapFileScan heapfileobj(string(projNames[0].relName), status);
 if (status != OK){ return status;}
 
// start scan
status = heapfileobj.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
if (status != OK){ return status;}

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