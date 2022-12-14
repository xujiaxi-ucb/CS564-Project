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
    AttrDesc projNamesDesc[projCnt];

    
    	//check if attr is null
    		//if null ,  call scanselect  and return okay
        	//else use the get info function for attrCat in catalog.h
        	//call scanselect then return okay
	int reclen=0;
	//out << " attrname is " << projNames[0].attrName;
	//cout << " attrname is " << projNames; projNames giving problems
    if (attr!=NULL){
		//cout << " attrname is " << projNames[0].attrName;
    	status=attrCat->getInfo(string(attr->relName),string(attr->attrName) , attrDesc);
    	if (status != OK){  return status; }
	}
	//convert projNames to ProjectNamesDesc
	for (int i = 0; i < projCnt; i++){
		
		status=attrCat->getInfo(string(projNames[i].relName),string(projNames[i].attrName) , projNamesDesc[i]);
		if (status != OK){  return status; }
		//cout << " projDesc name is  " << projNamesDesc[i].attrName;
	}
	

	// reclen is wrong, it wrongly shows summed length of all outputs, not 'one'
		//int recLen=attr->attrLen; //length of output tuple
		
	for (int i = 0; i < projCnt; i++)
	{   //cout << " attrname is " << projNames[i].attrLen;
		reclen += abs(projNames[i].attrLen);
	}
    

	status= ScanSelect(result,
		projCnt,
		projNamesDesc,
		&attrDesc,
		op,
		attrValue,
		reclen
		);
	cerr << " status from QU_select is " << status <<endl; 
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
 //cout << "Just got into scanselect" << reclen;
 char tempRecData[reclen]; //removed reclen
 //cout << "right after tempRecData" << endl;
 Record tempRec;
 tempRec.data = (void *) tempRecData;
 //cout << "Jright after temRecData" << endl;
 tempRec.length = reclen;
 //cout << "RIGHT before insertfilescan " << endl;

 Status status;
 InsertFileScan resultRel(result,status);
 //cout << "RIGHT after insertfilescan " << endl;
 if (status != OK){	return status;}
 //open table to be scanned as heapfileobject
 HeapFileScan heapfileobj(string(projNames[0].relName), status);
  //cout << "RIGHT after heapfilescan " << endl;
 if (status != OK){ return status;}
 //check if unconditional scan is required
  if(attrDesc==NULL){
 	 //How do you perform unconditional scan...
	 //filter=NULL;
	 // Jiaxi's code is below:
	  status = heapfileobj.startScan(0, 0, STRING,  NULL, op);
	  if (status != OK){ return status;}
  }
   //cout << "RIGHT after unconditional scan block " << endl;
  //check the attrType and convert filter accordingly
  // if (attrDesc->attrType == STRING){ atoi(filter)
  //  } No need to handle conversion. See Heapfile.c from line 429

 //start scan
 // should be startscan, not scanselect?
 // arguments are also wrong for startscan
 status= heapfileobj.startScan(attrDesc->attrOffset,attrDesc->attrLen,(Datatype)attrDesc->attrType, filter,op);
 if (status != OK){ return status;}
 //cout << "RIGHT after startescan " << endl;
 // scan
 RID currRID;
 Record currRec;
 int resultTupCnt = 0;
 while (heapfileobj.scanNext(currRID) == OK) {
	 cout << "scanning next record" << endl;
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
 cerr << " status from scanselect is " << status <<endl; //STATUS FROM SELECT IS OKAY SHOWING ERROR IS FROM ANOTHER FUNCTION
 return status;

}
