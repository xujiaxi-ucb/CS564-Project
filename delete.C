// CS564 Project: Query Operators
// Student name: Jiaxi Xu
// Student ID: 9083446352
// File purpose: implementation of the deletion method

#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
AttrDesc attrDesc;
Status status;
HeapFileScan* heapFileScan;
RID rid;
int intNumber;
float fNumber;
const char *filter;

if (relation.empty()) {
	return BADCATPARM;
}

// if attrName is null, delete everything in the relation
if (attrName.length() == 0) {
	// create a scan in the relation
	heapFileScan = new HeapFileScan(relation, status);
	if (status != OK) {
		return status;
	}
	// locate all the qualifying tuples using a filtered HeapFileScan
	heapFileScan->startScan(0,0,STRING,NULL,EQ);
	while ((status = heapFileScan->scanNext(rid)) != FILEEOF) {
		// delete the matched record
		status = heapFileScan->deleteRecord();
		if (status != OK) {
			return status;
		}
	}
	return OK;
}

// get the attribute's info
status = attrCat->getInfo(relation,attrName,attrDesc);
if (status != OK) {
	return status;
}

// if attrName is not full, create a new scan in the relation
heapFileScan = new HeapFileScan(relation, status);

// for different datatypes, convert attrValue into different filter types
if (type == INTEGER) {
intNumber = atoi(attrValue);
filter = (char *)&intNumber;
} else if (type == FLOAT) {
fNumber = atof(attrValue);
filter = (char *)&fNumber;
} else if (type == STRING) {
filter = attrValue;
}
// locate all the qualifying tuples using a filtered HeapFileScan
status = heapFileScan->startScan(attrDesc.attrOffset,attrDesc.attrLen,type, filter, op);
if (status != OK) {
	return status;
}

// delete the matched record
while ((status = heapFileScan->scanNext(rid)) == OK) {
		status = heapFileScan->deleteRecord();
		if (status != OK) {
			return status;
		}
}

// if deletion completed without errors, end scanning and return OK
heapFileScan->endScan();
return OK;
}
