#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
	RID rid;
	Record rec;
	InsertFileScan* ifs;
	Status status;
	int cnt;
	AttrDesc *attrs; // an array of AttrDesc
	
	status = attrCat -> getRelInfo(relation, cnt, attrs);
	if (status != OK) return status;
	
	// if the counts of attributes are not the same, return OK
	if (attrCnt!=cnt) {return OK;}
	
	// construct the record
	void* data;
	AttrDesc current;
	attrInfo current_info;
	int length_data = 0;
	int find = 0;
	
	for (int ind = 0; ind < cnt; ind ++) {
		current = attrs[ind];
		
		// find corresponding attrInfo
		for (int i = 0; i < attrCnt; i++) {
			if (attrList[i].attrName == current.attrName && attrList[i].attrType == current.attrType){ 
				current_info = attrList[i];
				find = 1;
			}
		}
		
		// if not find corresponding attrInfo from the input
		if (find == 0) {return OK;}
		
		length_data = length_data + current_info.attrLen;
		if (ind == 0) { //allocate memory to store the record
			data = (void *)malloc(length_data);
		}else { //reallocate memory to store the record
			data = (void *)realloc(data, length_data);
		}
		
		// copy data
		memcpy(&((char*) data)[current.attrOffset], current_info.attrValue, current_info.attrLen);
		
		find = 0;
	}
	
	// insertFileScan
	ifs = new InsertFileScan(relation, status);
	if (status != OK) return status;
	
	// Record
	rec.length = length_data;
	rec.data = data;
	status = ifs->insertRecord(rec, rid);
	delete ifs;
  	return status;
}
