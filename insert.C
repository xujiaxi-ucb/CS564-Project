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
	Record rec;
	Status status;
	int cnt;
	AttrDesc *attrs; // an array of AttrDesc
	
	// insertFileScan
	InsertFileScan* ifs = new InsertFileScan(relation, status);
	if (status != OK) return status;
	
	status = attrCat -> getRelInfo(relation, cnt, attrs);
	if (status != OK) return status;
	
	// if the counts of attributes are not the same, return OK
	if (attrCnt!=cnt) {return OK;}
	
	int length_data = 0; // calculate length of the record
	for (int i = 0; i < cnt; i++) {
        length_data += attrs[i].attrLen;
    }
    
	// construct the record
	char data[length_data];
	AttrDesc current;
	attrInfo current_info;
	int find = 0;
	
	for (int ind = 0; ind < cnt; ind ++) {
		current = attrs[ind];
		
		// find corresponding attrInfo
		for (int i = 0; i < attrCnt; i++) {
			if (strcmp(attrList[i].attrName, current.attrName) == 0){ 
				current_info = attrList[i];
				find = 1;
				break;
			}
		}
		
		// if not find corresponding attrInfo from the input
		if (find == 0) {return OK;}
		char * value;
		int temp1;
		float temp2;
		// convert to corresponding type
		switch (current_info.attrType) {
			case STRING:
				value = (char*)current_info.attrValue;
				break;
            case INTEGER:
                temp1 = atoi((char*)current_info.attrValue);
                value = (char*)&temp1;
                break;
            case FLOAT:
                temp2 = atof((char*)current_info.attrValue);
                value = (char*)&temp2;
                break;
        }
// 		if (ind == 0) { //allocate memory to store the record
// 			data = (void *)malloc(length_data);
// 		}else { //reallocate memory to store the record
// 			data = (void *)realloc(data, length_data);
// 		}
		
		memcpy(data + current.attrOffset, value, current.attrLen);
		// copy data
		// memcpy(&((char*)data)[current.attrOffset], current_info.attrValue, current_info.attrLen);
		find = 0;
	}
	
	// Record
	rec.length = length_data;
	rec.data = (void*)data;
	RID rid;
	status = ifs->insertRecord(rec, rid);
	delete ifs;
  	return status;
}
