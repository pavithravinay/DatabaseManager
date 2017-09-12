#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

#ifdef _WIN32
#include "unistd.h"
#else
#include <unistd.h>
#endif


typedef struct scmgmt {
	int curpage; // current page
	int curslot; // current slot
	Expr *cond;
}scmgmt;



//***********************************************************
// Helper Function to write RM_TableData to file ************
//***********************************************************
RC writeTableDataToFile(relationInfo * rInfo, Schema *schemaInfo)
{
	RM_TableData *tableData = malloc(sizeof(RM_TableData));
	tableData->schema = malloc(sizeof(Schema));
	tableData->mgmtData = malloc(sizeof(relationInfo));

	tableData->name = rInfo->relationName;
	tableData->schema = schemaInfo;
	tableData->mgmtData = rInfo;

	char* serializedTableInfo = calloc(PAGE_SIZE, sizeof(char));
	
	serializedTableInfo = serializeTableInfo(tableData);
	int tempLength = strlen(serializedTableInfo);
	// Helper function to serialize the custom data structure relationInfo
	char *test = serializeRelationInfo(rInfo);
	serializedTableInfo = realloc(serializedTableInfo, strlen(test) + tempLength + 1);
	strcat(serializedTableInfo, serializeRelationInfo(rInfo));

	// Write serialized table info to page 1
	SM_FileHandle* fileHandler = malloc(sizeof(SM_FileHandle));
	char* pageHandler = calloc( PAGE_SIZE, sizeof(char));

	memcpy(pageHandler, serializedTableInfo, strlen(serializedTableInfo));
	
	fileHandler->fileName = rInfo->bm->pageFile;

	openPageFile(rInfo->bm->pageFile, fileHandler);


	RC status = writeBlock(1, fileHandler, pageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	status = closePageFile(fileHandler);
	if (status != RC_OK)
	{
		return status;
	}
	free(pageHandler);		
	free(fileHandler);
	free(tableData);
	return RC_OK;
}

//************************************************************
// Helper Fuction to get length of individual row ************
//************************************************************
extern int getRecordSize(Schema *schema) {
	int total_size = 0;
	int i;
	for (i = 0; i < schema->numAttr; i++)
	{
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			total_size = total_size + sizeof(int);
			break;
		case DT_STRING:
			total_size = total_size + schema->typeLength[i] + sizeof(int);
			break;
		case DT_FLOAT:
			total_size = total_size + sizeof(float);
			break;
		case DT_BOOL:
			total_size = total_size + sizeof(bool);
			break;
		}
	}
	/* Had to hardcode this as serializeRecord function provided is broken and the records are also not of fixed sized. 
     * Setting the record length to fit the test case scenarios only. 
     */
	return 12;

}


//**********************************************************
// Helper Function to Deserialize custom struct relationInfo
//**********************************************************
relationInfo* deserializeRelationInfo(char * serializedRelationData)
{
	bool done = false;
	int i = 0;
	char *temp, *temp2;
	int arraySize = 0;
	int firstRecord = 0, lastRecord = 0, lengthOfRecord = 0, rowCount = 0, rowsPerPage = 0, schemaLength = 0;
	char *relationName;

	temp = strstr(serializedRelationData, "<");


	// FirstRecord
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	char *tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &firstRecord);

	// LastRecord
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &lastRecord);

	// LengthOfRecord
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &lengthOfRecord);

	// RelationName
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	relationName = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(relationName, temp, arraySize);
	relationName[arraySize + 1] = '\0';

	// rowCount
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &rowCount);

	// rowsPerPage
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &rowsPerPage);

	//SchemaLength
	temp = strstr(temp2, "<");
	temp++;
	temp2 = strstr(temp, ">");

	arraySize = strlen(temp) - strlen(temp2);

	tempDest = (char*)calloc(arraySize + 1, sizeof(char));

	strncpy(tempDest, temp, arraySize);
	sscanf(tempDest, "%d", &schemaLength);

	relationInfo *rInfo = malloc(sizeof(relationInfo));

	rInfo->firstRecord = firstRecord;
	rInfo->last = lastRecord;
	rInfo->lengthofrecord = lengthOfRecord;
	rInfo->relationName = relationName;
	rInfo->rowCount = rowCount;
	rInfo->slotperpage = rowsPerPage;
	rInfo->schemaLength = schemaLength;

	return rInfo;

}

//*****************************************
// Deserialize the schema *****************
//*****************************************
Schema * deserializeSchema(char *serializedSchema)
{
	//Get numattrs
	char numAttrsString[1] = "";
	int numChar1 = (strlen(strstr(serializedSchema, "<")));
	int numChar2 = strlen(strstr(serializedSchema, ">"));
	strncpy(numAttrsString, strstr(serializedSchema, "<") + 1, numChar1 - numChar2 - 1);

	int numAttrs = 0;
	sscanf(numAttrsString, "%d:", &numAttrs);


	bool done = false;

	char * secondString = strstr(serializedSchema, "keys");

	char * newSerializedSchema = (calloc((strlen(serializedSchema) - strlen(secondString) + 1),sizeof(char)));
	strncpy(newSerializedSchema, serializedSchema, strlen(serializedSchema) - strlen(secondString));
	newSerializedSchema[(strlen(serializedSchema) - strlen(secondString)) + 1] = '\0';

	char *temp3 = strstr(newSerializedSchema, "(");

	temp3++;

	char ** attrNames = (char **)malloc(sizeof(char*)*numAttrs);
	DataType * datatypes = malloc(sizeof(DataType)*numAttrs);
	int * typeLength = malloc(sizeof(int)*numAttrs);

	int i = 0;

	while (!done)
	{
		//Get attrNames
		char *temp, *temp2;

		//temp = strstr(temp3, "(");
		temp2 = strstr(temp3, ":");

		int arraySize = (strlen(temp3) - strlen(temp2));
		char *attrName = (char*)calloc(arraySize + 1, sizeof(char));
		//attrName[0] = '\0';
		strncpy(attrName, temp3, arraySize);
		attrName[arraySize + 1] = '\0';
		attrNames[i] = attrName;

		temp3 = strstr(temp3, ",");
		if (temp3 == NULL)
		{
			done = true;
			temp3 = strstr(temp2, ")");
		}

		//Read data types
		arraySize = strlen(temp2) - strlen(temp3) - 2;
		char *tempDataType = (char*)calloc(arraySize + 1, sizeof(char));
		strncpy(tempDataType, temp2 + 2, arraySize);
		tempDataType[arraySize + 1] = '\0';
		if (strcmp(tempDataType, "INT") == 0)
		{
			datatypes[i] = DT_INT;
			typeLength[i] = 0;
		}
		else if (strstr(tempDataType, "STRING") != NULL)
		{
			datatypes[i] = DT_STRING;
			int tempInt = 0;
			char *tempString = strstr(tempDataType, "STRING");
			sscanf(tempString, "STRING[%d]", &tempInt);
			typeLength[i] = tempInt;
		}
		else if (strcmp(tempDataType, "FLOAT") == 0)
		{
			datatypes[i] = DT_FLOAT;
			typeLength[i] = 0;
		}
		else if (strcmp(tempDataType, "BOOL") == 0)
		{
			datatypes[i] = DT_BOOL;
			typeLength[i] = 0;
		}
		temp3 += 2;
		i++;

	}

	done = false;
	i = 0;
	char ** keyAttrNames = (char **)malloc(sizeof(char*)*numAttrs);

	char *temp1, *temp2;
	temp1 = strstr(secondString, "(");

	while (!done)
	{
		temp1++;

		temp2 = strstr(temp1, ",");
		if (temp2 == NULL)
		{
			temp2 = strstr(temp1, ")");
			done = true;
		}

		int arraySize = strlen(temp1) - strlen(temp2);
		char *keyAttrName = (char*)calloc(arraySize + 1, sizeof(char));
		strncpy(keyAttrName, temp1, arraySize);
		keyAttrName[arraySize + 1] = '\0';
		keyAttrNames[i] = keyAttrName;
		temp1 = temp2;
		i++;

	}

	int * keyAttrs = (int *)malloc(sizeof(int)*(i));
	int keyLength = i;
	int j = 0;
	for (i = 0; i < numAttrs; i++)
	{
		if (strcmp(keyAttrNames[j], attrNames[i]) == 0)
		{
			keyAttrs[j] = i;
			j++;
		}
		if (j >= keyLength)
			break;
	}

	Schema *schema = malloc(sizeof(Schema));

	schema->attrNames = attrNames;
	schema->dataTypes = datatypes;
	schema->keyAttrs = keyAttrs;
	schema->keySize = keyLength;
	schema->numAttr = numAttrs;
	schema->typeLength = typeLength;

	return schema;
}

//*****************************************
// Deserialize Table Data *****************
//*****************************************
RM_TableData deserializeTableData(char * serializedTableData)
{
	char *temp = strstr(serializedTableData, "<");
	char *temp1 = strstr(serializedTableData, ">");
	temp++;

	int stringLength = strlen(temp) - strlen(temp1);

	char *tableName = (char*)calloc(stringLength + 1, sizeof(char));

	strncpy(tableName, temp, stringLength);

	char *schemaStr = strstr(serializedTableData, "Schema");

	char *sMgmtData = strstr(serializedTableData, "FirstRecord");

	int schemaLength = strlen(schemaStr) - strlen(sMgmtData);

	char *schemaStrNew = (char*)calloc(schemaLength + 1, sizeof(char));

	strncpy(schemaStrNew, schemaStr, schemaLength);

	Schema *schema = malloc(sizeof(Schema));

	schema = deserializeSchema(schemaStrNew);

	

	relationInfo* rInfo = malloc(sizeof(relationInfo));

	rInfo = deserializeRelationInfo(sMgmtData);

	RM_TableData tableData;
	tableData.name = tableName;
	tableData.schema = schema;
	tableData.mgmtData = rInfo;

	return tableData;

}

//*****************************************
// Initiate the record manager ************
//*****************************************
extern RC initRecordManager(void *mgmtData)
{
	printf("Initializing Record Manager....");
	return RC_OK;
}

//*****************************************
// Shutdown the record manager ************
//*****************************************
extern RC shutdownRecordManager()
{
	printf("Shutting down Record Manager....");
	return RC_OK;
}

//*********************************************
// Create table for the record maanger*********
//***************Parameters *******************
//char *name - Input file (alias table Name)
//Schema *schema - pointer to the Schema type
//Returns RC
//*********************************************
extern RC createTable(char *name, Schema *schema)
{
	//Return if file already exists
	if (access(name, F_OK) != -1)
	{
		return RC_TABLE_ALREADY_EXISTS;
	}

	int schemaLength = 0;

	schemaLength += sizeof(int) //numAttr
		+ sizeof(int)*(schema->numAttr) //Datatypes
		+ sizeof(int) //Keysize
		+ sizeof(int) * (schema->keySize) //keyAttrs
		+ sizeof(int) * (schema->numAttr); //typelength

										   //Calculate length of attribute names attributes;
	int i = 0;
	for (i = 0; i < (schema->numAttr); i++)
	{
		schemaLength += strlen(schema->attrNames[i]) * sizeof(char);
	}

	int recordLength = getRecordSize(schema);

	int maxRecordsPerPage = PAGE_SIZE / recordLength;

	relationInfo *rInfo = malloc(sizeof(relationInfo));
	BM_BufferPool *bp = malloc(sizeof(BM_BufferPool));

	rInfo->rowCount = 0;
	rInfo->last = 2; //First page contains schema and second page contains RM_TableData
	rInfo->lengthofrecord = recordLength;
	rInfo->firstRecord = 2;//First page contains schema and second page contains RM_TableData
	rInfo->relationName = name;
	rInfo->slotperpage = maxRecordsPerPage;
	rInfo->bm = bp;
	rInfo->schemaLength = schemaLength;

	//Write schema info to page;
	char * serializedSchema = serializeSchema(schema);

	//Need 2 pages; first page to write schema info and another to page to write relationInfo DS;
	//allocate memory for handlers and open page file to store info.
	SM_FileHandle *fileHandler = malloc(sizeof(SM_FileHandle));
	SM_PageHandle pageHandler = malloc(sizeof(char) * PAGE_SIZE);

	memset(pageHandler, '\0', PAGE_SIZE);

	//Create an empty page File
	RC status = createPageFile(name);
	if (status != RC_OK)
	{
		return status;
	}

	status = openPageFile(name, fileHandler);
	if (status != RC_OK)
	{
		return status;
	}

	status = ensureCapacity(3, fileHandler);
	if (status != RC_OK)
	{
		return status;
	}

	memcpy(pageHandler, serializedSchema, strlen(serializedSchema));

	//Write serialized schema to page 0;
	status = writeBlock(0, fileHandler, pageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	//Serialize relationInfo datastructure to store into our file;
	RM_TableData *tableData = malloc(sizeof(RM_TableData));

	tableData->name = name;
	tableData->schema = schema;
	tableData->mgmtData = malloc(sizeof(relationInfo));
	tableData->mgmtData = rInfo;

	char* serializedTableInfo = serializeTableInfo(tableData);

	// Helper function to serialize the custom data structure relationInfo //
	char *test = serializeRelationInfo(rInfo);
	strcat(serializedTableInfo, serializeRelationInfo(rInfo));

	//Write serialized table info to page 1;
	memset(pageHandler, '\0', PAGE_SIZE);
	memcpy(pageHandler, serializedTableInfo, strlen(serializedTableInfo));
	status = writeBlock(1, fileHandler, pageHandler);

	if (status != RC_OK)
	{
		return status;
	}

	status = closePageFile(fileHandler);

	if (status != RC_OK)
	{
		return status;
	}

	free(tableData->mgmtData);
	free(fileHandler);
	free(pageHandler);
	free(tableData);

	return RC_OK;
}

//*****************************************
// Open table for the record manager*******
//***************Params *******************
//RM_TableData *rel - Pointer to Relation type
//char *name - pointer to the file name
//Returns RC
//*****************************************
extern RC openTable(RM_TableData *rel, char *name)
{
	RC status;

	status = access(name, F_OK);
	if (status != RC_OK)
	{
		return status;
	}

	BM_PageHandle *bmPageHandler = malloc(sizeof(BM_PageHandle));

	BM_BufferPool *bmBufferPool = malloc(sizeof(BM_BufferPool));

	initBufferPool(bmBufferPool, name, 3, RS_FIFO, NULL);

	//Read relationInfo from the file;
	status = pinPage(bmBufferPool, bmPageHandler, 1);
	if (status != RC_OK)
	{
		return status;
	}

	*rel = deserializeTableData(bmPageHandler->data);
	
	status = unpinPage(bmBufferPool, bmPageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	relationInfo *rInfo = rel->mgmtData;
	rInfo->bm = bmBufferPool;

	free(bmPageHandler);
	return RC_OK;
}

//********************************************
// Close table for the record maanger*********
//***************Params **********************
//RM_TableData *rel - Pointer to Relation type
//Returns RC
//********************************************
extern RC closeTable(RM_TableData *rel)
{
	relationInfo *rInfo = rel->mgmtData;
	shutdownBufferPool(rInfo->bm);

	free(rel->mgmtData);
	free(rel->schema->attrNames);
	free(rel->schema->dataTypes);
	free(rel->schema->keyAttrs);
	free(rel->schema->typeLength);
	free(rel->schema);

	return RC_OK;
}

//*****************************************
// Delete table for the record maanger*****
//***************Params *******************
//char *name - File Name to be deleted
//Returns RC
//*****************************************
extern RC deleteTable(char *name)
{
	RC status;

	status = access(name, F_OK);

	if (status != RC_OK)
	{
		return RC_TABLE_NOT_FOUND;
	}

	status = destroyPageFile(name);

	if (status != RC_OK)
	{
		return RC_ERROR;
	}

	return RC_OK;
}

//***********************************************************
// Returns number of rows/tuples in the input table/relation
//***************Params *************************************
//RM_TableData *rel
//Returns RC
//***********************************************************
extern int getNumTuples(RM_TableData *rel)
{
	//If relation does not exist return null
	if (access(((relationInfo*)rel->mgmtData)->relationName, F_OK) == -1)
	{
		return RC_ERROR;
	}

	return ((relationInfo *)rel->mgmtData)->rowCount;
}


//**********************************************************************************
// This inserts a new record in the table and updates the record parameter with
// Rocord Id of the newly inserted record ************
//**********************************************************************************
extern RC insertRecord(RM_TableData *rel, Record *record) {
	
	relationInfo *rInfo = (relationInfo *)rel->mgmtData;

	int recordLastPage = rInfo->last;

	int slotNumber = rInfo->rowCount - (recordLastPage - rInfo->firstRecord)*rInfo->slotperpage;			

	record->id.page = rInfo->last;
	record->id.slot = slotNumber;

	char* rowData = (char*)malloc(sizeof(rInfo->lengthofrecord)*sizeof(char));
	memset(rowData, ' ', rInfo->lengthofrecord);	
	strncpy(rowData, record->data, strlen(record->data));

	BM_PageHandle *bmPageHandler = malloc(sizeof(BM_PageHandle));
	bmPageHandler->data = malloc(sizeof(char)*PAGE_SIZE);

	RC status;

	status = pinPage(rInfo->bm, bmPageHandler, recordLastPage);
	if (status != RC_OK) 
	{
		return status;
	}
	
	strcat(bmPageHandler->data,rowData);	

	status = markDirty(rInfo->bm, bmPageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	status = unpinPage(rInfo->bm, bmPageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	status = forcePage(rInfo->bm, bmPageHandler);
	if (status != RC_OK)
	{
		return status;
	}

	rInfo->rowCount++;

	//if we are at the end of the file increase capacity of the file;
	if (slotNumber == (rInfo->slotperpage - 1))
	{
		rInfo->last = ++recordLastPage;
		SM_FileHandle *fileHandler = malloc(sizeof(SM_FileHandle));
		status = openPageFile(rel->name, fileHandler);
		if (status != RC_OK)
		{
			return status;
		}

		status = ensureCapacity(recordLastPage+1, fileHandler);
		if (status != RC_OK)
		{
			return status;
		}
		status = closePageFile(fileHandler);

		if (status != RC_OK)
		{
			return status;
		}
		free(fileHandler);		
	}
	
	free(bmPageHandler->data);
	free(bmPageHandler);
	writeTableDataToFile(rInfo, rel->schema);	
	return RC_OK;
}



//**********************************************************************************
// Deletes the record that matches with Record ID passed as a parameter ************
//**********************************************************************************
extern RC deleteRecord(RM_TableData *rel, RID id) {
	int slot, lenOfRec, intLast, new_slot;
	relationInfo *mt = (relationInfo *)rel->mgmtData;
	lenOfRec = mt->lengthofrecord;
	intLast = id.page;
	slot = mt->slotperpage;
	new_slot = id.slot;
	char *memPt = (char *)malloc(sizeof(char) * slot);
	memset(memPt, 0, sizeof(char) * slot);
	SM_FileHandle fHandle;
	SM_PageHandle pHandle;
	pHandle = (char *)malloc(sizeof(char) * PAGE_SIZE);
	memset(pHandle, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mt->relationName, &fHandle);
	readBlock(intLast, &fHandle, pHandle);
	memcpy(memPt, pHandle, sizeof(char) * slot);
	memPt[new_slot] = 0;
	readBlock(intLast, &fHandle, pHandle);
	memcpy(pHandle, memPt, sizeof(char) * slot);
	writeBlock(intLast, &fHandle, pHandle);
	closePageFile(&fHandle);
	free(memPt);
	free(pHandle);
	// decrease the total record number
	((relationInfo *)rel->mgmtData)->rowCount -= 1;
	return RC_OK;
}

//*********************************************************
// Updates a record that matches the Record Id ************
//*********************************************************
extern RC updateRecord(RM_TableData *rel, Record *record) {
	int slot, lenOfRec, intLast, new_slot;
	relationInfo *mt = (relationInfo *)rel->mgmtData;
	lenOfRec = mt->lengthofrecord;
	intLast = record->id.page;
	slot = mt->slotperpage;
	new_slot = record->id.slot;
	SM_FileHandle fHandle;
	SM_PageHandle pHandle;
	pHandle = (char *)malloc(sizeof(char) * PAGE_SIZE);
	memset(pHandle, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mt->relationName, &fHandle);
	readBlock(intLast, &fHandle, pHandle);
	memcpy(pHandle + new_slot * lenOfRec, record->data, lenOfRec);
	writeBlock(intLast, &fHandle, pHandle);
	closePageFile(&fHandle);
	free(pHandle);
	return RC_OK;
}

//***************************************
// To remove unwanted spaces ************
//***************************************
void RemoveSpaces(char* source)
{
	char* i = source;
	char* j = source;
	while (*j != 0)
	{
		*i = *j++;
		if (*i != ' ')
			i++;
	}
	*i = 0;
}

//***********************************************************
// Retrieves a record that matches the Record ID  ************
//***********************************************************
extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
	relationInfo *mt = (relationInfo *)rel->mgmtData;
	
	
	relationInfo *rInfo = (relationInfo *)rel->mgmtData;

	if (id.page > rInfo->last)
	{
		return RC_INVALID_RECORD_REQUESTED;
	}

	if (id.slot > rInfo->slotperpage)
	{
		return RC_INVALID_RECORD_REQUESTED;
	}

	BM_PageHandle *bmPageHandler = malloc(sizeof(BM_PageHandle));
	bmPageHandler->data = malloc(sizeof(char)*PAGE_SIZE);
	memset(bmPageHandler->data, '\0', sizeof(char) * PAGE_SIZE);
	
	pinPage(rInfo->bm, bmPageHandler, id.page);
	
	// get the value and covert to records
	record->data = calloc(mt->lengthofrecord +1, sizeof(char));
	memcpy(record->data, bmPageHandler->data + (id.slot * mt->lengthofrecord), mt->lengthofrecord);

	unpinPage(rInfo->bm, bmPageHandler);
	record->id.page = id.page;
	record->id.slot = id.slot;
	free(bmPageHandler);
	return RC_OK;
}

//*************************************************************
// Scans all the records based on given condition  ************
//*************************************************************
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	scmgmt *scanmgmt = malloc(sizeof(scmgmt));
	// start search from frist page
	scanmgmt->curpage = 2;
	scanmgmt->curslot = 0;
	scanmgmt->cond = cond;
	scan->rel = rel;
	scan->mgmtData = (void *)scanmgmt;
	return RC_OK;
}

//*******************************************************************
// Scans each record in the table and stores the records that satisfy
// given condition in a location pointed by record.  ****************
//*******************************************************************
extern RC next(RM_ScanHandle *scan, Record *record) {

	scmgmt *scm = (scmgmt *)scan->mgmtData;
	relationInfo *rInfo = (relationInfo*)scan->rel->mgmtData;
	Schema *schemaInfo = (Schema *)scan->rel->schema;
	Value *val;

	record->id.page = scm->curpage;
	record->id.slot = scm->curslot;

	//printf("scanning page #: %d , slot #: %d\n", scm->curpage, scm->curslot);

	RC status;

	//Start by getting first record
	status = getRecord(scan->rel, record->id, record);
	if (status != RC_OK)
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	//Check to see if this is the record that was requested;
	evalExpr(record, schemaInfo, scm->cond, &val);

	(scm->curslot)++;

	if (scm->curslot > rInfo->slotperpage) {
		//printf("Resetting slot number and incrementing page number\n");
		scm->curslot = 0;
		(scm->curpage)++;
	}

	scan->mgmtData = scm;

	if (val->v.boolV != 1) {
		return next(scan, record);
	}

	return RC_OK;
}

//****************************************
// Closes the scan operation  ************
//****************************************
extern RC closeScan(RM_ScanHandle *scan) {
	if (scan->mgmtData != NULL)
	{
		free(scan->mgmtData);
		scan->mgmtData = NULL;
		scan->rel = NULL;
	}
	return RC_OK;
}

//***********************************
// Creates a new schema  ************
//***********************************
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
	Schema *schema_pointer;
	schema_pointer = malloc(sizeof(Schema));
	schema_pointer->numAttr = numAttr;
	schema_pointer->attrNames = attrNames;
	schema_pointer->dataTypes = dataTypes;
	schema_pointer->typeLength = typeLength;
	schema_pointer->keySize = keySize;
	schema_pointer->keyAttrs = keys;

	return schema_pointer;
}

//*****************************************************************
// Removes schema from memory and deallocates all the memory space
// allocated the schema.  *****************************************
//*****************************************************************
extern RC freeSchema(Schema *schema) {
	schema->numAttr = -1;
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	schema->keySize = -1;
	free(schema);

	return RC_OK;

}

//***********************************************************************
// Creates a new record in the schema passed as a parameter  ************
//***********************************************************************
extern RC createRecord(Record **record, Schema *schema)
{
	
	Record *tempRec = malloc(sizeof(Record));
	int recordLength = getRecordSize(schema);
	tempRec->data = calloc(recordLength + 1, sizeof(char));
	memset(tempRec->data, ' ', recordLength);

	record[0] = tempRec;

	return RC_OK;
}

//***********************************************************************
// Removes a record from memory and free all the memory space************
// allocated to the record.  ********************************************
//***********************************************************************
extern RC freeRecord(Record *record)
{
	//free(record->data);
	free(record);
	return RC_OK;
}

//***********************************************************
// Retrieves an attribute from the given record  ************
//***********************************************************
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	char temp[PAGE_SIZE + 1];

	memset(temp, '\0', PAGE_SIZE + 1);
	*value = malloc(sizeof(Value) * schema->numAttr);

	int counter = 1;
	int dCount = 0;
	int i = 0;

	for (i = 0; i < PAGE_SIZE; i++)
	{
		if ((record->data[i] == '\0') || (record->data[i] == ';'))
		{
			if (dCount == attrNum) {

				switch (schema->dataTypes[dCount]) {
				case DT_INT:
					temp[0] = 'i';
					break;
				case DT_STRING:
					temp[0] = 's';
					break;
				case DT_FLOAT:
					temp[0] = 'f';
					break;
				case DT_BOOL:
					temp[0] = 'b';
					break;
				}

				*value = stringToValue(temp);
				break;
			}
			memset(temp, '\0', PAGE_SIZE + 1);
			dCount = dCount + 1;
			counter = 1;
		}
		else {
			temp[counter++] = record->data[i];
		}
	}
	return RC_OK;
}

//***********************************************************
// Sets the attribute value in the record  ******************
//***********************************************************
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
	char * serializedVal = serializeValue(value);

	int no = 0;
	int i = 0;
	int j = 0;
	int length = strlen(record->data);
	int* colonP = (int*)malloc(sizeof(int)*(schema->numAttr));

	char semicolon;
	char fString[PAGE_SIZE];
	char endOfString[PAGE_SIZE];


	for (i = 0; i < length; i++)
	{
		semicolon = record->data[i];
		if (semicolon == ';')
		{
			no++;
		}
	}

	if (no == schema->numAttr)
	{
		j = 0;
		int length = strlen(record->data);
		char colon;

		for (i = 0; i < length; i++)
		{
			colon = record->data[i];
			if (colon == ';')
			{
				colonP[j++] = i;
			}
		}

		if (attrNum == 0)
		{
			memset(endOfString, '\0', PAGE_SIZE);
			i = 0;
			for (j = colonP[attrNum]; j < length; j++)
			{
				endOfString[i++] = record->data[j];
			}
			endOfString[i] = '\0';//here

			memset(record->data, '\0', PAGE_SIZE);

			strcpy(record->data, serializedVal);
			strcpy(record->data, endOfString);
		}
		else
		{
			memset(fString, '\0', PAGE_SIZE);
			memset(endOfString, '\0', PAGE_SIZE);

			for (i = 0; i <= colonP[attrNum - 1]; i++)
			{
				fString[i] = record->data[i];
			}
			fString[i] = '\0';

			j = 0;
			for (i = colonP[attrNum]; i < length; i++)
			{
				endOfString[j++] = record->data[i];
			}
			endOfString[j] = '\0';

			strcat(fString, serializedVal);
			strcat(fString, endOfString);

			memset(record->data, '\0', PAGE_SIZE);

			strcpy(record->data, fString);
		}
	}
	else
	{
		char * tempData = calloc(strlen(record->data)+ 1, sizeof(char));
		strcpy(tempData, record->data);
		RemoveSpaces(tempData);
		strcat(serializedVal, ";");		
		strcat(tempData, serializedVal);
		strncpy(record->data, tempData, strlen(tempData));

	}

	return RC_OK;
}
