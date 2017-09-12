Advanced Database Organization: Fall 2016

Assignment#3 - Record Manager

Team members:
Divya Vasireddy - A20370052
JayaVijay Jayavelu - A20379656
Naveen Sampath - A20373010
Pavithra Vinay - A20369869 


About this Assignment:
The record manager handles tables with a fixed schema. Clients can insert records, delete records, update records and scan through the records in a table.
A scan is associated with a search condition and only returns records that match the search condition. It will return all the rows if null is provided as search condition.


We have created 2 new structs namely “relationInfo” and “scmgmt”. 
“relationInfo” stores the row count, buffer pool, length of record, slot per page, first page, schema length, last page and relation Info. We are pointing it to the void pointer  mgmtData of struct “RM_TableData”.
“scmgmt” stores current page, current slot and the condition for scanning.


Modified below files from the template provided:
1. dberror.h - to include additional error codes
2. rm_serializer.c - Added a new function “extern char * serializeRelationInfo(relationInfo *rInfo)” to serialize relation info.
3. tables.h - added a new struct “relationInfo” and prototype for “extern char *serializeRelationInfo(relationInfo *rInfo)” function.


Added new error codes in dberror.h:
RC_ERROR - To return error in case of a failed operation
PAGE_ERROR - To check if pool position is valid
RC_TABLE_ALREADY_EXISTS - To check if the table name already exists
RC_TABLE_NOT_FOUND - To check if table exists
RC_INVALID_RECORD_REQUESTED - To check if requested record is valid
 

Additional functions have been added to achieve the result:
RC writeTableDataToFile(relationInfo * rInfo, Schema *schemaInfo) - To write serialized table data RM_TableData into file
extern int getRecordSize(Schema *schema) - To retrieve individual record length
relationInfo* deserializeRelationInfo(char * serializedRelationData) - To deserialize custom struct relation info
Schema * deserializeSchema(char *serializedSchema) - To deserialize schema
RM_TableData deserializeTableData(char * serializedTableData) - To deserialize table data RM_TableData
void RemoveSpaces(char* source) - To remove trailing spaces at the end of the record.


Deliverables: 
1. buffer_mgr_stat.c
2. buffer_mgr_stat.h
3. buffer_mgr.c
4. buffer_mgr.h
5. dberror.c
6. dberror.h
7. dt.h
8. expr.c
9. expr.h
10. record_mgr.c
11. record_mgr.h
12. rm_serializer.c
13. storage_mgr.c
14. storage_mgr.h
15. tables.h
16. test_assign3_1.c
17. test_expr.c
18. test_helper.h
19. Makefile
20. README


To execute:
1.Login to Fourier server
2.Create a new directory and copy the files provided in the zip file
3.Make sure “Makefile” is present the current directory
4.Type “make” (or “make all”) command, code will be compiled and two output files namely “test_assign3” and “test_expr” will be generated.  
5.Type “./testAssign3” and “./testExpr”, one at a time, to see the output.
  “test_assign3” executes the test cases provided in “test_assign3_1.c”
  “test_expr” executes the test cases in “test_expr.c” 
6.Use “make clean” command if you want to clear the output files
7.Repeat step 1-5 to run it again.





