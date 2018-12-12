#ifndef ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H

#include "file_io.h"

/* Execute the query */
int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array);

/* Check if a relation id exists in the intermediate_table, if it doesn't create a new table */
Relation* create_intermediate_table(int relation_Id, Entity **entity, Relation *relation, int *inter_table_number);

/* Check if the relation id exists in an intermediate table */
int exists_in_intermediate_table(int relation_Id, Entity *entity, int *inter_table_number, int *column);

/* Filter a relation and return an array with the rowIDs of the relation that satisfy the filter */
int32_t *filterRelation(int operator, int number, Relation *relation, uint32_t *count);

/*Filter a relation */
void relationFilter(Relation **original_relation, Entity **entity, int relation_Id, int operator, int number);

/* Join 2 Relations */
void relationJoin(Relation **relation1, Relation **relation2, Entity **entity, int relation_Id1, int relation_Id2);

/* Update a column of an intermediate depending on the relation_id according to the rowIDs */
void update_intermediate_table(int relation_Id, Entity **entity, int32_t * rowIDs, uint32_t rowId_count);

/* Print all the intermediate tables of an entity */
void printEntity(Entity *entity);

/* Print the query */
void print_query(Query_Info * query_info, char* query, int query_number);

/* Free the query */
void free_query(Query_Info * query_info);
#endif //ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
