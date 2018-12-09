#ifndef ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H

#include "file_io.h"

typedef struct intermediate_table_{
    uint32_t num_of_rows; // number of row IDs
    uint32_t num_of_columns; // number of Relation IDs in the intermediate table
    int32_t **intermediate_table; // array that holds the row IDS
    int *relationIDS_of_intermediate_table; // array that tells us which relation id refers to which column of the inter_table
}Intermediate_table;

/* Entity that holds the intermediate tables */
typedef struct entity_{
    int intermediate_tables_count;
    Intermediate_table **intermediate_tables; // The intermediate tables hold the joined and filtered row Ids.
}Entity;

/* Execute the query */
int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array);

/* Check if a relation id exists in the intermediate_table, if it doesn't create a new table */
int create_in_intermediate_table(int relation_Id, Entity **entity);

/* Check if the relation id exists in an intermediate table */
int exists_in_intermediate_table(int relation_Id, Entity *entity, int *inter_table_number, int *column);

/* Print the query */
void print_query(Query_Info * query_info, char* query, int query_number);

/* Free the query */
void free_query(Query_Info * query_info);
#endif //ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
