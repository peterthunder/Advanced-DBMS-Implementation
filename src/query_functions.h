#ifndef ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H

#include "file_io.h"
/* Execute the query*/
int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array);

/* Print the query */
void print_query(Query_Info * query_info, char* query, int query_number);

/* Free the query */
void free_query(Query_Info * query_info);
#endif //ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
