#ifndef ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H

#include "supportFunctions.h"
/* Execute the query*/
void execute_query(Query_Info * query_info);

/* Print the query */
void print_query(Query_Info * query_info, char* query, int query_number);

/* Free the query */
void free_query(Query_Info * query_info);
#endif //ADVANCED_DBMS_IMPLEMENTATION_QUERY_FUNCTIONS_H
