#ifndef ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
#define ADVANCED_DBMS_IMPLEMENTATION_PARSER_H

#include "supportFunctions.h"

typedef struct Query_ {
    int *relation_IDs;
    int relationId_count;
    int **filters;
    int filter_count;
    int **joins;
    int join_count;
    int **selections;
    int selection_count;

} Query_Info;

Query_Info *parse_query(char *query);

int isFilter(char *predicate);

#endif //ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
