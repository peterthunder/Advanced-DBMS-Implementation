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

Query_Info * createQueryInfo(void);

char **parseQueryParts(char *query, int *query_parts_count);

int parseRelationIDs(char *query_part, Query_Info **q);

int parsePredicates(char *query_part, Query_Info **q);

int parseSelections(char *query_part, Query_Info **q);

int parseFilter(char *token, int operator, Query_Info **q, int *current_filter);

int parseJoin(char *token, Query_Info **q, int *current_join);

bool isFilter(char *predicate);

#endif //ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
