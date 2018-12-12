#ifndef ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
#define ADVANCED_DBMS_IMPLEMENTATION_PARSER_H

#include "supportFunctions.h"


Query_Info *parse_query(char *query);

Query_Info * createQueryInfo(void);

char **parseQueryParts(char *query, int *query_parts_count);

void parseRelationIDs(char *query_part, Query_Info **q);

int parsePredicates(char *query_part, Query_Info **q);

void parseSelections(char *query_part, Query_Info **q);

int parseFilter(char *token, int operator, Query_Info **q, int *current_filter);

void parseJoin(char *token, Query_Info **q, int *current_join);

bool isFilter(char *predicate);

#endif //ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
