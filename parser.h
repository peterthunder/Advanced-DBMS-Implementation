#ifndef ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
#define ADVANCED_DBMS_IMPLEMENTATION_PARSER_H

#include "supportFunctions.h"
#include <errno.h>

typedef struct Query_{

}Query;


void *read_workload(char* base_path, char *workload_filename);
int isFilter(char *predicate);

#endif //ADVANCED_DBMS_IMPLEMENTATION_PARSER_H
