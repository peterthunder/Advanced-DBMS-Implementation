#ifndef ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H
#define ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H

#include "supportFunctions.h"
#include <inttypes.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>


typedef struct table_{
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t **column_indexes;
} Table;

Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes);

#endif //ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H
