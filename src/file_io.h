#ifndef ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H
#define ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "supportFunctions.h"

Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes);

char **getTableNames(int *num_of_tables, int *table_names_array_size);

#endif //ADVANCED_DBMS_IMPLEMENTATION_FILE_IO_H
