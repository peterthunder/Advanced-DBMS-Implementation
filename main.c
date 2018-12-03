#include "radixHashJoin.h"



int main(void) {

    testRHJ();

    int num_of_tables;
    uint64_t **mapped_tables;
    int *mapped_tables_sizes;

    Table **tables = read_tables(WORKDLOAD_BASE_PATH, TABLES_FILENAME, &num_of_tables, &mapped_tables, &mapped_tables_sizes);

    printf("\nNumber of colums of table 0: %ju\n\n", tables[0]->num_columns);
    

    // while
        // parse
        // execute


    read_workload(WORKDLOAD_BASE_PATH, WORKLOAD_FILENAME);


    /*De-allocate memory*/
    for (int i = 0; i < num_of_tables; i++) {
        munmap(mapped_tables[i], (size_t) mapped_tables_sizes[i]);
        free(tables[i]);
    }
    free(tables);


    return EXIT_SUCCESS;
}