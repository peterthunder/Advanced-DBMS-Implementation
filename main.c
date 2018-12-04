#include "radixHashJoin.h"

int main(void) {

    FILE *fptr;
    size_t size;
    int query_count = 0, num_of_tables, i, *mapped_tables_sizes;
    char *query = NULL, workload_path[1024];
    uint64_t **mapped_tables;

    testRHJ();

    Table **tables = read_tables(WORKDLOAD_BASE_PATH, TABLES_FILENAME, &num_of_tables, &mapped_tables, &mapped_tables_sizes);

    printf("\nNumber of columns of table 0: %ju\n\n", tables[0]->num_columns);

    workload_path[0] = '\0';
    /* Create the path of the file that contains the workload */
    strcpy(workload_path, WORKDLOAD_BASE_PATH);
    strcat(workload_path, WORKLOAD_FILENAME);

    /* Open the file on that path */
    fptr = fopen(workload_path, "r");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_path, strerror(errno));
        return -1;
    }

    /* Get queries */
    while (!feof(fptr)) {

        getline(&query, &size, fptr);
        query[strlen(query) - 1] = '\0';

        if (strcmp(query, "F") == 0) continue;

        if (strcmp(query, "\n") == 0 || strlen(query) == 0)
            continue;
        query_count++;

        printf("\n----------------------------------------------------------------\n");

        /* Parse Query */
        /* Get Query Parts */
        printf("Query[%d]: %s\n", query_count - 1, query);
        Query_Info *query_info = parse_query(query);
        execute_query(query_info);
        free_query(query_info);
    }
    printf("\n----------------------------------------------------------------");

    fclose(fptr);

    /*De-allocate memory*/
    for (i = 0; i < num_of_tables; i++) {
        munmap(mapped_tables[i], (size_t) mapped_tables_sizes[i]);
        free(tables[i]->column_indexes);
        free(tables[i]);
    }
    free(mapped_tables_sizes);
    free(mapped_tables);
    free(tables);
    free(query);

    return EXIT_SUCCESS;
}