#include "radixHashJoin.h"

int main(void) {

    FILE *fptr;
    size_t size;
    int query_count = 0, num_of_tables, i, j, *mapped_tables_sizes;
    char *query = NULL, workload_path[1024];
    uint64_t **mapped_tables;

    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    int32_t number_of_buckets = (int32_t) pow(2, n);


    testRHJ();

    Table **tables = read_tables(WORKLOAD_BASE_PATH, TABLES_FILENAME, &num_of_tables, &mapped_tables,
                                 &mapped_tables_sizes);

    printf("\nNumber of columns of table 0: %ju\n\n", tables[0]->num_columns);

    workload_path[0] = '\0';
    /* Create the path of the file that contains the workload */
    strcpy(workload_path, WORKLOAD_BASE_PATH);
    strcat(workload_path, WORKLOAD_FILENAME);

    /* Open the file on that path */
    fptr = fopen(workload_path, "r");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_path, strerror(errno));
        return -1;
    }

    Relation ***relation_array;

    relation_array = malloc(sizeof(Relation **) * num_of_tables);
    if (relation_array == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return -1;
    }
    for (i = 0; i < num_of_tables; i++) {

        relation_array[i] = malloc(sizeof(Relation *) * tables[i]->num_columns);
        if (relation_array[i] == NULL) {
            fprintf(stderr, "Malloc failed!\n");
            return -1;
        }

        printf(" Table %d has %ju columns.\n", i, tables[i]->num_columns);
        for (j = 0; j < tables[i]->num_columns; j++) {
            relation_array[i][j] = NULL;
        }
    }

    /* Get queries */
    while (!feof(fptr)) {

        getline(&query, &size, fptr);
        query[strlen(query) - 1] = '\0';

        if (strcmp(query, "F") == 0) continue;

        if (strcmp(query, "\n") == 0 || strlen(query) == 0)
            continue;
        query_count++;

        Query_Info *query_info = parse_query(query);
        if (query_info == NULL) {
            fprintf(stderr, "An error occurred while parsing the query: %s\nExiting program...\n", query);
            exit(-1);
        }
        //print_query(query_info, query, query_count);
        if ((execute_query(query_info, tables, &relation_array)) == -1) {
            fprintf(stderr, "An error occurred while executing the query: %s\nExiting program...\n", query);
            exit(-1);
        }
        free_query(query_info);
        /* if (query_count == 1)
             break; */
    }
    printf("\n----------------------------------------------------------------\n");

    fclose(fptr);

    /* De-allocate memory */
    for (i = 0; i < num_of_tables; i++) {

        for (j = 0; j < tables[i]->num_columns; j++) {

            if (relation_array[i][j] != NULL) {
             deAllocateRelation(&relation_array[i][j], number_of_buckets);
            }
        }
        free(relation_array[i]);
    }
    free(relation_array);

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