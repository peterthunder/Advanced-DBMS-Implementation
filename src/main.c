#include "radixHashJoin.h"

int main(void) {

    size_t size;
    int query_count = 0, num_of_tables, i, j, *mapped_tables_sizes;
    char *query = NULL, workload_path[1024];
    uint64_t **mapped_tables;
    Query_Info *query_info;

    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    clock_t start_t, end_t, total_t;


    if (USE_HARNESS) {
        fp_print = stderr;
    } else
        fp_print = stdout;


    //fprintf(fp_print, "Running Advance_DBMS_Implementation...\n");

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    number_of_buckets = (int32_t) myPow(2, n);

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);

    //fprintf(fp_print, "\nNumber of tables: %d\n\n", num_of_tables);

    if (USE_HARNESS)
        fp_read = stdin;
    else {
        /* Open the file on that path */
        if ((fp_read = fopen("workloads/small/small.work", "r")) == NULL) {
            fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_path, strerror(errno));
            return -1;
        }
    }


    Relation ***relation_array;

    relation_array = myMalloc(sizeof(Relation **) * num_of_tables);

    for (i = 0; i < num_of_tables; i++) {

        relation_array[i] = myMalloc(sizeof(Relation *) * tables[i]->num_columns);
        for (j = 0; j < tables[i]->num_columns; j++) {
            relation_array[i][j] = NULL;
        }
    }


    if (USE_HARNESS)
        fp_write = stdout;
    else {
        fp_write = fopen("results.txt", "wb");
        if (!fp_write) {
            fprintf(stderr, "Error opening file \"results.txt\": %s!\n", strerror(errno));
            exit(1);
        }
    }


    Sum_struct *sumStruct = sumStructureAllocationAndInitialization();
    long *sums;

    start_t = clock();

    /* Get queries */
    while (getline(&query, &size, fp_read) > 0) {

        query[strlen(query) - 1] = '\0';

        //fprintf(fp_print, "Query[%d]: %s\n", query_count, query);

        if (strcmp(query, "F") == 0) {

            // Print the sums to stdout before going to next batch of queries.
            writeSumsToStdout(sumStruct);

            // Reset sum_struct
            resetSumStructure(&sumStruct);

            /* Continue to the next batch of queries */
            continue;
        }

        query_count++;

        query_info = parse_query(query);

        if (((sums = execute_query(query_info, tables, &relation_array))) == NULL) {
            fprintf(stderr, "An error occurred while executing the query: %s\nExiting program...\n", query);
            exit(-1);
        }

        // Update sum struct
        sumStructureUpdate(&sumStruct, query_info, sums);

        free_query(query_info);
    }

    if (USE_HARNESS == FALSE)
        fclose(fp_read);    // Otherwise, on Harness-run this will be the stdin which we do not close.

    fclose(fp_write);

    /* De-allocate memory */
    free(sumStruct->sums_sizes);
    free(sumStruct->sums);
    free(sumStruct);

    for (i = 0; i < num_of_tables; i++) {
        for (j = 0; j < tables[i]->num_columns; j++) {
            if (relation_array[i][j] != NULL) {
                deAllocateRelation(&relation_array[i][j]);
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

    end_t = clock();
    total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);

    if (USE_HARNESS == FALSE)
        fprintf(fp_print, "\nFinished parsing and executing queries in %ld seconds!\n", total_t);

    return EXIT_SUCCESS;
}
