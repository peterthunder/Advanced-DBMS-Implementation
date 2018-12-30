#include "radixHashJoin.h"
#include "statistics_functions.h"

int main(void) {

    size_t size;
    int query_count = 0, num_of_tables, i, j, *mapped_tables_sizes;
    char *query = NULL;
    uint64_t **mapped_tables;   // Used for mapping and un-mapping the tables in/from memory.
    Query_Info *query_info;

    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    clock_t start_t, end_t, total_t;

    //fprintf(fp_print, "Running Advance_DBMS_Implementation...\n");

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    number_of_buckets = (int32_t) myPow(2, n);

    setIOStreams(&fp_read_tables, &fp_read_queries, &fp_write, &fp_print);

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);

    //fprintf(fp_print, "\nNumber of tables: %d\n\n", num_of_tables);


    // Gather statistics for each column of each table. Later, these will optimize the queries-execution.
    gatherInitialStatistics(&tables, num_of_tables);


    Relation ***relation_array = allocateAndInitializeRelationArray(tables, num_of_tables);

    Sum_struct *sumStruct = sumStructureAllocationAndInitialization();
    long *sums;

    if (USE_HARNESS == FALSE)
        start_t = clock();

    /* Get queries */
    while (getline(&query, &size, fp_read_queries) > 0) {

        query[strlen(query) - 1] = '\0';    // Remove "newLine"-character.

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


    if (USE_HARNESS == FALSE) {
        end_t = clock();
        total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);
        fprintf(fp_print, "\nFinished parsing and executing queries in %ld seconds!\n", total_t);
        if ( fclose(fp_read_queries) == EOF ) {    // Otherwise, on Harness-run this will be the stdin which we do not close.
            fprintf(stderr, "Error closing \"fp_read_queries\" without HARNESS: %s!\n", strerror(errno));
            return EXIT_FAILURE;
        }
    }

    if ( fclose(fp_write) == EOF ) {
        fprintf(stderr, "Error closing \"fp_write\": %s!\n", strerror(errno));
        return EXIT_FAILURE;
    }

    /* De-allocate memory */
    for (i = 0; i < num_of_tables; i++)
    {
        for (j = 0; j < tables[i]->num_columns; j++)
        {
            if (relation_array[i][j] != NULL) {
                deAllocateRelation(&relation_array[i][j]);
            }
            free(tables[i]->column_statistics[j]->d_array);
            free(tables[i]->column_statistics[j]);
        }
        free(relation_array[i]);

        munmap(mapped_tables[i], (size_t) mapped_tables_sizes[i]);
        free(tables[i]->column_indexes);
        free(tables[i]->column_statistics);
        free(tables[i]);
    }
    free(relation_array);

    free(mapped_tables_sizes);
    free(mapped_tables);
    free(tables);

    free(query);

    free(sumStruct->sums_sizes);
    free(sumStruct->sums);
    free(sumStruct);

    return EXIT_SUCCESS;
}
