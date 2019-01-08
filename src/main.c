#include "statistics_functions.h"

int main(void) {

    size_t size;
    int query_count = 0, num_of_tables, i, j, *mapped_tables_sizes;
    char *query = NULL;
    uint64_t **mapped_tables;   // Used for mapping and un-mapping the tables in/from memory.
    Query_Info *query_info;
    struct timeval start;

    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

/*    testRHJ();

    return 0;*/


    //fprintf(fp_print, "Running Advance_DBMS_Implementation...\n");

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    number_of_buckets = (int32_t) myPow(2, n);

    setIOStreams(&fp_read_tables, &fp_read_queries, &fp_write, &fp_print);

    threadpool = threadpool_init(number_of_buckets);

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);
    if (tables == NULL)
        exit(EXIT_FAILURE); // The related-error-message is printed inside "read_tables()".

    //fprintf(fp_print, "\nNumber of tables: %d\n\n", num_of_tables);

    Relation ***relation_array = allocateAndInitializeRelationArray(tables, num_of_tables);

    Sum_struct *sumStruct = sumStructureAllocationAndInitialization();
    long *sums;

    if (USE_HARNESS == FALSE)
        gettimeofday(&start, NULL);


    /* Get queries */
    while (getline(&query, &size, fp_read_queries) > 0) {

        query[strlen(query) - 1] = '\0';    // Remove "newLine"-character.

        if (strcmp(query, "F") == 0) {

            // Print the sums to stdout before going to next batch of queries.
            writeSumsToStdout(sumStruct);

            // Reset sum_struct
            resetSumStructure(&sumStruct);

            /* Continue to the next batch of queries */
            continue;
        }

        query_count++;

        //fprintf(fp_print, "Query_%d: %s\n", query_count, query);

        query_info = parse_query(query);

        /*  // DEBUG CODE:
        if ( query_count < 20 )
            continue;
        if ( query_count == 20 ) {
            query_info = parse_query(query);
            print_query(query_info, query, query_count);
        }
        if ( query_count > 20 )
            break;*/

#if PRINTING || DEEP_PRINTING
        fprintf(fp_print, "Original Query:");
        print_query(query_info, query, query_count);    // See the original query.
#endif

     /*   // DEBUG CODE:
        int queryWeWant = 27;
        if ( query_count < queryWeWant )
            continue;
        else if ( query_count > queryWeWant ) {
            free_query(query_info);
            break;
        }
        else {
            print_query(query_info, query, query_count);
            gatherPredicatesStatisticsForQuery(&query_info, tables, query_count);
        }
*/
        if ( gatherPredicatesStatisticsForQuery(&query_info, tables, query_count) == -1 ) {
            // Found that a filter returned zero results. Because every query is a connected graph, the produced results by the query-joins will be NULL.
            sums = myMalloc(sizeof(long *) * query_info->selection_count);
            for ( i = 0 ; i < query_info->selection_count ; i++ ) {
                sums[i] = 0;
            }

            sumStructureUpdate(&sumStruct, query_info, sums);
            free_query(query_info);
            continue;
        }



#if PRINTING || DEEP_PRINTING
        //fprintf(fp_print, "\nChanged Query, after gathering the statistics and reordering the joins:");
        //print_query(query_info, query, query_count);    // See the changed query after gathering the statistics and reordering the joins.
#endif

        if (((sums = execute_query(query_info, tables, &relation_array))) == NULL) {
            fprintf(stderr, "An error occurred while executing the query: %s\nExiting program...\n", query);
            exit(-1);
        }

        // Update sum struct
        sumStructureUpdate(&sumStruct, query_info, sums);

        free_query(query_info);
    }


    if (USE_HARNESS == FALSE) {
        struct timeval end;
        gettimeofday(&end, NULL);
        double elapsed_sec = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        fprintf(fp_print, "\nFinished parsing and executing queries in %.f milliseconds!\n", elapsed_sec * 1000);
        if (fclose(fp_read_queries) ==
            EOF) {    // Otherwise, on Harness-run this will be the stdin which we do not close.
            fprintf(stderr, "Error closing \"fp_read_queries\" without HARNESS: %s!\n", strerror(errno));
            return EXIT_FAILURE;
        }
    }

    if (fclose(fp_write) == EOF) {
        fprintf(stderr, "Error closing \"fp_write\": %s!\n", strerror(errno));
        return EXIT_FAILURE;
    }

    /* De-allocate memory */
    for (i = 0; i < num_of_tables; i++) {
        for (j = 0; j < tables[i]->num_columns; j++) {
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

    threadpool_destroy(threadpool);

    return EXIT_SUCCESS;
}
