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


    if (USE_HARNESS == FALSE) {
        fp_print = stdout;
    } else
        fp_print = stderr;


    fprintf(fp_print, "Running Advance_DBMS_Implementation...\n");
    start_t = clock();

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    number_of_buckets = (int32_t) myPow(2, n);

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);

    fprintf(fp_print, "\nNumber of tables: %d\n\n", num_of_tables);

    if (USE_HARNESS == false) {
        /* Open the file on that path */
        if ((fp_read = fopen("workloads/small/small.work", "r")) == NULL) {
            fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_path, strerror(errno));
            return -1;
        }
    } else
        fp_read = stdin;

    Relation ***relation_array;

    relation_array = myMalloc(sizeof(Relation **) * num_of_tables);

    for (i = 0; i < num_of_tables; i++) {

        relation_array[i] = myMalloc(sizeof(Relation *) * tables[i]->num_columns);
        for (j = 0; j < tables[i]->num_columns; j++) {
            relation_array[i][j] = NULL;
        }
    }


    if (USE_HARNESS == FALSE) {
        fp_write = fopen("results.txt", "wb");
        if (!fp_write) {
            perror("Error creating file!\n");
            exit(1);
        }
    } else
        fp_write = stdout;


    Sum_struct *sumStruct = myMalloc(sizeof(Sum_struct));
    sumStruct->full_size = 1;
    sumStruct->actual_size = 0;
    sumStruct->sums = myMalloc(sizeof(long *) * 1);
    sumStruct->sums_sizes = myMalloc(sizeof(long) * 1);

    /* Get queries */
    while (getline(&query, &size, fp_read) > 0) {

        query[strlen(query) - 1] = '\0';

        fprintf(fp_print, "Query[%d]: %s\n", query_count, query);

        if (strcmp(query, "F") == 0) {

            // Print sums before going to next group of queries.

            char tempLine[1024];
            char tempLine1[1024];

            for (int k = 0; k < sumStruct->actual_size; ++k) {

                tempLine[0] = '\0';
                tempLine1[0] = '\0';
                for (int l = 0; l < sumStruct->sums_sizes[k]; ++l) {

                    if (sumStruct->sums[k][l] == 0) {

                        strcat(tempLine, "NULL");

                    } else {
                        sprintf(tempLine1, "%ld", sumStruct->sums[k][l]);
                        strcat(tempLine, tempLine1);
                    }

                    if (l != sumStruct->sums_sizes[k] - 1)
                        strcat(tempLine, " ");
                }

                strcat(tempLine, "\n");

                fprintf(fp_print, "%s", tempLine);
                fputs(tempLine, fp_write);
            }

            // Re-inialize structures

            for (int k = 0; k < sumStruct->actual_size; ++k) {
                free(sumStruct->sums[k]);
            }

            sumStruct->actual_size = 0;
            sumStruct->full_size = 1;

            sumStruct->sums = realloc(sumStruct->sums, sizeof(long *) * 1);
            sumStruct->sums_sizes = realloc(sumStruct->sums_sizes, sizeof(long) * 1);

            continue;
        }

        query_count++;

        query_info = parse_query(query);

        if (((sumStruct->sums[sumStruct->actual_size] = execute_query(query_info, tables, &relation_array, stdout))) == NULL) {
            fprintf(stderr, "An error occurred while executing the query: %s\nExiting program...\n", query);
            exit(-1);
        }

        // Log the size of this query's sums-table.
        sumStruct->sums_sizes[sumStruct->actual_size] = query_info->selection_count;

        sumStruct->actual_size++;

        if (sumStruct->full_size == sumStruct->actual_size) {
            sumStruct->full_size <<= 1; // fast-multiply by 2
            sumStruct->sums = realloc(sumStruct->sums, sumStruct->full_size * sizeof(long *));
            sumStruct->sums_sizes = realloc(sumStruct->sums_sizes, sumStruct->full_size * sizeof(long));
        }

        free_query(query_info);

    }
    if (USE_HARNESS == FALSE)
        fclose(fp_read);
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

    fprintf(fp_print, "\nFinished parsing and executing queries in %ld seconds!\n", total_t);

    return EXIT_SUCCESS;
}