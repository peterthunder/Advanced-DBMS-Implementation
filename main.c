#include "supportFunctions.h"
#include "radixHashJoin.h"
#include "file_io.h"

int main(void) {

    clock_t start_t, end_t, total_t;
    // n  = cache size / maxsizeofbucket;

    int cache_size = 6 * (1024 * 1024); // Cache size is 6mb
    int i, j;
    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation **relations = malloc(sizeof(Relation *) * 10);
    uint32_t relation_size[10] = {200, 100, 10, 10, 10, 10, 10, 10, 10, 10};


    /* Allocate the relations and initialize them with random numbers from 0-200 */
    for (i = 0; i < 10; i++) {
        if ((relations[i] = allocateRelation(relation_size[i])) == NULL) {
            return EXIT_FAILURE;  // ErrorCode
        }
        initializeRelationWithRandomNumbers(&relations[i], number_of_buckets);
    }

    start_t = clock();

    /* Do Radix Hash Join on the conjunction of the relations */
    Result *result = RadixHashJoin(relations[0], relations[1], number_of_buckets);
    if (result == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return EXIT_FAILURE;
    }

    end_t = clock();

    total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);
    printf("Total time taken by CPU for RHS: %f\n", (double) total_t);
#if PRINTING
    printResults(result);
#endif

    int num_of_tables;
    uint64_t **mapped_tables;
    int *mapped_tables_sizes;

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);

    printf("N: %ju\n", tables[0]->num_columns);

    /* De-allocate memory*/
    for (i = 0; i < num_of_tables; i++) {
        munmap(mapped_tables[i], (size_t)mapped_tables_sizes[i]);
        free(tables[i]);
    }
    free(tables);

    for (i = 0; i < 10; i++) {
        free(relations[i]->tuples);
        free(relations[i]);
    }
    free(relations);

    do {
        Result *next_result = result->next_result;
        free(result);
        result = next_result;
    } while (result != NULL);


    return EXIT_SUCCESS;
}