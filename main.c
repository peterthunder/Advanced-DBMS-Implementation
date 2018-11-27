#include "supportFunctions.h"
#include "radixHashJoin.h"
#include "file_io.h"
#include "parser.h"

int main(void) {

    // n  = cache size / maxsizeofbucket;

    int cache_size = 6 * (1024 * 1024); // Cache size is 6mb
    int i, j;
    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation **relations = malloc(sizeof(Relation *) * 10);
    uint32_t relation_size[10] = {20, 10, 10, 10, 10, 10, 10, 10, 10, 10};


    /* Allocate the relations and initialize them with random numbers from 0-200 */
    for (i = 0; i < 10; i++) {
        if ((relations[i] = allocateRelation(relation_size[i])) == NULL) {
            return EXIT_FAILURE;  // ErrorCode
        }
        initializeRelationWithRandomNumbers(&relations[i], number_of_buckets);
    }

    /* Do Radix Hash Join on the conjunction of the relations */
    Result *result = RadixHashJoin(relations[0], relations[1], number_of_buckets);
    if (result == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return EXIT_FAILURE;
    }


#if PRINTING
    printResults(result);
#endif

    int num_of_tables;
    uint64_t **mapped_tables;
    int *mapped_tables_sizes;

    Table **tables = read_tables(&num_of_tables, &mapped_tables, &mapped_tables_sizes);

    printf("\nNumber of colums of table 0: %ju\n\n", tables[0]->num_columns);




    read_workload();




    /*De-allocate memory*/
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