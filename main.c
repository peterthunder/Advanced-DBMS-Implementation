#include "supportFunctions.h"
#include "radixHashJoin.h"

int main(void) {


    Result* current_result, *next_result;

    // n  = cache size / maxsizeofbucket;

    int cache_size = 6 * (1024 * 1024); // Cache size is 6mb
    int i;
    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation **relations = malloc(sizeof(Relation *) * 10);
    uint32_t relation_size[10] = {20, 10, 10, 10, 10, 10, 10, 10, 10, 10};

    int allocReturnCode = 0;
    /* Allocate the relations and initialize them with random numbers from 0-200 */
    for (i = 0; i < 10; i++) {
        if ((allocReturnCode = allocateRelation(&relations[i], relation_size[i])) == -1) {
            return allocReturnCode;  // ErrorCode
        }
        initializeRelationWithRandomNumbers(&relations[i], number_of_buckets);
    }

    /* Do Radix Hash Join on the conjunction of the relations */
    Result *result = RadixHashJoin(relations[0], relations[1], number_of_buckets);

    if(result == NULL){
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    printf("\n\n");
    current_result = result;
    do{
        printf("Number of tuples join in current result: %d\n", current_result->num_joined_rowIDs);
        printf("[RowIDR|RowIDS]\n");
        for(i=0; i < current_result->num_joined_rowIDs; i++){
            printf("   (%3d|%3d)\n", current_result->joined_rowIDs[i][0], current_result->joined_rowIDs[i][1]);
        }
        current_result = current_result->next_result;
    }while(current_result!=NULL);


    /* De-allocate memory*/
    for (i = 0; i < 10; i++) {
        free(relations[i]->tuples);
        free(relations[i]);
    }

    free(relations);

    do{
        next_result = result->next_result;
        free(result);
        result = next_result;
    }while(result!=NULL);

}