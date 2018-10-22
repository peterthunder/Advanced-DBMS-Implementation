#include "supportFunctions.h"
#include "radixHashJoin.h"


int main(void) {

    // n  = cache size / maxsizeofbucket;

    time_t t;
    //srand((unsigned) time(&t));

    int cache_size = 3 * 1024; // Cache size is 6mb

    int32_t n = 3;

    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation *relation1, *relation2;

    int initReturnCode = 0;
    if ( (initReturnCode = initializeRelations(&relation1, &relation2, number_of_buckets)) == -1 ) {
        return initReturnCode;  // ErrorCode
    }

    Result *result = RadixHashJoin(relation1, relation2, number_of_buckets);
    /*if(result == NULL){
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }*/

    free(relation1->tuples);
    free(relation2->tuples);

    free(relation1);
    free(relation2);
}

