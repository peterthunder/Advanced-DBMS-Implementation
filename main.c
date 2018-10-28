#include "supportFunctions.h"
#include "radixHashJoin.h"


int main(void) {

    // n  = cache size / maxsizeofbucket;

    time_t t;
    //srand((unsigned) time(&t));

    int cache_size = 3 * 1024; // Cache size is 6mb

    /* H1_PARAM is the number of the last-n bits of the 32-bit number we wanna keep */
    int32_t n = H1_PARAM;

    /* So we are going to use mod(%2^n) to get the last n bits, where 2^n is also the number of buckets */
    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation *relationR, *relationS;

    /* Initialize the relations with random numbers from 0-200 */
    int initReturnCode = 0;
    if ( (initReturnCode = initializeRelations(&relationR, &relationS, number_of_buckets, 20, 10, 1)) == -1 ) {
        return initReturnCode;  // ErrorCode
    }

    /* Do Radix Hash Join on the conjunction of the relations*/
    Result *result = RadixHashJoin(relationR, relationS, number_of_buckets);
    /*if(result == NULL){
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }*/

    free(relationR->tuples);
    free(relationS->tuples);

    free(relationR);
    free(relationS);
}

