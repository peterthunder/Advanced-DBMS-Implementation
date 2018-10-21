#include "supportFunctions.h"

int main(void) {

    // n  = cache size / maxsizeofbucket;

    time_t t;
    //srand((unsigned) time(&t));

    int cache_size = 3 * 1024; // Cache size is 6mb

    int32_t n = 3, i;

    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation *relation1 = malloc(sizeof(Relation));
    if (relation1 == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }
    Relation *relation2 = malloc(sizeof(Relation));
    if (relation2 == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }
    /* Matrix R and S sizes*/
    relation1->num_tuples = 10;
    relation2->num_tuples = 5;


    relation1->tuples = malloc(sizeof(Tuple) * relation1->num_tuples);
    if (relation1->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }
    relation2->tuples = malloc(sizeof(Tuple) * relation2->num_tuples);
    if (relation2->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    /* Fill matrix R with random numbers from 0-199*/
    for (i = 0; i < relation1->num_tuples; i++) {
        relation1->tuples[i].payload = rand() % 199;
        relation1->tuples[i].key = relation1->tuples[i].payload % number_of_buckets;
    }

    /* Fill matrix S with random numbers from 0-199*/
    for (i = 0; i < relation2->num_tuples; i++) {
        relation2->tuples[i].payload = rand() % 200;
        relation2->tuples[i].key = relation2->tuples[i].payload % number_of_buckets;
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

