#include <time.h>
#include "main.h"

/* Partition Relation */
void *partition(Relation* relation, Relation* relationNew, int number_of_buckets, int32_t **psum){

    int i, j;
    unsigned int indexOfNewR = 0;
    int32_t currHashAppearances = 0, currHashCounter = 0;

    /* Fill out the new Relation using the psum */
    for (i = 0; i < number_of_buckets; i++) {

        currHashCounter = 0;

        /* If this is the last bucket, find the number of occurrences*/
        if (i == number_of_buckets - 1)
            currHashAppearances = relation->num_tuples - psum[i][1];
        else
            currHashAppearances = psum[i + 1][1] - psum[i][1];

        if (currHashAppearances == 0)
            continue;

        /* Search occurrences of this bucket's key inside the relation table. */
        for (j = 0; j < relation->num_tuples; j++) {

            /* If all occurrences have been found, go to the next bucket.*/
            if (currHashCounter == currHashAppearances)
                break;

            /*If we find the current bucket's key in relation-table, append the relation-table's data to the new relation-table. */
            if (relation->tuples[j].key == psum[i][0]) {
                relationNew->tuples[indexOfNewR].key = relation->tuples[j].key;
                relationNew->tuples[indexOfNewR].payload = relation->tuples[j].payload;
                indexOfNewR++;
                currHashCounter++;
            }
        }

    }

}

/* Relation R: reIR, Relation S: reIS, Number of buckets: 2^n */
Result *RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets) {

    int i;

    //printRelation(reIR, 1);
    printRelation(reIS, 2);


    /* Allocate memory for new matrices R' and S' that will be used as hash tables */
    Relation *relationNewR = malloc(sizeof(Relation));
    Relation *relationNewS = malloc(sizeof(Relation));

    /* Matrix R' and S sizes'*/
    relationNewR->num_tuples = reIR->num_tuples;
    relationNewS->num_tuples = reIS->num_tuples;


    relationNewR->tuples = malloc(sizeof(Tuple) * relationNewR->num_tuples);
    relationNewS->tuples = malloc(sizeof(Tuple) * relationNewS->num_tuples);



    /*Allocate memory for an Histogram-2dArray with size (number_of_buckets * 2) */
    int32_t **histogram = malloc(sizeof(int32_t *) * number_of_buckets);
    if (histogram == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /*Allocate memory for Psum-2dArray with size (number_of_buckets * 2) */
    int32_t **psum = malloc(sizeof(int32_t *) * number_of_buckets);
    if (psum == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }


    for (i = 0; i < number_of_buckets; i++) {
        histogram[i] = malloc(sizeof(int32_t) * 2);
        if (histogram[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
        psum[i] = malloc(sizeof(int32_t) * 2);
        if (psum[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }

        /* Histogram contains Hash key-Number of hash key appearances */
        histogram[i][0] = i;
        histogram[i][1] = 0;

        /* Psum contains Hash key-Index of bucket */
        psum[i][0] = i;
        psum[i][1] = 0;
    }



    /* Fill out the histogram according to the hash values*/
    for (i = 0; i < reIR->num_tuples; i++) {
        histogram[reIR->tuples[i].key][1]++;
    }

    /* Fill out the psum array according to the histogram */

    for (i = 0; i < number_of_buckets; i++) {
        if (i == 0) {
            continue;
        } else {
            psum[i][1] = psum[i - 1][1] + histogram[i - 1][1];
        }
    }

    /* printf("\n-----Histogram-----\n");
     for (i = 0; i < number_of_buckets; i++) {
         printf("Histogram[%d]: %1d|%d\n", i, histogram[i][0], histogram[i][1]);
     }
     printf("\n");

     printf("\n-----Psum-----\n");
     for (i = 0; i < number_of_buckets; i++) {
         printf("Psum[%d]: %2d|%d\n", i, psum[i][0], psum[i][1]);
     }
     printf("\n");*/

    partition(reIR, relationNewR, number_of_buckets, psum);
    partition(reIS, relationNewS, number_of_buckets, psum);

    //printRelation(relationNewR, 3);
    printRelation(relationNewS, 4);


    /* De-allocate memory */
    for (i = 0; i < number_of_buckets; i++) {
        free(histogram[i]);
    }

    free(histogram);
    free(psum);

    free(relationNewR->tuples);
    free(relationNewS->tuples);

    free(relationNewR);
    free(relationNewS);

}

void printRelation(Relation *relation, int choice) {

    int i;
    if(choice == 1)
        printf("\nRelation R");
    else if (choice == 2)
        printf("\nRelation S");
    else if (choice == 3)
        printf("\nRelation R'");
    else if (choice == 4)
        printf("\nRelation S'");
    else
        return;

    for (i = 0; i < relation->num_tuples; i++) {
        printf("\n");
        printf("|%d|%3d|", relation->tuples[i].key, relation->tuples[i].payload);
    }

    printf("\n");
}

int main(void) {

    // n  = cache size / maxsizeofbucket;

    time_t t;
    //srand((unsigned) time(&t));

    int cache_size = 3 * 1024; // Cahse size is 6mb

    int32_t n = 3, i;

    int32_t number_of_buckets = (int32_t) pow(2, n);

    Relation *relation1 = malloc(sizeof(Relation));
    Relation *relation2 = malloc(sizeof(Relation));

    /* Matrix R and S sizes*/
    relation1->num_tuples = 10;
    relation2->num_tuples = 5;


    relation1->tuples = malloc(sizeof(Tuple) * relation1->num_tuples);
    relation2->tuples = malloc(sizeof(Tuple) * relation2->num_tuples);

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

