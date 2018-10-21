
#include "supportFunctions.h"

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

/* Create histogram of Relation */
int32_t **createHistogram(Relation* relation, int number_of_buckets){

    int i;

    /*Allocate memory for an Histogram-2dArray with size (number_of_buckets * 2) */
    int32_t **histogram = malloc(sizeof(int32_t *) * number_of_buckets);
    if (histogram == NULL) {
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

        /* Histogram contains Hash key-Number of hash key appearances */
        histogram[i][0] = i;
        histogram[i][1] = 0;
    }



    /* Fill out the histogram according to the hash values*/
    for (i = 0; i < relation->num_tuples; i++) {
        histogram[relation->tuples[i].key][1]++;
    }

    return histogram;
}

/* Create psum of Relation using its histogram*/
int32_t **createPsum(int number_of_buckets, int32_t **histogram){

    int i;

    /*Allocate memory for Psum-2dArray with size (number_of_buckets * 2) */
    int32_t **psum = malloc(sizeof(int32_t *) * number_of_buckets);
    if (psum == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    for (i = 0; i < number_of_buckets; i++) {

        psum[i] = malloc(sizeof(int32_t) * 2);
        if (psum[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }

        /* Psum contains Hash key-Index of bucket */
        psum[i][0] = i;
        psum[i][1] = 0;
    }

    for (i = 0; i < number_of_buckets; i++) {
        if (i == 0) {
            continue;
        } else {
            psum[i][1] = psum[i - 1][1] + histogram[i - 1][1];
        }
    }


    return psum;
}

/* Relation R: reIR, Relation S: reIS, Number of buckets: 2^n */
Result *RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets) {

    int i;

/*
    reIS->tuples[reIS->num_tuples-1].payload = 59;
    reIS->tuples[reIS->num_tuples-1].key = 59 % number_of_buckets;
*/

    /* Allocate memory for new matrices R' and S' that will be used as hash tables */
    Relation *relationNewR = malloc(sizeof(Relation));
    if (relationNewR == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    Relation *relationNewS = malloc(sizeof(Relation));
    if (relationNewS == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Matrix R' and S sizes'*/
    relationNewR->num_tuples = reIR->num_tuples;
    relationNewS->num_tuples = reIS->num_tuples;

    relationNewR->tuples = malloc(sizeof(Tuple) * relationNewR->num_tuples);
    if (relationNewR->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    relationNewS->tuples = malloc(sizeof(Tuple) * relationNewS->num_tuples);
    if (relationNewS->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Create Relation R histogram and psum */
    int32_t **histogramR = createHistogram(reIR, number_of_buckets);
    if (histogramR == NULL) {
        return NULL;
    }
    int32_t **psumR = createPsum(number_of_buckets, histogramR);
    if (psumR == NULL) {
        return NULL;
    }
    /* Create Relation S histogram and psum */
    int32_t **histogramS = createHistogram(reIS, number_of_buckets);
    if (histogramS == NULL) {
        return NULL;
    }
    int32_t **psumS = createPsum(number_of_buckets, histogramS);
    if (psumS == NULL) {
        return NULL;
    }

    /* Partition the relations */
    partition(reIR, relationNewR, number_of_buckets, psumR);
    partition(reIS, relationNewS, number_of_buckets, psumS);

   printAll( 3, reIR, reIS, histogramR, histogramS, psumR, psumS, relationNewR, relationNewS, number_of_buckets);

    /* De-allocate memory */
    for (i = 0; i < number_of_buckets; i++) {
        free(histogramR[i]);
        free(histogramS[i]);
        free(psumR[i]);
        free(psumS[i]);
    }

    free(histogramR);
    free(psumR);

    free(histogramS);
    free(psumS);

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

void printHistogram(int32_t** histogram, int choice, int number_of_buckets){

    int i;
    if(choice == 1)
        printf("\n-----HistogramR-----\n");
    else
        printf("\n-----HistogramS----\n");
    for (i = 0; i < number_of_buckets; i++) {
        printf("Histogram[%d]: %1d|%d\n", i, histogram[i][0], histogram[i][1]);
    }
    printf("\n");
}

void printPsum(int32_t** psum, int choice, int number_of_buckets){

    int i;

    if(choice == 1)
        printf("\n-----PsumR-----\n");
    else
        printf("\n-----PsumS----\n");

    for (i = 0; i < number_of_buckets; i++) {
        printf("Psum[%d]: %1d|%d\n", i, psum[i][0], psum[i][1]);
    }
    printf("\n");
}

void printAll( int choice,  Relation *reIR, Relation *reIS,int32_t** histogramR, int32_t** histogramS,
               int32_t** psumR, int32_t** psumS, Relation *newReIR, Relation *newReIS, int number_of_buckets){

    if(choice==1)
        printRelation(reIR, 1);
    else if(choice==2)
        printRelation(reIS, 2);
    else{
        printRelation(reIR, 1);
        printRelation(reIS, 2);
    }

    if(choice==1){
        printHistogram(histogramR, 2, number_of_buckets);
        printPsum(psumR, 2, number_of_buckets);
    }
    else if(choice==2){
        printHistogram(histogramS, 2, number_of_buckets);
        printPsum(psumS, 2, number_of_buckets);
    }
    else{
        printHistogram(histogramR, 1, number_of_buckets);
        printPsum(psumR, 1, number_of_buckets);
        printHistogram(histogramS, 2, number_of_buckets);
        printPsum(psumS, 2, number_of_buckets);
    }

    if(choice==1)
        printRelation(newReIR, 3);
    else if(choice==2)
        printRelation(newReIS, 4);
    else{
        printRelation(newReIR, 3);
        printRelation(newReIS, 4);
    }
}