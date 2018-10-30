#include "supportFunctions.h"

int allocateRelation(Relation **rel, uint32_t num_tuples) {

    *rel = malloc(sizeof(Relation));
    if (*rel == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    /* Matrix R and S sizes*/
    (*rel)->num_tuples = num_tuples;

    (*rel)->tuples = malloc(sizeof(Tuple) * (*rel)->num_tuples);
    if ((*rel)->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

}

void initializeRelationWithRandomNumbers(Relation **rel, int number_of_buckets) {

    //time_t t;
    //srand((unsigned) time(&t));
    int32_t i;

    /* Fill matrix R with random numbers from 0-199*/
    for (i = 0; i < (*rel)->num_tuples; i++) {
        (*rel)->tuples[i].payload = rand() % 199;
        (*rel)->tuples[i].key = (*rel)->tuples[i].payload % number_of_buckets;
        (*rel)->tuples[i].rowID = i+1;
    }
}

void printRelation(Relation *relation, int choice) {

    int i;
    switch (choice) {
        case 1:
            printf("\n   Relation R\n[h1| h2| p |rID]\n----------------");
            break;
        case 2:
            printf("\n   Relation S\n[h1| h2| p |rID]\n----------------");
            break;
        case 3:
            printf("\n   Relation R'\n[h1| h2| p |rID]\n----------------");
            break;
        case 4:
            printf("\n   Relation S'\n[h1| h2| p |rID]\n----------------");
            break;
        default:
            return;
    }

    for (i = 0; i < relation->num_tuples; i++) {
        printf("\n");
        printf("|%2d|%3d|%3d|%2d|", relation->tuples[i].key, relation->tuples[i].payload % H2_PARAM,
               relation->tuples[i].payload, relation->tuples[i].rowID);
    }

    printf("\n");
}

void printHistogram(int32_t **histogram, int choice, int number_of_buckets) {

    int i;

    if (choice == 1)
        printf("\n----HistogramR----\n");
    else
        printf("\n----HistogramS----\n");

    for (i = 0; i < number_of_buckets; i++) {
        printf("Histogram[%d]: %1d|%d\n", i, histogram[i][0], histogram[i][1]);
    }
    printf("\n");
}

void printPsum(int32_t **psum, int choice, int number_of_buckets) {

    int i;

    if (choice == 1)
        printf("\n----PsumR----\n");
    else
        printf("\n----PsumS----\n");

    for (i = 0; i < number_of_buckets; i++) {
        printf("Psum[%d]: %1d|%d\n", i, psum[i][0], psum[i][1]);
    }
    printf("\n");
}

void printAllForPartition(int choice, Relation *reIR, Relation *reIS, int32_t **histogramR, int32_t **histogramS,
                          int32_t **psumR, int32_t **psumS, Relation *newReIR, Relation *newReIS,
                          int number_of_buckets) {

    switch (choice) {
        case 1:
            printRelation(reIR, 1);

            printHistogram(histogramR, 1, number_of_buckets);
            printPsum(psumR, 1, number_of_buckets);

            printRelation(newReIR, 3);
            break;
        case 2:
            printRelation(reIS, 2);

            printHistogram(histogramS, 2, number_of_buckets);
            printPsum(psumS, 2, number_of_buckets);

            printRelation(newReIS, 4);
            break;
        default:    // Print for both relations.
            printRelation(reIR, 1);
            printRelation(reIS, 2);

            printHistogram(histogramR, 1, number_of_buckets);
            printPsum(psumR, 1, number_of_buckets);
            printHistogram(histogramS, 2, number_of_buckets);
            printPsum(psumS, 2, number_of_buckets);

            printRelation(newReIR, 3);
            printRelation(newReIS, 4);
    }

}

void printChainArray(int number_of_buckets, int32_t **psum, Relation *relationNew, int **chain) {
    int32_t i, j;
    int32_t difference;

    for (i = 0; i < number_of_buckets; i++) {
        if (i == number_of_buckets - 1)
            difference = relationNew->num_tuples - psum[i][1];
        else
            difference = psum[i + 1][1] - psum[i][1];
        printf("------------------------------\n");
        if (chain[i] != NULL)
            for (j = 0; j < difference; j++) {
                printf("Bucket: %d-%d - Chain element: %d\n", i, j+1, chain[i][j]);
            }
        else
            printf("Bucket %d - is empty.\n", i);
    }
}

void printResult(Result *result) {
    Result* current_result;

    printf("\n\n");
    current_result = result;
    do {
        printf("Number of tuples join in current result: %d\n", current_result->num_joined_rowIDs);
        printf("[RowIDR|RowIDS]\n");
        for (int i=0; i < current_result->num_joined_rowIDs; i++) {
            printf("   (%3d|%3d)\n", current_result->joined_rowIDs[i][0], current_result->joined_rowIDs[i][1]);
        }
        current_result = current_result->next_result;
    } while(current_result!=NULL);
}
