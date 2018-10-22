#include "supportFunctions.h"


int initializeRelations(Relation **reIR, Relation **reIS, int number_of_buckets) {

    time_t t;
    //srand((unsigned) time(&t));

    *reIR = malloc(sizeof(Relation));
    if (*reIR == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    *reIS = malloc(sizeof(Relation));
    if (*reIS == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    /* Matrix R and S sizes*/
    (*reIR)->num_tuples = 10;
    (*reIS)->num_tuples = 5;

    (*reIR)->tuples = malloc(sizeof(Tuple) * (*reIR)->num_tuples);
    if ( (*reIR)->tuples == NULL ) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    (*reIS)->tuples = malloc(sizeof(Tuple) * (*reIS)->num_tuples);
    if ( (*reIS)->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return -1;
    }

    fillRelationsWithRundNums(reIR, reIS, number_of_buckets);

}


void fillRelationsWithRundNums(Relation **reIR, Relation **reIS, int number_of_buckets) {

    int32_t i;

    /* Fill matrix R with random numbers from 0-199*/
    for (i = 0; i < (*reIR)->num_tuples; i++) {
        (*reIR)->tuples[i].payload = rand() % 199;
        (*reIR)->tuples[i].key = (*reIR)->tuples[i].payload % number_of_buckets;
    }

    /* Fill matrix S with random numbers from 0-199*/
    for (i = 0; i < (*reIS)->num_tuples; i++) {
        (*reIS)->tuples[i].payload = rand() % 200;
        (*reIS)->tuples[i].key = (*reIS)->tuples[i].payload % number_of_buckets;
    }

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
