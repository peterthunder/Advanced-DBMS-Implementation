#ifndef ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H

#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>

/* Type definition for a tuple */
typedef struct tuple{
    int32_t key;
    int32_t payload;
}Tuple;


/* Type definition for a relation */
typedef struct relation{
    Tuple *tuples;
    uint32_t num_tuples;
}Relation;

typedef struct result{

}Result;


int initializeRelations(Relation **reIR, Relation **reIS, int number_of_buckets);

void fillRelationsWithRundNums(Relation **reIR, Relation **reIS, int number_of_buckets);

/* Radix Hash Join */
Result* RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets);

/* Partition Relation */
void *partition(Relation* relation, Relation* relationNew, int number_of_buckets, int32_t **psum);

/* Create histogram of Relation */
int32_t **createHistogram(Relation* relation, int number_of_buckets);

/* Create psum of Relation using its histogram */
int32_t **createPsum(int number_of_buckets, int32_t **histogram);

/* Print the relation */
void printRelation(Relation *relation, int choice);

/* Print the Histogram */
void printHistogram(int32_t** histogram, int choice, int number_of_buckets);

/* Print the Psum */
void printPsum(int32_t** psum, int choice, int number_of_buckets);

/* Print everything */
void printAll(int choice, Relation *reIR, Relation *reIS,int32_t** histogramR, int32_t** histogramS,
               int32_t** psumR, int32_t** psumS, Relation *newReIR, Relation *newReIS, int number_of_buckets);


#endif //ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
