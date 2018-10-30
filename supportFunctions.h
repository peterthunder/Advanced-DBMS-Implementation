#ifndef ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H

#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>

#define H1_PARAM 3 // Number of bits we keep after the 1st hash function pass
#define H2_PARAM 101 // The number we use in the 2nd hash function as mod

/* Type definition for a tuple */
typedef struct tuple{
    int32_t key; // hashvalue after 1st hash function
    int32_t rowID; // rowID of this tuple
    int32_t payload; // true value of this tuple
}Tuple;

/* Type definition for a relation */
typedef struct relation{
    Tuple *tuples;
    uint32_t num_tuples;
}Relation;

typedef struct result{

}Result;

int allocateRelations(Relation **reIR, Relation **reIS, uint32_t numOfTuplesR, uint32_t numOfTUplesS);

void initializeRelationsWithRandNums(Relation **reIR, Relation **reIS, int number_of_buckets);

/* Print the relation */
void printRelation(Relation *relation, int choice);

/* Print the Histogram */
void printHistogram(int32_t** histogram, int choice, int number_of_buckets);

/* Print the Psum */
void printPsum(int32_t** psum, int choice, int number_of_buckets);

/* Print Chain of every bucket*/
void printChainArrays(int number_of_buckets, int32_t **psum, Relation *relationNew, int **chain);

/* Print everything */
void printAll(int choice, Relation *reIR, Relation *reIS,int32_t** histogramR, int32_t** histogramS,
               int32_t** psumR, int32_t** psumS, Relation *newReIR, Relation *newReIS, int number_of_buckets);


#endif //ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
