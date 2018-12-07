#ifndef ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H

#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include "parser.h"

#define H1_PARAM 3 // Number of bits we PRINTING keep after the 1st hash function pass
#define H2_PARAM 101 // The number we use in the 2nd hash function as mod
#define JOINED_ROWIDS_NUM ((1024 * 1024) / 8)
#define TRUE true
#define FALSE false
#define EQUAL 0
#define GREATER 1
#define LESS 2
#define WORKLOAD_BASE_PATH  "workloads/small/"
#define TABLES_FILENAME "small.init"
#define WORKLOAD_FILENAME "small.work"


/* Type definition for a tuple */
typedef struct tuple {
    int32_t key; // rowID of this tuple
    int32_t payload; // true value of this tuple
} Tuple;

/* Type definition for a relation */
typedef struct relation {
    Tuple *tuples;
    uint32_t num_tuples;
    struct relation *paritioned_relation;
    int32_t **psum;
    int32_t **chain;
    int32_t **bucket_index;
    bool is_built;
    bool is_partitioned;
    bool is_full_column;
} Relation;

/* Type definition for a result */
typedef struct result {
    int32_t joined_rowIDs[JOINED_ROWIDS_NUM][2];
    int num_joined_rowIDs;
    struct result* next_result;
} Result;

/* Struct that holds the information of all the columns of a single table */
typedef struct table_{
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t **column_indexes;
} Table;

Relation* allocateRelation(uint32_t num_tuples, bool is_complete);

void initializeRelation(Relation **rel, Table **tables, int table_number, int column_number);

void initializeRelationWithRandomNumbers(Relation **rel, int number_of_buckets);

/* Print the relation */
void printRelation(Relation *relation, int choice);

/* Print the Histogram */
void printHistogram(int32_t **histogram, int choice, int number_of_buckets);

/* Print the Psum */
void printPsum(int32_t **psum, int choice, int number_of_buckets);

/* Print everything */
void printAllForPartition(int choice, Relation *reIR, Relation *reIS, int32_t **histogramR, int32_t **histogramS,
                          int32_t **psumR, int32_t **psumS, Relation *newReIR, Relation *newReIS,
                          int number_of_buckets);

/* Print Chain of every bucket*/
void printChainArray(int number_of_buckets, int32_t **psum, Relation *relationNew, int **chain);

void printResults(Result *result);

int myAtoi(char *str);

#endif //ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
