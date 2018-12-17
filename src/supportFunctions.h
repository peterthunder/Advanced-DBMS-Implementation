#ifndef ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H

#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <errno.h>



#include "struct_and_const_definitions.h"
#include "parser.h"




Relation* allocateRelation(uint32_t num_tuples, bool is_complete);

void initializeRelation(Relation **rel, Table **tables, int table_number, int column_number);

void initializeRelationWithRandomNumbers(Relation **rel);

Sum_struct * sumStructureAllocationAndInitialization();

void sumStructureUpdate(Sum_struct **sumStruct, Query_Info *query_info, long*sums);

void resetSumStructure(Sum_struct **sumStruct);

void writeSumsToStdout(Sum_struct *sumStruct);

/* Print the relation */
void printRelation(Relation *relation, int choice);

/* Print the Histogram */
void printHistogram(int32_t **histogram, int choice);

/* Print the Psum */
void printPsum(int32_t **psum, int choice);

/* Print everything */
void printAllForPartition(int choice, Relation *reIR, Relation *reIS, int32_t **histogramR, int32_t **histogramS, int32_t **psumR, int32_t **psumS,
                          Relation *newReIR, Relation *newReIS);

/* Print Chain of every bucket*/
void printChainArray(int32_t **psum, Relation *relationNew, int **chain);

void printResults(Result *result);

/* Print all the intermediate tables of an entity */
void printEntity(Entity *entity);

/* Print the query */
void print_query(Query_Info * query_info, char* query, int query_number);

void* myMalloc(size_t sizeOfAllocation);

int myAtoi(char *str);

long int myPow(int x,int n);


#endif //ADVANCED_DBMS_IMPLEMENTATION_SUPPORTFUNCTIONS_H
