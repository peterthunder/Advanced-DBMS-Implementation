#ifndef ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H
#define ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H

#include "query_functions.h"
#include "threadpool.h"

Threadpool *threadpool;

typedef struct sum_calc_struct{
    int start;
    int end;
    int selection_num;
    long sum;
    int inter_table_num;
    int inter_column_num;
    Entity *entity;
    Query_Info *query_info;
    Table **tables;
}Sum_calc_struct;

typedef struct partition_struct {
    int32_t start;
    int32_t hashValue;
    int hashApperances;
    Relation *relation;
    Relation *newRelation;
} Partition_struct;


typedef struct histo_struct {
    int start;
    int end;
    int32_t *histogram;
    Relation *relation;
} Histogram_struct;

void testRHJ();

void thread_calculate_sums(Sum_calc_struct **sum_calc_struct);

/* Radix Hash Join */
Result *RadixHashJoin(Relation **reIR, Relation **reIS);

/*Thread function for partition of each bucket*/
void thread_partition(Partition_struct **partition_struct);

/* Partition Relation */
void partition(Relation *relation, Relation **relationNew, int32_t **psum);

/* Thread function for histogram creation */
void thread_histogram(Histogram_struct **histogram_struct);

/* Create histogram of Relation */
int32_t **createHistogram(Relation *relation);

/* Create psum of Relation using its histogram */
int32_t **createPsum(int32_t **histogram);

void allocateAndInitializeBucketIndexAndChain(int ***chain, int ***bucket_index);

/* Build the Index and the Chain Arrays of the relation with the less amount of tuples */
void buildSmallestPartitionedRelationIndex(Relation *rel, int32_t **psum, int32_t ***bucket_index, int32_t ***chain);

/* Join two relations and return the result. */
Result *joinRelations(Relation *relWithIndex, Relation *relNoIndex, int32_t **psumWithIndex, int32_t **psumNoIndex,
                      int32_t **bucket_index, int32_t **chain,
                      bool is_R_relation_first);

/* De-allocate Relation memory */
void deAllocateRelation(Relation **relation);

/* De-allocate Result memory */
void deAllocateResult(Result **result);


#endif //ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H
