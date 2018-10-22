#ifndef ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H
#define ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H

#include "supportFunctions.h"


/* Radix Hash Join */
Result* RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets);

/* Partition Relation */
void *partition(Relation* relation, Relation* relationNew, int number_of_buckets, int32_t **psum);

/* Create histogram of Relation */
int32_t **createHistogram(Relation* relation, int number_of_buckets);

/* Create psum of Relation using its histogram */
int32_t **createPsum(int number_of_buckets, int32_t **histogram);


#endif //ADVANCED_DBMS_IMPLEMENTATION_RADIXHASHJOIN_H
