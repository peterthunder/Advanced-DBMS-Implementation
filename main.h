#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <math.h>
#include <stdlib.h>

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

/* Radix Hash Join */
Result* RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets);
/* Print out the relation*/
void printRelation(Relation *relation, int choice);
/* Partition Relation */
void *partition(Relation* relation, Relation* relationNew, int number_of_buckets, int32_t **psum);