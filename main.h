#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <math.h>

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
Result* RadixHashJoin(Relation *reIR, Relation *reIS){

    int i;

    for(i=0; i<2; i++){
        printf("reIR: key %d, payload %d -- reIS: key %d, payload %d\n",
               reIR->tuples[i].key, reIR->tuples[i].payload, reIS->tuples[i].key, reIS->tuples[i].payload);
    }

}

