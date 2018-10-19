#include "main.h"


void main(){

    // n  = cache size / maxsizeofbucket;

    int n = 3;

    int32_t mod  = (int32_t)pow(2,n);

    Relation *relation1 = malloc(sizeof(Relation));
    Relation *relation2 = malloc(sizeof(Relation));

    relation1->tuples = malloc(sizeof(Tuple)* 2);
    relation2->tuples = malloc(sizeof(Tuple)* 2);


    relation1->tuples[0].payload = 8;
    relation1->tuples[0].key = relation1->tuples[0].payload % mod;
    relation1->tuples[1].payload = 32;
    relation1->tuples[1].key = relation1->tuples[1].payload % mod;

    relation2->tuples[0].payload = 7;
    relation2->tuples[0].key = relation2->tuples[0].payload % mod;
    relation2->tuples[1].payload = 43;
    relation2->tuples[1].key = relation2->tuples[1].payload % mod;

    RadixHashJoin(relation1, relation2);

    free(relation1->tuples);
    free(relation2->tuples);

    free(relation1);
    free(relation2);
}

