#include "test_support_functions.h"

int number_of_buckets;

void initializeRelations(Relation **relation1, Relation **relation2) {

    int i;

    *relation1 = allocateRelation(4, TRUE);
    *relation2 = allocateRelation(3, TRUE);

    for (i = 0; i < (*relation1)->num_tuples; i++) {
        (*relation1)->tuples[i].payload = i + 1;
        (*relation1)->tuples[i].key = i + 1;
    }

    (*relation2)->tuples[0].payload = 1;
    (*relation2)->tuples[1].payload = 1;
    (*relation2)->tuples[2].payload = 3;

    (*relation2)->tuples[0].key = 1;
    (*relation2)->tuples[1].key = 2;
    (*relation2)->tuples[2].key = 3;
}

void freeMemory(Relation *relation1, Relation *relation2, int32_t **histogram1, int32_t **histogram2,
                int32_t **psum1, int32_t **psum2) {
    int i;
    if (histogram1 != NULL && histogram2 != NULL) {
        for (i = 0; i < number_of_buckets; i++) {
            free(histogram1[i]);
            free(histogram2[i]);
        }
        free(histogram1);
        free(histogram2);
    }

    if (psum1 != NULL && psum2 != NULL) {
        for (i = 0; i < number_of_buckets; i++) {
            free(psum1[i]);
            free(psum2[i]);
        }
        free(psum1);
        free(psum2);
    }
    free(relation1->tuples);
    free(relation1);
    free(relation2->tuples);
    free(relation2);
}

void fillHistograms(int32_t ***histogram1, int32_t ***histogram2, Relation *relation1) {

    int i;
    /*Allocate memory for an Histogram-2dArray with size (number_of_buckets * 2) */
    *histogram1 = myMalloc(sizeof(int32_t *) * number_of_buckets);

    for (i = 0; i < number_of_buckets; i++) {
        (*histogram1)[i] = myMalloc(sizeof(int32_t) * 2);
        /* Histogram contains Hash key-Number of hash key appearances */
        (*histogram1)[i][0] = i;
        (*histogram1)[i][1] = 0;
    }

    for (i = 1; i < relation1->num_tuples + 1; i++) {
        (*histogram1)[i][1] = 1;
    }

    *histogram2 = createHistogram(relation1, number_of_buckets);
}

void fillPsums(int32_t ***psum1, int32_t ***psum2, int32_t **histogram2) {
    int i;
    /*Allocate memory for an Histogram-2dArray with size (number_of_buckets * 2) */
    *psum1 = myMalloc(sizeof(int32_t *) * number_of_buckets);

    for (i = 0; i < number_of_buckets; i++) {
        (*psum1)[i] = myMalloc(sizeof(int32_t) * 2);

        /* Histogram contains Hash key-Number of hash key appearances */
        (*psum1)[i][0] = i;
        (*psum1)[i][1] = 0;
    }

    (*psum1)[0][1] = 0;
    (*psum1)[1][1] = 0;
    (*psum1)[2][1] = 1;
    (*psum1)[3][1] = 2;
    (*psum1)[4][1] = 3;
    (*psum1)[5][1] = 4;
    (*psum1)[6][1] = 4;
    (*psum1)[7][1] = 4;

    *psum2 = createPsum(number_of_buckets, histogram2);
}
