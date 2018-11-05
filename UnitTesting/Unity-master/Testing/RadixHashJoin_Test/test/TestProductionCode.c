#include "../../../src/unity.h"
#include "../../../../../radixHashJoin.h"
#include "../../../../../supportFunctions.h"


//sometimes you may want to get at local data in a module.
//for example: If you plan to pass by reference, this could be useful
//however, it should often be avoided
extern int Counter;
int number_of_buckets;

void setUp(void) {
    //This is run before EACH TEST
    number_of_buckets = 8;
}

void tearDown(void) {
}

void initializeRelations(Relation **relation1, Relation **relation2) {

    int i;

    *relation1 = allocateRelation(4);
    *relation2 = allocateRelation(3);

    for (i = 0; i < (*relation1)->num_tuples; i++) {
        (*relation1)->tuples[i].payload = i + 1;
        (*relation1)->tuples[i].key = i + 1 % number_of_buckets;
        (*relation1)->tuples[i].rowID = i + 1;
    }

    (*relation2)->tuples[0].rowID = 1;
    (*relation2)->tuples[1].rowID = 2;
    (*relation2)->tuples[2].rowID = 3;

    (*relation2)->tuples[0].payload = 1;
    (*relation2)->tuples[1].payload = 1;
    (*relation2)->tuples[2].payload = 3;

    (*relation2)->tuples[0].key = (*relation2)->tuples[0].payload % number_of_buckets;
    (*relation2)->tuples[1].key = (*relation2)->tuples[1].payload % number_of_buckets;
    (*relation2)->tuples[2].key = (*relation2)->tuples[2].payload % number_of_buckets;

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
    *histogram1 = malloc(sizeof(int32_t *) * number_of_buckets);
    if (*histogram1 == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return;
    }

    for (i = 0; i < number_of_buckets; i++) {
        (*histogram1)[i] = malloc(sizeof(int32_t) * 2);
        if ((*histogram1)[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return;
        }

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
    *psum1 = malloc(sizeof(int32_t *) * number_of_buckets);
    if (*psum1 == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return;
    }

    for (i = 0; i < number_of_buckets; i++) {
        (*psum1)[i] = malloc(sizeof(int32_t) * 2);
        if ((*psum1)[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return;
        }

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

/* Check histogram me vash to paradeigma ths ekfwnhshs*/
void test_Check_Histogram(void) {

    int i;
    Relation *relation1;
    Relation *relation2;

    initializeRelations(&relation1, &relation2);

    int32_t **histogram1;
    int32_t **histogram2;
    fillHistograms(&histogram1, &histogram2, relation1);

/*    printf("Fake");
    printHistogram(histogram1, 1, number_of_buckets);
    printf("Real");
    printHistogram(histogram2, 1, number_of_buckets);*/
    /* Compare */
    for (i = 0; i < number_of_buckets; i++) {
        TEST_ASSERT_EQUAL_INT32_ARRAY(histogram1[i], histogram2[i], 2);
    }

    freeMemory(relation1, relation2, histogram1, histogram2, NULL, NULL);

}

/* Check psum me vash to paradeigma ths ekfwnhshs*/
void test_Check_Psum(void) {

    int i;
    Relation *relation1;
    Relation *relation2;

    initializeRelations(&relation1, &relation2);

    int32_t **histogram1;
    int32_t **histogram2;
    fillHistograms(&histogram1, &histogram2, relation1);

    int32_t **psum1;
    int32_t **psum2;

    fillPsums(&psum1, &psum2, histogram2);

    /* Compare */
/*    printf("Fake");
    printPsum(psum1, 1, number_of_buckets);
    printf("Real");
    printPsum(psum2, 1, number_of_buckets);*/

    for (i = 0; i < number_of_buckets; i++) {
        TEST_ASSERT_EQUAL_INT32_ARRAY(psum1[i], psum2[i], 2);
    }

    freeMemory(relation1, relation2, histogram1, histogram2, psum1, psum2);
}

/* Check partitioned relation */
void test_Partitioned_relation(void) {

    int i;

    uint32_t numTuples1 = 4;
    uint32_t numTuples2 = 3;

    Relation *relation1;
    Relation *relation2;
    Relation *newrelation1 = allocateRelation(numTuples1);
    Relation *newrelation2 = allocateRelation(numTuples2);
    Relation *newrelation1FAKE = allocateRelation(numTuples1);
    Relation *newrelation2FAKE = allocateRelation(numTuples2);


    initializeRelations(&relation1, &relation2);

    int32_t **histogram1 = createHistogram(relation1, number_of_buckets);
    int32_t **histogram2 = createHistogram(relation2, number_of_buckets);
    int32_t **psum1 = createPsum(number_of_buckets, histogram1);
    int32_t **psum2 = createPsum(number_of_buckets, histogram2);

    partition(relation1, &newrelation1, number_of_buckets, psum1);
    partition(relation2, &newrelation2, number_of_buckets, psum2);


    int32_t p_array1[4] = {1, 2, 3, 4};
    int32_t key_array1[4] = {1, 2, 3, 4};
    int32_t rId_array1[4] = {1, 2, 3, 4};

    int32_t p_array2[3] = {1, 1, 3};
    int32_t key_array2[3] = {1, 1, 3};
    int32_t rId_array2[3] = {1, 2, 3};


    /* InitializeFakeRel1 */
    for (i = 0; i < numTuples1; i++) {
        newrelation1FAKE->tuples[i].payload = p_array1[i];
        newrelation1FAKE->tuples[i].key = key_array1[i];
        newrelation1FAKE->tuples[i].rowID = rId_array1[i];
    }

    /* InitializeFakeRel2 */
    for (i = 0; i < numTuples2; i++) {
        newrelation2FAKE->tuples[i].payload = p_array2[i];
        newrelation2FAKE->tuples[i].key = key_array2[i];
        newrelation2FAKE->tuples[i].rowID = rId_array2[i];
    }

/*    printRelation(relation1, 1);
    printRelation(newrelation1FAKE, 3);
    printRelation(relation2, 2);
    printRelation(newrelation2FAKE, 4);*/

    for (i = 0; i < newrelation1->num_tuples; i++) {
        TEST_ASSERT_EQUAL_INT32(newrelation1->tuples[i].payload, newrelation1FAKE->tuples[i].payload);
        TEST_ASSERT_EQUAL_INT32(newrelation1->tuples[i].key, newrelation1FAKE->tuples[i].key);
        TEST_ASSERT_EQUAL_INT32(newrelation1->tuples[i].rowID, newrelation1FAKE->tuples[i].rowID);
    }


    for (i = 0; i < newrelation2->num_tuples; i++) {
        TEST_ASSERT_EQUAL_INT32(newrelation2->tuples[i].payload, newrelation2FAKE->tuples[i].payload);
        TEST_ASSERT_EQUAL_INT32(newrelation2->tuples[i].key, newrelation2FAKE->tuples[i].key);
        TEST_ASSERT_EQUAL_INT32(newrelation2->tuples[i].rowID, newrelation2FAKE->tuples[i].rowID);
    }

    freeMemory(relation1, relation2, histogram1, histogram2, psum1, psum2);

    free(newrelation1->tuples);
    free(relation1);
    free(newrelation2->tuples);
    free(relation2);
    free(newrelation1FAKE->tuples);
    free(newrelation1FAKE);
    free(newrelation2FAKE->tuples);
    free(newrelation2FAKE);


}