#include "../../../src/unity.h"
#include "../../../../../src/radixHashJoin.h"
#include "../../../../../src/supportFunctions.h"
#include "test_support_functions.h"



//sometimes you may want to get at local data in a module.
//for example: If you plan to pass by reference, this could be useful
//however, it should often be avoided
extern int number_of_buckets;

void setUp(void) {
    //This is run before EACH TEST
    number_of_buckets = 8;
}

void tearDown(void) {
}

/* Check histogram me vash to paradeigma ths ekfwnhshs */
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

/* Check psum me vash to paradeigma ths ekfwnhshs */
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
    Relation *newrelation1 = allocateRelation(numTuples1, TRUE);
    Relation *newrelation2 = allocateRelation(numTuples2, TRUE);
    Relation *newrelation1FAKE = allocateRelation(numTuples1, TRUE);
    Relation *newrelation2FAKE = allocateRelation(numTuples2, TRUE);


    initializeRelations(&relation1, &relation2);

    int32_t **histogram1 = createHistogram(relation1, number_of_buckets);
    int32_t **histogram2 = createHistogram(relation2, number_of_buckets);
    int32_t **psum1 = createPsum(number_of_buckets, histogram1);
    int32_t **psum2 = createPsum(number_of_buckets, histogram2);

    partition(relation1, &newrelation1, number_of_buckets, psum1);
    partition(relation2, &newrelation2, number_of_buckets, psum2);


    int32_t p_array1[4] = {1, 2, 3, 4};
    int32_t key_array1[4] = {1, 2, 3, 4};

    int32_t p_array2[3] = {1, 1, 3};
    int32_t key_array2[3] = {1, 2, 3};


    /* InitializeFakeRel1 */
    for (i = 0; i < numTuples1; i++) {
        newrelation1FAKE->tuples[i].payload = p_array1[i];
        newrelation1FAKE->tuples[i].key = key_array1[i];
    }

    /* InitializeFakeRel2 */
    for (i = 0; i < numTuples2; i++) {
        newrelation2FAKE->tuples[i].payload = p_array2[i];
        newrelation2FAKE->tuples[i].key = key_array2[i];
    }

/*    printRelation(relation1, 1);
    printRelation(newrelation1FAKE, 3);
    printRelation(relation2, 2);
    printRelation(newrelation2FAKE, 4);*/

    for (i = 0; i < newrelation1->num_tuples; i++) {
        TEST_ASSERT_EQUAL_INT32(newrelation1->tuples[i].payload, newrelation1FAKE->tuples[i].payload);
        TEST_ASSERT_EQUAL_INT32(newrelation1->tuples[i].key, newrelation1FAKE->tuples[i].key);
    }


    for (i = 0; i < newrelation2->num_tuples; i++) {
        TEST_ASSERT_EQUAL_INT32(newrelation2->tuples[i].payload, newrelation2FAKE->tuples[i].payload);
        TEST_ASSERT_EQUAL_INT32(newrelation2->tuples[i].key, newrelation2FAKE->tuples[i].key);
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
