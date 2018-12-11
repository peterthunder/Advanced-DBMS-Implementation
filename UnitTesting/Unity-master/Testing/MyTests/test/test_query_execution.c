#include "test_query_execution.h"


void check_filter_relation(void)
{
    int operator = GREATER;
    int numToFilterWith = 20;
    uint32_t rowIDsCount;
    uint32_t numOfTuples = 6;

    Relation* relation = allocateRelation(numOfTuples , TRUE);


    int32_t key_array[6] = {1, 2, 3, 4, 5, 6};
    int32_t payload_array[6] = {10, 15, 25, 30, 40, 50};


    /* InitializeFakeRel1 */
    for (int i = 0; i < numOfTuples; i++) {
        relation->tuples[i].key = key_array[i];
        relation->tuples[i].payload = payload_array[i];
    }


    int32_t* rowIDs = filterRelation(operator, numToFilterWith, relation, &rowIDsCount);

    // We expect the <rowIDsCount> to be 4, since only <25>, <30>, <40> and <50> are GREATER than <20>.

    TEST_ASSERT_EQUAL_INT_MESSAGE(4, rowIDsCount, "RowIDs-number was wrong after filtering the relation!");
}
