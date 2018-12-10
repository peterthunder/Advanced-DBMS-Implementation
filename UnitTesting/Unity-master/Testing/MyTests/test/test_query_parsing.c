#include "test_query_parsing.h"
#include "../../../../../src/parser.h"
#include "../../../../../src/query_functions.h"

void check_Query_Parsing(void)
{
    char* query = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";

    Query_Info *qInfo = parse_query(query);

    TEST_ASSERT_NOT_NULL_MESSAGE(qInfo, "Query-parsing failed!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->filter_count, 1, "Filter count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->join_count, 2, "Join count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->selection_count, 2, "Selections count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->relationId_count, 3, "RelationIDs count was wrong!");

    // Enable the following test if you want to see a failing example
    /*query = "3 0 1|0.2=1.0&0.dvdsddsdsdsd1=2.0&0.2>3499|1.2 0.1| 9 0 0";

    qInfo = parse_query(query);

    TEST_ASSERT_NOT_NULL_MESSAGE(qInfo, "Query-parsing reported error for invalid query-string.");*/
}
