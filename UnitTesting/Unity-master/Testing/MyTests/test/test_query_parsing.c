#include "test_query_parsing.h"
#include "../../../../../src/parser.h"
#include "../../../../../src/query_functions.h"

void check_Query_Parsing(void)
{
    char* query = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";

    fprintf(stderr, "\n\nTesting parsing of valid query: \"%s\"\n", query);

    Query_Info *qInfo = parse_query(query);

    TEST_ASSERT_NOT_NULL_MESSAGE(qInfo, "Query-parsing failed!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->filter_count, 1, "Filter count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->join_count, 2, "Join count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->selection_count, 2, "Selections count was wrong!");

    TEST_ASSERT_EQUAL_INT_MESSAGE(qInfo->relationId_count, 3, "RelationIDs count was wrong!");

    fprintf(stderr, "\n\nParsing succeeded for valid query: \"%s\"\n", query);

    // Test an invalid query-string. Our function should report an error by returning "NULL".

    query = "3 0     1|0.2=1.0&0.dvds.dds.dsd.sd1=2.0&0.2>3499|1.2 0.1||| 9 0 0";

    fprintf(stderr, "\n\nTesting parsing of the invalid query: \"%s\"\n", query);

    qInfo = parse_query(query);

    // The result should be "NULL", since this is not a valid query.

    TEST_ASSERT_NULL_MESSAGE(qInfo, "\"parse_query()\" failed to detect an invalid query-string!");
}
