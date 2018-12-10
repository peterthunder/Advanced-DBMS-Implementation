#include "../../../src/unity.h"
#include "../../../../../src/radixHashJoin.h"
#include "../../../../../src/parser.h"
#include "../../../../../src/supportFunctions.h"

#include "test_support_functions.h"
#include "test_query_parsing.h"
#include "test_radix_hash_join.h"
#include "test_file_io.h"


//sometimes you may want to get at local data in a module.
//for example: If you plan to pass by reference, this could be useful
//however, it should often be avoided

void setUp(void) {
    //This is run before EACH TEST
    number_of_buckets = 8;
}

void tearDown(void) {
}

/* Check histogram me vash to paradeigma ths ekfwnhshs */
void test_Check_Histogram(void) {
    check_Histogram();
}

/* Check psum me vash to paradeigma ths ekfwnhshs */
void test_Check_Psum(void) {
    check_Psum();
}

/* Check partitioned relation */
void test_Check_Partitioned_relation(void) {
    check_Partitioned_relation();
}

/* Check Query Parsing */
void test_Check_Query_Parsing(void) {
    check_Query_Parsing();
}
