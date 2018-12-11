#include "test_file_io.h"


void check_read_tables(void)
{
    int num_of_tables, *mapped_tables_sizes;
    uint64_t **mapped_tables;

    char workload_base_path[30];
    char* intermediate_path = "../../../../";
    // This test runs from the "UnitTesting/Unity-master/Testing/MyTests" directory, so we have to set the right path.

    strcat(workload_base_path, intermediate_path);
    strcat(workload_base_path, WORKLOAD_BASE_PATH);

    //printf("Workload_base_path: %s\n", workload_base_path);   // DEBUG!

    Table** tables = read_tables(workload_base_path, TABLES_FILENAME, &num_of_tables, &mapped_tables, &mapped_tables_sizes);

    TEST_ASSERT_NOT_NULL_MESSAGE(tables, "Reading tables failed!");
}
