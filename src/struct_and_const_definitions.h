#ifndef ADVANCED_DBMS_IMPLEMENTATION_STRUCT_AND_CONST_DEFINITIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_STRUCT_AND_CONST_DEFINITIONS_H

#define H1_PARAM 4 // Number of bits we PRINTING keep after the 1st hash function pass
#define H2_PARAM 101 // The number we use in the 2nd hash function as mod
#define JOINED_ROWIDS_NUM ((1024 * 1024) / 8)
#define TRUE true
#define FALSE false
#define EQUAL 0
#define GREATER 1
#define LESS 2
#define WORKLOAD_BASE_PATH  "workloads/small/"
#define USE_HARNESS false


int32_t number_of_buckets;
FILE *fp_read,*fp_write, *fp_print;

/* Type definition for a tuple */
typedef struct tuple {
    int32_t key; // rowID of this tuple
    int32_t payload; // true value of this tuple
} Tuple;


/* Type definition for a relation */
typedef struct relation {
    Tuple *tuples;
    uint32_t num_tuples;
    struct relation *paritioned_relation;
    int32_t **psum;
    int32_t **chain;
    int32_t **bucket_index;
    bool is_built;
    bool is_partitioned;
    bool is_full_column;
} Relation;


/* Type definition for a result */
typedef struct result {
    int32_t joined_rowIDs[JOINED_ROWIDS_NUM][2];
    int num_joined_rowIDs;
    struct result* next_result;
} Result;


/* Struct that holds the information of all the columns of a single table */
typedef struct table_{
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t **column_indexes;
} Table;


typedef struct Query_ {
    int *relation_IDs;
    int relationId_count;
    int **filters;
    int filter_count;
    int **joins;
    int join_count;
    int **selections;
    int selection_count;
} Query_Info;


typedef struct intermediate_table_{
    uint32_t num_of_rows; // number of row IDs
    uint32_t num_of_columns; // number of Relation IDs in the intermediate table
    int32_t **inter_table; // array that holds the row IDS
    int *relationIDS_of_inter_table; // array that tells us which relation id refers to which column of the inter_table
}Intermediate_table;


/* Entity that holds the intermediate tables */
typedef struct entity_{
    int max_count;
    int inter_tables_count;
    Intermediate_table **inter_tables; // The intermediate tables hold the joined and filtered row Ids.
}Entity;


typedef struct sum_struct{
    int full_size;
    int actual_size;
    long **sums;
    int *sums_sizes;
}Sum_struct;


#endif //ADVANCED_DBMS_IMPLEMENTATION_STRUCT_AND_CONST_DEFINITIONS_H