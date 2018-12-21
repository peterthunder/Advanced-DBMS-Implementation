#include "supportFunctions.h"


void setIOStreams(FILE **fp_read_tables, FILE **fp_read_queries, FILE **fp_write, FILE **fp_print) {

    if ( USE_HARNESS ) {
        *fp_read_tables = stdin;
        *fp_read_queries = stdin;
        *fp_write = stdout;
        *fp_print = stderr;
    }
    else {
        /* Open the file on that path */
        if ( (*fp_read_tables = fopen("workloads/small/small.init", "r")) == NULL ) {
            fprintf(stderr, "Error opening file workloads/small/small.init: %s!\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
        /* Open the file on that path */
        if ( (*fp_read_queries = fopen("workloads/small/small.work", "r")) == NULL ) {
            fprintf(stderr, "Error opening file \"workloads/small/small.work\": %s!\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
        if ( (*fp_write = fopen("results.txt", "wb")) == NULL ) {
            fprintf(stderr, "Error opening file \"results.txt\": %s!\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
        *fp_print = stdout;
    }
}

Relation *** allocateAndInitializeRelationArray(Table **tables, int num_of_tables) {

    Relation ***relation_array = myMalloc(sizeof(Relation **) * num_of_tables);

    for (int i = 0; i < num_of_tables; i++) {
        relation_array[i] = myMalloc(sizeof(Relation *) * tables[i]->num_columns);
        for (int j = 0; j < tables[i]->num_columns; j++) {
            relation_array[i][j] = NULL;
        }
    }
    return relation_array;
}

Relation *allocateRelation(uint32_t num_tuples, bool is_complete) {

    Relation *rel = myMalloc(sizeof(Relation));

    /* Matrix R and S sizes*/
    rel->num_tuples = num_tuples;
    rel->tuples = myMalloc(sizeof(Tuple) * (rel->num_tuples));
    rel->psum = NULL;
    rel->paritioned_relation = NULL;
    rel->is_partitioned = FALSE;
    rel->chain = NULL;
    rel->bucket_index = NULL;
    rel->is_built = FALSE;
    /* If the relation is filled from the original table, then it's complete */
    /* If the relation is filled from the intermediate table then it's incomplete */
    rel->is_full_column = is_complete;

    return rel;
}

void initializeRelation(Relation **rel, Table **tables, int table_number, int column_number) {

    int32_t i;

    for (i = 0; i < (*rel)->num_tuples; i++) {
        (*rel)->tuples[i].payload = (int32_t) tables[table_number]->column_indexes[column_number][i];
        (*rel)->tuples[i].key = i + 1;
    }
}

void initializeRelationWithRandomNumbers(Relation **rel) {

    int32_t i;

    /* Fill matrix R with random numbers from 0-199*/
    for (i = 0; i < (*rel)->num_tuples; i++) {
        (*rel)->tuples[i].payload = rand() % 199;
        (*rel)->tuples[i].key = i + 1;
    }
}

Sum_struct * sumStructureAllocationAndInitialization() {

    Sum_struct *sumStruct = myMalloc(sizeof(Sum_struct));
    sumStruct->full_size = 1;
    sumStruct->actual_size = 0;
    sumStruct->sums = myMalloc(sizeof(long *) * 1);
    sumStruct->sums_sizes = myMalloc(sizeof(long) * 1);
    return sumStruct;
}

void sumStructureUpdate(Sum_struct **sumStruct, Query_Info *query_info, long*sums){

    (*sumStruct)->sums[(*sumStruct)->actual_size] = sums;

    (*sumStruct)->sums_sizes[(*sumStruct)->actual_size] = query_info->selection_count;

    (*sumStruct)->actual_size++;

    if ((*sumStruct)->full_size == (*sumStruct)->actual_size) {
        (*sumStruct)->full_size <<= 1; // fast-multiply by 2
        (*sumStruct)->sums = realloc((*sumStruct)->sums, (*sumStruct)->full_size * sizeof(long *));
        (*sumStruct)->sums_sizes = realloc((*sumStruct)->sums_sizes, (*sumStruct)->full_size * sizeof(long));
    }
}

void resetSumStructure(Sum_struct **sumStruct){

    for (int k = 0; k < (*sumStruct)->actual_size; ++k) {
        free((*sumStruct)->sums[k]);
    }

    (*sumStruct)->actual_size = 0;
    (*sumStruct)->full_size = 1;

    (*sumStruct)->sums = realloc((*sumStruct)->sums, sizeof(long *) * 1);
    (*sumStruct)->sums_sizes = realloc((*sumStruct)->sums_sizes, sizeof(long) * 1);

    /* Flush stdout to clear the output buffer */
    fflush(stdout);
}

void writeSumsToStdout(Sum_struct *sumStruct){

    char tempLine[1024];
    char tempLine1[1024];

    for (int k = 0; k < sumStruct->actual_size; ++k) {

        tempLine[0] = '\0';
        tempLine1[0] = '\0';
        for (int l = 0; l < sumStruct->sums_sizes[k]; ++l) {

            if (sumStruct->sums[k][l] == 0) {

                strcat(tempLine, "NULL");

            } else {
                sprintf(tempLine1, "%ld", sumStruct->sums[k][l]);
                strcat(tempLine, tempLine1);
            }

            if (l != sumStruct->sums_sizes[k] - 1)
                strcat(tempLine, " ");
        }

        strcat(tempLine, "\n");

        //fprintf(fp_print, "%s", tempLine);
        fputs(tempLine, fp_write);
    }
}

void printRelation(Relation *relation, int choice) {

    int i;
    switch (choice) {
        case 1:
            printf("\n   Relation R\n[h1| h2| p |rID]\n----------------");
            break;
        case 2:
            printf("\n   Relation S\n[h1| h2| p |rID]\n----------------");
            break;
        case 3:
            printf("\n   Relation R'\n[h1| h2| p |rID]\n----------------");
            break;
        case 4:
            printf("\n   Relation S'\n[h1| h2| p |rID]\n----------------");
            break;
        default:
            return;
    }

    for (i = 0; i < relation->num_tuples; i++) {
        printf("\n");
        printf("|%2d|%3d|%3d|%2d|", relation->tuples[i].payload % H1_PARAM, relation->tuples[i].payload % H2_PARAM,
               relation->tuples[i].payload, relation->tuples[i].key);

        if(i==5)break;
    }

    printf("\n");
}

void printHistogram(int32_t **histogram, int choice) {

    int i;

    if (choice == 1)
        printf("\n----HistogramR----\n");
    else
        printf("\n----HistogramS----\n");

    for (i = 0; i < number_of_buckets; i++) {
        printf("Histogram[%d]: %1d|%d\n", i, histogram[i][0], histogram[i][1]);
    }
    printf("\n");
}

void printPsum(int32_t **psum, int choice) {

    int i;

    if (choice == 1)
        printf("\n----PsumR----\n");
    else
        printf("\n----PsumS----\n");

    for (i = 0; i < number_of_buckets; i++) {
        printf("Psum[%d]: %1d|%d\n", i, psum[i][0], psum[i][1]);
    }
    printf("\n");
}

void printAllForPartition(int choice, Relation *reIR, Relation *reIS, int32_t **histogramR, int32_t **histogramS, int32_t **psumR, int32_t **psumS,
                          Relation *newReIR, Relation *newReIS) {

    switch (choice) {
        case 1:
            printRelation(reIR, 1);

            printHistogram(histogramR, 1);
            printPsum(psumR, 1);

            printRelation(newReIR, 3);
            break;
        case 2:
            printRelation(reIS, 2);

            printHistogram(histogramS, 2);
            printPsum(psumS, 2);

            printRelation(newReIS, 4);
            break;
        default:    // Print for both relations.
            printRelation(reIR, 1);
            printRelation(reIS, 2);

            printHistogram(histogramR, 1);
            printPsum(psumR, 1);
            printHistogram(histogramS, 2);
            printPsum(psumS, 2);

            printRelation(newReIR, 3);
            printRelation(newReIS, 4);
    }
}

void printChainArray(int32_t **psum, Relation *relationNew, int **chain) {
    int32_t i, j;
    int32_t difference;

    for (i = 0; i < number_of_buckets; i++) {
        if (i == number_of_buckets - 1)
            difference = relationNew->num_tuples - psum[i][1];
        else
            difference = psum[i + 1][1] - psum[i][1];
        printf("------------------------------\n");
        if (chain[i] != NULL)
            for (j = 0; j < difference; j++) {
                printf("Bucket: %d-%d - Chain element: %d\n", i, j + 1, chain[i][j]);
            }
        else
            printf("Bucket %d - is empty.\n", i);
    }
}

void printResults(Result *result) {

    Result *current_result;

    printf("\n\n");
    current_result = result;
    do {
        printf("Number of tuples joined in current result: %d\n", current_result->num_joined_rowIDs);
        printf("[RowIDR|RowIDS]\n");
        for (int i = 0; i < current_result->num_joined_rowIDs; i++) {
            printf("   (%3d|%3d)\n", current_result->joined_rowIDs[i][0], current_result->joined_rowIDs[i][1]);
            if(i==5)break;
        }
        current_result = current_result->next_result;
    } while (current_result != NULL);
}

void printEntity(Entity *entity) {

    printf("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

    int i, j, k;
    for (i = 0; i < entity->inter_tables_count; i++) {
        if (entity->inter_tables[i] != NULL) {
            for (j = 0; j < entity->inter_tables[i]->num_of_columns; j++) {
                printf("Relation: %d\n", entity->inter_tables[i]->relationIDS_of_inter_table[j]);
                for (k = 0; k < entity->inter_tables[i]->num_of_rows; k++) {
                    printf("RowID: %d\n", entity->inter_tables[i]->inter_table[k][j]);
                    if (k == 5)break;
                }
            }
            printf("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }


    }
}

void print_query(Query_Info *query_info, char *query, int query_number) {

    int i;

    printf("\n----------------------------------------------------------------\n");

    printf("Query[%d]: %s\n", query_number - 1, query);

    printf("\tSELECT ");
    if (query_info->selections != NULL) {
        for (i = 0; i < query_info->selection_count; i++) {
            if (query_info->selections[i] != NULL)
                printf("SUM(\"%d\".c%d)", query_info->selections[i][0], query_info->selections[i][1]);
            if (i != query_info->selection_count - 1)
                printf(", ");
        }
    }

    printf("\n\tFROM ");
    for (i = 0; i < query_info->relationId_count; i++) {
        printf("r%d \"%d\"", query_info->relation_IDs[i], i);
        if (i != query_info->relationId_count - 1)
            printf(", ");
    }

    printf("\n\tWHERE ");

    if (query_info->joins != NULL) {
        for (i = 0; i < query_info->join_count; i++) {
            if (query_info->joins[i] != NULL)
                printf("%d.c%d=%d.c%d", query_info->joins[i][0], query_info->joins[i][1], query_info->joins[i][2],
                       query_info->joins[i][3]);
            if (query_info->filter_count != 0)
                printf(" and ");
        }
    }

    if (query_info->filters != NULL) {
        for (i = 0; i < query_info->filter_count; i++) {
            if (query_info->filters[i] != NULL) {
                switch (query_info->filters[i][2]) {
                    case EQUAL:
                        printf("%d.c%d=%d", query_info->filters[i][0], query_info->filters[i][1],
                               query_info->filters[i][3]);
                        break;
                    case LESS:
                        printf("%d.c%d<%d", query_info->filters[i][0], query_info->filters[i][1],
                               query_info->filters[i][3]);
                        break;
                    case GREATER:
                        printf("%d.c%d>%d", query_info->filters[i][0], query_info->filters[i][1],
                               query_info->filters[i][3]);
                        break;
                    default:
                        fprintf(stderr, "\nInvalid operator found: %d\n\n", query_info->filters[i][2]);
                        return;
                }
            }
            if (i != query_info->filter_count - 1)
                printf(" and ");
        }
    }

    printf("\n");
}

void *myMalloc(size_t sizeOfAllocation) {

    void *ptr = malloc(sizeOfAllocation);
    if (ptr == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        exit(EXIT_FAILURE);
    } else
        return ptr;
}

// Taken From: https://www.geeksforgeeks.org/write-your-own-atoi/
// A simple atoi() function
int myAtoi(char *str) {
    int res = 0; // Initialize result

    // Iterate through all characters of input string and
    // update result
    for (int i = 0; str[i] != '\0'; ++i)
        res = res * 10 + str[i] - '0';

    // return result.
    return res;
}

// Source: https://stackoverflow.com/a/15265294
long int myPow(int x, int n) {

    int i;
    int number = 1;

    for (i = 0; i < n; ++i)
        number *= x;

    return (number);
}
