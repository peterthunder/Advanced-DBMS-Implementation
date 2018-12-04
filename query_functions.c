#include "query_functions.h"

void execute_query(Query_Info *query_info) {
    // Create intermediate tables and do the Joins.

}

void print_query(Query_Info *query_info, char* query, int query_number) {

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

}

void free_query(Query_Info *query_info) {

    int i;

    free(query_info->relation_IDs);

    for (i = 0; i < query_info->join_count; i++)
        free(query_info->joins[i]);

    free(query_info->joins);


    for (i = 0; i < query_info->filter_count; i++)
        free(query_info->filters[i]);

    free(query_info->filters);


    for (i = 0; i < query_info->selection_count; i++)
        free(query_info->selections[i]);

    free(query_info->selections);


    free(query_info);
}