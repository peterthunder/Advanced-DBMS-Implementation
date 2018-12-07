#include "query_functions.h"

int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array) {

    // Create intermediate tables and do the Joins.

    int i, column_number, table_number, j = 0;
    //printf("\n");
    for (i = 0; i < query_info->join_count; i++) {

        /* 12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2
         *
         * 0.2 = 1.0
         *
         * 0 -> 12
         * 1 - > 1
         *
         * */

        table_number = query_info->relation_IDs[query_info->joins[i][0]];
        column_number = query_info->joins[i][1];

        if ((*relation_array)[table_number][column_number] == NULL) {

            //printf("++Creating Relation %d.%d from join++\n", table_number, column_number);

            (*relation_array)[table_number][column_number] = allocateRelation((uint32_t) tables[table_number]->num_tuples);
            if ((*relation_array)[table_number][column_number] == NULL) {
                fprintf(stderr, "Malloc failed!\n");
                return -1;
            }

            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }

        /* while(j!=10){
             printf("P: %d , RID: %d \n", (*relation_array)[table_number][column_number]->tuples[j].payload, (*relation_array)[table_number][column_number]->tuples[j].key);
             j++;
         }
         getchar();
         */


        table_number = query_info->relation_IDs[query_info->joins[i][2]];
        column_number = query_info->joins[i][3];

        if ((*relation_array)[table_number][column_number] == NULL) {
            //printf("++Creating Relation %d.%d from join++\n", table_number, column_number);
            (*relation_array)[table_number][column_number] = allocateRelation((uint32_t) tables[table_number]->num_tuples);
            if ((*relation_array)[table_number][column_number] == NULL) {
                fprintf(stderr, "Malloc failed!\n");
                return -1;
            }

            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);

        }
    }


    for (i = 0; i < query_info->filter_count; i++) {

        table_number = query_info->relation_IDs[query_info->filters[i][0]];
        column_number = query_info->filters[i][1];

        if ((*relation_array)[table_number][column_number] == NULL) {
           // printf("--Creating Relation %d.%d from filter--\n", table_number, column_number);
            (*relation_array)[table_number][column_number] = allocateRelation((uint32_t) tables[table_number]->num_tuples);
            if ((*relation_array)[table_number][column_number] == NULL) {
                fprintf(stderr, "Malloc failed!\n");
                return -1;
            }

            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }
    }



    /*
 ----------------------------------------------------------------
        Query 44

        13 13|0.1=1.2&1.6=8220|1.5

        SELECT SUM("1".c5)
        FROM r13 "0", r13 "1"
        WHERE 0.c1=1.c2 and 1.c6=8220

 ----------------------------------------------------------------

        Query 27

        12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2

        SELECT SUM("2".c1), SUM("0".c1), SUM("0".c2)
        FROM r12 "0", r1 "1", r6 "2", r12 "3"
        WHERE 0.c2=1.c0 and 1.c0=2.c1 and 0.c1=3.c2 and 3.c0<33199

 ----------------------------------------------------------------
     */




    /*   0 2 4|     0.1=1.2 &   1.0=2.1 &   0.1>3000     |   0.0 1.1    */




    //filters

    //self joins

    // joins

    return 0;
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