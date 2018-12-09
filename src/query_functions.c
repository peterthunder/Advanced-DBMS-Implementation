#include "radixHashJoin.h"

int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array) {

    // Create intermediate tables and do the Joins.

    int i, column_number, table_number, j = 0, row_id_count = 0, operator, number;
    int32_t *rowIDs;

    Entity *entity = myMalloc(sizeof(Entity));
    entity->intermediate_tables_count = 0;
    entity->intermediate_tables = myMalloc(
            sizeof(Intermediate_table *) * (query_info->filter_count + query_info->join_count));

    for (i = 0; i < query_info->filter_count; i++) {

        table_number = query_info->relation_IDs[query_info->filters[i][0]];
        column_number = query_info->filters[i][1];
        operator = query_info->filters[i][2];
        number = query_info->filters[i][3];

        if ((*relation_array)[table_number][column_number] == NULL) {
            (*relation_array)[table_number][column_number] = allocateRelation(
                    (uint32_t) tables[table_number]->num_tuples, TRUE);   // Allocation-errors are handled internally.
            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }

        // if in intermediate
        create_in_intermediate_table(query_info->filters[i][0], &entity);
        row_id_count = filterRelation(LESS, 100, (*relation_array)[table_number][column_number], &rowIDs);
        // else full column
        row_id_count = filterRelation(LESS, 100, (*relation_array)[table_number][column_number], &rowIDs);

        // update intermediate

        for (j = 0; j < row_id_count; j++) {
            printf("Rid: %d\n", rowIDs[j]);
        }


    }



    /* while(j!=10){
         printf("\nP: %d , RID: %d \n", (*relation_array)[table_number][column_number]->tuples[j].payload, (*relation_array)[table_number][column_number]->tuples[j].key);
         j++;
     }
     getchar();
     */

    //printf("\n");
    for (i = 0; i < query_info->join_count; i++) {

        /* 12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2
         *
         * 0.2 = 1.0
         *
         * Relation 0 -> Table number 12
         * Relation 1 - > Table number 1
         *
         * */

        table_number = query_info->relation_IDs[query_info->joins[i][0]];
        column_number = query_info->joins[i][1];

        if ((*relation_array)[table_number][column_number] == NULL) {
            (*relation_array)[table_number][column_number] = allocateRelation(
                    (uint32_t) tables[table_number]->num_tuples, TRUE);    // Allocation-errors are handled internally.
            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }

        table_number = query_info->relation_IDs[query_info->joins[i][2]];
        column_number = query_info->joins[i][3];

        if ((*relation_array)[table_number][column_number] == NULL) {
            (*relation_array)[table_number][column_number] = allocateRelation(
                    (uint32_t) tables[table_number]->num_tuples, TRUE);    // Allocation-errors are handled internally.
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

    // joins

    free(entity);


    return 0;
}


int create_in_intermediate_table(int relation_Id, Entity **entity) {

    int inter_table_number, column, i;
    Relation *relation;

    if (exists_in_intermediate_table(relation_Id, *entity, &inter_table_number, &column) == -1) {
        //doesnt exist
        printf("Doesn't exist.\n");
        (*entity)->intermediate_tables[(*entity)->intermediate_tables_count] = myMalloc(sizeof(Intermediate_table));
        (*entity)->intermediate_tables[(*entity)->intermediate_tables_count]->num_of_rows = 0;
        (*entity)->intermediate_tables[(*entity)->intermediate_tables_count]->num_of_columns = 0;
        (*entity)->intermediate_tables[(*entity)->intermediate_tables_count]->intermediate_table = NULL;
        (*entity)->intermediate_tables[(*entity)->intermediate_tables_count]->relationIDS_of_intermediate_table = NULL;
        (*entity)->intermediate_tables_count++;

    } else {
        //exists
        relation = allocateRelation((*entity)->intermediate_tables[inter_table_number]->num_of_rows, FALSE);
        for (i = 0; i < (*entity)->intermediate_tables[inter_table_number]->num_of_rows; i++) {
            relation->tuples[i].payload = (*entity)->intermediate_tables[inter_table_number]->intermediate_table[i][column];
            relation->tuples[i].key = i + 1;
        }
    }

    return -1;
}

int exists_in_intermediate_table(int relation_Id, Entity *entity, int *inter_table_number, int *column) {

    int i, j;

    for (i = 0; i < entity->intermediate_tables_count; i++) {
        for (j = 0; j < entity->intermediate_tables[i]->num_of_columns; j++) {
            if (entity->intermediate_tables[i]->relationIDS_of_intermediate_table[j] == relation_Id) {
                *inter_table_number = i;
                *column = j;
                return 0;
            }
        }
    }

    return -1;

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