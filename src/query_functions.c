#include "radixHashJoin.h"

int execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array, int number_of_buckets) {

    // Create intermediate tables and do the Joins.

    int i, column_number, table_number, j = 0, k = 0, operator, number, column_number1, column_number2, table_number1, table_number2;
    uint32_t rowId_count = 0;
    int32_t *rowIDs;
    Relation *relation, *relation1, *relation2;

    /* Entity structure that will hold all the intermediate tables */
    Entity *entity = myMalloc(sizeof(Entity));
    entity->inter_tables_count = 0;
    entity->inter_tables = myMalloc(sizeof(Intermediate_table *) * (query_info->filter_count + query_info->join_count));

    /* Filter filter the relations*/
    for (i = 0; i < query_info->filter_count; i++) {

        table_number = query_info->relation_IDs[query_info->filters[i][0]]; // number of the table that has the the relation that needs to be filtered
        column_number = query_info->filters[i][1]; // in which column of that table
        operator = query_info->filters[i][2]; // what operation is the filter
        number = query_info->filters[i][3]; // and with what number

        /* If the relation does not exist in the relation array*/
        if ((*relation_array)[table_number][column_number] == NULL) {
            printf("Creating -> T: %d, C: %d\n", table_number, column_number);
            /*  Create the relation with a TRUE flag, that means it is a full column*/
            (*relation_array)[table_number][column_number] = allocateRelation((uint32_t) tables[table_number]->num_tuples, TRUE);
            /*  And fill the relation from the tables array*/
            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }

        /* Check if the relation_id is already in an intermediate table.
        * If it isn't, create a new intermediate table, filter the relation(full column) and fill the new inter table with the row ids that satisfy the filter.
        * Otherwise, if the relation exists in an intermediate table, get the relation from it, based on the rowIDs, filter the relation and update the intermediate table.*/
        relation = create_intermediate_table(query_info->filters[i][0], &entity, (*relation_array)[table_number][column_number]);

        if (relation != NULL) {
            /* Get the rowIDs of the relation that satisfy the filter*/
            rowIDs = filterRelation(operator, number, relation, &rowId_count);

            /* Update the intermediate table according to those rowIDs*/
            update_intermediate_table(query_info->filters[i][0], &entity, rowIDs, rowId_count);

            /*  Free the relation because it isn't needed*/
            deAllocateRelation(&relation, number_of_buckets);

        } else {

            printf("Table count: %d\n", entity->inter_tables_count);

            /* Get the rowIDs of the relation that satisfy the filter*/
            rowIDs = filterRelation(operator, number, (*relation_array)[table_number][column_number], &rowId_count);

            /* Fill the intermediate table according to those rowIDs*/
            entity->inter_tables[entity->inter_tables_count - 1]->num_of_columns = 1;
            entity->inter_tables[entity->inter_tables_count - 1]->num_of_rows = rowId_count;
            entity->inter_tables[entity->inter_tables_count - 1]->inter_table = myMalloc(sizeof(int32_t *) * rowId_count);

            for (j = 0; j < rowId_count; j++) {
                entity->inter_tables[entity->inter_tables_count - 1]->inter_table[j] = myMalloc(sizeof(int32_t) * 1);
                entity->inter_tables[entity->inter_tables_count - 1]->inter_table[j][0] = rowIDs[j];
            }

            entity->inter_tables[entity->inter_tables_count - 1]->relationIDS_of_inter_table = myMalloc(sizeof(int) * 1);
            entity->inter_tables[entity->inter_tables_count - 1]->relationIDS_of_inter_table[0] = query_info->filters[i][0];

        }
        free(rowIDs);
    }


    /* Then Join them */
    for (i = 0; i < query_info->join_count; i++) {

        table_number1 = query_info->relation_IDs[query_info->joins[i][0]];
        column_number1 = query_info->joins[i][1];

        if ((*relation_array)[table_number1][column_number1] == NULL) {
            printf("Creating -> T: %d, C: %d - ", table_number1, column_number1);
            (*relation_array)[table_number1][column_number1] = allocateRelation((uint32_t) tables[table_number1]->num_tuples, TRUE);
            initializeRelation(&(*relation_array)[table_number1][column_number1], tables, table_number1, column_number1);
        } else
            printf("Already Created -> T: %d, C: %d - ", table_number1, column_number1);

        table_number2 = query_info->relation_IDs[query_info->joins[i][2]];
        column_number2 = query_info->joins[i][3];

        if ((*relation_array)[table_number2][column_number2] == NULL) {
            printf("Creating -> T: %d, C: %d\n", table_number2, column_number2);
            (*relation_array)[table_number2][column_number2] = allocateRelation((uint32_t) tables[table_number2]->num_tuples, TRUE);
            initializeRelation(&(*relation_array)[table_number2][column_number2], tables, table_number2, column_number2);
        } else
            printf("Already Created -> T: %d, C: %d\n", table_number2, column_number2);

        relationJoin(&(*relation_array)[table_number1][column_number1], &(*relation_array)[table_number2][column_number2], &entity,
                     query_info->joins[i][0], query_info->joins[i][2], number_of_buckets);

        break;
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


    //printEntity(entity);


    for (i = 0; i < entity->inter_tables_count; i++) {

        for (j = 0; j < entity->inter_tables[i]->num_of_rows; j++)
            free(entity->inter_tables[i]->inter_table[j]);

        free(entity->inter_tables[i]->relationIDS_of_inter_table);
        free(entity->inter_tables[i]->inter_table);
        free(entity->inter_tables[i]);
    }

    free(entity->inter_tables);
    free(entity);


    return 0;
}

Relation *create_intermediate_table(int relation_Id, Entity **entity, Relation *original_relation) {

    int inter_table_number, inter_column, i;
    Relation *relation;
    int rId;

    /* Check if the relation_id arg exists in the entity */
    if (exists_in_intermediate_table(relation_Id, *entity, &inter_table_number, &inter_column) == -1) {
        /* If it doesn't exist, create a new intermediate table and initialize it */
        printf("Doesn't exist.\n");
        (*entity)->inter_tables[(*entity)->inter_tables_count] = myMalloc(sizeof(Intermediate_table));
        (*entity)->inter_tables[(*entity)->inter_tables_count]->num_of_rows = 0;
        (*entity)->inter_tables[(*entity)->inter_tables_count]->num_of_columns = 0;
        (*entity)->inter_tables[(*entity)->inter_tables_count]->inter_table = NULL;
        (*entity)->inter_tables[(*entity)->inter_tables_count]->relationIDS_of_inter_table = NULL;
        (*entity)->inter_tables_count++;
        return NULL;
    } else {
        /* Else if it exists, we need to create a relation from the intermediate table with the rowIDs and payload of the relation_id */
        relation = allocateRelation((*entity)->inter_tables[inter_table_number]->num_of_rows, FALSE);
        for (i = 0; i < (*entity)->inter_tables[inter_table_number]->num_of_rows; i++) {
            rId = (*entity)->inter_tables[inter_table_number]->inter_table[i][inter_column] - 1; // <- ayto einai to row id-1(oi pinakes 3ekinane apto 0),
                                                                                                // oxi to swsto payload ap' ton tables
            relation->tuples[i].payload = original_relation->tuples[rId].payload; // sto original relation, s' ayto to rowID einai to swsto payload
            relation->tuples[i].key = i + 1;
        }
        return relation;
    }
}

int exists_in_intermediate_table(int relation_Id, Entity *entity, int *inter_table_number, int *column) {

    int i, j;

    /* For every intermediate table */
    for (i = 0; i < entity->inter_tables_count; i++) {
        /* For every column in the intermediate table */
        for (j = 0; j < entity->inter_tables[i]->num_of_columns; j++) {
            /* Check if the current relation_id[j] in the relationIDs is the same as the relation id passed as an arg */
            if (entity->inter_tables[i]->relationIDS_of_inter_table[j] == relation_Id) {
                /* If it is the same
                 * Return the number of the intermediate table(i) and the column(j) */
                *inter_table_number = i;
                *column = j;
                return 0;
            }
        }
    }

    return -1;
}

int32_t *filterRelation(int operator, int number, Relation *relation, uint32_t *count) {

    int i;
    int32_t *rowIDs;
    (*count) = 0;

    switch (operator) {
        case EQUAL:
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload == number) {
                    (*count)++;
                }
            }
            rowIDs = myMalloc(sizeof(uint32_t) * (*count));
            (*count) = 0;
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload == number) {
                    rowIDs[(*count)] = relation->tuples[i].key;
                    (*count)++;
                }
            }
            break;
        case LESS:
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload < number) {
                    (*count)++;
                }
            }
            rowIDs = myMalloc(sizeof(uint32_t) * (*count));
            (*count) = 0;
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload < number) {
                    rowIDs[(*count)] = relation->tuples[i].key;
                    (*count)++;
                }
            }
            break;
        case GREATER:
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload > number) {
                    (*count)++;
                }
            }
            rowIDs = myMalloc(sizeof(uint32_t) * (*count));
            (*count) = 0;
            for (i = 0; i < relation->num_tuples; i++) {
                if (relation->tuples[i].payload > number) {
                    rowIDs[(*count)] = relation->tuples[i].key;
                    (*count)++;
                }
            }
            break;
        default:
            fprintf(stderr, "\nInvalid operator found: %d\n\n", operator);
            exit(EXIT_FAILURE);
    }

    return rowIDs;
}

void relationJoin(Relation **relation1, Relation **relation2, Entity **entity, int relation_Id1, int relation_Id2, int number_of_buckets) {

    int ret1, ret2, table_number1, table_number2, column_number1, column_number2;
    Relation *new_relation1, *new_relation2;
    Result *result;

    ret1 = exists_in_intermediate_table(relation_Id1, *entity, &table_number1, &column_number1);
    ret2 = exists_in_intermediate_table(relation_Id2, *entity, &table_number2, &column_number2);

    printf("Ret1: %d, Ret2: %d\n", ret1, ret2);
    if (ret1 == -1 && ret2 == -1) {
        // both not in intermediate tables
        new_relation1 = create_intermediate_table(relation_Id1, entity, *relation1);
        if (new_relation1 == NULL) {
            printf("Inter_t was just created.\n");
        }
        result = RadixHashJoin(relation1, relation2, number_of_buckets);
        // printResults(result);
        deAllocateResult(&result);
    } else if (ret1 != -1 && ret2 == -1) {
        printf("lalalalal1\n");
    } else if (ret1 == -1 && ret2 != -1) {
        printf("lalalalala2\n");
    } else {
        // both in intermediate tables
        printf("lalalalala3\n");
    }
}

void update_intermediate_table(int relation_Id, Entity **entity, int32_t *rowIDs, uint32_t rowId_count) {

    int inter_table_number, inter_column, i;

    /* Get the intermediate table index and the column that the given relationId exists */
    exists_in_intermediate_table(relation_Id, *entity, &inter_table_number, &inter_column);

    /* Create a new intermediate table and fill it from the old one according to the rowIDs that satisfy the filter */
    int32_t **new_inter_table = myMalloc(sizeof(int32_t *) * rowId_count);
    for (i = 0; i < rowId_count; i++) {
        new_inter_table[i] = myMalloc(sizeof(int32_t) * 1);
        new_inter_table[i][0] = (*entity)->inter_tables[inter_table_number]->inter_table[rowIDs[i] - 1][inter_column];
        //printf("rId: %d\n", new_inter_table[i][0]);
    }
    /* Free the old intermediate table */
    for (i = 0; i < (*entity)->inter_tables[inter_table_number]->num_of_rows; i++)
        free((*entity)->inter_tables[inter_table_number]->inter_table[i]);

    free((*entity)->inter_tables[inter_table_number]->inter_table);

    /* Point to the new intermediate table */
    (*entity)->inter_tables[inter_table_number]->num_of_rows = rowId_count;
    (*entity)->inter_tables[inter_table_number]->inter_table = new_inter_table;
}

void printEntity(Entity *entity) {

    printf("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

    int i, j, k;
    for (i = 0; i < entity->inter_tables_count; i++) {
        for (j = 0; j < entity->inter_tables[i]->num_of_columns; j++) {
            printf("Relation: %d\n", entity->inter_tables[i]->relationIDS_of_inter_table[j]);
            for (k = 0; k < entity->inter_tables[i]->num_of_rows; k++) {
                printf("RowID: %d\n", entity->inter_tables[i]->inter_table[k][j]);
            }
        }
        printf("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
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