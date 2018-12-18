#include "radixHashJoin.h"


long * execute_query(Query_Info *query_info, Table **tables, Relation ****relation_array, FILE *fp) {

    int i, column_number, table_number, j = 0, operator, number, column_number1, column_number2, table_number1, table_number2;

    /* Entity structure that will hold all the intermediate tables */
    Entity *entity = myMalloc(sizeof(Entity));
    entity->inter_tables_count = 0;
    entity->max_count = query_info->filter_count + query_info->join_count;
    entity->inter_tables = myMalloc(sizeof(Intermediate_table *) * entity->max_count);
    for (i = 0; i < entity->max_count; i++) {
        entity->inter_tables[i] = NULL;
    }

    /* Filter filter the relations */
    for (i = 0; i < query_info->filter_count; i++) {

        table_number = query_info->relation_IDs[query_info->filters[i][0]]; // number of the table that has the the relation that needs to be filtered
        column_number = query_info->filters[i][1]; // in which column of that table
        operator = query_info->filters[i][2]; // what operation is the filter
        number = query_info->filters[i][3]; // and with what number

        /* If the relation does not exist in the relation array */
        if ((*relation_array)[table_number][column_number] == NULL) {
            /*  Create the relation with a TRUE flag, that means it is a full column*/
            (*relation_array)[table_number][column_number] = allocateRelation((uint32_t) tables[table_number]->num_tuples, TRUE);
            /*  And fill the relation from the tables array*/
            initializeRelation(&(*relation_array)[table_number][column_number], tables, table_number, column_number);
        }
        relationFilter(&(*relation_array)[table_number][column_number], &entity, query_info->filters[i][0], operator, number);
    }

    /* Then Join them */
    for (i = 0; i < query_info->join_count; i++) {

        table_number1 = query_info->relation_IDs[query_info->joins[i][0]];
        column_number1 = query_info->joins[i][1];

        if ((*relation_array)[table_number1][column_number1] == NULL) {
            (*relation_array)[table_number1][column_number1] = allocateRelation((uint32_t) tables[table_number1]->num_tuples, TRUE);
            initializeRelation(&(*relation_array)[table_number1][column_number1], tables, table_number1, column_number1);
        }

        table_number2 = query_info->relation_IDs[query_info->joins[i][2]];
        column_number2 = query_info->joins[i][3];

        if ((*relation_array)[table_number2][column_number2] == NULL) {
            (*relation_array)[table_number2][column_number2] = allocateRelation((uint32_t) tables[table_number2]->num_tuples, TRUE);
            initializeRelation(&(*relation_array)[table_number2][column_number2], tables, table_number2, column_number2);
        }

        relationJoin(&(*relation_array)[table_number1][column_number1], &(*relation_array)[table_number2][column_number2],
                     &entity, query_info->joins[i][0], query_info->joins[i][2]);
    }

    long* sums = calculateSums(entity, query_info, tables, fp);

    //printEntity(entity);

    for (i = 0; i < entity->max_count; i++) {
        if (entity->inter_tables[i] != NULL) {
            for (j = 0; j < entity->inter_tables[i]->num_of_rows; j++)
                free(entity->inter_tables[i]->inter_table[j]);

            free(entity->inter_tables[i]->relationIDS_of_inter_table);
            free(entity->inter_tables[i]->inter_table);
            free(entity->inter_tables[i]);
        }
    }

    free(entity->inter_tables);
    free(entity);

    return sums;
}

Relation *create_intermediate_table(int relation_Id, Entity **entity, Relation *original_relation, int *inter_table_number) {

    int inter_column, i;
    Relation *relation;
    int rId;

    /* Check if the relation_id arg exists in the entity */
    if (exists_in_intermediate_table(relation_Id, *entity, inter_table_number, &inter_column) == -1) {
        /* If it doesn't exist, create a new intermediate table and initialize it */
        //printf("Int_t: %d - Doesn't exist.\n", *inter_table_number);
        (*entity)->inter_tables[*inter_table_number] = myMalloc(sizeof(Intermediate_table));
        (*entity)->inter_tables[*inter_table_number]->num_of_rows = 0;
        (*entity)->inter_tables[*inter_table_number]->num_of_columns = 0;
        (*entity)->inter_tables[*inter_table_number]->inter_table = NULL;
        (*entity)->inter_tables[*inter_table_number]->relationIDS_of_inter_table = NULL;
        (*entity)->inter_tables_count++;
        return NULL;
    } else {
        /* Else if it exists, we need to create a relation from the intermediate table with the rowIDs and payload of the relation_id */
        relation = allocateRelation((*entity)->inter_tables[*inter_table_number]->num_of_rows, FALSE);
        for (i = 0; i < (*entity)->inter_tables[*inter_table_number]->num_of_rows; i++) {
            rId = (*entity)->inter_tables[*inter_table_number]->inter_table[i][inter_column] - 1; // <- ayto einai to row id-1(oi pinakes 3ekinane apto 0),
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
    for (i = 0; i < entity->max_count; i++) {
        /* Check if the inter table exists*/
        if (entity->inter_tables[i] != NULL) {
            //printf("In here - i: %d\n", i);
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
    }

    /* For every intermediate table */
    for (i = 0; i < entity->max_count; i++) {
        if (entity->inter_tables[i] == NULL) {
            *inter_table_number = i;
            //printf("In here2 - i: %d\n", i);
            break;
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

void relationFilter(Relation **original_relation, Entity **entity, int relation_Id, int operator, int number) {

    int i;
    uint32_t rowId_count = 0;
    int32_t *rowIDs;
    Relation *relation;
    int inter_number_table;

    /* Check if the relation_id is already in an intermediate table.
        * If it isn't, create a new intermediate table, filter the relation(full column) and fill the new inter table with the row ids that satisfy the filter.
        * Otherwise, if the relation exists in an intermediate table, get the relation from it, based on the rowIDs, filter the relation and update the intermediate table.*/
    relation = create_intermediate_table(relation_Id, entity, *original_relation, &inter_number_table);

    if (relation != NULL) {
        /* Get the rowIDs of the relation that satisfy the filter*/
        rowIDs = filterRelation(operator, number, relation, &rowId_count);

        /* Update the intermediate table according to those rowIDs*/
        update_intermediate_table(relation_Id, entity, rowIDs, rowId_count);

        /*  Free the relation because it isn't needed*/
        deAllocateRelation(&relation);

    } else {

        /* Get the rowIDs of the relation that satisfy the filter*/
        rowIDs = filterRelation(operator, number, *original_relation, &rowId_count);

        /* Fill the intermediate table according to those rowIDs*/
        (*entity)->inter_tables[inter_number_table]->num_of_columns = 1;
        (*entity)->inter_tables[inter_number_table]->num_of_rows = rowId_count;
        (*entity)->inter_tables[inter_number_table]->inter_table = myMalloc(sizeof(int32_t *) * rowId_count);

        for (i = 0; i < rowId_count; i++) {
            (*entity)->inter_tables[inter_number_table]->inter_table[i] = myMalloc(sizeof(int32_t) * 1);
            (*entity)->inter_tables[inter_number_table]->inter_table[i][0] = rowIDs[i];
        }

        (*entity)->inter_tables[inter_number_table]->relationIDS_of_inter_table = myMalloc(sizeof(int) * 1);
        (*entity)->inter_tables[inter_number_table]->relationIDS_of_inter_table[0] = relation_Id;

    }
    free(rowIDs);
}

void relationJoin(Relation **relation1, Relation **relation2, Entity **entity, int relation_Id1, int relation_Id2) {

    int ret1, ret2, inter_table_number, inter_table_number1, inter_table_number2, column_number1, column_number2, i, j, *row_ids;
    uint32_t result_counter = 0, columns = 0, counter = 0;
    Relation *new_relation1, *new_relation2;
    Result *result, *current_result;
    int32_t **new_inter_table;

    ret1 = exists_in_intermediate_table(relation_Id1, *entity, &inter_table_number1, &column_number1);
    ret2 = exists_in_intermediate_table(relation_Id2, *entity, &inter_table_number2, &column_number2);

    /* If none of the relations exists in an intermediate table */
    if (ret1 == -1 && ret2 == -1) {
        /* Create the intermediate table */
        create_intermediate_table(relation_Id1, entity, *relation1, &inter_table_number);
        result = RadixHashJoin(relation1, relation2);

        /* Count the number of the results(rows) */
        current_result = result;
        do {
            result_counter = result_counter + current_result->num_joined_rowIDs;
            current_result = current_result->next_result;
        } while (current_result != NULL);

        /* Fill the number of rows, columns and the relation_ids array */
        (*entity)->inter_tables[inter_table_number]->num_of_rows = result_counter;
        (*entity)->inter_tables[inter_table_number]->num_of_columns = 2;
        (*entity)->inter_tables[inter_table_number]->relationIDS_of_inter_table = myMalloc(sizeof(int *) * 2);
        (*entity)->inter_tables[inter_table_number]->relationIDS_of_inter_table[0] = relation_Id1;
        (*entity)->inter_tables[inter_table_number]->relationIDS_of_inter_table[1] = relation_Id2;

        /* Create the intermediate table with size (num_of_rows * 2)  */
        (*entity)->inter_tables[inter_table_number]->inter_table = myMalloc(sizeof(int32_t *) * result_counter);
        for (i = 0; i < (*entity)->inter_tables[inter_table_number]->num_of_rows; i++) {
            (*entity)->inter_tables[inter_table_number]->inter_table[i] = myMalloc(sizeof(int32_t) * 2);
        }

        /* Reset the counter and the current_result pointer */
        result_counter = 0;
        current_result = result;
        do {
            /* Then do a for from counter until counter+ num_joined_rowIDs of the current result */
            for (i = 0; i < current_result->num_joined_rowIDs; i++) {
                (*entity)->inter_tables[inter_table_number]->inter_table[i + result_counter][0] = current_result->joined_rowIDs[i][0];
                (*entity)->inter_tables[inter_table_number]->inter_table[i + result_counter][1] = current_result->joined_rowIDs[i][1];
            }
            /* Update the counter */
            result_counter = result_counter + current_result->num_joined_rowIDs;
            /* Go to the next result */
            current_result = current_result->next_result;
        } while (current_result != NULL);

        deAllocateResult(&result);
    }
    /* If one of the relations exists in an intermediate table */
    else if ((ret1 != -1 && ret2 == -1) || ret1 == -1) {

        if (ret1 != -1) {
            new_relation1 = create_intermediate_table(relation_Id1, entity, *relation1, &inter_table_number);
        } else {
            new_relation2 = create_intermediate_table(relation_Id2, entity, *relation2, &inter_table_number);
            inter_table_number1 = inter_table_number2;
        }

        if (ret1 != -1)
            result = RadixHashJoin(&new_relation1, relation2);
        else
            result = RadixHashJoin(&new_relation2, relation1);

        /* Count the number of the results(rows) */
        current_result = result;
        do {
            result_counter = result_counter + current_result->num_joined_rowIDs;
            current_result = current_result->next_result;
        } while (current_result != NULL);

        /* Fill the number of rows, columns and the relation_ids array */
        columns = (*entity)->inter_tables[inter_table_number1]->num_of_columns + 1;

        (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table = realloc(
                (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table, sizeof(int *) * columns);

        if (ret1 != -1)
            (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table[columns - 1] = relation_Id2;
        else
            (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table[columns - 1] = relation_Id1;

        /* Create a new intermediate table with size (num_of_rows * 2) */
        new_inter_table = myMalloc(sizeof(int32_t *) * result_counter);

        /* Reset the counter and the current_result pointer */
        result_counter = 0;
        current_result = result;
        do {
            /* Then do a for from counter until counter+ num_joined_rowIDs of the current result */
            for (i = 0; i < current_result->num_joined_rowIDs; i++) {
                new_inter_table[i + result_counter] = myMalloc(sizeof(int32_t) * columns);
                for (j = 0; j < columns - 1; j++) {
                    new_inter_table[i + result_counter][j] = (*entity)->inter_tables[inter_table_number1]->inter_table[current_result->joined_rowIDs[i][0] - 1][j];
                }
                new_inter_table[i + result_counter][j] = current_result->joined_rowIDs[i][1];
            }
            /* Update the counter */
            result_counter = result_counter + current_result->num_joined_rowIDs;
            /* Go to the next result */
            current_result = current_result->next_result;
        } while (current_result != NULL);

        /* Free all the structures of the old intermediate table */
        for (i = 0; i < (*entity)->inter_tables[inter_table_number1]->num_of_rows; i++)
            free((*entity)->inter_tables[inter_table_number1]->inter_table[i]);

        free((*entity)->inter_tables[inter_table_number1]->inter_table);

        /* Point to the new intermediate table */
        (*entity)->inter_tables[inter_table_number1]->inter_table = new_inter_table;
        (*entity)->inter_tables[inter_table_number1]->num_of_rows = result_counter;
        (*entity)->inter_tables[inter_table_number1]->num_of_columns = (*entity)->inter_tables[inter_table_number1]->num_of_columns + 1;

        deAllocateResult(&result);
    }
    /* If both of the relations exist in the same or a different intermediate table */
    else {
        new_relation1 = create_intermediate_table(relation_Id1, entity, *relation1, &inter_table_number1);
        new_relation2 = create_intermediate_table(relation_Id2, entity, *relation2, &inter_table_number2);

        /* If both of the relations are in the same intermediate table, then self join */
        if (inter_table_number1 == inter_table_number2) {

            exists_in_intermediate_table(relation_Id1, *entity, &inter_table_number1, &column_number1);
            exists_in_intermediate_table(relation_Id2, *entity, &inter_table_number2, &column_number2);

            /* Find how many elements are the same */
            for (i = 0; i < (*entity)->inter_tables[inter_table_number1]->num_of_rows; i++) {
                if ((*relation1)->tuples[(int) (*entity)->inter_tables[inter_table_number1]->inter_table[i][column_number1] - 1].payload
                    == (*relation2)->tuples[(int) (*entity)->inter_tables[inter_table_number2]->inter_table[i][column_number2] - 1].payload)
                    counter++;
            }

            /* Allocate an array to save the row_ids of the elements that are the same */
            row_ids = myMalloc(sizeof(int) * counter);
            counter = 0;
            for (i = 0; i < (*entity)->inter_tables[inter_table_number1]->num_of_rows; i++) {
                if ((*relation1)->tuples[(int) (*entity)->inter_tables[inter_table_number1]->inter_table[i][column_number1] - 1].payload
                    == (*relation2)->tuples[(int) (*entity)->inter_tables[inter_table_number1]->inter_table[i][column_number2] - 1].payload) {
                    row_ids[counter] = i;
                    counter++;
                }
            }

            /* Create a new intermediate table and fill according to the row_ids array allocated above */
            new_inter_table = myMalloc(sizeof(int *) * counter);
            for (i = 0; i < counter; i++) {
                new_inter_table[i] = myMalloc(sizeof(int) * (*entity)->inter_tables[inter_table_number1]->num_of_columns);
                for (j = 0; j < (*entity)->inter_tables[inter_table_number1]->num_of_columns; j++) {
                    new_inter_table[i][j] = (*entity)->inter_tables[inter_table_number1]->inter_table[row_ids[i]][j];
                }
            }

            /* Free all the structures of the old intermediate table */
            for (i = 0; i < (*entity)->inter_tables[inter_table_number1]->num_of_rows; i++) {
                free((*entity)->inter_tables[inter_table_number1]->inter_table[i]);
            }
            free((*entity)->inter_tables[inter_table_number1]->inter_table);

            /* Point to the new intermediate table */
            (*entity)->inter_tables[inter_table_number1]->num_of_rows = counter;
            (*entity)->inter_tables[inter_table_number1]->inter_table = new_inter_table;

            deAllocateRelation(&new_relation1);
            deAllocateRelation(&new_relation2);
            free(row_ids);
        }
        /* Else just join them and combine the 2 intermediate tables */
        else {
            result = RadixHashJoin(&new_relation1, &new_relation2);

            /* Count the number of the results(rows) */
            current_result = result;
            do {
                result_counter = result_counter + current_result->num_joined_rowIDs;
                current_result = current_result->next_result;
            } while (current_result != NULL);


            /* Fill the number of rows, columns and the relation_ids array */
            columns = (*entity)->inter_tables[inter_table_number1]->num_of_columns + (*entity)->inter_tables[inter_table_number2]->num_of_columns;

            (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table = realloc((*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table,
                                                                                               sizeof(int *) * columns);

            for (i = 0; i < (*entity)->inter_tables[inter_table_number2]->num_of_columns; i++) {

                (*entity)->inter_tables[inter_table_number1]->relationIDS_of_inter_table[(*entity)->inter_tables[inter_table_number1]->num_of_columns + i]
                        = (*entity)->inter_tables[inter_table_number2]->relationIDS_of_inter_table[i];
            }


            /* Create a new intermediate table with size (num_of_rows * 2) */
            new_inter_table = myMalloc(sizeof(int32_t *) * result_counter);

            /*  Reset the counter and the current_result pointer */
            result_counter = 0;
            current_result = result;
            do {
                /* Then do a for from counter until counter+ num_joined_rowIDs of the current result */
                for (i = 0; i < current_result->num_joined_rowIDs; i++) {
                    new_inter_table[i + result_counter] = myMalloc(sizeof(int32_t) * columns);
                    for (j = 0; j < columns; j++) {
                        if (j < (*entity)->inter_tables[inter_table_number1]->num_of_columns)
                            new_inter_table[i + result_counter][j] = (*entity)->inter_tables[inter_table_number1]->inter_table[current_result->joined_rowIDs[i][0] - 1][j];
                        else
                            new_inter_table[i + result_counter][j] = (*entity)->inter_tables[inter_table_number2]->inter_table[current_result->joined_rowIDs[i][1] - 1][j - (*entity)->inter_tables[inter_table_number1]->num_of_columns];
                    }
                }
                /* Update the counter */
                result_counter = result_counter + current_result->num_joined_rowIDs;
                /* Go to the next result */
                current_result = current_result->next_result;
            } while (current_result != NULL);

            /* Free all the structures of the old intermediate tables */
            for (i = 0; i < (*entity)->inter_tables[inter_table_number1]->num_of_rows; i++)
                free((*entity)->inter_tables[inter_table_number1]->inter_table[i]);

            for (i = 0; i < (*entity)->inter_tables[inter_table_number2]->num_of_rows; i++)
                free((*entity)->inter_tables[inter_table_number2]->inter_table[i]);


            free((*entity)->inter_tables[inter_table_number2]->relationIDS_of_inter_table);

            free((*entity)->inter_tables[inter_table_number1]->inter_table);
            free((*entity)->inter_tables[inter_table_number2]->inter_table);


            /* Point to the new intermediate table */
            (*entity)->inter_tables[inter_table_number1]->inter_table = new_inter_table;
            free((*entity)->inter_tables[inter_table_number2]);
            (*entity)->inter_tables[inter_table_number2] = NULL;
            (*entity)->inter_tables[inter_table_number1]->num_of_rows = result_counter;
            (*entity)->inter_tables[inter_table_number1]->num_of_columns = columns;

            deAllocateResult(&result);
        }
    }
}

long* calculateSums(Entity *entity, Query_Info *query_info, Table **tables, FILE *fp) {

    //printf("\n");

    int64_t sum = 0, rid;
    int i, j, inter_table_number, inter_column_number, ret, table, column;


    long* sums = myMalloc(sizeof(long) * query_info->selection_count);


    for (i = 0; i < query_info->selection_count; i++) {
        sum = 0;
        ret = exists_in_intermediate_table(query_info->selections[i][0], entity, &inter_table_number, &inter_column_number);

        for (j = 0; j < entity->inter_tables[inter_table_number]->num_of_rows; j++) {
            rid = (int32_t) entity->inter_tables[inter_table_number]->inter_table[j][inter_column_number] - 1;
            table = query_info->relation_IDs[query_info->selections[i][0]];
            column = query_info->selections[i][1];
            //printf("Sum on t: %d and c: %d\n", table, column);
            sum = sum + tables[table]->column_indexes[column][rid];
        }
        if (i == query_info->selection_count - 1)
            if (sum == 0){
                sums[i] = 0;
            }
            else{
                sums[i] = (long) sum;
            }
        else {

            if (sum == 0){
                sums[i] = 0;
            }
            else{
                sums[i] = (long) sum;
            }
        }
    }

    return sums;
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
    }
    /* Free the old intermediate table */
    for (i = 0; i < (*entity)->inter_tables[inter_table_number]->num_of_rows; i++)
        free((*entity)->inter_tables[inter_table_number]->inter_table[i]);

    free((*entity)->inter_tables[inter_table_number]->inter_table);

    /* Point to the new intermediate table */
    (*entity)->inter_tables[inter_table_number]->num_of_rows = rowId_count;
    (*entity)->inter_tables[inter_table_number]->inter_table = new_inter_table;
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
