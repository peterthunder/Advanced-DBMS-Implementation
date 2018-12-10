#include "parser.h"


Query_Info *parse_query(char *query) {

    int query_parts_count = 0, i;
    char **query_parts;

    Query_Info *query_info = createQueryInfo();    // Allocation-errors are handled internally.

    /* Parse the Query Parts*/
    query_parts = parseQueryParts(query, &query_parts_count);     // Allocation-errors are handled internally.

    /* Parse the Relation IDs*/
    parseRelationIDs(query_parts[0], &query_info);    // Allocation-errors are handled internally.

    /* Parse the Predicates */
    if ((parsePredicates(query_parts[1], &query_info)) == -1) {
        printf("An error occurred while parsing the predicates!\n");
        return NULL;
    }

    /* Parse the Selections */
    parseSelections(query_parts[2], &query_info);    // Allocation-errors are handled internally.

    // De-allocate temporary memory.
    for (i = 0; i < query_parts_count; i++) {
        free(query_parts[i]);
    }
    free(query_parts);

    return query_info;
}

/* Allocate and Initialize Query Info */
Query_Info * createQueryInfo(void){

    Query_Info *q = myMalloc(sizeof(Query_Info));

    // Initialize struct.
    q->relation_IDs = NULL;
    q->relationId_count = 0;
    q->filters = NULL;
    q->filter_count = 0;
    q->joins = NULL;
    q->join_count = 0;
    q->selections = NULL;
    q->selection_count = 0;

    return q;
}

/* Parse Query */
char **parseQueryParts(char *query, int *query_parts_count) {

    int i;
    char query_part[1024], *token, *saveptr, **query_parts;

    /* Find Num of Parts */
    strcpy(query_part, query);
    token = strtok_r(query_part, "|", &saveptr);
    while (token != NULL) {
        token = strtok_r(NULL, "|", &saveptr);
        (*query_parts_count)++;
    }

    assert((*query_parts_count) == 3);
    /* Get Query Parts */
    query_parts = myMalloc(sizeof(char *) * (3));

    for (i = 0; i < (*query_parts_count); i++) {
        query_parts[i] = myMalloc(sizeof(char) * 1024);
    }
    (*query_parts_count) = 0;
    /* Parse Query */
    strcpy(query_part, query);
    token = strtok_r(query_part, "|", &saveptr);
    while (token != NULL) {
        strcpy(query_parts[(*query_parts_count)], token);
        token = strtok_r(NULL, "|", &saveptr);
        (*query_parts_count)++;
    }

    return query_parts;
}

/* Parse the Relation IDs */
void parseRelationIDs(char *query_part, Query_Info **q) {

    char *token, *saveptr, dummy_string[1024];

    /* Find the num of Relation IDs*/
    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->relationId_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }

    (*q)->relation_IDs = myMalloc(sizeof(int) * (*q)->relationId_count);
    (*q)->relationId_count = 0;

    /* Save the relation IDs in an array */
    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->relation_IDs[(*q)->relationId_count] = myAtoi(token);
        (*q)->relationId_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }
}

/* Parse the Predicates */
int parsePredicates(char *query_part, Query_Info **q) {

    char dummy_string[1024], *token, *saveptr;
    int i, current_filter = 0, current_join = 0;

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, "&", &saveptr);
    while (token != NULL) {
        if (isFilter(token))
            (*q)->filter_count++;
        else
            (*q)->join_count++;

        token = strtok_r(NULL, "&", &saveptr);
    }

    if ((*q)->join_count > 0) {
        (*q)->joins = myMalloc(sizeof(int *) * (*q)->join_count);
        for (i = 0; i < (*q)->join_count; i++)
            (*q)->joins[i] = myMalloc(sizeof(int) * 4);
    }

    if ((*q)->filter_count > 0) {
        (*q)->filters = myMalloc(sizeof(int *) * (*q)->filter_count);
        for (i = 0; i < (*q)->filter_count; i++)
            (*q)->filters[i] = myMalloc(sizeof(int) * 4);
    }

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, "&", &saveptr);
    while (token != NULL) {

        if ((strstr(token, ">")) != NULL) {
            if (parseFilter(token, GREATER, q, &current_filter) == -1)
                return -1;
        }
        else if ((strstr(token, "<")) != NULL) {
            if (parseFilter(token, LESS, q, &current_filter) == -1)
                return -1;
        }
        else if ((strstr(token, "=")) != NULL) {
            if (isFilter(token)) {
                if (parseFilter(token, EQUAL, q, &current_filter) == -1)
                    return -1;
            }
            else
                parseJoin(token, q, &current_join);
        } else {
            perror("Unknown character!\n");
            return -1;
        }
        token = strtok_r(NULL, "&", &saveptr);
    }

    return 0;
}

/* Parse the selections */
void parseSelections(char *query_part, Query_Info **q){

    int i, current_selection = 0;
    char dummy_string[1024], *token, *saveptr, *token1, *saveptr1;

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->selection_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }

    (*q)->selections = myMalloc(sizeof(int *) * (*q)->selection_count);
    for (i = 0; i < (*q)->selection_count; i++)
        (*q)->selections[i] = myMalloc(sizeof(int) * 2);

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        token1 = strtok_r(token, ".", &saveptr1);
        (*q)->selections[current_selection][0] = myAtoi(token1);
        token1 = strtok_r(NULL, ".", &saveptr1);
        (*q)->selections[current_selection][1] = myAtoi(token1);
        token = strtok_r(NULL, " ", &saveptr);
        current_selection++;
    }
}

/* Parse a Filter */
int parseFilter(char *token, int operator, Query_Info **q, int *current_filter) {

    char *saveptr2, *saveptr3, *token1;

    if (operator == EQUAL)
        token1 = strtok_r(token, "=", &saveptr2);
    else if (operator == LESS)
        token1 = strtok_r(token, "<", &saveptr2);
    else if (operator == GREATER)
        token1 = strtok_r(token, ">", &saveptr2);
    else {
        fprintf(stderr, "\nInvalid operator found in parseFilter!\n\n");
        return -1;
    }

    while (token1 != NULL) {
        if (strstr(token, ".") != NULL) {
            token1 = strtok_r(token, ".", &saveptr3);
            (*q)->filters[*current_filter][0] = myAtoi(token1);
            token1 = strtok_r(NULL, ".", &saveptr3);
            (*q)->filters[*current_filter][1] = myAtoi(token1);
        } else {
            (*q)->filters[*current_filter][2] = operator;
            (*q)->filters[*current_filter][3] = myAtoi(token1);
            (*current_filter)++;
        }

        if (operator == EQUAL)
            token1 = strtok_r(NULL, "=", &saveptr2);
        else if (operator == LESS)
            token1 = strtok_r(NULL, "<", &saveptr2);
        else
            token1 = strtok_r(NULL, ">", &saveptr2);

    }
    return 0;
}

/* Parse a Join */
void parseJoin(char *token, Query_Info **q, int *current_join) {

    int join_part = 0;
    char *saveptr1, *saveptr2, *token1, *token2;

    token1 = strtok_r(token, "=", &saveptr1);
    while (token1 != NULL) {
        if (strstr(token1, ".") != NULL) {
            token2 = strtok_r(token1, ".", &saveptr2);
            (*q)->joins[*current_join][join_part] = myAtoi(token2);
            token2 = strtok_r(NULL, ".", &saveptr2);
            (*q)->joins[*current_join][join_part + 1] = myAtoi(token2);
            token1 = strtok_r(NULL, "=", &saveptr1);
            if (token1 == NULL) (*current_join)++;
        }
        join_part = 2;
    }
}

bool isFilter(char *predicate) {

    int dots_count = 0;

    char *pred, predi[1024];
    strcpy(predi, predicate);

    while ((pred = strstr(predi, ".")) != NULL) {
        dots_count++;
        memcpy(pred, pred + 1, strlen(pred + 1) + 1);
    }

    if (dots_count >= 2)
        return FALSE;
    else
        return TRUE;
}
