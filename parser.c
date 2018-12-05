#include "parser.h"


Query_Info *parse_query(char *query) {

    int query_parts_count = 0, i;
    char **query_parts;

    Query_Info *query_info = createQueryInfo();
    if (query_info == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return NULL;
    }

    /* Parse the Query Parts*/
    if ((query_parts = parseQueryParts(query, &query_parts_count)) == NULL) {
        printf("An error occurred while parsing the query parts!\n");
        return NULL;
    }

    /* Parse the Relation IDs*/
    if ((parseRelationIDs(query_parts[0], &query_info)) == -1) {
        printf("An error occurred while parsing the relation ids!\n");
        return NULL;
    }

    /* Parse the Predicates */
    if ((parsePredicates(query_parts[1], &query_info)) == -1) {
        printf("An error occurred while parsing the predicates!\n");
        return NULL;
    }

    /* Parse the selections */
    if ((parseSelections(query_parts[2], &query_info)) == -1) {
        printf("An error occurred while parsing the predicates!\n");
        return NULL;
    }

    // De-allocate temporary memory.
    for (i = 0; i < query_parts_count; i++) {
        free(query_parts[i]);
    }
    free(query_parts);

    return query_info;
}

/* Allocate and Initialize Query Info */
Query_Info * createQueryInfo(void){

    Query_Info *q = malloc(sizeof(Query_Info));
    if (q == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return NULL;
    }

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
    query_parts = malloc(sizeof(char *) * (3));
    if (query_parts == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return NULL;
    }
    for (i = 0; i < (*query_parts_count); i++) {
        query_parts[i] = malloc(sizeof(char) * 1024);
        if (query_parts[i] == NULL) {
            fprintf(stderr, "Malloc failed!\n");
            return NULL;
        }
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
int parseRelationIDs(char *query_part, Query_Info **q) {

    char *token, *saveptr, dummy_string[1024];

    /* Find the num of Relation IDs*/
    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->relationId_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }

    (*q)->relation_IDs = malloc(sizeof(int) * (*q)->relationId_count);
    if ((*q)->relation_IDs == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return -1;
    }
    (*q)->relationId_count = 0;

    /* Save the relation IDs in an array */
    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->relation_IDs[(*q)->relationId_count] = myAtoi(token);
        (*q)->relationId_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }

    return 0;
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
        (*q)->joins = malloc(sizeof(int *) * (*q)->join_count);
        if ((*q)->joins == NULL) {
            fprintf(stderr, "Malloc failed!\n");
            return -1;
        }
        for (i = 0; i < (*q)->join_count; i++) {
            (*q)->joins[i] = malloc(sizeof(int) * 4);
            if ((*q)->joins[i] == NULL) {
                fprintf(stderr, "Malloc failed!\n");
                return -1;
            }
        }
    }

    if ((*q)->filter_count > 0) {
        (*q)->filters = malloc(sizeof(int *) * (*q)->filter_count);
        if ((*q)->filters == NULL) {
            fprintf(stderr, "Malloc failed!\n");
            return -1;
        }
        for (i = 0; i < (*q)->filter_count; i++) {
            (*q)->filters[i] = malloc(sizeof(int) * 4);
            if ((*q)->filters[i] == NULL) {
                fprintf(stderr, "Malloc failed!\n");
                return -1;
            }
        }

    }

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, "&", &saveptr);
    while (token != NULL) {

        if ((strstr(token, ">")) != NULL)
            parseFilter(token, GREATER, q, &current_filter);

        else if ((strstr(token, "<")) != NULL)
            parseFilter(token, LESS, q, &current_filter);

        else if ((strstr(token, "=")) != NULL) {

            if (isFilter(token))
                parseFilter(token, EQUAL, q, &current_filter);
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
int parseSelections(char *query_part, Query_Info **q){

    int i,  current_selection = 0;
    char dummy_string[1024], *token, *saveptr, *token1, *saveptr1;

    strcpy(dummy_string, query_part);
    token = strtok_r(dummy_string, " ", &saveptr);
    while (token != NULL) {
        (*q)->selection_count++;
        token = strtok_r(NULL, " ", &saveptr);
    }

    (*q)->selections = malloc(sizeof(int *) * (*q)->selection_count);
    if ((*q)->selections == NULL) {
        fprintf(stderr, "Malloc failed!\n");
        return -1;
    }
    for (i = 0; i < (*q)->selection_count; i++) {
        (*q)->selections[i] = malloc(sizeof(int) * 2);
        if ((*q)->selections[i] == NULL) {
            fprintf(stderr, "Malloc failed!\n");
            return -1;
        }
    }

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

    return 0;
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
int parseJoin(char *token, Query_Info **q, int *current_join) {

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

    return 0;
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
