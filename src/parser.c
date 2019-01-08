#include "parser.h"


Query_Info *parse_query(char *query) {

    int query_parts_count = 0, i;
    char **query_parts;

    Query_Info *query_info = createQueryInfo();

    /* Parse the Query Parts*/
    query_parts = parseQueryParts(query, &query_parts_count);     // Allocation-errors are handled internally.
    if (query_parts == NULL) {
        fprintf(stderr, "\nAn error occurred while parsing the query-parts!\n");
        return NULL;
    }

    /* Parse the Relation IDs*/
    parseRelationIDs(query_parts[0], &query_info);

    /* Parse the Predicates */
    if ((parsePredicates(query_parts[1], &query_info)) == -1) {
        fprintf(stderr, "\nAn error occurred while parsing the predicates!\n");
        return NULL;
    }

    /* Parse the Selections */
    parseSelections(query_parts[2], &query_info);

    // De-allocate temporary memory.
    for (i = 0; i < query_parts_count; i++) {
        free(query_parts[i]);
    }
    free(query_parts);

    return query_info;
}

/* Allocate and Initialize Query Info */
Query_Info *createQueryInfo(void) {

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

    if (*query_parts_count != 3) {
        fprintf(stderr, "\nWrong number of query parts! Received %d, while expecting 3.\n", *query_parts_count);
        return NULL;
    }

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
        } else if ((strstr(token, "<")) != NULL) {
            if (parseFilter(token, LESS, q, &current_filter) == -1)
                return -1;
        } else if ((strstr(token, "=")) != NULL) {
            if (isFilter(token)) {
                if (parseFilter(token, EQUAL, q, &current_filter) == -1)
                    return -1;
            } else
                parseJoin(token, q, &current_join);
        } else {
            fprintf(stderr, "\nUnknown character!\n");
            return -1;
        }
        token = strtok_r(NULL, "&", &saveptr);
    }

    return 0;
}

/* Parse the selections */
void parseSelections(char *query_part, Query_Info **q) {

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
        else    // It will be GREATER, no doubt
            token1 = strtok_r(NULL, ">", &saveptr2);
    }
    return 0;
}

/* Parse a Join */
void parseJoin(char *token, Query_Info **q, int *join_counter) {

    int join_part = 0;
    char *saveptr1, *saveptr2, *token1, *token2;

    token1 = strtok_r(token, "=", &saveptr1);
    while (token1 != NULL) {
        if (strstr(token1, ".") != NULL) {
            token2 = strtok_r(token1, ".", &saveptr2);
            (*q)->joins[*join_counter][join_part] = myAtoi(token2);
            token2 = strtok_r(NULL, ".", &saveptr2);
            (*q)->joins[*join_counter][join_part + 1] = myAtoi(token2);
            token1 = strtok_r(NULL, "=", &saveptr1);
            if (token1 == NULL)
                (*join_counter)++;
        }
        join_part = 2;
    }

    // If the same join exists two-times then delete the second one (Be aware that one of them may be reversed).
    // Test: Query_20: 9 1 11|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0
    // Query_35: 7 0 9|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2\
    // Query_38: 7 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1

    if ( isCurrentJoinDuplicate(q, *join_counter) )
    {
        // Update the "join_counter"
        (*join_counter) --;
        (*q)->join_count --;

        // Free this join and recreate it
        free((*q)->joins[(*join_counter)]);
        (*q)->joins[(*join_counter)] = myMalloc(sizeof(int) * 4);

        // Reduce allocated space
        free((*q)->joins[(*q)->join_count]);
        (*q)->joins = myRealloc((*q)->joins, (sizeof(int *) * (*q)->join_count));

        //printJoins(*q, (*join_counter));
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

    if (dots_count < 2)
        return TRUE;
        // If we want to handle the "filter": 0.1=0.2
        // In this case we have to use 5 cells in our "filters"-table to store the filter-members
        /*else if ( dots_count == 2 &&   tableNum is the same in both sides  )
          return TRUE;*/
    else
        return FALSE;
}


bool isCurrentJoinDuplicate(Query_Info **q, int joinCount) {
    //printJoins(*q, joinCount);

    int n = joinCount - 1;

    for ( int i = 0; i < n; i++ ) {
        if ( (*q)->joins[i][0] == (*q)->joins[n][0]
            && (*q)->joins[i][1] == (*q)->joins[n][1]
            && (*q)->joins[i][2] == (*q)->joins[n][2]
            && (*q)->joins[i][3] == (*q)->joins[n][3] ) {
#if PRINTING
            printSame(q, i, n);
#endif
            return TRUE;
        }   // Check reversed.
        else if ( (*q)->joins[i][0] == (*q)->joins[n][2]
                 && (*q)->joins[i][1] == (*q)->joins[n][3]
                 && (*q)->joins[i][2] == (*q)->joins[n][0]
                 && (*q)->joins[i][3] == (*q)->joins[n][1] ) {
#if PRINTING
            printSame(q, i, n);
#endif
            return TRUE;
        }
    }
    return FALSE;
}


void printJoins(Query_Info *q, int joinCount)
{
    fprintf(fp_print, "Joins = %d\n", joinCount);
    for ( int i = 0; i < joinCount; i++ ) {
        for ( int j = 0; j < 4; j++ ) {
            fprintf(fp_print, "[%d]", q->joins[i][j]);
        }
        fprintf(fp_print, "\n");
    }
}


void printSame(Query_Info **q, int i, int j)
{
    fprintf(fp_print, "Found same Join! -> ");
    for ( int k = 0; k < 4; k++ ) {
        fprintf(fp_print, "[%d]", (*q)->joins[i][k]);
    }
    fprintf(fp_print, " == ");
    for ( int k = 0; k < 4; k++ ) {
        fprintf(fp_print, "[%d]", (*q)->joins[j][k]);
    }
    fprintf(fp_print, " -> Removing it from query..\n");
}
