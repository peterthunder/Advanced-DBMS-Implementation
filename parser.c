#include "parser.h"


Query_Info *parse_query(char *query) {

    char *token1, *token2, *token3, *saveptr1, *saveptr2, *saveptr3;
    int query_parts_count = 0, i, isfilter;
    int current_join = 0, current_filter = 0, current_selection = 0, join_part = 0;
    char **query_parts, query_part[1024];

    Query_Info *q = malloc(sizeof(Query_Info));
    if (q == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
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

    /* Parse Query */
    /* Find Num of Parts */
    strcpy(query_part, query);
    token1 = strtok_r(query_part, "|", &saveptr1);
    while (token1 != NULL) {
        token1 = strtok_r(NULL, "|", &saveptr1);
        query_parts_count++;
    }

    assert(query_parts_count == 3);

    query_parts = malloc(sizeof(char *) * query_parts_count);
    if (query_parts == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    for (i = 0; i < query_parts_count; i++) {
        query_parts[i] = malloc(sizeof(char) * 1024);
        if (query_parts[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
    }
    query_parts_count = 0;
    /* Parse Query */
    /* Get Query Parts */
    strcpy(query_part, query);
    token1 = strtok_r(query_part, "|", &saveptr1);
    while (token1 != NULL) {
        strcpy(query_parts[query_parts_count], token1);
        token1 = strtok_r(NULL, "|", &saveptr1);
        query_parts_count++;
    }


    /* Parse the Relation IDs*/
    /* Find the num of Relation IDs*/
    strcpy(query_part, query_parts[0]);
    token1 = strtok_r(query_part, " ", &saveptr1);
    while (token1 != NULL) {
        q->relationId_count++;
        token1 = strtok_r(NULL, " ", &saveptr1);
    }

    q->relation_IDs = malloc(sizeof(int) * q->relationId_count);
    if (q->relation_IDs == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    q->relationId_count = 0;

    /* Save the relation IDs in an array */
    strcpy(query_part, query_parts[0]);
    token1 = strtok_r(query_parts[0], " ", &saveptr1);
    while (token1 != NULL) {
        q->relation_IDs[q->relationId_count] = myAtoi(token1);
        q->relationId_count++;
        token1 = strtok_r(NULL, " ", &saveptr1);
    }

    /* Parse the Predicates */
    strcpy(query_part, query_parts[1]);
    token1 = strtok_r(query_part, "&", &saveptr1);
    while (token1 != NULL) {
        if (isFilter(token1))
            q->filter_count++;
        else
            q->join_count++;

        token1 = strtok_r(NULL, "&", &saveptr1);
    }

    if (q->join_count > 0) {
        q->joins = malloc(sizeof(int *) * q->join_count);
        if (q->joins == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
        for (i = 0; i < q->join_count; i++) {
            q->joins[i] = malloc(sizeof(int) * 4);
            if (q->joins[i] == NULL) {
                printf("Malloc failed!\n");
                perror("Malloc");
                return NULL;
            }
        }
    }

    if (q->filter_count > 0) {
        q->filters = malloc(sizeof(int *) * q->filter_count);
        if (q->filters == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
        for (i = 0; i < q->filter_count; i++) {
            q->filters[i] = malloc(sizeof(int) * 4);
            if (q->filters[i] == NULL) {
                printf("Malloc failed!\n");
                perror("Malloc");
                return NULL;
            }
        }

    }

    strcpy(query_part, query_parts[1]);
    token1 = strtok_r(query_part, "&", &saveptr1);
    while (token1 != NULL) {
        if ((strstr(token1, ">")) != NULL) {
            token2 = strtok_r(token1, ">", &saveptr2);
            while (token2 != NULL) {
                if (strstr(token2, ".") != NULL) {
                    token3 = strtok_r(token2, ".", &saveptr3);
                    q->filters[current_filter][0] = myAtoi(token3);
                    token3 = strtok_r(NULL, ".", &saveptr3);
                    q->filters[current_filter][1] = myAtoi(token3);

                } else {
                    q->filters[current_filter][2] = GREATER;
                    q->filters[current_filter][3] = myAtoi(token2);
                    current_filter++;

                }
                token2 = strtok_r(NULL, ">", &saveptr2);
            }
        } else if ((strstr(token1, "<")) != NULL) {
            token2 = strtok_r(token1, "<", &saveptr2);
            while (token2 != NULL) {
                if (strstr(token2, ".") != NULL) {
                    token3 = strtok_r(token2, ".", &saveptr3);
                    q->filters[current_filter][0] = myAtoi(token3);
                    token3 = strtok_r(NULL, ".", &saveptr3);
                    q->filters[current_filter][1] = myAtoi(token3);

                } else {
                    q->filters[current_filter][2] = LESS;
                    q->filters[current_filter][3] = myAtoi(token2);
                    current_filter++;

                }
                token2 = strtok_r(NULL, "<", &saveptr2);
            }
        } else if ((strstr(token1, "=")) != NULL) {
            isfilter = isFilter(token1);
            token2 = strtok_r(token1, "=", &saveptr2);
            while (token2 != NULL) {
                if (strstr(token2, ".") != NULL) {
                    token3 = strtok_r(token2, ".", &saveptr3);
                    if (isfilter)
                        q->filters[current_filter][0] = myAtoi(token3);
                    else
                        q->joins[current_join][join_part] = myAtoi(token3);
                    token3 = strtok_r(NULL, ".", &saveptr3);
                    if (isfilter)
                        q->filters[current_filter][1] = myAtoi(token3);
                    else
                        q->joins[current_join][join_part + 1] = myAtoi(token3);
                    token2 = strtok_r(NULL, "=", &saveptr2);
                    if (token2 == NULL)current_join++;
                } else {
                    q->filters[current_filter][2] = EQUAL;
                    q->filters[current_filter][3] = myAtoi(token2);
                    current_filter++;
                    token2 = strtok_r(NULL, "=", &saveptr2);
                }
                join_part = 2;
            }
        } else {
            perror("Unknown character!\n");
            return NULL;
        }
        join_part = 0;
        token1 = strtok_r(NULL, "&", &saveptr1);
    }

    /* Parse the selections */
    strcpy(query_part, query_parts[2]);
    token1 = strtok_r(query_part, " ", &saveptr1);
    while (token1 != NULL) {
        q->selection_count++;
        token1 = strtok_r(NULL, " ", &saveptr1);
    }

    q->selections = malloc(sizeof(int *) * q->selection_count);
    for (i = 0; i < q->selection_count; i++) {
        q->selections[i] = malloc(sizeof(int) * 2);
    }

    /*  SELECT SUM("1".c2), SUM("0".c1)
        FROM r3 "0", r0 "1", r1 "2"
        WHERE 1.c1=5000 and 0.c2=1.c0 and 0.c1=2.c0 and 0.c2>3499
     */

    strcpy(query_part, query_parts[2]);
    token1 = strtok_r(query_part, " ", &saveptr1);
    while (token1 != NULL) {
        token2 = strtok_r(token1, ".", &saveptr2);
        q->selections[current_selection][0] = myAtoi(token2);
        token2 = strtok_r(NULL, ".", &saveptr2);
        q->selections[current_selection][1] = myAtoi(token2);
        token1 = strtok_r(NULL, " ", &saveptr1);
        current_selection++;
    }


    // De-allocate temporary memory.
    for (i = 0; i < query_parts_count; i++) {
        free(query_parts[i]);
    }
    free(query_parts);

    return q;
}

int isFilter(char *predicate) {

    int dots_count = 0;

    char *pred, predi[1024];
    strcpy(predi, predicate);

    while ((pred = strstr(predi, ".")) != NULL) {
        dots_count++;
        memcpy(pred, pred + 1, strlen(pred+1)+1);
    }

    if (dots_count >= 2)
        return FALSE;
    else
        return TRUE;

}
