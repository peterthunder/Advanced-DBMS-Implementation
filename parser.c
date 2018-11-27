#include "parser.h"

void *read_workload() {

    FILE *fptr;
    size_t size;
    char *token1, *token2, *token3, *saveptr1, *saveptr2, *saveptr3;
    int **filters = NULL, **joins = NULL, *relation_IDs = NULL, **selections = NULL, isfilter;
    int query_parts_count = 0, relationId_count = 0, join_count = 0, filter_count = 0, selection_count = 0, i, dot_count = 0, query_count = 0;
    int current_join = 0, current_filter = 0, current_selection = 0, join_part = 0;
    char query_parts[3][1024], *query = NULL, workload_filename[1024] = "workloads/small/small.work", query_part[1024], *join = NULL, dummy[1024];

    /* Open the file on that path */
    fptr = fopen(workload_filename, "r");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_filename, strerror(errno));
        return NULL;
    }

    /* Get queries */
    while (!feof(fptr)) {

        getline(&query, &size, fptr);
        query[strlen(query) - 1] = '\0';
        if (strcmp(query, "F") == 0) continue;

        if (strcmp(query, "\n") == 0 || strlen(query) == 0)
            continue;
        query_count++;
        /* Parse Query */
        /* Get Query Parts */
        printf("\n----------------------------------------------------------------\n");
        printf("Query[%d]: %s\n", query_count - 1, query);
        token1 = strtok_r(query, "|", &saveptr1);
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
            relationId_count++;
            token1 = strtok_r(NULL, " ", &saveptr1);
        }

        relation_IDs = malloc(sizeof(int) * relationId_count);
        if (relation_IDs == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
        relationId_count = 0;

        /* Save the relation IDs in an array */
        strcpy(query_part, query_parts[0]);
        token1 = strtok_r(query_parts[0], " ", &saveptr1);
        while (token1 != NULL) {
            relation_IDs[relationId_count] = myAtoi(token1);
            relationId_count++;
            token1 = strtok_r(NULL, " ", &saveptr1);
        }

        /* Parse the Predicates */
        strcpy(query_part, query_parts[1]);
        token1 = strtok_r(query_part, "&", &saveptr1);
        while (token1 != NULL) {
            join = token1;
            while ((join = strstr(join, ".")) != NULL) {
                dot_count++;
                strcpy(join, join + 1);
            }

            if (dot_count == 2)
                join_count++;
            else
                filter_count++;

            dot_count = 0;
            token1 = strtok_r(NULL, "&", &saveptr1);
        }

        if (join_count > 0) {
            joins = malloc(sizeof(int *) * join_count);
            if (joins == NULL) {
                printf("Malloc failed!\n");
                perror("Malloc");
                return NULL;
            }
            for (i = 0; i < join_count; i++) {
                joins[i] = malloc(sizeof(int) * 4);
                if (joins[i] == NULL) {
                    printf("Malloc failed!\n");
                    perror("Malloc");
                    return NULL;
                }
            }
        }

        if (filter_count > 0) {
            filters = malloc(sizeof(int *) * filter_count);
            if (filters == NULL) {
                printf("Malloc failed!\n");
                perror("Malloc");
                return NULL;
            }
            for (i = 0; i < join_count; i++) {
                filters[i] = malloc(sizeof(int) * 4);
                if (filters[i] == NULL) {
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
                        filters[current_filter][0] = myAtoi(token3);
                        token3 = strtok_r(NULL, ".", &saveptr3);
                        filters[current_filter][1] = myAtoi(token3);
                        token2 = strtok_r(NULL, "=", &saveptr2);
                    } else {
                        filters[current_filter][2] = GREATER;
                        filters[current_filter][3] = myAtoi(token2);
                        current_filter++;
                        token2 = strtok_r(NULL, ">", &saveptr2);
                    }
                }
            } else if ((strstr(token1, "<")) != NULL) {
                token2 = strtok_r(token1, "<", &saveptr2);
                while (token2 != NULL) {
                    if (strstr(token2, ".") != NULL) {
                        token3 = strtok_r(token2, ".", &saveptr3);
                        filters[current_filter][0] = myAtoi(token3);
                        token3 = strtok_r(NULL, ".", &saveptr3);
                        filters[current_filter][1] = myAtoi(token3);
                        token2 = strtok_r(NULL, "=", &saveptr2);
                    } else {
                        filters[current_filter][2] = LESS;
                        filters[current_filter][3] = myAtoi(token2);
                        current_filter++;
                        token2 = strtok_r(NULL, "<", &saveptr2);
                    }
                }
            } else if ((strstr(token1, "=")) != NULL) {
                strcpy(dummy, token1);
                isfilter = isFilter(dummy);
                token2 = strtok_r(token1, "=", &saveptr2);
                while (token2 != NULL) {
                    if (strstr(token2, ".") != NULL) {
                        token3 = strtok_r(token2, ".", &saveptr3);
                        if (isfilter)
                            filters[current_filter][0] = myAtoi(token3);
                        else
                            joins[current_join][join_part] = myAtoi(token3);
                        token3 = strtok_r(NULL, ".", &saveptr3);
                        if (isfilter)
                            filters[current_filter][1] = myAtoi(token3);
                        else
                            joins[current_join][join_part + 1] = myAtoi(token3);
                        token2 = strtok_r(NULL, "=", &saveptr2);
                        if (token2 == NULL)current_join++;
                    } else {
                        filters[current_filter][2] = EQUAL;
                        filters[current_filter][3] = myAtoi(token2);
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
            selection_count++;
            token1 = strtok_r(NULL, " ", &saveptr1);
        }

        selections = malloc(sizeof(int *) * selection_count);
        for (i = 0; i < selection_count; i++) {
            selections[i] = malloc(sizeof(int) * 2);
        }

        /*  SELECT SUM("1".c2), SUM("0".c1)
            FROM r3 "0", r0 "1", r1 "2"
            WHERE 1.c1=5000 and 0.c2=1.c0 and 0.c1=2.c0 and 0.c2>3499
         */

        strcpy(query_part, query_parts[2]);
        token1 = strtok_r(query_part, " ", &saveptr1);
        while (token1 != NULL) {
            token2 = strtok_r(token1, ".", &saveptr2);
            selections[current_selection][0] = myAtoi(token2);
            token2 = strtok_r(NULL, ".", &saveptr2);
            selections[current_selection][1] = myAtoi(token2);
            token1 = strtok_r(NULL, " ", &saveptr1);
            current_selection++;
        }

        printf("\n");

        //break;


        printf("\tSELECT ");
        if (selections != NULL) {
            for (i = 0; i < selection_count; i++) {
                if (selections[i] != NULL)
                    printf("SUM(\"%d\".c%d)", selections[i][0], selections[i][1]);
                if (i != selection_count - 1)
                    printf(", ");
            }
        }

        printf("\n\tFROM ");
        for (i = 0; i < relationId_count; i++) {
            printf("r%d \"%d\"", relation_IDs[i], i);
            if (i != relationId_count - 1)
                printf(", ");
        }

        printf("\n\tWHERE ");

        if (joins != NULL) {
            for (i = 0; i < join_count; i++) {
                if (joins[i] != NULL)
                    printf("%d.c%d=%d.c%d", joins[i][0], joins[i][1], joins[i][2], joins[i][3]);
                if (filter_count != 0)
                    printf(" and ");
            }
        }

        if (filters != NULL) {
            for (i = 0; i < filter_count; i++) {
                if (filters[i] != NULL)
                    printf("%d.c%d(%d)%d", filters[i][0], filters[i][1], filters[i][2], filters[i][3]);
                if (i != filter_count - 1)
                    printf(" and ");
            }
        }

        free(relation_IDs);

        if (joins != NULL) {
            for (i = 0; i < join_count; i++) {
                if (joins[i] != NULL)
                    free(joins[i]);
            }
            free(joins);
        }

        if (filters != NULL) {
            for (i = 0; i < filter_count; i++) {
                if (filters[i] != NULL)
                    free(filters[i]);
            }
            free(filters);
        }

        if (selections != NULL) {
            for (i = 0; i < selection_count; i++) {
                if (selections[i] != NULL)
                    free(selections[i]);
            }
            free(selections);
        }

        filters = NULL, joins = NULL;
        query_parts_count = 0, relationId_count = 0, join_count = 0, filter_count = 0, selection_count = 0, dot_count = 0;
        current_join = 0, current_filter = 0, current_selection = 0, join_part = 0;

        query[0] = '\0';
    }

    printf("\n----------------------------------------------------------------");

    fclose(fptr);
}

int isFilter(char *predicate) {

    int dots_count = 0;

    char *la = predicate;

    while ((la = strstr(la, ".")) != NULL) {
        dots_count++;
        strcpy(la, la + 1);
    }

    if (dots_count == 2)
        return FALSE;
    else
        return TRUE;

}