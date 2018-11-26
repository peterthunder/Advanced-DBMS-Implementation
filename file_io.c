#include "file_io.h"

// A simple atoi() function
int myAtoi(char *str) {
    int res = 0; // Initialize result

    // Iterate through all characters of input string and
    // update result
    for (int i = 0; str[i] != '\0'; ++i)
        res = res * 10 + str[i] - '0';

    // return result.
    return res;
}


Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes) {
    printf("\n# Mmapping tables to memory and initializing structures.\n");

    FILE *fptr1;
    int fd, current_table = 0, i;
    size_t size;
    struct stat st;
    char *table_name = NULL;
    char base_path[1024] = "workloads/small/", init_filename[1024] = "small.init", init_path[1024], table_path[1024];
    /* Init */
    *num_of_tables = 0;

    init_path[0] = '\0';
    /* Create the path of the file that contains the names of the tables */
    strcpy(init_path, base_path);
    strcat(init_path, init_filename);

    printf("  -Path of the file with the mapped_tables names: %s\n", init_path);

    /* Open the file on that path */
    fptr1 = fopen(init_path, "r");
    if (fptr1 == NULL) {
        fprintf(stderr, "Error opening file \"%s\": %s!\n", init_path, strerror(errno));
        return NULL;
    }

    /* Count the number of tables */
    while (!feof(fptr1)) {
        getline(&table_name, &size, fptr1);
        if (strcmp(table_name, "\n") == 0 || strlen(table_name) == 0)
            continue;
        (*num_of_tables)++;
        table_name[0] = '\0';
    }

    printf("  -Number of tables: %d\n", *num_of_tables);

    rewind(fptr1);

    /* Allocate all the memory needed and initialize all the structures */
    Table **tables = malloc(sizeof(Table *) * (*num_of_tables));
    for (i = 0; i < *num_of_tables; i++) {
        tables[i] = malloc(sizeof(Table));
        tables[i]->num_tuples = 0;
        tables[i]->num_columns = 0;
        tables[i]->column_indexes = NULL;
    }

    *mapped_tables = malloc(sizeof(uint64_t *) * (*num_of_tables));
    for (i = 0; i < (*num_of_tables); i++)
        (*mapped_tables)[i] = malloc(sizeof(uint64_t *));

    *mapped_tables_sizes = malloc(sizeof(int) * (*num_of_tables));
    for (i = 0; i < (*num_of_tables); i++)
        (*mapped_tables_sizes)[i] = -1;

    /* Read the names of the tables line by line */
    while (!feof(fptr1)) {
#if PRINTING
        printf("\n");
#endif
        //table_name[0] = '\0';
        /* Get the name of the mapped_tables file */
        getline(&table_name, &size, fptr1);
        /* If you are on the last line, break*/
        if (strcmp(table_name, "\n") == 0 || strlen(table_name) == 0)
            continue;
        table_name[strlen(table_name) - 1] = '\0';
        /* Create the path of the mapped_tables */
        strcpy(table_path, base_path);
        strcat(table_path, table_name);
#if PRINTING
        printf("Path of the %d-th mapped_tables: %s\n", current_table, table_path);
#endif
        /* Open the mapped_tables */
        if ((fd = open(table_path, O_RDWR, 0)) == -1) {
            fprintf(stderr, "Error opening file \"%s\": %s!\n", table_path, strerror(errno));
            return NULL;
        }
        /* Get the size of the mapped_tables */
        if (fstat(fd, &st) == -1) {
            perror("fstat failed");
            return NULL;
        }
        size = (size_t) st.st_size;
        (*mapped_tables_sizes)[current_table] = (int) size;

        /* MAP the whole table to a pointer */
        (*mapped_tables)[current_table] = mmap(0, size, PROT_READ | PROT_EXEC, MAP_SHARED, fd, 0);
        if ((*mapped_tables)[current_table] == MAP_FAILED)
            perror("error reading mapped_tables file");
#if PRINTING
        printf("%d-th mapped_tables: numTuples: %ju and numColumns: %ju\n", current_table,
               (*mapped_tables)[current_table][0], (*mapped_tables)[current_table][1]);
#endif
        /* Initialize each table's variables */
        tables[current_table]->num_tuples = (*mapped_tables)[current_table][0];
        tables[current_table]->num_columns = (*mapped_tables)[current_table][1]; // OR (**mapped_tables + current_table)[1];
        tables[current_table]->column_indexes = malloc(sizeof(uint64_t *) * tables[current_table]->num_columns);
        for (i = 0; i < tables[current_table]->num_columns; i++) {
            tables[current_table]->column_indexes[i] = &(*mapped_tables)[current_table][2 + i *
                                                                                            tables[current_table]->num_tuples];
        }

        close(fd);
        current_table++;
#if PRINTING
        printf("-------------------------------------------------------\n");
#endif
        table_name[0] = '\0';
    }

    printf(" -Finished mmapping tables to memory and initializing structures.\n");

    fclose(fptr1);
    free(table_name);
    return tables;
}

void *read_workload() {

    FILE *fptr;
    size_t size;
    char *saveptr1, *saveptr2, *saveptr3, *token;
    int **filters = NULL, **joins = NULL,  *relation_IDs = NULL;

    int query_count = 0, relationId_count = 0, join_count = 0, filter_count = 0, selection_count = 0, i, dot_count = 0;
    char query_parts[3][1024], *query = NULL, workload_filename[1024] = "workloads/small/small.work", query_part[1024], *join;

    /* Open the file on that path */
    fptr = fopen(workload_filename, "r");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening file \"%s\": %s!\n", workload_filename, strerror(errno));
        return NULL;
    }

    /* Count the number of tables */
    while (!feof(fptr)) {

        getline(&query, &size, fptr);
        query[strlen(query) - 1] = '\0';

        if (strcmp(query, "\n") == 0 || strlen(query) == 0)
            continue;

        /* Parse Query */
        /* Get Query Parts */
        printf("Query: %s\n\n", query);
        token = strtok_r(query, "|", &saveptr1);
        while (token != NULL) {
            strcpy(query_parts[query_count], token);
            token = strtok_r(NULL, "|", &saveptr1);
            query_count++;
        }

        for (i = 0; i < query_count; i++) {
            printf("Query_part[%d]: %s\n", i, query_parts[i]);
        }

        /* Parse the Relation IDs*/
        /* Find the num of Relation IDs*/
        strcpy(query_part, query_parts[0]);
        token = strtok_r(query_part, " ", &saveptr1);
        while (token != NULL) {
            relationId_count++;
            token = strtok_r(NULL, " ", &saveptr1);
        }

        printf("Relation id count: %d \n", relationId_count);
        //printf("%s\n", query_parts[0]);

        relation_IDs = malloc(sizeof(int) * relationId_count);
        if (relation_IDs == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }
        relationId_count = 0;

        /* Save the relation IDs in an array */
        strcpy(query_part, query_parts[0]);
        token = strtok_r(query_parts[0], " ", &saveptr1);
        while (token != NULL) {
            relation_IDs[relationId_count] = myAtoi(token);
            relationId_count++;
            //printf("In here\n");
            token = strtok_r(NULL, " ", &saveptr1);
        }

        for (i = 0; i < relationId_count; i++) {
            printf("Relation_ID[%d]: %d\n", i, relation_IDs[i]);
        }


        /* Parse the Predicates */
        strcpy(query_part, query_parts[1]);
        token = strtok_r(query_part, "&", &saveptr1);
        while (token != NULL) {
            join = token;
            while ((join = strstr(join, ".")) != NULL) {
                dot_count++;
                strcpy(join, join + 1);
                printf("%s\n", join);
            }

            if (dot_count == 2)
                join_count++;
            else
                filter_count++;

            dot_count = 0;
            token = strtok_r(NULL, "&", &saveptr1);
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

        printf("Predicate count: %d \n", join_count);

        /*  SELECT SUM("1".c2), SUM("0".c1)
            FROM r3 "0", r0 "1", r1 "2"
            WHERE 0.c2=1.c0 and 0.c1=c2.0 and 0.c2>3499
         */


        break;


        query[0] = '\0';
    }

    if (relation_IDs != NULL) {
        free(relation_IDs);
    }

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

    fclose(fptr);
}