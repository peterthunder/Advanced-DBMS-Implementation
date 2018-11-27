#include "file_io.h"

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