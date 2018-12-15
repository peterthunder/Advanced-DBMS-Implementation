#include "file_io.h"

Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes) {

    fprintf(fp_print, "\n# Mmapping tables to memory and initializing structures.\n");

    FILE *fptr = NULL;
    int fd, i, j, table_names_array_size = 2;
    size_t size;
    struct stat st;
    char *table_name = NULL;
    char table_path[1024];

    char **table_names = malloc(sizeof(char *) * 2);
    for (i = 0; i < 2; i++) {
        table_names[i] = malloc(sizeof(char) * 1024);
    }

    /* Init */
    *num_of_tables = 0;

    if (USE_HARNESS == FALSE) {
        /* Open the file on that path */
        if ((fptr = fopen("workloads/small/small.init", "r")) == NULL) {
            fprintf(stderr, "Error opening file workloads/small/small.init: %s!\n", strerror(errno));
            return NULL;
        }
    }else{
        fptr = stdin;
    }


    /* Count the number of tables */
    while (1) {
        getline(&table_name, &size, fptr);

        table_name[strlen(table_name) - 1] = '\0';

        //fprintf(fp_msg, "%s\n", table_name);

        if (strcmp(table_name, "Done") == 0 || strcmp(table_name, "\n") == 0 || strlen(table_name) == 0)
            break;

        strcpy(table_names[*num_of_tables], table_name);
        (*num_of_tables)++;

        if (table_names_array_size == *num_of_tables) {
            table_names_array_size = table_names_array_size * 2;
            table_names = realloc(table_names, (size_t) table_names_array_size * sizeof(char *));
            for (i = *num_of_tables; i < table_names_array_size; i++) {
                table_names[i] = malloc(sizeof(char) * 1024);
            }
        }
        table_name[0] = '\0';
    }

    if (USE_HARNESS == FALSE)
        fclose(fptr);

    /* Allocate all the memory needed and initialize all the structures */
    Table **tables = myMalloc(sizeof(Table *) * (*num_of_tables));

    for (i = 0; i < *num_of_tables; i++) {
        tables[i] = myMalloc(sizeof(Table));
        tables[i]->num_tuples = 0;
        tables[i]->num_columns = 0;
        tables[i]->column_indexes = NULL;
    }

    *mapped_tables = myMalloc(sizeof(uint64_t *) * (*num_of_tables));

    *mapped_tables_sizes = myMalloc(sizeof(int) * (*num_of_tables));

    for (i = 0; i < (*num_of_tables); i++)
        (*mapped_tables_sizes)[i] = -1;

    /* Read the names of the tables line by line */
    for (i = 0; i < *num_of_tables; i++) {

        /* Create the path of the mapped_tables */
        strcpy(table_path, "workloads/small/");
        strcat(table_path, table_names[i]);

        //fprintf(fp_msg, "%s\n", table_path);

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
        (*mapped_tables_sizes)[i] = (int) size;

        /* MAP the whole table to a pointer */
        (*mapped_tables)[i] = mmap(0, size, PROT_READ | PROT_EXEC, MAP_SHARED, fd, 0);
        if ((*mapped_tables)[i] == MAP_FAILED)
            perror("error reading mapped_tables file");
#if PRINTING
        printf("%d-th mapped_tables: numTuples: %ju and numColumns: %ju\n", current_table,
               (*mapped_tables)[current_table][0], (*mapped_tables)[current_table][1]);
#endif
        /* Initialize each table's variables */
        tables[i]->num_tuples = (*mapped_tables)[i][0];
        tables[i]->num_columns = (*mapped_tables)[i][1]; // OR (**mapped_tables + current_table)[1];
        tables[i]->column_indexes = myMalloc(sizeof(uint64_t *) * tables[i]->num_columns);

        for (j = 0; j < tables[i]->num_columns; j++) {
            tables[i]->column_indexes[j] = &(*mapped_tables)[i][2 + j * tables[i]->num_tuples];
        }

        close(fd);
#if PRINTING
        printf("-------------------------------------------------------\n");
#endif
    }

    fprintf(fp_print, " -Finished mmapping tables to memory and initializing structures.\n");

    for(i=0; i<table_names_array_size; i++)
        free(table_names[i]);

    free(table_names);
    free(table_name);

    return tables;
}