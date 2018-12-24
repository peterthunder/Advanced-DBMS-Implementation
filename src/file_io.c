#include "file_io.h"

Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes) {

    //fprintf(fp_print, "\n# Mmapping tables to memory and initializing structures.\n");

    int fd, i, j, table_names_array_size = 2;
    size_t size;
    struct stat st;
    char *table_name = NULL;
    char table_path[1024];

    char **table_names = myMalloc(sizeof(char *) * 2);
    for (i = 0; i < 2; i++) {
        table_names[i] = myMalloc(sizeof(char) * 1024);
    }

    /* Init */
    *num_of_tables = 0;

    /* Count the number of tables */
    while ( getline(&table_name, &size, fp_read_tables) > 0 ) {

        table_name[strlen(table_name) - 1] = '\0';    // Remove "newLine"-character.

        //fprintf(fp_print, "%s\n", table_name);

        if ( strcmp(table_name, "Done") == 0 || strcmp(table_name, "\n") == 0 )
            break;

        strcpy(table_names[*num_of_tables], table_name);
        (*num_of_tables)++;

        if (table_names_array_size == *num_of_tables) {
            table_names_array_size <<= 1; // fast-multiply by 2
            table_names = realloc(table_names, (size_t) table_names_array_size * sizeof(char *));
            for (i = *num_of_tables; i < table_names_array_size; i++) {
                table_names[i] = myMalloc(sizeof(char) * 1024);
            }
        }
        table_name[0] = '\0';
    }

    if ( USE_HARNESS == FALSE )
        fclose(fp_read_tables);   // Otherwise, on HARNESS this will be the stdin.

    /* Allocate all the memory needed and initialize all the structures */
    Table **tables = myMalloc(sizeof(Table *) * (*num_of_tables));

    for (i = 0; i < *num_of_tables; i++) {
        tables[i] = myMalloc(sizeof(Table));
        tables[i]->num_tuples = 0;
        tables[i]->num_columns = 0;
        tables[i]->column_indexes = NULL;
        tables[i]->column_statistics = NULL;
    }

    *mapped_tables = myMalloc(sizeof(uint64_t *) * (*num_of_tables));

    *mapped_tables_sizes = myMalloc(sizeof(int) * (*num_of_tables));

    for (i = 0; i < (*num_of_tables); i++)
        (*mapped_tables_sizes)[i] = -1;

    /* Read the names of the tables line by line */
    for (i = 0; i < *num_of_tables; i++) {

        /* Create the path of the mapped_tables */
        if ( USE_HARNESS )
            strcpy(table_path, "../../workloads/small/");
        else
            strcpy(table_path, "workloads/small/");
        strcat(table_path, table_names[i]);

        //fprintf(fp_print, "%s\n", table_path);

#if PRINTING || DEEP_PRINTING
        fprintf(fp_print, "Path of the %d-th mapped_tables: %s\n", i, table_path);
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
            fprintf(stderr, "Error reading mapped_tables file!\n");
#if PRINTING || DEEP_PRINTING
        printf("%d-th mapped_tables: numTuples: %ju and numColumns: %ju\n", i, (*mapped_tables)[i][0], (*mapped_tables)[i][1]);
#endif
        /* Initialize each table's variables */
        tables[i]->num_tuples = (*mapped_tables)[i][0];
        tables[i]->num_columns = (*mapped_tables)[i][1];
        tables[i]->column_indexes = myMalloc(sizeof(uint64_t *) * tables[i]->num_columns);
        tables[i]->column_statistics = myMalloc(sizeof(ColumnStats) * tables[i]->num_columns);

        for (j = 0; j < tables[i]->num_columns; j++) {
            tables[i]->column_indexes[j] = &(*mapped_tables)[i][2 + j * tables[i]->num_tuples];
            tables[i]->column_statistics[j] = myMalloc(sizeof(ColumnStats));
            tables[i]->column_statistics[j]->l = 999999;    // ~1 million
            tables[i]->column_statistics[j]->u = 0;
            tables[i]->column_statistics[j]->f = 0;
            tables[i]->column_statistics[j]->d = 0;
        }

        close(fd);
#if PRINTING || DEEP_PRINTING
        printf("-------------------------------------------------------\n");
#endif
    }

    //fprintf(fp_print, " -Finished mmapping tables to memory and initializing structures.\n");

    for(i=0; i<table_names_array_size; i++)
        free(table_names[i]);

    free(table_names);
    free(table_name);

    return tables;
}
