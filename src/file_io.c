#include "file_io.h"
#include "statistics_functions.h"

Table **read_tables(int *num_of_tables, uint64_t ***mapped_tables, int **mapped_tables_sizes) {

    //fprintf(fp_print, "\n# Mmapping tables to memory and initializing structures.\n");


    int fd, i, table_names_array_size;
    size_t size;
    struct stat st;
    char basePath[30];
    char table_path[1024];
    struct timeval start;

    char **table_names = getTableNames(num_of_tables, &table_names_array_size);

    if (USE_HARNESS == FALSE) {
        if (fclose(fp_read_tables) == EOF) {   // Otherwise, on HARNESS this will be the stdin.
            fprintf(stderr, "Error closing \"fp_read_tables\" without HARNESS: %s!\n", strerror(errno));
            return NULL;
        }
        gettimeofday(&start, NULL);
        strcpy(basePath, "workloads/small/");
    } else {
        strcpy(basePath, "../../workloads/small/");
    }

    /* Allocate all the memory needed and initialize all the structures */
    Table **tables = myMalloc(sizeof(Table *) * (*num_of_tables));

    *mapped_tables = myMalloc(sizeof(uint64_t *) * (*num_of_tables));

    *mapped_tables_sizes = myMalloc(sizeof(int) * (*num_of_tables));
    for (i = 0; i < (*num_of_tables); i++)
        (*mapped_tables_sizes)[i] = -1;

    /* Read the names of the tables line by line */
    for (i = 0; i < *num_of_tables; i++) {

        /* Create the path of the mapped_tables */
        strcpy(table_path, basePath);
        strcat(table_path, table_names[i]);

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
        if ((*mapped_tables)[i] == MAP_FAILED) {
            fprintf(stderr, "Error reading mapped_tables file!\n");
            return NULL;
        }

#if PRINTING || DEEP_PRINTING
        printf("%d-th mapped_tables: numTuples: %ju and numColumns: %ju\n", i, (*mapped_tables)[i][0], (*mapped_tables)[i][1]);
#endif
        initializeTable(&tables[i], (*mapped_tables)[i]);

        if (close(fd) == -1) {
            fprintf(stderr, "Error closing file \"%s\": %s!\n", table_path, strerror(errno));
            return NULL;
        }

        // Gather statistics for each column of this table. Later, these will optimize the execution of the queries.
        gatherInitialStatisticsForTable(&tables[i]);

#if PRINTING || DEEP_PRINTING
        printf("-------------------------------------------------------\n");
#endif
    }

    //fprintf(fp_print, " -Finished mmapping tables to memory and initializing structures.\n");

    for (i = 0; i < table_names_array_size; i++)
        free(table_names[i]);

    free(table_names);

    if (USE_HARNESS == FALSE) {
        struct timeval end;
        gettimeofday(&end, NULL);
        double elapsed_sec = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        fprintf(fp_print, "\nFinished mapping tables and gathering initial statistics in %.f milliseconds!\n",
                elapsed_sec * 1000);
    }

#if PRINTING || DEEP_PRINTING
    printInitialStatistics(tables, *num_of_tables);
#endif

    return tables;
}


char **getTableNames(int *num_of_tables, int *table_names_array_size) {
    int i;
    size_t size;
    char *table_name = NULL;

    char **table_names = myMalloc(sizeof(char *) * 2);
    for (i = 0; i < 2; i++) {
        table_names[i] = myMalloc(sizeof(char) * 1024);
    }

    /* Init */
    *num_of_tables = 0;
    *table_names_array_size = 2;

    /* Count the number of tables */
    while (getline(&table_name, &size, fp_read_tables) > 0) {

        if (strcmp(table_name, "Done\n") == 0 || strcmp(table_name, "\n") == 0)
            break;

        table_name[strlen(table_name) - 1] = '\0';    // Remove "newLine"-character.

        //fprintf(fp_print, "%s\n", table_name);

        strcpy(table_names[*num_of_tables], table_name);
        (*num_of_tables)++;

        if ((*table_names_array_size) == *num_of_tables) {
            (*table_names_array_size) <<= 1; // fast-multiply by 2
            table_names = myRealloc(table_names, (size_t) (*table_names_array_size) * sizeof(char *));
            for (i = *num_of_tables; i < (*table_names_array_size); i++) {
                table_names[i] = myMalloc(sizeof(char) * 1024);
            }
        }
    }

    free(table_name);   // "getline()" allocates space internally, which WE have to free.

    return table_names;
}


void initializeTable(Table **table, uint64_t *mapped_table) {
    (*table) = myMalloc(sizeof(Table));

    /* Initialize each table's variables */
    (*table)->num_tuples = mapped_table[0];
    (*table)->num_columns = mapped_table[1];
    (*table)->column_indexes = myMalloc(sizeof(uint64_t *) * (*table)->num_columns);
    (*table)->column_statistics = myMalloc(sizeof(ColumnStats *) * (*table)->num_columns);

    for (int j = 0; j < (*table)->num_columns; j++) {
        (*table)->column_indexes[j] = &mapped_table[2 + j * (*table)->num_tuples];
        (*table)->column_statistics[j] = myMalloc(sizeof(ColumnStats));
        (*table)->column_statistics[j]->l = 999999;    // Set it to ~1 million, to make sure it will be decreased later.
        (*table)->column_statistics[j]->u = 0;
        (*table)->column_statistics[j]->f = 0;
        (*table)->column_statistics[j]->d = 0;
        (*table)->column_statistics[j]->d_array = NULL;
        (*table)->column_statistics[j]->d_array_size = 0;
        (*table)->column_statistics[j]->initialSizeExcededSize = FALSE;
    }
}