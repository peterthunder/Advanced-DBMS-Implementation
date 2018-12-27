#include "statistics_functions.h"


/**
 * For every column of every table we retrieve: {l, u, f, d}
 * l = lower_value, u = upper_value, f = count_of_all_the_values, d = count_of_the_distinct_values
 * @param tables
 * @param num_of_tables
 */
void gatherInitialStatistics(Table ***tables, int num_of_tables) {

    clock_t start_t, end_t, total_t;

    if ( USE_HARNESS == FALSE )
        start_t = clock();

    // Get l, u, f, d

    // For each table.
    for ( int table_num = 0; table_num < num_of_tables ; table_num++ )
    {
        // For each column
        for ( int col_num = 0 ; col_num < (*tables)[table_num]->num_columns ; col_num++ )
        {
            // For values inside the column
            for ( int value_index = 0 ; value_index < (*tables)[table_num]->num_tuples ; value_index++ )
            {
                //fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %ju!\n", table_num, col_num, value_index, (*tables)[table_num]->column_indexes[col_num][value_index]);  // DEBUG!

                // Get l
                if ( (*tables)[table_num]->column_indexes[col_num][value_index] < (*tables)[table_num]->column_statistics[col_num]->l )
                    (*tables)[table_num]->column_statistics[col_num]->l = (*tables)[table_num]->column_indexes[col_num][value_index];

                // Get u
                if ( (*tables)[table_num]->column_indexes[col_num][value_index] > (*tables)[table_num]->column_statistics[col_num]->u )
                    (*tables)[table_num]->column_statistics[col_num]->u = (*tables)[table_num]->column_indexes[col_num][value_index];
            }

            // Get f
            (*tables)[table_num]->column_statistics[col_num]->f = (*tables)[table_num]->num_tuples;

            // Get d
            // Create the boolean-array for d-computation
            u_int64_t sizeOfArray = (*tables)[table_num]->column_statistics[col_num]->u - (*tables)[table_num]->column_statistics[col_num]->l + 1;
            bool isSizeLargerThanAccepted = FALSE;

            if ( sizeOfArray > MAX_BOOL_TABLE_NUM ) {
                sizeOfArray = MAX_BOOL_TABLE_NUM;
                isSizeLargerThanAccepted = TRUE;
            }

            bool d_array[sizeOfArray];
            for ( int i = 0 ; i < sizeOfArray ; i++ ) {
                d_array[i] = FALSE;
            }

            // Set the distinct values equal to the sum of all of the values. Later, we will decrement it each time we re-access the same value.
            (*tables)[table_num]->column_statistics[col_num]->d = (*tables)[table_num]->column_statistics[col_num]->f;

            u_int64_t indexOfValueToSetTrue;

            // Do another pass on the values of this column to
            for ( int value_index = 0 ; value_index < (*tables)[table_num]->num_tuples ; value_index++ ) {

                if ( isSizeLargerThanAccepted )
                    indexOfValueToSetTrue = (*tables)[table_num]->column_indexes[col_num][value_index] - (*tables)[table_num]->column_statistics[col_num]->l % MAX_BOOL_TABLE_NUM;
                else
                    indexOfValueToSetTrue = (*tables)[table_num]->column_indexes[col_num][value_index] - (*tables)[table_num]->column_statistics[col_num]->l;

              /*  fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %6ju, indexOfValueToSetTrue = %ju!\n",
                        table_num, col_num, value_index, (*tables)[table_num]->column_indexes[col_num][value_index], indexOfValueToSetTrue);  // DEBUG!
              */

                if ( d_array[indexOfValueToSetTrue] == TRUE )   // We re-access the same value.
                    (*tables)[table_num]->column_statistics[col_num]->d --; // So we have one less value to be distinct.
                else
                    d_array[indexOfValueToSetTrue] = TRUE;

            }// end of-each-value
        }// end of-each-column
    }// end of-each-table

    if ( USE_HARNESS == FALSE ) {
        end_t = clock();
        total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);
        fprintf(fp_print, "\nFinished gathering initial statistics in %ld seconds!\n", total_t);
    }

#if PRINTING || DEEP_PRINTING
    printStatistics((*tables), num_of_tables);
#endif
}
