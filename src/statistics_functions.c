#include "statistics_functions.h"


/**
 * For every column of every table we retrieve: {l, u, f, d}
 * l = lower_value, u = upper_value, f = count_of_all_the_values, d = count_of_the_distinct_values
 * @param table
 * @param num_of_tables
 */
void gatherInitialStatisticsForTable(Table **table) {

    u_int64_t indexOfValueToSetTrue;

    // Get l, u, f, d

    // For each column
    for ( int col_num = 0 ; col_num < (*table)->num_columns ; col_num++ )
    {
        // For values inside the column
        for ( int value_index = 0 ; value_index < (*table)->num_tuples ; value_index++ )
        {
            //fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %ju!\n", table_num, col_num, value_index, (*table)->column_indexes[col_num][value_index]);  // DEBUG!

            // Get l
            if ( (*table)->column_indexes[col_num][value_index] < (*table)->column_statistics[col_num]->l )
                (*table)->column_statistics[col_num]->l = (*table)->column_indexes[col_num][value_index];

            // Get u
            if ( (*table)->column_indexes[col_num][value_index] > (*table)->column_statistics[col_num]->u )
                (*table)->column_statistics[col_num]->u = (*table)->column_indexes[col_num][value_index];
        }

        // Get f
        (*table)->column_statistics[col_num]->f = (*table)->num_tuples;

        // Get d
        // Create the boolean-array for d-computation
        u_int64_t sizeOfArray = (*table)->column_statistics[col_num]->u - (*table)->column_statistics[col_num]->l + 1;
        bool isSizeLargerThanAccepted = FALSE;

        if ( sizeOfArray > MAX_BOOL_TABLE_NUM ) {
            isSizeLargerThanAccepted = TRUE;
            sizeOfArray = MAX_BOOL_TABLE_NUM;
        }

        (*table)->column_statistics[col_num]->initialSizeExcededSize = isSizeLargerThanAccepted;
        (*table)->column_statistics[col_num]->d_array_size = sizeOfArray;
        (*table)->column_statistics[col_num]->d_array = myMalloc(sizeof(bool) * sizeOfArray);

        for ( int i = 0 ; i < sizeOfArray ; i++ ) {
            (*table)->column_statistics[col_num]->d_array[i] = FALSE;
        }

        // Set the distinct values equal to the sum of all of the values. Later, we will decrement it each time we re-access the same value.
        (*table)->column_statistics[col_num]->d = (*table)->column_statistics[col_num]->f;

        // Do another pass on the values of this column to
        for ( int value_index = 0 ; value_index < (*table)->num_tuples ; value_index++ ) {

            if ( isSizeLargerThanAccepted )
                indexOfValueToSetTrue = (*table)->column_indexes[col_num][value_index] - (*table)->column_statistics[col_num]->l % MAX_BOOL_TABLE_NUM;
            else
                indexOfValueToSetTrue = (*table)->column_indexes[col_num][value_index] - (*table)->column_statistics[col_num]->l;

          /*  fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %6ju, indexOfValueToSetTrue = %ju!\n",
                    table_num, col_num, value_index, (*table)->column_indexes[col_num][value_index], indexOfValueToSetTrue);  // DEBUG!
          */

            if ( (*table)->column_statistics[col_num]->d_array[indexOfValueToSetTrue] == TRUE )   // We re-access the same value.
                (*table)->column_statistics[col_num]->d --; // So we have one less value to be distinct.
            else
                (*table)->column_statistics[col_num]->d_array[indexOfValueToSetTrue] = TRUE;

        }// end of-each-value
    }// end of-each-column
}


short gatherPredicatesStatisticsForQuery(Query_Info **qInfo, Table **tables, int query_count) {

#if PRINTING || DEEP_PRINTING
    clock_t start_t, end_t, total_t;
    if ( USE_HARNESS == FALSE )
        start_t = clock();
#endif

    int numOfTablesToBeUsed = (*qInfo)->relationId_count;    // Num of tables to be used in this query.
    QueryTableStatistics** statistic_tables = createStatisticsTables(*qInfo, tables, numOfTablesToBeUsed);

    // Gather statistics for Filters
    if ( gatherStatisticsForFilters(qInfo, tables, &statistic_tables) == -1 ) {
        fprintf(fp_print, "Filter-statistics indicated that this query contains a filter producing zero-results, thus the query will produce NULL-results!\n");
        return -1;
    }

    // ALLIWS SUNEXIZOUME

    // Gather statistics for Joins



    // Based on the gathered statistics, run  the BestTree() Algorithm to find the best order between Joins




    // Change Joins' order inside Query-info so that execute_query() will execute the Joins in their best order.



#if PRINTING || DEEP_PRINTING
    if ( USE_HARNESS == FALSE ) {
        end_t = clock();
        total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);
        fprintf(fp_print, "\nFinished gathering predicates statistics and re-ordering the joins (not done yet) for query_%d in %ld seconds!\n", query_count, total_t);
    }
    printPredicatesStatistics(statistic_tables, numOfTablesToBeUsed);
#endif

    // De-allocate space.
    for ( int i = 0 ; i < numOfTablesToBeUsed ; i++ ) {
        for (int j = 0; j < statistic_tables[i]->num_columns ; j++) {
            free(statistic_tables[i]->column_statistics[j]);
        }
        free(statistic_tables[i]->column_statistics);
        free(statistic_tables[i]);
    }
    free(statistic_tables);

    return 0;
}


short gatherStatisticsForFilters(Query_Info **qInfo, Table **tables, QueryTableStatistics ***statistic_tables)
{
    for ( int i = 0 ; i < (*qInfo)->filter_count ; i++ )
    {
        if ( (*qInfo)->filters[i] != NULL )
        {
            int operator = (*qInfo)->filters[i][2];
            int usedTableNum = (*qInfo)->filters[i][0];
            int filterColNum = (*qInfo)->filters[i][1];
            uint64_t k = (uint64_t) (*qInfo)->filters[i][3];
            int realTableNum = (*qInfo)->relation_IDs[usedTableNum];

            switch ( operator ) // Switch on the operator.
            {
                case EQUAL:
                    if ( gatherStatisticsForFilterOperatorEqual(usedTableNum, realTableNum, filterColNum, k, statistic_tables, tables) == -1 )
                        return -1;
                    else
                        break;
                case LESS:
                    if ( gatherStatisticsForFilterOperatorLess(usedTableNum, realTableNum, filterColNum, k, statistic_tables, tables) == -1 )
                        return -1;
                    else
                        break;
                case GREATER:
                    if ( gatherStatisticsForFilterOperatorGreater(usedTableNum, realTableNum, filterColNum, k, statistic_tables, tables) == -1 )
                        return -1;
                    else
                        break;
                default:
                    fprintf(stderr, "\nInvalid operator found: %d\n\n", operator);
                    return -2;
            }
        }
    }
    return 0;
}


short gatherStatisticsForFilterOperatorEqual(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables)
{
    //  e.g. Query_2: 5 0|0.2=1.0&0.3=9881|1.1 0.2 1.0
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d=%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // l'a = k
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->l = k;

    // u'a = k
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->u = k;

    // f'a = fa/da if k belongs to da, otherwise 0.
    // d'a = 1 if k belongs to da, otherwise 0.
    if ( does_k_belongs_to_d_array(k, realTableNum, filterColNum, tables) ) {

        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) ((double)tables[realTableNum]->column_statistics[filterColNum]->f
                                                                                                    / tables[realTableNum]->column_statistics[filterColNum]->d);
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = 1;
    }
    else {
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = 0;
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = 0;

        // TODO - This filter will produce zero results.. take action!
        return -1;
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);

    return 0;
}


short gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables,
                                            Table **tables) {
    //  e.g. Query_5: 6 1 12|0.1=1.0&1.0=2.2&0.0<62236|1.0
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d<%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k > ua, then:
    //  k = ua.
    if ( k > tables[realTableNum]->column_statistics[filterColNum]->u ) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is more than upper(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->u, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->u;
    }
    else if ( k < tables[realTableNum]->column_statistics[filterColNum]->l ) {
        doesKBelongInRange = FALSE;
        // Else if k < la, then we should mark somehow that the result of this filter will be an intermediate table with NO results..
        // We can't throw aay the filter altogether.. as whatever results this is about to bring.. in this case.. zero.. we have to use this info when making the joins..
        // (one of the tables to join will be assumed to be empty)
    }

    // Compute the statistics:

    // l'a = la
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->l = tables[realTableNum]->column_statistics[filterColNum]->l;

    // u'a = k -1
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->u = k - 1;

    // Mathematics:

    /* if ( k != ua ) {  if it belongs in range.
        f'a = (k-la)/(ua-la)*fa
        d'a = (k-la)/(ua-la)*da
        So..
        fraction = (k-la)/(ua-la)
        f'a = fraction * fa
        d'a = fraction * da
    }
    else {  k == ua
        f'a = (ua-la)/(ua-la)*fa = fa
        d'a = (ua-la)/(ua-la)*da = da
    }
    */

    if ( doesKBelongInRange ) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double)(k - tables[realTableNum]->column_statistics[filterColNum]->l)
                          / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction * tables[realTableNum]->column_statistics[filterColNum]->f);
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction * tables[realTableNum]->column_statistics[filterColNum]->d);
    }
    else {
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = tables[realTableNum]->column_statistics[filterColNum]->f;
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = tables[realTableNum]->column_statistics[filterColNum]->d;

        // TODO - This filter will produce zero results.. take action!
        return -1;
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);

    return 0;
}


short gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables,
                                               Table **tables)
{
    //  e.g. Query_1: 3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d>%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k < la, then:
    //  k = la.
    if ( k < tables[realTableNum]->column_statistics[filterColNum]->l ) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is less than lower(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->l, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->l;
    }
    else if ( k > tables[realTableNum]->column_statistics[filterColNum]->u ) {
        doesKBelongInRange = FALSE;
        // Else if k > ua, then we should mark somehow that the result of this filter will be an intermediate table with NO results..
        // We can't throw away the filter altogether.. as whatever results this is about to bring.. in this case.. zero.. we have to use this info when making the joins..
        // (one of the tables to join will be assumed to be empty)
    }


    // If it is in range, then we can use the <k> as follows:

    // l'a = k + 1
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->l = k+1;

    // u'a = ua
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->u = tables[realTableNum]->column_statistics[filterColNum]->u;


    // Mathematics:

    /* if ( k != la ) {  if it belongs in range.
        f'a = (ua-k)/(ua-la)*fa
        d'a = (ua-k)/(ua-la)*da
        So..
        fraction = (ua-k)/(ua-la)
        f'a = fraction * fa
        d'a = fraction * da
    }
    else {  k == la
        f'a = (ua-la)/(ua-la)*fa = fa
        d'a = (ua-la)/(ua-la)*da = da
    }
    */


    if ( doesKBelongInRange ) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double)(tables[realTableNum]->column_statistics[filterColNum]->u - k)
                    / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        // DEBUG PRINTS
        /*fprintf(fp_print, "l: %ju\n", tables[realTableNum]->column_statistics[filterColNum]->l);
        fprintf(fp_print, "u: %ju\n", tables[realTableNum]->column_statistics[filterColNum]->u);
        fprintf(fp_print, "k: %ju\n", k);
        fprintf(fp_print, "u - k: %ju\n", (tables[realTableNum]->column_statistics[filterColNum]->u - k));
        fprintf(fp_print, "u - l: %ju\n", (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l));
        fprintf(fp_print, "Fraction: (u-k)/(u-l): %lf\n", fraction);
        fprintf(fp_print, "d: %ju\n", tables[realTableNum]->column_statistics[filterColNum]->d);
        fprintf(fp_print, "f: %ju\n", tables[realTableNum]->column_statistics[filterColNum]->f);*/


        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction * tables[realTableNum]->column_statistics[filterColNum]->f);
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction * tables[realTableNum]->column_statistics[filterColNum]->d);
    }
    else {
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = tables[realTableNum]->column_statistics[filterColNum]->f;
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = tables[realTableNum]->column_statistics[filterColNum]->d;

        // TODO - This filter will produce zero results.. take action!
        return -1;
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);

    return 0;
}


bool does_k_belongs_to_d_array(long k, int realTableNum, int colNum, Table **tables)
{
    uint64_t indexOfValueToInvestigate;

    if ( tables[realTableNum]->column_statistics[colNum]->initialSizeExcededSize )
        indexOfValueToInvestigate = k - tables[realTableNum]->column_statistics[colNum]->l % MAX_BOOL_TABLE_NUM;
    else
        indexOfValueToInvestigate = k - tables[realTableNum]->column_statistics[colNum]->l;

    if ( tables[realTableNum]->column_statistics[colNum]->d_array[indexOfValueToInvestigate] == TRUE ) {   // We re-access the same value.
#if DEEP_PRINTING
        fprintf(fp_print, "\nK=%ju exists inside d_array of table[%d], column[%d]\n\n", k, realTableNum, colNum);
#endif
        return TRUE;
    }
    else {
#if DEEP_PRINTING
        fprintf(fp_print, "\nK=%ju does not exist inside d_array of table[%d], column[%d]\n\n", k, realTableNum, colNum);
#endif
        return FALSE;
    }
}


void setStatisticsForOtherColumnsOfTheFilteredTable(int usedTableNum, int realTableNum, int filterColNum, QueryTableStatistics ***statistic_tables, Table **tables)
{
    // "a" is referred to the filtered column, whereas "c" is referred to every other column.

    // f'a
    uint64_t f_filter_new = (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f;

    // fa
    uint64_t f_filter_old = tables[realTableNum]->column_statistics[filterColNum]->f;

    for ( int i = 0 ; i < tables[realTableNum]->num_columns ; i++ )
    {
        if ( i != filterColNum )  // For all columns except the one used by the filter.
        {
            // l'c = la
            (*statistic_tables)[usedTableNum]->column_statistics[i]->l = tables[realTableNum]->column_statistics[i]->l;

            // u'c = ua
            (*statistic_tables)[usedTableNum]->column_statistics[i]->u = tables[realTableNum]->column_statistics[i]->u;

            // f'c = f'a
            (*statistic_tables)[usedTableNum]->column_statistics[i]->f = f_filter_new;

            // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

            // Mathematics:
                // if f'a == fa (when k was out of range), then:
                //  d'c = dc * (1 - (1 - 1)^(fc/dc))
                //  d'c = dc * (1 - 0^(fc/dc))
                //  d'c = dc * (1 - 0)
                //  d'c = dc * 1
                //  d'c = dc

            // Implementation:
            if ( f_filter_new != f_filter_old ) {
                // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

                uint64_t d_non_filter_old = tables[realTableNum]->column_statistics[i]->d;
                uint64_t f_non_filter_old = tables[realTableNum]->column_statistics[i]->f;

                uint64_t base = (uint64_t)(1 - ((double)f_filter_new / f_filter_old));
                uint64_t exponent = (uint64_t)((double)f_non_filter_old / d_non_filter_old);

                uint64_t powResult = myPow_uint64_t(base, exponent);

                (*statistic_tables)[usedTableNum]->column_statistics[i]->d = d_non_filter_old * (1 - powResult);
            }
            else {
                // It gets simplified to:  d'c = dc
                (*statistic_tables)[usedTableNum]->column_statistics[i]->d = tables[realTableNum]->column_statistics[i]->d;
            }
        }
    }
}


/**
 * Allocate space for the new statistics which will be gathered for the columns of the tables which participate in this query.
 * @param qInfo
 * @param tables
 * @return
 */
QueryTableStatistics** createStatisticsTables(Query_Info *qInfo, Table **tables, int numOfTablesToBeUsed)
{
    QueryTableStatistics** statistic_tables = myMalloc(sizeof(QueryTableStatistics *) * numOfTablesToBeUsed);

    // Initialize tables.
    for ( int i = 0 ; i < numOfTablesToBeUsed ; i++ )
    {
        statistic_tables[i] = myMalloc(sizeof(QueryTableStatistics));

        // Set the "real"-tables' numbers
        statistic_tables[i]->realNumOfTable = qInfo->relation_IDs[i];
        //printf("table[%d] has real name: %d\n", i, statistic_tables[i]->realNumOfTable);    // DEBUG!

        statistic_tables[i]->num_columns = tables[statistic_tables[i]->realNumOfTable]->num_columns;
        statistic_tables[i]->column_statistics = myMalloc(sizeof(ColumnStats *) * statistic_tables[i]->num_columns);
        for ( int j = 0 ; j < statistic_tables[i]->num_columns ; j++ ) {
            statistic_tables[i]->column_statistics[j] = myMalloc(sizeof(ColumnStats));
            statistic_tables[i]->column_statistics[j]->l = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->l;
            statistic_tables[i]->column_statistics[j]->u = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->u;
            statistic_tables[i]->column_statistics[j]->f = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->f;
            statistic_tables[i]->column_statistics[j]->d = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->d;
            statistic_tables[i]->column_statistics[j]->d_array = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->d_array;
            statistic_tables[i]->column_statistics[j]->d_array_size = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->d_array_size;
            statistic_tables[i]->column_statistics[j]->initialSizeExcededSize = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->initialSizeExcededSize;
            //printf("Malloc-ed space for table[%d], column[%d]\n", i, j);
        }
    }

    return statistic_tables;
}
