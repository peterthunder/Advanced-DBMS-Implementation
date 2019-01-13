#include "statistics_functions.h"


/**
 * For every column of the given table we retrieve: {l, u, f, d}
 * l = lower_value, u = upper_value, f = count_of_all_the_values, d = count_of_the_distinct_values
 * @param table
 * @param num_of_tables
 */
void gatherInitialStatisticsForTable(Table **table)
{
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

        (*table)->column_statistics[col_num]->initialSizeExceededMax = isSizeLargerThanAccepted;
        (*table)->column_statistics[col_num]->d_array_size = sizeOfArray;
        (*table)->column_statistics[col_num]->d_array = myMalloc(sizeof(bool) * sizeOfArray);

        for ( int i = 0 ; i < sizeOfArray ; i++ ) {
            (*table)->column_statistics[col_num]->d_array[i] = FALSE;
        }

        // Set the distinct values equal to the sum of all of the values. Later, we will decrement it each time we re-access the same value.
        (*table)->column_statistics[col_num]->d = (*table)->column_statistics[col_num]->f;

        // Do another pass on the values of this column to mark the distinct values.
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


short gatherPredicatesStatisticsForQuery(Query_Info **qInfo, Table **tables, int query_count)
{

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

    //fprintf(fp_print, "For filters:\n");
    //printPredicatesStatistics(statistic_tables, numOfTablesToBeUsed);     // DEBUG!


    // Gather statistics for Joins
    gatherStatisticsForJoins(qInfo, &statistic_tables, numOfTablesToBeUsed);



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
    if (does_k_belong_to_d_array(k, realTableNum, filterColNum, tables) ) {

        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) ((double)(tables[realTableNum]->column_statistics[filterColNum]->f)
                                                                                                    / tables[realTableNum]->column_statistics[filterColNum]->d);
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = 1;
    }
    else {
        // f'a = 0
        // d'a = 0
        // Because the queries are connected-graphs, we know that if a predicate returns zero-results (as in this case), then the general-result for this query will be zero.
        // So we don't set anything, instead we signal to stop processing this query.
        return -1;
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);
    return 0;
}


short gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables)
{
    //  e.g. Query_5: 6 1 12|0.1=1.0&1.0=2.2&0.0<62236|1.0
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d<%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k > ua, then:  k = ua
    if ( k > tables[realTableNum]->column_statistics[filterColNum]->u ) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is more than upper(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->u, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->u;
    }
    else if ( k < tables[realTableNum]->column_statistics[filterColNum]->l ) {
        // Else if k < la, then, after execution, this filter will produce an intermediate table with NO results..
        // As all expected queries as supposed to be connected-graphs.. the result of this filter will zero-out every other result, so the end result of this query will be NULL.
        return -1;
    }

    // Compute the statistics:
    // l'a = la,  Nothing needs to be done.. they are already the same..

    // u'a = k - 1 (as we want LESS, not LESS-EQUAL)
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
    }*/

    if ( doesKBelongInRange ) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double)(k - tables[realTableNum]->column_statistics[filterColNum]->l)
                          / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        // f'a = fraction * fa
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction * (double)(*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f);
        // d'a = fraction * da
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction * (double)(*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d);
    }
    else {
        // f'a = fa
        // d'a = da
        // Nothing needs to be done.. they are already the same..
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);
    return 0;
}


short gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables)
{
    //  e.g. Query_1: 3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d>%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k < la, then: k = la.
    if ( k < tables[realTableNum]->column_statistics[filterColNum]->l ) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is less than lower(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->l, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->l;
    }
    else if ( k > tables[realTableNum]->column_statistics[filterColNum]->u ) {
        // Else if k > ua, then, after execution, this filter will produce an intermediate table with NO results..
        // As all expected queries as supposed to be connected-graphs.. the result of this filter will zero-out every other result, so the end result of this query will be NULL.
        return -1;
    }

    // If it is in range, then we can use the <k> as follows:

    // l'a = k + 1
    (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->l = k + 1;
    // u'a = ua, Nothing needs to be done.. they are already the same..

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
    }*/

    if ( doesKBelongInRange ) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double)(tables[realTableNum]->column_statistics[filterColNum]->u - k)
                    / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        /*fprintf(fp_print, "\nTable[%d][%d] -> f'a = (ua-k)/(ua-la)*fa -> f'a = (%ju-%ju)/(%ju-%ju)*%ju",
                realTableNum, filterColNum, tables[realTableNum]->column_statistics[filterColNum]->u, k, tables[realTableNum]->column_statistics[filterColNum]->u,
                tables[realTableNum]->column_statistics[filterColNum]->l, tables[realTableNum]->column_statistics[filterColNum]->f);*/

        // f'a = fraction * fa
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction * (double)(*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f);

        //fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->f);

        // d'a = fraction * da
        (*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction * (double)(*statistic_tables)[usedTableNum]->column_statistics[filterColNum]->d);
    }
    else {
        // f'a = fa
        // d'a = da
        // Nothing needs to be done.. they are already the same..
    }

    // Other columns of his table..
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistic_tables, tables);
    return 0;
}


bool does_k_belong_to_d_array(long k, int realTableNum, int colNum, Table **tables)
{
    uint64_t indexOfValueToInvestigate;

    if ( tables[realTableNum]->column_statistics[colNum]->initialSizeExceededMax )
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
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

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

                /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                        realTableNum, i, d_non_filter_old, f_filter_new, f_filter_old, f_non_filter_old, d_non_filter_old);*/

                setDistinctWithComplexCalculation(&(*statistic_tables)[usedTableNum]->column_statistics[i]->d, d_non_filter_old, f_filter_new, f_filter_old, f_non_filter_old,
                                                  d_non_filter_old);

                //fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum]->column_statistics[i]->d);
            }
            else {
                // It gets simplified to:  d'c = dc
                // Nothing needs to be done.. they are the same..
            }
        }
    }
}


void setDistinctWithComplexCalculation(uint64_t *receiver_d, uint64_t factor, uint64_t base_numerator, uint64_t base_denominator, uint64_t exponent_numerator,
                                       uint64_t exponent_denominator)
{
    double base = 1 - ((double)(base_numerator) / base_denominator);
    uint64_t exponent = (uint64_t)((double)(exponent_numerator) / exponent_denominator);

    double powResult = myPow_uint64_t(base, exponent);

    (*receiver_d) = (uint64_t) llabs((long long int)((double)(factor) * (1 - powResult)));  // llabs() -> get absolute from "long long int"

    if ( (*receiver_d) == 0 )   // A column cannot have less than 1 distinct values. Convert value to one from rounded-to-zero.
        (*receiver_d) = 1;
}


void gatherStatisticsForJoins(Query_Info **qInfo, QueryTableStatistics ***statistic_tables, int numOfTablesToBeUsed)
{
    //fprintf(fp_print, "For joins:\n");

    for ( int i = 0 ; i < (*qInfo)->join_count ; i++ )
    {
        /*if ( i == 1 )
            break;*/

        if ( (*qInfo)->joins[i] != NULL )
        {
            int joinTableNum1 = (*qInfo)->joins[i][0];
            int colNum1 = (*qInfo)->joins[i][1];
            int joinTableNum2 = (*qInfo)->joins[i][2];
            int colNum2 = (*qInfo)->joins[i][3];

            int realTableNum1 = (*qInfo)->relation_IDs[joinTableNum1];
            int realTableNum2 = (*qInfo)->relation_IDs[joinTableNum2];

            if ( joinTableNum1 == joinTableNum2 ) {
                if (colNum1 == colNum2) {
                    gatherStatisticsForJoinAutocorrelation(statistic_tables, joinTableNum1, colNum1);
                }
                else {
                    gatherStatisticsForJoinSameTableDiffColumns(statistic_tables, realTableNum1, joinTableNum1, colNum1, colNum2);
                }
            }
            else
                gatherStatisticsForJoinBetweenDifferentTables(statistic_tables, realTableNum1, realTableNum2, joinTableNum1, joinTableNum2, colNum1, colNum2);

            //printPredicatesStatistics((*statistic_tables), numOfTablesToBeUsed);     // DEBUG!
        }
    }
}


void gatherStatisticsForJoinAutocorrelation(QueryTableStatistics ***statistic_tables, int usedTableNum, int colNum)
{
    // l'A = lA
    // u'A = uA
    // Nothing needs to be done.. they are already the same..

    // f'A = fA * fA / n ,   n = uAB - lAB + 1

    uint64_t n = (*statistic_tables)[usedTableNum]->column_statistics[colNum]->u - (*statistic_tables)[usedTableNum]->column_statistics[colNum]->l + 1;

    double fraction = (double)((*statistic_tables)[usedTableNum]->column_statistics[colNum]->f * (*statistic_tables)[usedTableNum]->column_statistics[colNum]->f) / n;

    (*statistic_tables)[usedTableNum]->column_statistics[colNum]->f = (uint64_t) fraction;

    // d'A = dA, Nothing needs to be done.. they are already the same..

    // Set statistics for other columns of this table.
    uint64_t f_join_new = (*statistic_tables)[usedTableNum]->column_statistics[colNum]->f;

    for ( int i = 0 ; i < (*statistic_tables)[usedTableNum]->num_columns ; i++ )
    {
        if ( i != colNum )  // For all columns except the one used by the filter.
        {
            // l'c = lc
            // u'c = uc
            // d'c = dc
            // Nothing needs to be done.. they are already the same..

            // f'c = f'A
            (*statistic_tables)[usedTableNum]->column_statistics[i]->f = f_join_new;
        }
    }
}


void gatherStatisticsForJoinSameTableDiffColumns(QueryTableStatistics ***statistic_tables, int realTableNum, int usedTableNum, int colNum1, int colNum2)
{
    // Set the la and lb with the max of both values.
    uint64_t la = (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->l;
    uint64_t lb = (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->l;
    uint64_t max_l = (la > lb) ? la : lb;
    (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->l = (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->l = max_l;

    // Set the ua and ub with the min of both values
    uint64_t ua = (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->u;
    uint64_t ub = (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->u;
    uint64_t min_u = (ua < ub) ? ua : ub;
    (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->u = (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->u = min_u;

    // f, d
    uint64_t n = min_u - max_l + 1;
    uint64_t fraction = 0;

    // f'a = f'b = f / n
    uint64_t fa = (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->f;
    fraction = (uint64_t)((double)fa / n);

    uint64_t f_join_old = (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->f;
    (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->f = fraction;
    (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->f = fraction;

    // d′A = d′B = dA * (1 − (1 − f'a/fa)^(fa/da))

    /*fprintf(fp_print, "\nTable[%d][%d]-> d′A = Table[%d][%d]->d′B = dA * (1 − (1 − f'a/fa)^(fa/da)) -> d′A = d′B = %ju * (1 − (1 − %ju/%ju)^(%ju/%ju))",
                    realTableNum, colNum1, realTableNum, colNum2, (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d,
                    (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->f, fa, fa, (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d);
*/
    setDistinctWithComplexCalculation
    (&(*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d, (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d,
            (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->f, fa, fa, (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d);

    // d'B = d'A
    (*statistic_tables)[usedTableNum]->column_statistics[colNum2]->d = (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d;

    //fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum]->column_statistics[colNum1]->d);

    setStatisticsForOtherColumnsOfJoinSameTableDiffColumns(usedTableNum, realTableNum, colNum1, statistic_tables, f_join_old);
}


void setStatisticsForOtherColumnsOfJoinSameTableDiffColumns(int usedTableNum, int realTableNum, int joinColumn, QueryTableStatistics ***statistic_tables, uint64_t f_join_old)
{
    // "a" is referred to the filtered column, whereas "c" is referred to every other column.

    // f'a
    uint64_t f_join_new = (*statistic_tables)[usedTableNum]->column_statistics[joinColumn]->f;

    for ( int i = 0 ; i < (*statistic_tables)[usedTableNum]->num_columns ; i++ )
    {
        if ( i != joinColumn )  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // f'c = f'a
            (*statistic_tables)[usedTableNum]->column_statistics[i]->f = f_join_new;

            // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

            // Mathematics:
            // if f'a == fa (when k was out of range), then:
            //  d'c = dc * (1 - (1 - 1)^(fc/dc))
            //  d'c = dc * (1 - 0^(fc/dc))
            //  d'c = dc * (1 - 0)
            //  d'c = dc * 1
            //  d'c = dc

            // Implementation:
            if ( f_join_new != f_join_old ) {
                // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

                uint64_t d_non_filter_old = (*statistic_tables)[usedTableNum]->column_statistics[i]->d;
                uint64_t f_non_filter_old = (*statistic_tables)[usedTableNum]->column_statistics[i]->f;

                fprintf(fp_print, "\nTable[%d][%d] -> d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                        realTableNum, i, d_non_filter_old, f_join_new, f_join_old, f_non_filter_old, d_non_filter_old);

                setDistinctWithComplexCalculation(&(*statistic_tables)[usedTableNum]->column_statistics[i]->d, d_non_filter_old, f_join_new, f_join_old, f_non_filter_old,
                                                  d_non_filter_old);

                fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum]->column_statistics[i]->d);
            }
            else {
                // It gets simplified to:  d'c = dc
                // Nothing needs to be done.. they are already the same..
            }
        }
    }
}


void gatherStatisticsForJoinBetweenDifferentTables(QueryTableStatistics ***statistic_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                                   int colNum2)
{
    // Set the la and lb with the max of both values.
    uint64_t la = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->l;
    uint64_t lb = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->l;
    uint64_t max_l = (la > lb) ? la : lb;
    (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->l = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->l = max_l;

    // Set the ua and ub with the min of both values
    uint64_t ua = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->u;
    uint64_t ub = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->u;
    uint64_t min_u = (ua < ub) ? ua : ub;
    (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->u = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->u = min_u;

    // f, d
    uint64_t n = min_u - max_l + 1;
    uint64_t fraction = 0;

    // f'a = f'b = fa * fb / n
    uint64_t fa = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->f;
    uint64_t fb = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->f;
    fraction = (uint64_t)((double)(fa * fb) / n);

    (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->f = fraction;
    (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->f = fraction;

    // d'a = d'b = da * db / n
    uint64_t da = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->d;
    uint64_t db = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->d;

    fraction = (uint64_t)((double)(da * db) / n);
    if ( fraction == 0 )
        fraction = 1;

    (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->d = fraction;
    (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->d = fraction;

    // Set statistics for other columns of this table.
    setStatisticsForOtherColumnsOfTheJoinedTables(statistic_tables, realTableNum1, realTableNum2, usedTableNum1, usedTableNum2, colNum1, colNum2, da, db);
}


void setStatisticsForOtherColumnsOfTheJoinedTables(QueryTableStatistics ***statistic_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                                   int colNum2, uint64_t da, uint64_t db)
{
    // "A" & "B" is referred to the joined columns, whereas "c" is referred to every other column.

    // f'A = f'B
    uint64_t f_join_new = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->f;

    // Set statistics for Table_A
    // d'A
    uint64_t d_join_new_A = (*statistic_tables)[usedTableNum1]->column_statistics[colNum1]->d;
    // dA
    uint64_t d_join_old_A = da;

    for ( int i = 0 ; i < (*statistic_tables)[usedTableNum1]->num_columns ; i++ )
    {
        if ( i != colNum1 )  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // d'c = dcA * (1 - (1 - (d'A/dA))^(fc/dcA))
            uint64_t f_non_join_old_A = (*statistic_tables)[usedTableNum1]->column_statistics[i]->f;
            uint64_t d_non_join_old_A = (*statistic_tables)[usedTableNum1]->column_statistics[i]->d;

            /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dcA * (1 - (1 - (d'A/dA))^(fc/dcA)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                    realTableNum1, i, d_non_join_old_A, d_join_new_A, d_join_old_A, f_non_join_old_A, d_non_join_old_A);*/

            setDistinctWithComplexCalculation(&(*statistic_tables)[usedTableNum1]->column_statistics[i]->d, d_non_join_old_A, d_join_new_A, d_join_old_A, f_non_join_old_A,
                                              d_non_join_old_A);

            //fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum1]->column_statistics[i]->d);

            // f'c = f'A
            (*statistic_tables)[usedTableNum1]->column_statistics[i]->f = f_join_new;
        }
    }

    // Set statistics for Table_B
    // d'B
    uint64_t d_join_new_B = (*statistic_tables)[usedTableNum2]->column_statistics[colNum2]->d;
    // dB
    uint64_t d_join_old_B = db;

    for ( int i = 0 ; i < (*statistic_tables)[usedTableNum2]->num_columns ; i++ )
    {
        if ( i != colNum2 )  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // d'c = dcB * (1 - (1 - (d'B/dB))^(fc/dcB))
            uint64_t f_non_join_old_B = (*statistic_tables)[usedTableNum2]->column_statistics[i]->f;
            uint64_t d_non_join_old_B = (*statistic_tables)[usedTableNum2]->column_statistics[i]->d;

            /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dcB * (1 - (1 - (d'B/dB))^(fc/dcB)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                    realTableNum2, i, d_non_join_old_B, d_join_new_B, d_join_old_B, f_non_join_old_B, d_non_join_old_B);
            */
            setDistinctWithComplexCalculation(&(*statistic_tables)[usedTableNum2]->column_statistics[i]->d, d_non_join_old_B, d_join_new_B, d_join_old_B, f_non_join_old_B,
                                              d_non_join_old_B);

            //fprintf(fp_print, " = %ju\n", (*statistic_tables)[usedTableNum2]->column_statistics[i]->d);

            // f'c = f'B (=f'A)
            (*statistic_tables)[usedTableNum2]->column_statistics[i]->f = f_join_new;
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
            statistic_tables[i]->column_statistics[j]->initialSizeExceededMax = tables[statistic_tables[i]->realNumOfTable]->column_statistics[j]->initialSizeExceededMax;
        }
    }
    return statistic_tables;
}
