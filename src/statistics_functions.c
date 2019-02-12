#include "statistics_functions.h"


/**
 * For every column of the given table we retrieve: {l, u, f, d}
 * l = lower_value, u = upper_value, f = count_of_all_the_values, d = count_of_the_distinct_values
 * @param table
 * @param num_of_tables
 */
void gatherInitialStatisticsForTable(Table **table) {
    u_int64_t indexOfValueToSetTrue;

    // Get l, u, f, d

    // For each column
    for (int col_num = 0; col_num < (*table)->num_columns; col_num++) {
        // For values inside the column
        for (int value_index = 0; value_index < (*table)->num_tuples; value_index++) {
            //fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %ju!\n", table_num, col_num, value_index, (*table)->column_indexes[col_num][value_index]);  // DEBUG!

            // Get l
            if ((*table)->column_indexes[col_num][value_index] < (*table)->column_statistics[col_num]->l)
                (*table)->column_statistics[col_num]->l = (*table)->column_indexes[col_num][value_index];

            // Get u
            if ((*table)->column_indexes[col_num][value_index] > (*table)->column_statistics[col_num]->u)
                (*table)->column_statistics[col_num]->u = (*table)->column_indexes[col_num][value_index];
        }

        // Get f
        (*table)->column_statistics[col_num]->f = (*table)->num_tuples;

        // Get d
        // Create the boolean-array for d-computation
        u_int64_t sizeOfArray = (*table)->column_statistics[col_num]->u - (*table)->column_statistics[col_num]->l + 1;
        bool isSizeLargerThanAccepted = FALSE;

        if (sizeOfArray > MAX_BOOL_TABLE_NUM) {
            isSizeLargerThanAccepted = TRUE;
            sizeOfArray = MAX_BOOL_TABLE_NUM;
        }

        (*table)->column_statistics[col_num]->initialSizeExceededMax = isSizeLargerThanAccepted;
        (*table)->column_statistics[col_num]->d_array_size = sizeOfArray;
        (*table)->column_statistics[col_num]->d_array = myMalloc(sizeof(bool) * sizeOfArray);

        for (int i = 0; i < sizeOfArray; i++) {
            (*table)->column_statistics[col_num]->d_array[i] = FALSE;
        }

        // Set the distinct values equal to the sum of all of the values. Later, we will decrement it each time we re-access the same value.
        (*table)->column_statistics[col_num]->d = (*table)->column_statistics[col_num]->f;

        // Do another pass on the values of this column to mark the distinct values.
        for (int value_index = 0; value_index < (*table)->num_tuples; value_index++) {

            if (isSizeLargerThanAccepted)
                indexOfValueToSetTrue = (*table)->column_indexes[col_num][value_index] - (*table)->column_statistics[col_num]->l % MAX_BOOL_TABLE_NUM;
            else
                indexOfValueToSetTrue = (*table)->column_indexes[col_num][value_index] - (*table)->column_statistics[col_num]->l;

            /*  fprintf(fp_print, "Table[%d] , column[%d], value[%4d] = %6ju, indexOfValueToSetTrue = %ju!\n",
                      table_num, col_num, value_index, (*table)->column_indexes[col_num][value_index], indexOfValueToSetTrue);  // DEBUG!
            */

            if ((*table)->column_statistics[col_num]->d_array[indexOfValueToSetTrue] == TRUE)   // We re-access the same value.
                (*table)->column_statistics[col_num]->d--; // So we have one less value to be distinct.
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
    QueryTableStatistics **statistics_tables = createStatisticsTables(*qInfo, tables, numOfTablesToBeUsed);

    // Gather statistics for Filters
    if (gatherStatisticsForFilters(qInfo, tables, &statistics_tables) == -1) {
#if PRINTING || DEEP_PRINTING
        fprintf(fp_print, "Filter-statistics indicated that this query contains a filter producing zero-results, thus the query will produce NULL-results!\n");
#endif
        freeStatisticsTables(statistics_tables, numOfTablesToBeUsed);
        return -1;
    }

    //fprintf(fp_print, "For filters:\n");
    //printPredicatesStatistics(statistics_tables, numOfTablesToBeUsed);     // DEBUG!


    // Gather statistics for Joins
    /* gatherStatisticsForJoins(qInfo, &statistics_tables, numOfTablesToBeUsed); */

    // Based on the gathered statistics, run  the BestTree() Algorithm to find the best order between Joins

    Query_Info *query_info_with_reordered_joins;

    query_info_with_reordered_joins = bestTree(*qInfo, &statistics_tables, numOfTablesToBeUsed);

    if (query_info_with_reordered_joins != NULL)
        *qInfo = query_info_with_reordered_joins;

    // Change Joins' order inside Query-info so that execute_query() will execute the Joins in their best order.


#if PRINTING || DEEP_PRINTING
    if ( USE_HARNESS == FALSE ) {
        end_t = clock();
        total_t = (clock_t) ((double) (end_t - start_t) / CLOCKS_PER_SEC);
        fprintf(fp_print, "\nFinished gathering predicates statistics and re-ordering the joins (not done yet) for query_%d in %ld seconds!\n", query_count, total_t);
    }
    printPredicatesStatistics(statistics_tables, numOfTablesToBeUsed);
#endif

    // De-allocate space.
    freeStatisticsTables(statistics_tables, numOfTablesToBeUsed);
    return 0;
}


short gatherStatisticsForFilters(Query_Info **qInfo, Table **tables, QueryTableStatistics ***statistics_tables) {
    for (int i = 0; i < (*qInfo)->filter_count; i++) {
        if ((*qInfo)->filters[i] != NULL) {
            int operator = (*qInfo)->filters[i][2];
            int usedTableNum = (*qInfo)->filters[i][0];
            int filterColNum = (*qInfo)->filters[i][1];
            uint64_t k = (uint64_t) (*qInfo)->filters[i][3];
            int realTableNum = (*qInfo)->relation_IDs[usedTableNum];

            switch (operator) // Switch on the operator.
            {
                case EQUAL:
                    if (gatherStatisticsForFilterOperatorEqual(usedTableNum, realTableNum, filterColNum, k, statistics_tables, tables) == -1)
                        return -1;
                    else
                        break;
                case LESS:
                    if (gatherStatisticsForFilterOperatorLess(usedTableNum, realTableNum, filterColNum, k, statistics_tables, tables) == -1)
                        return -1;
                    else
                        break;
                case GREATER:
                    if (gatherStatisticsForFilterOperatorGreater(usedTableNum, realTableNum, filterColNum, k, statistics_tables, tables) == -1)
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


short gatherStatisticsForFilterOperatorEqual(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables) {
    //  e.g. Query_2: 5 0|0.2=1.0&0.3=9881|1.1 0.2 1.0
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d=%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // l'a = k
    (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->l = k;
    // u'a = k
    (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->u = k;

    // f'a = fa/da if k belongs to da, otherwise 0.
    // d'a = 1 if k belongs to da, otherwise 0.
    if (does_k_belong_to_d_array(k, realTableNum, filterColNum, tables)) {

        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) ((double) (tables[realTableNum]->column_statistics[filterColNum]->f)
                                                                                             / tables[realTableNum]->column_statistics[filterColNum]->d);
        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->d = 1;
    } else {
        // f'a = 0
        // d'a = 0
        // Because the queries are connected-graphs, we know that if a predicate returns zero-results (as in this case), then the general-result for this query will be zero.
        // So we don't set anything, instead we signal to stop processing this query.
        return -1;
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistics_tables, tables);
    return 0;
}


short gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables) {
    //  e.g. Query_5: 6 1 12|0.1=1.0&1.0=2.2&0.0<62236|1.0
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d<%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k > ua, then:  k = ua
    if (k > tables[realTableNum]->column_statistics[filterColNum]->u) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is more than upper(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->u, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->u;
    } else if (k < tables[realTableNum]->column_statistics[filterColNum]->l) {
        // Else if k < la, then, after execution, this filter will produce an intermediate table with NO results..
        // As all expected queries as supposed to be connected-graphs.. the result of this filter will zero-out every other result, so the end result of this query will be NULL.
        return -1;
    }

    // Compute the statistics:
    // l'a = la,  Nothing needs to be done.. they are already the same..

    // u'a = k - 1 (as we want LESS, not LESS-EQUAL)
    (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->u = k - 1;

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

    if (doesKBelongInRange) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double) (k - tables[realTableNum]->column_statistics[filterColNum]->l)
                          / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        // f'a = fraction * fa
        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction *
                                                                                             (double) (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f);
        // d'a = fraction * da
        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction *
                                                                                             (double) (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->d);
    } else {
        // f'a = fa
        // d'a = da
        // Nothing needs to be done.. they are already the same..
    }

    // Other columns of his table
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistics_tables, tables);
    return 0;
}


short gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables) {
    //  e.g. Query_1: 3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
#if PRINTING || DEEP_PRINTING
    fprintf(fp_print, "\nGathering statistics for filter: %d.c%d>%ju, using the realTableNum: %d\n", usedTableNum, filterColNum, k, realTableNum);
#endif

    // First check if k E [la, ua]
    bool doesKBelongInRange = TRUE;
    // If not, then we should set the <k> as follows:
    // If  k < la, then: k = la.
    if (k < tables[realTableNum]->column_statistics[filterColNum]->l) {
        doesKBelongInRange = FALSE;
        fprintf(fp_print, "\nK(%ju) is less than lower(%ju) in table[%d]", k, tables[realTableNum]->column_statistics[filterColNum]->l, realTableNum);
        k = tables[realTableNum]->column_statistics[filterColNum]->l;
    } else if (k > tables[realTableNum]->column_statistics[filterColNum]->u) {
        // Else if k > ua, then, after execution, this filter will produce an intermediate table with NO results..
        // As all expected queries as supposed to be connected-graphs.. the result of this filter will zero-out every other result, so the end result of this query will be NULL.
        return -1;
    }

    // If it is in range, then we can use the <k> as follows:

    // l'a = k + 1
    (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->l = k + 1;
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

    if (doesKBelongInRange) {
        // Calculate the fraction first, which is used in both d'a and f'a.
        double fraction = (double) (tables[realTableNum]->column_statistics[filterColNum]->u - k)
                          / (tables[realTableNum]->column_statistics[filterColNum]->u - tables[realTableNum]->column_statistics[filterColNum]->l);

        /*fprintf(fp_print, "\nTable[%d][%d] -> f'a = (ua-k)/(ua-la)*fa -> f'a = (%ju-%ju)/(%ju-%ju)*%ju",
                realTableNum, filterColNum, tables[realTableNum]->column_statistics[filterColNum]->u, k, tables[realTableNum]->column_statistics[filterColNum]->u,
                tables[realTableNum]->column_statistics[filterColNum]->l, tables[realTableNum]->column_statistics[filterColNum]->f);*/

        // f'a = fraction * fa
        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f = (uint64_t) (fraction *
                                                                                             (double) (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f);

        //fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f);

        // d'a = fraction * da
        (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->d = (uint64_t) (fraction *
                                                                                             (double) (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->d);
    } else {
        // f'a = fa
        // d'a = da
        // Nothing needs to be done.. they are already the same..
    }

    // Other columns of his table..
    setStatisticsForOtherColumnsOfTheFilteredTable(usedTableNum, realTableNum, filterColNum, statistics_tables, tables);
    return 0;
}


bool does_k_belong_to_d_array(long k, int realTableNum, int colNum, Table **tables) {
    uint64_t indexOfValueToInvestigate;

    if (tables[realTableNum]->column_statistics[colNum]->initialSizeExceededMax)
        indexOfValueToInvestigate = k - tables[realTableNum]->column_statistics[colNum]->l % MAX_BOOL_TABLE_NUM;
    else
        indexOfValueToInvestigate = k - tables[realTableNum]->column_statistics[colNum]->l;

    if (tables[realTableNum]->column_statistics[colNum]->d_array[indexOfValueToInvestigate] == TRUE) {   // We re-access the same value.
#if DEEP_PRINTING
        fprintf(fp_print, "\nK=%ju exists inside d_array of table[%d], column[%d]\n\n", k, realTableNum, colNum);
#endif
        return TRUE;
    } else {
#if DEEP_PRINTING
        fprintf(fp_print, "\nK=%ju does not exist inside d_array of table[%d], column[%d]\n\n", k, realTableNum, colNum);
#endif
        return FALSE;
    }
}


void setStatisticsForOtherColumnsOfTheFilteredTable(int usedTableNum, int realTableNum, int filterColNum, QueryTableStatistics ***statistics_tables, Table **tables) {
    // "a" is referred to the filtered column, whereas "c" is referred to every other column.

    // f'a
    uint64_t f_filter_new = (*statistics_tables)[usedTableNum]->column_statistics[filterColNum]->f;
    // fa
    uint64_t f_filter_old = tables[realTableNum]->column_statistics[filterColNum]->f;

    for (int i = 0; i < tables[realTableNum]->num_columns; i++) {
        if (i != filterColNum)  // For all columns except the one used by the filter.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // f'c = f'a
            (*statistics_tables)[usedTableNum]->column_statistics[i]->f = f_filter_new;

            // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

            // Mathematics:
            // if f'a == fa (when k was out of range), then:
            //  d'c = dc * (1 - (1 - 1)^(fc/dc))
            //  d'c = dc * (1 - 0^(fc/dc))
            //  d'c = dc * (1 - 0)
            //  d'c = dc * 1
            //  d'c = dc

            // Implementation:
            if (f_filter_new != f_filter_old) {
                // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

                uint64_t d_non_filter_old = tables[realTableNum]->column_statistics[i]->d;
                uint64_t f_non_filter_old = tables[realTableNum]->column_statistics[i]->f;

                /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                        realTableNum, i, d_non_filter_old, f_filter_new, f_filter_old, f_non_filter_old, d_non_filter_old);*/

                setDistinctWithComplexCalculation(&(*statistics_tables)[usedTableNum]->column_statistics[i]->d, d_non_filter_old, f_filter_new, f_filter_old, f_non_filter_old,
                                                  d_non_filter_old);

                //fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum]->column_statistics[i]->d);
            } else {
                // It gets simplified to:  d'c = dc
                // Nothing needs to be done.. they are the same..
            }
        }
    }
}


void setDistinctWithComplexCalculation(uint64_t *receiver_d, uint64_t factor, uint64_t base_numerator, uint64_t base_denominator, uint64_t exponent_numerator,
                                       uint64_t exponent_denominator) {
    double base = 1 - ((double) (base_numerator) / base_denominator);
    uint64_t exponent = (uint64_t) ((double) (exponent_numerator) / exponent_denominator);

    double powResult = myPow_uint64_t(base, exponent);

    (*receiver_d) = (uint64_t) llabs((long long int) ((double) (factor) * (1 - powResult)));  // llabs() -> get absolute from "long long int"

    if ((*receiver_d) == 0)   // A column cannot have less than 1 distinct values. Convert value to one from rounded-to-zero.
        (*receiver_d) = 1;
}


void gatherStatisticsForJoins(Query_Info **qInfo, QueryTableStatistics ***statistics_tables, int numOfTablesToBeUsed) {
    //fprintf(fp_print, "For joins:\n");

    int joinTableNum1, colNum1, joinTableNum2, colNum2, realTableNum1, realTableNum2;

    for (int i = 0; i < (*qInfo)->join_count; i++) {
        /*if ( i == 1 )
            break;*/

        if ((*qInfo)->joins[i] != NULL) {
            joinTableNum1 = (*qInfo)->joins[i][0];
            colNum1 = (*qInfo)->joins[i][1];
            joinTableNum2 = (*qInfo)->joins[i][2];
            colNum2 = (*qInfo)->joins[i][3];

            realTableNum1 = (*qInfo)->relation_IDs[joinTableNum1];
            realTableNum2 = (*qInfo)->relation_IDs[joinTableNum2];

            if (joinTableNum1 == joinTableNum2) {
                if (colNum1 == colNum2) {
                    gatherStatisticsForJoinAutocorrelation(statistics_tables, joinTableNum1, colNum1);
                } else {
                    gatherStatisticsForJoinSameTableDiffColumns(statistics_tables, realTableNum1, joinTableNum1, colNum1, colNum2);
                }
            } else {
                gatherStatisticsForJoinBetweenDifferentTables(statistics_tables, realTableNum1, realTableNum2, joinTableNum1, joinTableNum2, colNum1, colNum2);
            }

            //printPredicatesStatistics((*statistics_tables), numOfTablesToBeUsed);     // DEBUG!
        }
    }
}


void gatherStatisticsForJoinAutocorrelation(QueryTableStatistics ***statistics_tables, int usedTableNum, int colNum) {
    // l'A = lA
    // u'A = uA
    // Nothing needs to be done.. they are already the same..

    // f'A = fA * fA / n ,   n = uAB - lAB + 1

    uint64_t n = (*statistics_tables)[usedTableNum]->column_statistics[colNum]->u - (*statistics_tables)[usedTableNum]->column_statistics[colNum]->l + 1;

    double fraction = (double) ((*statistics_tables)[usedTableNum]->column_statistics[colNum]->f * (*statistics_tables)[usedTableNum]->column_statistics[colNum]->f) / n;

    (*statistics_tables)[usedTableNum]->column_statistics[colNum]->f = (uint64_t) fraction;

    // d'A = dA, Nothing needs to be done.. they are already the same..

    // Set statistics for other columns of this table.
    uint64_t f_join_new = (*statistics_tables)[usedTableNum]->column_statistics[colNum]->f;

    for (int i = 0; i < (*statistics_tables)[usedTableNum]->num_columns; i++) {
        if (i != colNum)  // For all columns except the one used by the filter.
        {
            // l'c = lc
            // u'c = uc
            // d'c = dc
            // Nothing needs to be done.. they are already the same..

            // f'c = f'A
            (*statistics_tables)[usedTableNum]->column_statistics[i]->f = f_join_new;
        }
    }
}


void gatherStatisticsForJoinSameTableDiffColumns(QueryTableStatistics ***statistics_tables, int realTableNum, int usedTableNum, int colNum1, int colNum2) {
    // Set the la and lb with the max of both values.
    uint64_t la = (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->l;
    uint64_t lb = (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->l;
    uint64_t max_l = (la > lb) ? la : lb;
    (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->l = (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->l = max_l;

    // Set the ua and ub with the min of both values
    uint64_t ua = (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->u;
    uint64_t ub = (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->u;
    uint64_t min_u = (ua < ub) ? ua : ub;
    (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->u = (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->u = min_u;

    // f, d
    uint64_t n = min_u - max_l + 1;
    uint64_t fraction = 0;

    // f'a = f'b = f / n
    uint64_t fa = (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->f;
    fraction = (uint64_t) ((double) fa / n);

    uint64_t f_join_old = (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->f;
    (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->f = fraction;
    (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->f = fraction;

    // d′A = d′B = dA * (1 − (1 − f'a/fa)^(fa/da))

    /*fprintf(fp_print, "\nTable[%d][%d]-> d′A = Table[%d][%d]->d′B = dA * (1 − (1 − f'a/fa)^(fa/da)) -> d′A = d′B = %ju * (1 − (1 − %ju/%ju)^(%ju/%ju))",
                    realTableNum, colNum1, realTableNum, colNum2, (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d,
                    (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->f, fa, fa, (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d);
*/
    setDistinctWithComplexCalculation
            (&(*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d, (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d,
             (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->f, fa, fa, (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d);

    // d'B = d'A
    (*statistics_tables)[usedTableNum]->column_statistics[colNum2]->d = (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d;

    //fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum]->column_statistics[colNum1]->d);

    setStatisticsForOtherColumnsOfJoinSameTableDiffColumns(usedTableNum, realTableNum, colNum1, statistics_tables, f_join_old);
}


void setStatisticsForOtherColumnsOfJoinSameTableDiffColumns(int usedTableNum, int realTableNum, int joinColumn, QueryTableStatistics ***statistics_tables, uint64_t f_join_old) {
    // "a" is referred to the filtered column, whereas "c" is referred to every other column.

    // f'a
    uint64_t f_join_new = (*statistics_tables)[usedTableNum]->column_statistics[joinColumn]->f;

    for (int i = 0; i < (*statistics_tables)[usedTableNum]->num_columns; i++) {
        if (i != joinColumn)  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // f'c = f'a
            (*statistics_tables)[usedTableNum]->column_statistics[i]->f = f_join_new;

            // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

            // Mathematics:
            // if f'a == fa (when k was out of range), then:
            //  d'c = dc * (1 - (1 - 1)^(fc/dc))
            //  d'c = dc * (1 - 0^(fc/dc))
            //  d'c = dc * (1 - 0)
            //  d'c = dc * 1
            //  d'c = dc

            // Implementation:
            if (f_join_new != f_join_old) {
                // d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc))

                uint64_t d_non_filter_old = (*statistics_tables)[usedTableNum]->column_statistics[i]->d;
                uint64_t f_non_filter_old = (*statistics_tables)[usedTableNum]->column_statistics[i]->f;

                fprintf(fp_print, "\nTable[%d][%d] -> d'c = dc * (1 - (1 - (f'a/fa))^(fc/dc)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                        realTableNum, i, d_non_filter_old, f_join_new, f_join_old, f_non_filter_old, d_non_filter_old);

                setDistinctWithComplexCalculation(&(*statistics_tables)[usedTableNum]->column_statistics[i]->d, d_non_filter_old, f_join_new, f_join_old, f_non_filter_old,
                                                  d_non_filter_old);

                fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum]->column_statistics[i]->d);
            } else {
                // It gets simplified to:  d'c = dc
                // Nothing needs to be done.. they are already the same..
            }
        }
    }
}


void
gatherStatisticsForJoinBetweenDifferentTables(QueryTableStatistics ***statistics_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                              int colNum2) {
    // Set the la and lb with the max of both values.
    uint64_t la = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->l;
    uint64_t lb = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->l;
    uint64_t max_l = (la > lb) ? la : lb;
    (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->l = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->l = max_l;

    // Set the ua and ub with the min of both values
    uint64_t ua = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->u;
    uint64_t ub = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->u;
    uint64_t min_u = (ua < ub) ? ua : ub;
    (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->u = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->u = min_u;

    // f, d
    uint64_t n = min_u - max_l + 1;
    uint64_t fraction = 0;

    // f'a = f'b = fa * fb / n
    uint64_t fa = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->f;
    uint64_t fb = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->f;
    fraction = (uint64_t) ((double) (fa * fb) / n);

    (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->f = fraction;
    (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->f = fraction;

    // d'a = d'b = da * db / n
    uint64_t da = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->d;
    uint64_t db = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->d;

    fraction = (uint64_t) ((double) (da * db) / n);
    if (fraction == 0)
        fraction = 1;

    (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->d = fraction;
    (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->d = fraction;

    // Set statistics for other columns of this table.
    setStatisticsForOtherColumnsOfTheJoinedTables(statistics_tables, realTableNum1, realTableNum2, usedTableNum1, usedTableNum2, colNum1, colNum2, da, db);
}


void
setStatisticsForOtherColumnsOfTheJoinedTables(QueryTableStatistics ***statistics_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                              int colNum2, uint64_t da, uint64_t db) {
    // "A" & "B" is referred to the joined columns, whereas "c" is referred to every other column.

    // f'A = f'B
    uint64_t f_join_new = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->f;

    // Set statistics for Table_A
    // d'A
    uint64_t d_join_new_A = (*statistics_tables)[usedTableNum1]->column_statistics[colNum1]->d;
    // dA
    uint64_t d_join_old_A = da;

    for (int i = 0; i < (*statistics_tables)[usedTableNum1]->num_columns; i++) {
        if (i != colNum1)  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // d'c = dcA * (1 - (1 - (d'A/dA))^(fc/dcA))
            uint64_t f_non_join_old_A = (*statistics_tables)[usedTableNum1]->column_statistics[i]->f;
            uint64_t d_non_join_old_A = (*statistics_tables)[usedTableNum1]->column_statistics[i]->d;

            /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dcA * (1 - (1 - (d'A/dA))^(fc/dcA)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                    realTableNum1, i, d_non_join_old_A, d_join_new_A, d_join_old_A, f_non_join_old_A, d_non_join_old_A);*/

            setDistinctWithComplexCalculation(&(*statistics_tables)[usedTableNum1]->column_statistics[i]->d, d_non_join_old_A, d_join_new_A, d_join_old_A, f_non_join_old_A,
                                              d_non_join_old_A);

            //fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum1]->column_statistics[i]->d);

            // f'c = f'A
            (*statistics_tables)[usedTableNum1]->column_statistics[i]->f = f_join_new;
        }
    }

    // Set statistics for Table_B
    // d'B
    uint64_t d_join_new_B = (*statistics_tables)[usedTableNum2]->column_statistics[colNum2]->d;
    // dB
    uint64_t d_join_old_B = db;

    for (int i = 0; i < (*statistics_tables)[usedTableNum2]->num_columns; i++) {
        if (i != colNum2)  // For all columns except the one used by the join.
        {
            // l'c = lc
            // u'c = uc
            // Nothing needs to be done.. they are already the same..

            // d'c = dcB * (1 - (1 - (d'B/dB))^(fc/dcB))
            uint64_t f_non_join_old_B = (*statistics_tables)[usedTableNum2]->column_statistics[i]->f;
            uint64_t d_non_join_old_B = (*statistics_tables)[usedTableNum2]->column_statistics[i]->d;

            /*fprintf(fp_print, "\nTable[%d][%d] -> d'c = dcB * (1 - (1 - (d'B/dB))^(fc/dcB)) -> d'c = %ju * (1 - (1 - (%ju/%ju))^(%ju/%ju))",
                    realTableNum2, i, d_non_join_old_B, d_join_new_B, d_join_old_B, f_non_join_old_B, d_non_join_old_B);
            */
            setDistinctWithComplexCalculation(&(*statistics_tables)[usedTableNum2]->column_statistics[i]->d, d_non_join_old_B, d_join_new_B, d_join_old_B, f_non_join_old_B,
                                              d_non_join_old_B);

            //fprintf(fp_print, " = %ju\n", (*statistics_tables)[usedTableNum2]->column_statistics[i]->d);

            // f'c = f'B (=f'A)
            (*statistics_tables)[usedTableNum2]->column_statistics[i]->f = f_join_new;
        }
    }
}


/**
 * Allocate space for the new statistics which will be gathered for the columns of the tables which participate in this query.
 * @param qInfo
 * @param tables
 * @return
 */
QueryTableStatistics **createStatisticsTables(Query_Info *qInfo, Table **tables, int numOfTablesToBeUsed) {
	
    QueryTableStatistics **statistics_tables = myMalloc(sizeof(QueryTableStatistics *) * numOfTablesToBeUsed);

    for (int i = 0; i < numOfTablesToBeUsed; i++) {
        statistics_tables[i] = myMalloc(sizeof(QueryTableStatistics));

        // Set the "real"-tables' numbers
        statistics_tables[i]->realNumOfTable = qInfo->relation_IDs[i];
        //fprintf(fp_print,"table[%d] has real name: %d\n", i, statistics_tables[i]->realNumOfTable);    // DEBUG!

        statistics_tables[i]->num_columns = tables[statistics_tables[i]->realNumOfTable]->num_columns;
        statistics_tables[i]->column_statistics = myMalloc(sizeof(ColumnStats *) * statistics_tables[i]->num_columns);
        for (int j = 0; j < statistics_tables[i]->num_columns; j++) {
            statistics_tables[i]->column_statistics[j] = myMalloc(sizeof(ColumnStats));
            statistics_tables[i]->column_statistics[j]->l = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->l;
            statistics_tables[i]->column_statistics[j]->u = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->u;
            statistics_tables[i]->column_statistics[j]->f = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->f;
            statistics_tables[i]->column_statistics[j]->d = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->d;
            statistics_tables[i]->column_statistics[j]->d_array = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->d_array;
            statistics_tables[i]->column_statistics[j]->d_array_size = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->d_array_size;
            statistics_tables[i]->column_statistics[j]->initialSizeExceededMax = tables[statistics_tables[i]->realNumOfTable]->column_statistics[j]->initialSizeExceededMax;
        }
    }
    return statistics_tables;
}


QueryTableStatistics **copyStatisticsTables(QueryTableStatistics **old_statistics_tables, int numOfTablesToBeUsed)
{
    QueryTableStatistics **statistics_tables = myMalloc(sizeof(QueryTableStatistics *) * numOfTablesToBeUsed);

    for (int i = 0; i < numOfTablesToBeUsed; i++) {

        statistics_tables[i] = myMalloc(sizeof(QueryTableStatistics));

        // Set the "real"-tables' numbers
        statistics_tables[i]->realNumOfTable = old_statistics_tables[i]->realNumOfTable;
        //fprintf(fp_print,"table[%d] has real name: %d\n", i, statistics_tables[i]->realNumOfTable);    // DEBUG!

        statistics_tables[i]->num_columns = old_statistics_tables[i]->num_columns;
        statistics_tables[i]->column_statistics = myMalloc(sizeof(ColumnStats *) * statistics_tables[i]->num_columns);
        for (int j = 0; j < statistics_tables[i]->num_columns; j++) {
            statistics_tables[i]->column_statistics[j] = myMalloc(sizeof(ColumnStats));
            statistics_tables[i]->column_statistics[j]->l = old_statistics_tables[i]->column_statistics[j]->l;
            statistics_tables[i]->column_statistics[j]->u = old_statistics_tables[i]->column_statistics[j]->u;
            statistics_tables[i]->column_statistics[j]->f = old_statistics_tables[i]->column_statistics[j]->f;
            statistics_tables[i]->column_statistics[j]->d = old_statistics_tables[i]->column_statistics[j]->d;
            statistics_tables[i]->column_statistics[j]->d_array = old_statistics_tables[i]->column_statistics[j]->d_array;
            statistics_tables[i]->column_statistics[j]->d_array_size = old_statistics_tables[i]->column_statistics[j]->d_array_size;
            statistics_tables[i]->column_statistics[j]->initialSizeExceededMax = old_statistics_tables[i]->column_statistics[j]->initialSizeExceededMax;
        }
    }
    return statistics_tables;
}


void freeStatisticsTables(QueryTableStatistics **statistics_tables, int numOfTablesToBeUsed) {
	
    for (int i = 0; i < numOfTablesToBeUsed; i++) {
        for (int j = 0; j < statistics_tables[i]->num_columns; j++) {
            free(statistics_tables[i]->column_statistics[j]);
        }
        free(statistics_tables[i]->column_statistics);
        free(statistics_tables[i]);
    }
    free(statistics_tables);
}


Query_Info *bestTree(Query_Info *query_info, QueryTableStatistics ***statistics_tables, int numOfTablesToBeUsed) {

    /* There is less than 1 joins, then return the same query_info */
    if (query_info->join_count <= 1)
        return NULL;

    Adjacent **adjacency_matrix;
    int left, right, i, j;
    int filter_joins_count = 0, filter_joins_max_count = 2;
    int *joins_that_are_filters = myMalloc(sizeof(int) * 2);

    /* Create an adjacency matrix and fill it according to the joins */
    adjacency_matrix = myMalloc(sizeof(Adjacent *) * query_info->relationId_count);
    for (i = 0; i < query_info->relationId_count; i++) {
        adjacency_matrix[i] = myMalloc(sizeof(Adjacent) * query_info->relationId_count);
        for (j = 0; j < query_info->relationId_count; j++) {
            adjacency_matrix[i][j].is_neighbour = 0;
        }
    }

    /* For every join, we need to initialize the adjacency matrix */
    for (i = 0; i < query_info->join_count; i++) {
        left = query_info->joins[i][0];
        right = query_info->joins[i][2];
        /* If the join_number_count is 0, then it is the fist join that fills our matrix*/
        if (adjacency_matrix[left][right].is_neighbour == 0 &&
            adjacency_matrix[right][left].is_neighbour == 0) {

            adjacency_matrix[left][right].is_neighbour = 1;
            adjacency_matrix[left][right].join_number = i;
            adjacency_matrix[right][left].is_neighbour = 1;
            adjacency_matrix[right][left].join_number = i;
        } else {
            /*Else it is a filter */
            // fprintf(fp_print, "\nJoin: %d is a filter.\n", i);
            if (filter_joins_count == filter_joins_max_count) {
                filter_joins_max_count = filter_joins_max_count * 2;
                joins_that_are_filters = myRealloc(joins_that_are_filters, sizeof(int) * filter_joins_max_count);
            }
            joins_that_are_filters[filter_joins_count] = i;
            filter_joins_count++;
        }
    }

    // fprintf(fp_print, "\n  ");
    for (i = 0; i < query_info->relationId_count; i++){

        // fprintf(fp_print, " %d ", i);
    }

    // fprintf(fp_print, "\n");

    for (i = 0; i < query_info->relationId_count; i++) {
        //fprintf(fp_print, "%d |", i);
        for (j = 0; j < query_info->relationId_count; j++) {
            //fprintf(fp_print, "%d| ", adjacency_matrix[i][j].is_neighbour);
        }
        //fprintf(fp_print, "\n");
    }
    //fprintf(fp_print, "\n");

    int set_count = 0, k;
    bool is_same;
    unsigned int number = 0;

    /* Set number is the number of joins minus the number of joins that are filters */
    int set_number = query_info->join_count - filter_joins_count;

    /* Create an array of sets that will contain the info for every set */
    Set *sets = myMalloc(set_number * sizeof(Set));

    for (i = 0; i < set_number; i++) {
        sets[i].set_number = 0;
        sets[i].join_order_count = 0;
    }

    /* First, we want to find sets of 2 */
    /* So for every relation we check to find a neighbour in the adjacency matrix */
    for (i = 0; i < query_info->relationId_count; i++) {

        for (j = 0; j < query_info->relationId_count; j++) {

            /* If a relation is adjacent to another, we want to insert a new set in the array of sets */
            if (i != j && adjacency_matrix[i][j].is_neighbour) {

                is_same = FALSE;
                // fprintf(fp_print, "Join %d: (%d, %d)\n", adjacency_matrix[i][j].join_number, i, j);

                number = 0 | (unsigned int) myPow(2, i);
                number = number | (unsigned int) myPow(2, j);

                /* If there are no other sets, then we "insert" it in the array */
                if (set_count == 0) {
                    sets[set_count].set_number = number;
                    sets[set_count].join_order = myMalloc(sizeof(int) * query_info->join_count);
                    sets[set_count].join_order[sets[set_count].join_order_count] = adjacency_matrix[i][j].join_number;
                    sets[set_count].relations_in_set = 2;
                    sets[set_count].join_order_count++;
                    set_count++;
                } else {

                    /* Otherwise we need to check so that the set doesn't already exist in our sets */
                    for (k = 0; k < set_count; k++) {
                        /* We check that the set_number is different and the join number is different */
                        if (((sets[k].set_number >> j) & 1)) {
                            is_same = TRUE;
                            break;
                        }
                    }
                    if (is_same)
                        continue;

                    sets[set_count].set_number = number;
                    sets[set_count].join_order = myMalloc(sizeof(int) * query_info->join_count);
                    sets[set_count].join_order[sets[set_count].join_order_count] = adjacency_matrix[i][j].join_number;
                    sets[set_count].relations_in_set = 2;
                    sets[set_count].join_order_count++;
                    //sets[i].join_order_count++;
                    set_count++;
                }
            }
        }
    }

    //fprintf(fp_print, "\n");

    int pow, left_join, right_join;
    int joinTableNum1, colNum1, joinTableNum2, colNum2, realTableNum1, realTableNum2;

    /* After we find all the sets of 2, we want to join the relations in our sets to get the statistics */
    for (i = 0; i < set_count; i++) {

        left_join = -1;
        right_join = -1;

        for (j = 0; j < query_info->relationId_count; j++) {
            pow = (int) myPow(2, j);
            if ((sets[i].set_number & pow) == pow) {
                if (left_join == -1)
                    left_join = j;
                else if (right_join == -1)
                    right_join = j;
            }
        }

        // fprintf(fp_print, "Set[%d]: %d, (%d, %d) - Join #: %d, ", i, sets[i].set_number, left_join, right_join, sets[i].join_order[0]);

        // Gather statistics for every join.
        sets[i].tableStatistics = copyStatisticsTables(*statistics_tables, numOfTablesToBeUsed);

        joinTableNum1 = query_info->joins[sets[i].join_order[0]][0];
        colNum1 = query_info->joins[sets[i].join_order[0]][1];
        joinTableNum2 = query_info->joins[sets[i].join_order[0]][2];
        colNum2 = query_info->joins[sets[i].join_order[0]][3];

        realTableNum1 = query_info->relation_IDs[joinTableNum1];
        realTableNum2 = query_info->relation_IDs[joinTableNum2];

        gatherStatisticsForJoinBetweenDifferentTables(&sets[i].tableStatistics, realTableNum1, realTableNum2, joinTableNum1, joinTableNum2, colNum1, colNum2);

        sets[i].cost_of_join = 0;
        sets[i].size_of_join_result = sets[i].tableStatistics[joinTableNum1]->column_statistics[colNum1]->f;

        // printPredicatesStatistics(sets[i].tableStatistics, numOfTablesToBeUsed);

        // fprintf(fp_print, "Size of join result: %ju\n", sets[i].size_of_join_result);

        // fprintf(fp_print, "==============================================================\n");
    }


    int index = 0, l, m;
    int relation_id_starting_number = 2, *relation_ids_in_set;
    int new_set_count, new_set_max_count;
    Set *new_sets;
    //sets[2].size_of_join_result = 500; // DEBUGGING

    /* Next, we need to find sets of 3, 4, etc... */
    for (;;) {

        break;

        relation_ids_in_set = myMalloc(sizeof(int) * relation_id_starting_number);

        /* Create an array of new sets that will contain the info for every new set */
        new_set_count = 0;
        new_set_max_count = 2;
        new_sets = myMalloc(sizeof(Set) * new_set_max_count);
        /* To do that, we want to check the neighbours of the relations in our sets */
        for (i = 0; i < set_count; i++) {

            /* So we create an array with the relation ids that exist in our set */
            for (j = 0; j < query_info->relationId_count; j++) {

                if (sets[i].set_number & (1 << j)) {
                    relation_ids_in_set[index] = j;
                    index++;
                }
            }

            /* Then we need to find neighbours of the relations in our set */
            for (k = 0; k < index; k++) {

                /* So for every relation */
                for (j = 0; j < query_info->relationId_count; j++) {

                    is_same = FALSE;

                    /* We check that it doesn't exist in our set already and that it is adjacent of the current relation */
                    if (!((sets[i].set_number >> j) & 1) && adjacency_matrix[relation_ids_in_set[k]][j].is_neighbour) {

                        // Kai an den yparxei hdh sto current set, tote ftia3e ena kainoyrgio set me (relations_in_set + 1) ari8mo relations
                        // Prepei na psa3eis na mhn yparxei se ola ta joins mesa sto join order, oxi mono to prwto(gia na leitoyrgei dynamika)
                        // h tsekare me bash ton ari8mo, bitwise (https://stackoverflow.com/questions/523724/c-c-check-if-one-bit-is-set-in-i-e-int-variable)

                        /*
                         * sets[i].set_number & (1 << j)  will return the bit position or 0 depending on if the bit is actually enabled
                         * ((sets[i].set_number >> j) & 1)  will return either a 1 or 0 if the bit is enabled and not the position
                         */

                        fprintf(fp_print, "Join %d", sets[i].join_order[0]);
                        fprintf(fp_print, " -> Join %d\n", adjacency_matrix[relation_ids_in_set[k]][j].join_number);
                        fprintf(fp_print, "(%d, %d)", query_info->joins[sets[i].join_order[0]][0], query_info->joins[sets[i].join_order[0]][2]);
                        fprintf(fp_print, ", %d\n", j);
                        // new_sets[new_set_count].join_order_count;

                        /* If it is only the first set, then just insert */
                        if (new_set_count == 0) {

                            new_sets[new_set_count].set_number = sets[i].set_number | (1 << j);

                            new_sets[new_set_count].join_order = myMalloc(sizeof(int) * query_info->join_count);
                            for (m = 0; m < sets[i].join_order_count; m++)
                                new_sets[new_set_count].join_order[m] = sets[i].join_order[m];

                            new_sets[new_set_count].join_order[m] = adjacency_matrix[relation_ids_in_set[k]][j].join_number;
                            new_sets[new_set_count].join_order_count = sets[i].join_order_count + 1;
                            new_sets[new_set_count].relations_in_set = sets[i].relations_in_set + 1;
                            new_sets[new_set_count].cost_of_join = sets[i].cost_of_join + sets[i].size_of_join_result;

                            new_sets[new_set_count].tableStatistics = copyStatisticsTables(sets[i].tableStatistics, numOfTablesToBeUsed);

                            new_set_count++;

                            continue;
                        }

                        for (l = 0; l < new_set_count; l++) {

                            //fprintf(fp_print,"L is %d.\n", l);

                            //fprintf(fp_print,"Number1: %d, number2: %d\n", new_sets[l].set_number, sets[i].set_number);

                            if (new_sets[l].set_number == (sets[i].set_number | (1 << j))) {
                                fprintf(fp_print, "Same number.\n");
                                is_same = TRUE;
                                break;
                            }
                        }
                        if (is_same) {
                            if (sets[i].cost_of_join + sets[i].size_of_join_result > new_sets[l].cost_of_join) {
                                //fprintf(fp_print,"Old cost_of_join: %ju\n", new_sets[l].cost_of_join);
                                //fprintf(fp_print,"New cost_of_join: %ju\n", sets[i].cost_of_join + sets[i].size_of_join_result);
                                continue;
                            } else {
                                fprintf(fp_print, "New set is better!\n");

                                new_sets[l].set_number = sets[i].set_number | (1 << j);

                                for (m = 0; m < sets[i].join_order_count; m++)
                                    new_sets[l].join_order[m] = sets[i].join_order[m];

                                new_sets[l].join_order[m] = adjacency_matrix[relation_ids_in_set[k]][j].join_number;
                                new_sets[l].join_order_count = sets[i].join_order_count + 1;
                                new_sets[l].relations_in_set = sets[i].relations_in_set + 1;
                                new_sets[l].cost_of_join = sets[i].cost_of_join + sets[i].size_of_join_result;

                                freeStatisticsTables(new_sets[l].tableStatistics, numOfTablesToBeUsed);
                                new_sets[l].tableStatistics = copyStatisticsTables(sets[i].tableStatistics, numOfTablesToBeUsed);

                                continue;
                            }

                        }
                        if (new_set_count == new_set_max_count) {
                            new_set_max_count *= 2;
                            new_sets = myRealloc(new_sets, sizeof(Set) * new_set_max_count);
                        }

                        new_sets[new_set_count].set_number = sets[i].set_number | (1 << j);

                        new_sets[new_set_count].join_order = myMalloc(sizeof(int) * query_info->join_count);
                        for (m = 0; m < sets[i].join_order_count; m++)
                            new_sets[new_set_count].join_order[m] = sets[i].join_order[m];

                        new_sets[new_set_count].join_order[m] = adjacency_matrix[relation_ids_in_set[k]][j].join_number;
                        new_sets[new_set_count].join_order_count = sets[i].join_order_count + 1;
                        new_sets[new_set_count].relations_in_set = sets[i].relations_in_set + 1;
                        new_sets[new_set_count].cost_of_join = sets[i].cost_of_join + sets[i].size_of_join_result;

                        new_sets[new_set_count].tableStatistics = copyStatisticsTables(sets[i].tableStatistics, numOfTablesToBeUsed);

                        new_set_count++;
                    }

                }
            }

            index = 0;
        }

        for (i = 0; i < set_count; i++) {
            freeStatisticsTables(sets[i].tableStatistics, numOfTablesToBeUsed);
            free(sets[i].join_order);
        }
        free(sets);
        sets = new_sets;
        set_count = new_set_count;
        new_sets = NULL;
        relation_id_starting_number++;
        free(relation_ids_in_set);

        int dummy_int = 0;
        for (i = 0; i < set_count; i++) {

            dummy_int = 0;

            fprintf(fp_print, "Set %d - Relations in set: %d and set: (", i, sets[i].relations_in_set);
            for (j = 0; j < query_info->relationId_count; j++) {

                if (sets[i].set_number & (1 << j)) {
                    fprintf(fp_print, "%d", j);
                    dummy_int++;
                    if (dummy_int != sets[i].relations_in_set)
                        fprintf(fp_print, ", ");
                }
            }
            fprintf(fp_print, ")\n");

            fprintf(fp_print, " Best cost until now: %ju\n", sets[i].cost_of_join);

            for (j = 0; j < sets[i].join_order_count; j++)
                fprintf(fp_print, " Join %d", sets[i].join_order[j]);

            fprintf(fp_print, "\n");

        }

        break;
    }

    int best_set;

    best_set = 0;
    for (i = 1; i < set_count; i++) {
        if (sets[i].cost_of_join + sets[i].size_of_join_result < sets[best_set].cost_of_join + sets[best_set].size_of_join_result) {
            best_set = i;
        }
    }

    int current_join = 0;
    int **reordered_joins = myMalloc(sizeof(int *) * query_info->join_count);

    for (i = 0; i < query_info->join_count; i++)
        reordered_joins[i] = myMalloc(sizeof(int) * 4);

    reordered_joins[current_join][0] = query_info->joins[sets[best_set].join_order[0]][0];
    reordered_joins[current_join][1] = query_info->joins[sets[best_set].join_order[0]][1];
    reordered_joins[current_join][2] = query_info->joins[sets[best_set].join_order[0]][2];
    reordered_joins[current_join][3] = query_info->joins[sets[best_set].join_order[0]][3];
    current_join++;

    for (i = 0; i < set_count; i++) {
        if (i != best_set) {
            reordered_joins[current_join][0] = query_info->joins[sets[i].join_order[0]][0];
            reordered_joins[current_join][1] = query_info->joins[sets[i].join_order[0]][1];
            reordered_joins[current_join][2] = query_info->joins[sets[i].join_order[0]][2];
            reordered_joins[current_join][3] = query_info->joins[sets[i].join_order[0]][3];
            current_join++;
        }
    }

    for (i = 0; i < filter_joins_count; i++) {
        reordered_joins[current_join][0] = query_info->joins[joins_that_are_filters[i]][0];
        reordered_joins[current_join][1] = query_info->joins[joins_that_are_filters[i]][1];
        reordered_joins[current_join][2] = query_info->joins[joins_that_are_filters[i]][2];
        reordered_joins[current_join][3] = query_info->joins[joins_that_are_filters[i]][3];
    }


    for (i = 0; i < query_info->join_count; i++)
        free(query_info->joins[i]);
    free(query_info->joins);

    query_info->joins = reordered_joins;

    //print_query(query_info, "0 13 7 10|0.0=1.2&0.0=2.1&0.0=3.2&1.2>295|3.2 0.0", 40);

    for (i = 0; i < set_count; i++) {

        freeStatisticsTables(sets[i].tableStatistics, numOfTablesToBeUsed);
        free(sets[i].join_order);
    }
    free(sets);
    free(joins_that_are_filters);


    for (i = 0; i < query_info->relationId_count; i++)
        free(adjacency_matrix[i]);

    free(adjacency_matrix);

    return query_info;
}