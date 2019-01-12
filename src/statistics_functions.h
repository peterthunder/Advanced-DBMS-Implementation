#ifndef ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H

#include "radixHashJoin.h"

void gatherInitialStatisticsForTable(Table **table);

short gatherPredicatesStatisticsForQuery(Query_Info **qInfo, Table **tables, int query_count);

short gatherStatisticsForFilters(Query_Info **qInfo, Table **tables, QueryTableStatistics ***statistic_tables);

short gatherStatisticsForFilterOperatorEqual(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables);

short gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables);

short gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables, Table **tables);

bool does_k_belong_to_d_array(long k, int realTableNum, int colNum, Table **tables);

void setStatisticsForOtherColumnsOfTheFilteredTable(int usedTableNum, int realTableNum, int filterColNum, QueryTableStatistics ***statistic_tables, Table **tables);

void setDistinctWithComplexCalculation(uint64_t *receiver_d, uint64_t factor, uint64_t base_numerator, uint64_t base_denominator, uint64_t exponent_numerator,
                                       uint64_t exponent_denominator);

void gatherStatisticsForJoins(Query_Info **qInfo, QueryTableStatistics ***statistic_tables, int numOfTablesToBeUsed);

void gatherStatisticsForJoinAutocorrelation(QueryTableStatistics ***statistic_tables, int usedTableNum, int colNum);

void gatherStatisticsForJoinBetweenDifferentTables(QueryTableStatistics ***statistic_tables, int usedTableNum1, int usedTableNum2, int colNum1, int colNum2);

void setStatisticsForOtherColumnsOfTheJoinedTables(QueryTableStatistics ***statistic_tables, int usedTableNum1, int usedTableNum2, int colNum1, int colNum2, uint64_t da, uint64_t db);

QueryTableStatistics** createStatisticsTables(Query_Info *qInfo, Table **tables, int numOfTablesToBeUsed);


#endif //ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
