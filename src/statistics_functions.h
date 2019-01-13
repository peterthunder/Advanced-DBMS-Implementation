#ifndef ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H

#include "radixHashJoin.h"

void gatherInitialStatisticsForTable(Table **table);

short gatherPredicatesStatisticsForQuery(Query_Info **qInfo, Table **tables, int query_count);

short gatherStatisticsForFilters(Query_Info **qInfo, Table **tables, QueryTableStatistics ***statistics_tables);

short gatherStatisticsForFilterOperatorEqual(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables);

short gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables);

short gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistics_tables, Table **tables);

bool does_k_belong_to_d_array(long k, int realTableNum, int colNum, Table **tables);

void setStatisticsForOtherColumnsOfTheFilteredTable(int usedTableNum, int realTableNum, int filterColNum, QueryTableStatistics ***statistics_tables, Table **tables);

void setDistinctWithComplexCalculation(uint64_t *receiver_d, uint64_t factor, uint64_t base_numerator, uint64_t base_denominator, uint64_t exponent_numerator,
                                       uint64_t exponent_denominator);

void gatherStatisticsForJoins(Query_Info **qInfo, QueryTableStatistics ***statistics_tables, int numOfTablesToBeUsed);

void gatherStatisticsForJoinAutocorrelation(QueryTableStatistics ***statistics_tables, int usedTableNum, int colNum);

void gatherStatisticsForJoinSameTableDiffColumns(QueryTableStatistics ***statistics_tables, int realTableNum, int usedTableNum, int colNum1, int colNum2);

void setStatisticsForOtherColumnsOfJoinSameTableDiffColumns(int usedTableNum, int realTableNum, int joinColumn, QueryTableStatistics ***statistics_tables, uint64_t f_join_old);

void gatherStatisticsForJoinBetweenDifferentTables(QueryTableStatistics ***statistics_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                                   int colNum2);

void setStatisticsForOtherColumnsOfTheJoinedTables(QueryTableStatistics ***statistics_tables, int realTableNum1, int realTableNum2, int usedTableNum1, int usedTableNum2, int colNum1,
                                                   int colNum2, uint64_t da, uint64_t db);

QueryTableStatistics** createStatisticsTables(Query_Info *qInfo, Table **tables, int numOfTablesToBeUsed);

void freeStatisticsTables(QueryTableStatistics** statistics_tables, int numOfTablesToBeUsed);

#endif //ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
