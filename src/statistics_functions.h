#ifndef ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H

#include "radixHashJoin.h"

void gatherInitialStatisticsForTable(Table **table);

void gatherPredicatesStatisticsForQuery(Query_Info **qInfo, Table **tables, int query_count);

void gatherStatisticsForFilters(Query_Info **qInfo, Table **tables, QueryTableStatistics ***statistic_tables);

void gatherStatisticsForFilterOperatorEqual(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables,
                                            Table **tables);

void gatherStatisticsForFilterOperatorLess(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables,
                                           Table **tables);

void gatherStatisticsForFilterOperatorGreater(int usedTableNum, int realTableNum, int filterColNum, uint64_t k, QueryTableStatistics ***statistic_tables,
                                              Table **tables);

bool does_k_belongs_to_d_array(long k ,int realTableNum, int colNum, Table **tables);

void setStatisticsForOtherColumnsOfTheFilteredTable(int usedTableNum, int realTableNum, int filterColNum, QueryTableStatistics ***statistic_tables, Table **tables);

QueryTableStatistics** createStatisticsTables(Query_Info *qInfo, Table **tables, int numOfTablesToBeUsed);


#endif //ADVANCED_DBMS_IMPLEMENTATION_STATISTICS_FUNCTIONS_H
