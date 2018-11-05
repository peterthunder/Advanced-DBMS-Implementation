#ifndef ADVANCED_DBMS_IMPLEMENTATION_TEST_SUPPORT_FUNCTIONS_H
#define ADVANCED_DBMS_IMPLEMENTATION_TEST_SUPPORT_FUNCTIONS_H

#include "../../../../../supportFunctions.h"
#include "../../../../../radixHashJoin.h"

void initializeRelations(Relation **relation1, Relation **relation2);
void freeMemory(Relation *relation1, Relation *relation2, int32_t **histogram1, int32_t **histogram2,  int32_t **psum1, int32_t **psum2);
void fillHistograms(int32_t ***histogram1, int32_t ***histogram2, Relation *relation1);
void fillPsums(int32_t ***psum1, int32_t ***psum2, int32_t **histogram2);

#endif //ADVANCED_DBMS_IMPLEMENTATION_TEST_SUPPORT_FUNCTIONS_H
