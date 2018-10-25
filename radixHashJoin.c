#include "radixHashJoin.h"


/* Relation R: reIR, Relation S: reIS, Number of buckets: 2^n */
Result *RadixHashJoin(Relation *reIR, Relation *reIS, int number_of_buckets) {

    int i, j;

    /* Allocate memory for new matrices R' and S' that will be used as hash tables */
    Relation *relationNewR = malloc(sizeof(Relation));
    if (relationNewR == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    Relation *relationNewS = malloc(sizeof(Relation));
    if (relationNewS == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Matrix R' and S sizes'*/
    relationNewR->num_tuples = reIR->num_tuples;
    relationNewS->num_tuples = reIS->num_tuples;

    relationNewR->tuples = malloc(sizeof(Tuple) * relationNewR->num_tuples);
    if (relationNewR->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }
    relationNewS->tuples = malloc(sizeof(Tuple) * relationNewS->num_tuples);
    if (relationNewS->tuples == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Create Relation R histogram and psum */
    int32_t **histogramR = createHistogram(reIR, number_of_buckets);
    if (histogramR == NULL) {
        return NULL;
    }
    int32_t **psumR = createPsum(number_of_buckets, histogramR);
    if (psumR == NULL) {
        return NULL;
    }
    /* Create Relation S histogram and psum */
    int32_t **histogramS = createHistogram(reIS, number_of_buckets);
    if (histogramS == NULL) {
        return NULL;
    }
    int32_t **psumS = createPsum(number_of_buckets, histogramS);
    if (psumS == NULL) {
        return NULL;
    }

    /* Partition the relations */
    partition(reIR, relationNewR, number_of_buckets, psumR);
    partition(reIS, relationNewS, number_of_buckets, psumS);

    printAll(1, reIR, reIS, histogramR, histogramS, psumR, psumS, relationNewR, relationNewS, number_of_buckets);

    printf("\n\n");

    int32_t hashValAppearances;
    /* Create an array that contains the chain arrays */
    int **chain = malloc(sizeof(int *) * number_of_buckets);
    if (chain == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Create an array that contains bucket_index arrays of size H2_PARAM(for example 101) each */
    int **bucket_index = malloc(sizeof(int *) * number_of_buckets);
    if (bucket_index == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    /* Initialise pointers to NULL */
    for (i = 0; i < number_of_buckets; i++) {
        chain[i] = NULL;
        bucket_index[i] = NULL;
    }

    /* Build the bucket_index and the chain arrays of the smaller relation */
    if (1) {
        for (i = 0; i < number_of_buckets; i++) {

            /* Calculate the appearances of the hashvalue of a certain bucket according to the Psum */
            if (i == number_of_buckets - 1)
                hashValAppearances = relationNewR->num_tuples - psumR[i][1];
            else
                hashValAppearances = psumR[i + 1][1] - psumR[i][1];

            /* Allocate memory for the ith-chain array, same size as the ith-bucket,
             * only if the hashValueAppearances are greater than 0 */
            if (hashValAppearances <= 0)
                continue;
            else {
                chain[i] = malloc(sizeof(int) * hashValAppearances);
                if (chain[i] == NULL) {
                    printf("Malloc failed!\n");
                    perror("Malloc");
                    return NULL;
                }

                for (j = 0; j < hashValAppearances; j++) {
                    chain[i][j] = j;
                }

                /* Also, create a bucket_index array only if the hashValueAppearances are greater than 0,
                 * to save memory */
                bucket_index[i] = malloc(sizeof(int) * H2_PARAM);
                if (bucket_index[i] == NULL) {
                    printf("Malloc failed!\n");
                    perror("Malloc");
                    return NULL;
                }

                for (j = 0; j < H2_PARAM; j++)
                    bucket_index[i][j] = 0;
            }
        }

        //buildIndexAndChain(relationNewR, chain, bucket_index);

    } else {

    }

    printChainArrays(number_of_buckets, psumR, relationNewR, chain);

    /* De-allocate memory */
    for (i = 0; i < number_of_buckets; i++) {
        free(histogramR[i]);
        free(histogramS[i]);
        free(psumR[i]);
        free(psumS[i]);
        if (chain[i] != NULL)
            free(chain[i]);
        if (bucket_index[i] != NULL)
            free(bucket_index[i]);
    }

    free(chain);
    free(bucket_index);

    free(histogramR);
    free(psumR);

    free(histogramS);
    free(psumS);

    free(relationNewR->tuples);
    free(relationNewS->tuples);

    free(relationNewR);
    free(relationNewS);

}

/* Partition Relation */
void *partition(Relation *relation, Relation *relationNew, int number_of_buckets, int32_t **psum) {

    int i, j;
    unsigned int indexOfNewR = 0;
    int32_t currHashAppearances = 0, currHashCounter = 0;

    /* Fill out the new Relation using the psum */
    for (i = 0; i < number_of_buckets; i++) {

        currHashCounter = 0;

        /* If this is the last bucket, find the number of occurrences*/
        if (i == number_of_buckets - 1)
            currHashAppearances = relation->num_tuples - psum[i][1];
        else
            currHashAppearances = psum[i + 1][1] - psum[i][1];

        if (currHashAppearances == 0)
            continue;

        /* Search occurrences of this bucket's key inside the relation table. */
        for (j = 0; j < relation->num_tuples; j++) {

            /* If all occurrences have been found, go to the next bucket.*/
            if (currHashCounter == currHashAppearances)
                break;

            /*If we find the current bucket's key in relation-table, append the relation-table's data to the new relation-table. */
            if (relation->tuples[j].key == psum[i][0]) {
                relationNew->tuples[indexOfNewR].key = relation->tuples[j].key;
                relationNew->tuples[indexOfNewR].payload = relation->tuples[j].payload;
                indexOfNewR++;
                currHashCounter++;
            }
        }
    }
}

/* Create histogram of Relation */
int32_t **createHistogram(Relation *relation, int number_of_buckets) {

    int i;

    /*Allocate memory for an Histogram-2dArray with size (number_of_buckets * 2) */
    int32_t **histogram = malloc(sizeof(int32_t *) * number_of_buckets);
    if (histogram == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }


    for (i = 0; i < number_of_buckets; i++) {
        histogram[i] = malloc(sizeof(int32_t) * 2);
        if (histogram[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }

        /* Histogram contains Hash key-Number of hash key appearances */
        histogram[i][0] = i;
        histogram[i][1] = 0;
    }



    /* Fill out the histogram according to the hash values*/
    for (i = 0; i < relation->num_tuples; i++) {
        histogram[relation->tuples[i].key][1]++;
    }

    return histogram;
}

/* Create psum of Relation using its histogram */
int32_t **createPsum(int number_of_buckets, int32_t **histogram) {

    int i;

    /*Allocate memory for Psum-2dArray with size (number_of_buckets * 2) */
    int32_t **psum = malloc(sizeof(int32_t *) * number_of_buckets);
    if (psum == NULL) {
        printf("Malloc failed!\n");
        perror("Malloc");
        return NULL;
    }

    for (i = 0; i < number_of_buckets; i++) {

        psum[i] = malloc(sizeof(int32_t) * 2);
        if (psum[i] == NULL) {
            printf("Malloc failed!\n");
            perror("Malloc");
            return NULL;
        }

        /* Psum contains Hash key-Index of bucket */
        psum[i][0] = i;
        psum[i][1] = 0;
    }

    for (i = 0; i < number_of_buckets; i++) {
        if (i == 0) {
            continue;
        } else {
            psum[i][1] = psum[i - 1][1] + histogram[i - 1][1];
        }
    }

    return psum;
}

/* Build the Index and the Chain Arrays of the relation with the less amount of tuples */
int buildIndexAndChain(Relation *relationNewR, int **Chain, int32_t **bucket) {
    return 0;
}
