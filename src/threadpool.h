#ifndef ADVANCED_DBMS_IMPLEMENTATION_THREADPOOL_H
#define ADVANCED_DBMS_IMPLEMENTATION_THREADPOOL_H

#include "supportFunctions.h"

typedef struct thread{
    pthread_t pthread;
    int num;                        /* Id of the thread, for debugging */
    struct Threadpool *threadpool; /* Pointer to the actual thread*/
} thread;

typedef struct Job {
    struct Job *previous;                   /* Pointer to the previous job */
    void   (*function)(void* arg);
    void (*arg);
} Job;

typedef struct Jobqueue {
    pthread_mutex_t rw_jobmtx;           /* Mutex for use when accessing the queue */
    pthread_mutex_t wait_jobmtx;        /* Mutex to wait on when queue is empty */
    int has_jobs;                      /* While this is equal to 0, then wait */
    Job *front;                       /* Pointer to the front of the queue */
    Job *rear;                       /* Pointer to the back of the queue */
    int len;                        /* Number of jobs in the queue */
    pthread_cond_t empty;          /* Condition to wait when jobqueue is empty */
} Jobqueue;


typedef struct Threadpool {
    thread **pthreads;                     /* Array of pointers to the threads */
    Jobqueue jobqueue;                    /* Queue for the jobs of the threads */
    volatile int num_of_threads_alive;   /* Wait in thread_pool_init until all threads were created */
    pthread_mutex_t threadmtx;          /* Mutex for use when accessing the threads_alive argument */

    volatile int jobs_done;
    pthread_mutex_t jobs_done_mtx;
    pthread_cond_t all_jobs_done;

} Threadpool;


/* Threadpool functions */
Threadpool* threadpool_init(int num_of_threads);
void threadpool_destroy(Threadpool *threadpool);
void threadpool_add_job(Threadpool *threadpool, void (*function)(void*), void *arg);

#endif //ADVANCED_DBMS_IMPLEMENTATION_THREADPOOL_H
