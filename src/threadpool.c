#include "threadpool.h"

static volatile int keep_alive; /* Argument to tell the threads to continue their work or stop their work*/

/* Thread function */
static void thread_do(thread *thread_);

/* Jobqueue functions */
static void jobqueue_init(Jobqueue *jobqueue);

void jobqueue_push(void (*function)(void *), void *arg, Jobqueue *jobqueue);

static Job *jobqueue_pull(Jobqueue *jobqueue);

/* Implementations */
Threadpool * threadpool_init(int num_of_threads) {

    //fprintf(fp_print, "Running threadpool_init()...\n");

    int i;
    if (num_of_threads < 0)
        num_of_threads = 0;

    /* Create the threadpool struct*/
    Threadpool *threadpool = myMalloc(sizeof(Threadpool));

    /* Array with pointers to the threads */
    threadpool->pthreads = myMalloc(sizeof(thread *) * num_of_threads);

    threadpool->num_of_threads_alive = 0;

    /* Initialize the mutex for accessing num_of_threads argument */
    pthread_mutex_init(&(threadpool->threadmtx), NULL);


    threadpool->jobs_done = 0;
    /* Initialize the mutex for accessing jobs_done argument */
    pthread_mutex_init(&(threadpool->jobs_done_mtx), NULL);

    /* Initialize the mutex for waiting until all jobs are done */
    pthread_cond_init(&(threadpool->all_jobs_done), NULL);

    /* Initialise the job queue */
    jobqueue_init(&threadpool->jobqueue);


    //fprintf(fp_print,"Creating the threads...\n");

    keep_alive = 1;

    /* Create threads */
    for (i = 0; i < num_of_threads; i++) {
        //fprintf(fp_print, "Creating thread %d...\n", i);
        threadpool->pthreads[i] = myMalloc(sizeof(thread));
        threadpool->pthreads[i]->num = i;
        threadpool->pthreads[i]->threadpool = threadpool;
        pthread_create(&threadpool->pthreads[i]->pthread, NULL, (void *) thread_do,
                       (void *) threadpool->pthreads[i]); /* Else if serve do job 2*/

        pthread_detach(threadpool->pthreads[i]->pthread);
    }


    //fprintf(fp_print, "Waiting for all threads to be created!\n");
    /* Wait until all the threads were created */
    while (threadpool->num_of_threads_alive != num_of_threads) {
        //printf("Threads alive: %d\n", threadpool->num_of_threads_alive);
    }

    return threadpool;
}

void threadpool_add_job(Threadpool *threadpool, void (*function)(void *), void *arg) {
    //printf("Adding job to the queue\n");
    jobqueue_push(function, arg, &threadpool->jobqueue);
}

void threadpool_destroy(Threadpool *threadpool) {

    //fprintf(fp_print, "Destroying threadpool... \n");

    int i;

    volatile int threads_total = threadpool->num_of_threads_alive;

    /* End each thread 's infinite loop */
    keep_alive = 0;

    /* Wait for all threads to end */
    /* Give one second to kill idle threads */
    double TIMEOUT = 1.0;
    time_t start, end;
    double tpassed = 0.0;
    time (&start);
    /* Wait for all threads to end */
    while (tpassed < TIMEOUT && threadpool->num_of_threads_alive) {
        //printf("Waiting for all threads to end! - Threads alive: %d \n", threadpool->num_of_threads_alive);
        pthread_mutex_lock(&threadpool->jobqueue.wait_jobmtx);
        threadpool->jobqueue.has_jobs = 1;
        pthread_cond_signal(&threadpool->jobqueue.empty);
        pthread_mutex_unlock(&threadpool->jobqueue.wait_jobmtx);
        time (&end);
        tpassed = difftime(end,start);
    }

    while (threadpool->num_of_threads_alive) {
        pthread_mutex_lock(&threadpool->jobqueue.wait_jobmtx);
        threadpool->jobqueue.has_jobs = 1;
        pthread_cond_signal(&threadpool->jobqueue.empty);
        pthread_mutex_unlock(&threadpool->jobqueue.wait_jobmtx);
        sleep(1);
    }

    /* Clear the queue*/
    /* Delete everything that was left in the queue */
    while (threadpool->jobqueue.len > 0) {
        //printf("Deleting what was left in the queue!\n");
        free(jobqueue_pull(&threadpool->jobqueue));
    }

    /* Free jobqueue mutexes and conditions*/
    pthread_cond_destroy(&threadpool->jobqueue.empty);
    pthread_mutex_destroy(&threadpool->jobqueue.wait_jobmtx);
    pthread_mutex_destroy(&threadpool->jobqueue.rw_jobmtx);

    /*Delete the thread pool*/
    for (i = 0; i < threads_total; i++) {
        free(threadpool->pthreads[i]);
    }

    free(threadpool->pthreads);
    pthread_mutex_destroy(&threadpool->threadmtx);
    pthread_cond_destroy(&threadpool->all_jobs_done);
    pthread_mutex_destroy(&threadpool->jobs_done_mtx);

    free(threadpool);
}

void thread_do(thread *thread_) {

    Threadpool *threadpool = thread_->threadpool;
    /* Lock and unlock mutex when accessing threadpool argument*/
    pthread_mutex_lock(&threadpool->threadmtx);
    //fprintf(fp_print, "I am server thread and i am alive!\n");
    threadpool->num_of_threads_alive++;
    pthread_mutex_unlock(&threadpool->threadmtx);

    /* Endless loop until we have to finish*/
    while (keep_alive) {
        /* Wait for a job */
        pthread_mutex_lock(&threadpool->jobqueue.wait_jobmtx);
        while (threadpool->jobqueue.has_jobs != 1) {
            pthread_cond_wait(&threadpool->jobqueue.empty, &threadpool->jobqueue.wait_jobmtx);
        }
        threadpool->jobqueue.has_jobs = 0;
        pthread_mutex_unlock(&threadpool->jobqueue.wait_jobmtx);
        /* If i should continue */
        if (keep_alive) {

            Job *job = jobqueue_pull(&threadpool->jobqueue);
            //printf("I am thread %d and i got a job!\n", thread_->num);
            if (job != NULL) {
                /* Do whatever you have to do */
                void (*func_buff)(void *);
                void *arg_buff;
                func_buff = job->function;
                arg_buff = job->arg;
                func_buff(arg_buff);

                free(job);
            }
        }
    }

    //fprintf(fp_print, "Keep alive is now 0!\n");

    /* Lock and unlock mutex when accessing threadpool argument*/
    pthread_mutex_lock(&threadpool->threadmtx);
    threadpool->num_of_threads_alive--;
    pthread_mutex_unlock(&threadpool->threadmtx);

    //fprintf(fp_print, "I am thread %d and i am done!\n", thread_->num);
}

void jobqueue_init(Jobqueue *jobqueue) {

    //fprintf(fp_print,"Initializing jobqueue...\n");

    /* Initialize queue */
    jobqueue->len = 0;
    jobqueue->front = NULL;
    jobqueue->rear = NULL;
    jobqueue->has_jobs = 0;

    /* Initialize the mutex for accessing the jobqueue */
    pthread_mutex_init(&(jobqueue->rw_jobmtx), NULL);

    /* Initialize the mutex for waiting for items */
    pthread_mutex_init(&(jobqueue->wait_jobmtx), NULL);

    /* Condition to wait on when jobqueue is empty*/
    pthread_cond_init(&(jobqueue->empty), NULL);
}

void jobqueue_push(void (*function)(void *), void *arg, Jobqueue *jobqueue) {

    //printf("Pushing a job to the queue...\n");

    /* Create a new job with the passed variables */
    Job *job = myMalloc(sizeof(Job));

    job->previous = NULL;

    job->function = function;
    job->arg = arg;

    /* Lock the mutex while accessing jobqueue */
    pthread_mutex_lock(&jobqueue->rw_jobmtx);


    /* If there are no jobs in the queue*/
    if (jobqueue->len == 0) {
        jobqueue->rear = job;
        jobqueue->front = job;
    }
        /* If there is at least one job in the queue*/
    else {
        jobqueue->rear->previous = job;
        jobqueue->rear = job;
    }

    /* Increase the length of the queue arg*/
    jobqueue->len++;

    /* Signal threads waiting on empty queue*/
    pthread_mutex_lock(&jobqueue->wait_jobmtx);
    jobqueue->has_jobs = 1;
    pthread_cond_signal(&jobqueue->empty);
    pthread_mutex_unlock(&jobqueue->wait_jobmtx);

    /* Unlock the mutex */
    pthread_mutex_unlock(&jobqueue->rw_jobmtx);
}

static Job *jobqueue_pull(Jobqueue *jobqueue) {

    //printf("Pulling a job from the queue...\n");

    /* Lock the mutex while accessing jobqueue*/
    pthread_mutex_lock(&jobqueue->rw_jobmtx);

    /* Get the job in the front of the queue */
    Job *job = jobqueue->front;

    /* If there is only one job in the queue, then initialize rear and front to NULL*/
    if (jobqueue->len == 1) {
        jobqueue->front = NULL;
        jobqueue->rear = NULL;
        jobqueue->len = 0;
    } else if (jobqueue->len > 1) {
        jobqueue->front = job->previous;
        jobqueue->len--;

        /* Signal that there are jobs in the queue */
        pthread_mutex_lock(&jobqueue->wait_jobmtx);
        jobqueue->has_jobs = 1;
        pthread_cond_signal(&jobqueue->empty);
        pthread_mutex_unlock(&jobqueue->wait_jobmtx);
    }

    /* Unlock the mutex */
    pthread_mutex_unlock(&jobqueue->rw_jobmtx);

    return job;
}
