
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#include "barrier.h"

barrier*
make_barrier(int nn)
{
    barrier* bb = malloc(sizeof(barrier));
    assert(bb != 0);

	pthread_mutex_t lock;
	pthread_cond_t condv = PTHREAD_COND_INITIALIZER;

	pthread_mutex_init(&lock, 0);

	bb->lock = lock;
	bb->condv = condv;
    bb->count = nn; 
    bb->seen  = 0;
    return bb;
}

void
barrier_wait(barrier* bb)
{
	bb->seen ++;

	pthread_mutex_lock(&(bb->lock));

    while (bb->seen < bb->count) {
        pthread_cond_wait(&(bb->condv), &(bb->lock));	
    }

	pthread_cond_broadcast(&(bb->condv));
	pthread_mutex_unlock(&(bb->lock));

}

void
free_barrier(barrier* bb)
{
    free(bb);
}

