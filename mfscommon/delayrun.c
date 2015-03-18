#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <inttypes.h>
// #include <stdio.h>

#include "massert.h"
#include "clocks.h"

typedef struct _heapelem {
	void (*fn)(void *);
	void *udata;
	uint64_t firetime;
} heapelem;

static heapelem *heap;
static uint32_t heapelements;
static uint32_t heapsize;
static uint8_t exitflag;
static uint8_t waiting;
static pthread_mutex_t dlock;
static pthread_cond_t dcond;
static pthread_t delay_th;

#define PARENT(x) (((x)-1)/2)
#define CHILD(x) (((x)*2)+1)

void delay_heap_sort_down(void) {
	uint32_t l,r,m;
	uint32_t pos = 0;
	heapelem x;
	while (pos<heapelements) {
		l = CHILD(pos);
		r = l+1;
		if (l>=heapelements) {
			return;
		}
		m = l;
		if (r<heapelements && heap[r].firetime < heap[l].firetime) {
			m = r;
		}
		if (heap[pos].firetime <= heap[m].firetime) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[m];
		heap[m] = x;
		pos = m;
	}
}

uint8_t delay_heap_sort_up(void) {
	uint32_t pos = heapelements-1;
	uint32_t p;
	heapelem x;
	while (pos>0) {
		p = PARENT(pos);
		if (heap[pos].firetime >= heap[p].firetime) {
			return 0;
		}
		x = heap[pos];
		heap[pos] = heap[p];
		heap[p] = x;
		pos = p;
	}
	return 1;
}

void* delay_scheduler(void *arg) {
	uint64_t now;
	struct timespec ts;
	struct timeval tv;
	void (*fn)(void *);
	void *udata;

	zassert(pthread_mutex_lock(&dlock));
	while (1) {
		if (exitflag) {
			zassert(pthread_mutex_unlock(&dlock));
			return arg;
		}
		if (heapelements>0) {
			now = monotonic_useconds();
//			printf("now: %"PRIu64" ; heap[0].firetime: %"PRIu64"\n",now,heap[0].firetime);
			if (now < heap[0].firetime) {
				gettimeofday(&tv, NULL);
				ts.tv_sec = tv.tv_sec + (heap[0].firetime - now) / 1000000;
				ts.tv_nsec = (tv.tv_usec + ((heap[0].firetime - now) % 1000000)) * 1000;
				while (ts.tv_nsec >= 1000000000) { // "if" should be enough here
					ts.tv_sec ++;
					ts.tv_nsec -= 1000000000;
				}
				waiting = 1;
				pthread_cond_timedwait(&dcond,&dlock,&ts);
				waiting = 0;
			} else {
				fn = heap[0].fn;
				udata = heap[0].udata;
				heapelements--;
				if (heapelements>0) {
					heap[0] = heap[heapelements];
					delay_heap_sort_down();
				}
				zassert(pthread_mutex_unlock(&dlock));
				(*fn)(udata);
				zassert(pthread_mutex_lock(&dlock));
			}
		} else {
			waiting = 1;
			zassert(pthread_cond_wait(&dcond,&dlock));
			waiting = 0;
		}
	}
	zassert(pthread_mutex_unlock(&dlock));
	return NULL;
}

void delay_run (void (*fn)(void *),void *udata,uint64_t useconds) {
	zassert(pthread_mutex_lock(&dlock));
	if (heapelements>=heapsize) {
		heapsize *= 2;
		heap = realloc(heap,sizeof(heapelem)*heapsize);
	}
	heap[heapelements].fn = fn;
	heap[heapelements].udata = udata;
	heap[heapelements].firetime = monotonic_useconds()+useconds;
//	printf("fire time: %"PRIu64"\n",heap[heapelements].firetime);
	heapelements++;
	if (delay_heap_sort_up() && waiting) {
		zassert(pthread_cond_signal(&dcond));
	}
	zassert(pthread_mutex_unlock(&dlock));
}

void delay_term(void) {
	zassert(pthread_mutex_lock(&dlock));
	exitflag = 1;
	if (waiting) {
		zassert(pthread_cond_signal(&dcond));
	}
	zassert(pthread_mutex_unlock(&dlock));
	zassert(pthread_join(delay_th,NULL));
	zassert(pthread_cond_destroy(&dcond));
	zassert(pthread_mutex_destroy(&dlock));
	free(heap);
	heap = NULL;
	heapsize = 0;
	heapelements = 0;
}

void delay_init(void) {
	pthread_attr_t thattr;
	sigset_t oldset;
	sigset_t newset;

	exitflag = 0;
	waiting = 0;
	heap = malloc(sizeof(heapelem)*1024);
	heapelements = 0;
	heapsize = 1024;
	zassert(pthread_mutex_init(&dlock,NULL));
	zassert(pthread_cond_init(&dcond,NULL));

	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	zassert(pthread_sigmask(SIG_BLOCK, &newset, &oldset));
	zassert(pthread_create(&delay_th,&thattr,delay_scheduler,NULL));
	zassert(pthread_sigmask(SIG_SETMASK, &oldset, NULL));
	zassert(pthread_attr_destroy(&thattr));
}
