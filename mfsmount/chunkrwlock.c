#include <stdlib.h>
#include <inttypes.h>
#include <pthread.h>

#include "massert.h"
#include "chunkrwlock.h"

typedef struct chunkrec {
	uint32_t inode;
	uint32_t indx;
	uint8_t writing;
	uint32_t active_readers_cnt;
	uint32_t waiting_readers_cnt;
	uint32_t waiting_writers_cnt;
	pthread_cond_t rcond,wcond;
	struct chunkrec *next,**prev;
} chunkrec;

static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;
static chunkrec* hashtab[1024];
static chunkrec* freeblocks;
static uint32_t freeblockscnt;

void chunkrwlock_init(void) {
	uint32_t i;
	pthread_mutex_lock(&glock);
	for (i=0 ; i<1024 ; i++) {
		hashtab[i] = NULL;
	}
	freeblocks=NULL;
	freeblockscnt=0;
	pthread_mutex_unlock(&glock);
}

void chunkrwlock_term(void) {
	uint32_t i;
	chunkrec *cr;
	pthread_mutex_lock(&glock);
	while (freeblocks!=NULL) {
		cr = freeblocks;
		zassert(pthread_cond_destroy(&(cr->rcond)));
		zassert(pthread_cond_destroy(&(cr->wcond)));
		freeblocks = cr->next;
		free(cr);
	}
	for (i=0 ; i<1024 ; i++) {
		massert(hashtab[i]==NULL,"chunkrwlock hashmap not empty during termination");
	}
	pthread_mutex_unlock(&glock);
}

static inline chunkrec* chunkrwlock_get(uint32_t inode,uint32_t indx) {
	chunkrec *cr;
	uint32_t hash;
	pthread_mutex_lock(&glock);
	hash = ((inode * 0xF52D) + (indx ^ 0x423)) & 1023;
	for (cr=hashtab[hash] ; cr ; cr=cr->next) {
		if (cr->inode == inode && cr->indx==indx) {
			return cr;
		}
	}
	if (freeblocks==NULL) {
		cr = malloc(sizeof(chunkrec));
		passert(cr);
		zassert(pthread_cond_init(&(cr->rcond),NULL));
		zassert(pthread_cond_init(&(cr->wcond),NULL));
	} else {
		cr = freeblocks;
		freeblocks = cr->next;
		freeblockscnt--;
	}
	cr->inode = inode;
	cr->indx = indx;
	cr->writing = 0;
	cr->active_readers_cnt = 0;
	cr->waiting_readers_cnt = 0;
	cr->waiting_writers_cnt = 0;
	cr->prev = hashtab+hash;
	cr->next = hashtab[hash];
	if (cr->next) {
		cr->next->prev = &(cr->next);
	}
	hashtab[hash] = cr;
	return cr;
}

static inline void chunkrwlock_release(chunkrec *cr) {
	if ((cr->writing | cr->active_readers_cnt | cr->waiting_readers_cnt | cr->waiting_writers_cnt)==0) {
		*(cr->prev) = cr->next;
		if (cr->next) {
			cr->next->prev = cr->prev;
		}
		if (freeblockscnt>1024) {
			zassert(pthread_cond_destroy(&(cr->rcond)));
			zassert(pthread_cond_destroy(&(cr->wcond)));
			free(cr);
		} else {
			cr->prev = NULL;
			cr->next = freeblocks;
			freeblocks = cr;
			freeblockscnt++;
		}
	}
	pthread_mutex_unlock(&glock);
}

void chunkrwlock_rlock(uint32_t inode,uint32_t indx) {
	chunkrec *cr;

	cr = chunkrwlock_get(inode,indx);
	cr->waiting_readers_cnt++;
	while (cr->writing | cr->waiting_writers_cnt) {
		zassert(pthread_cond_wait(&(cr->rcond),&glock));
	}
	cr->waiting_readers_cnt--;
	cr->active_readers_cnt++;
	chunkrwlock_release(cr);
}

void chunkrwlock_runlock(uint32_t inode,uint32_t indx) {
	chunkrec *cr;

	cr = chunkrwlock_get(inode,indx);
	cr->active_readers_cnt--;
	if (cr->active_readers_cnt==0 && cr->waiting_writers_cnt) {
		zassert(pthread_cond_signal(&(cr->wcond)));
	}
	chunkrwlock_release(cr);
}

void chunkrwlock_wlock(uint32_t inode,uint32_t indx) {
	chunkrec *cr;

	cr = chunkrwlock_get(inode,indx);
	cr->waiting_writers_cnt++;
	while (cr->active_readers_cnt | cr->writing) {
		zassert(pthread_cond_wait(&(cr->wcond),&glock));
	}
	cr->waiting_writers_cnt--;
	cr->writing = 1;
	chunkrwlock_release(cr);
}

void chunkrwlock_wunlock(uint32_t inode,uint32_t indx) {
	chunkrec *cr;

	cr = chunkrwlock_get(inode,indx);
	cr->writing = 0;
	if (cr->waiting_writers_cnt) {
		zassert(pthread_cond_signal(&(cr->wcond)));
	} else if (cr->waiting_readers_cnt) {
		zassert(pthread_cond_broadcast(&(cr->rcond)));
	}
	chunkrwlock_release(cr);
}
