/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#include "massert.h"
#include "sockets.h"
#include "hashfn.h"
#include "portable.h"
#include "lwthread.h"

#define CONN_CACHE_HASHSIZE 256
#define CONN_CACHE_HASH(ip,port) (hash32((ip)^((port)<<16))%(CONN_CACHE_HASHSIZE))

typedef struct _connentry {
	uint32_t ip;
	uint16_t port;
	int fd;
	struct _connentry *lrunext,**lruprev;
	struct _connentry *hashnext,**hashprev;
} connentry;

static pthread_t main_thread;
static int keep_alive;

static connentry *conncachetab;
static connentry *conncachehash[CONN_CACHE_HASHSIZE];
static uint32_t capacity;
static connentry *lruhead,**lrutail;
static connentry *freehead;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

static inline void conncache_remove(connentry *ce,int closeflag) {
	if (ce->lrunext!=NULL) {
		ce->lrunext->lruprev = ce->lruprev;
	} else {
		lrutail = ce->lruprev;
	}
	*(ce->lruprev) = ce->lrunext;
	if (ce->hashnext!=NULL) {
		ce->hashnext->hashprev = ce->hashprev;
	}
	*(ce->hashprev) = ce->hashnext;
	ce->lrunext = NULL;
	ce->lruprev = NULL;
	ce->hashnext = freehead;
	ce->hashprev = NULL;
	freehead = ce;
	if (closeflag) {
		tcpclose(ce->fd);
	}
	ce->fd = -1;
}

void conncache_insert(uint32_t ip,uint16_t port,int fd) {
	uint32_t hash;
	connentry *ce;

	hash = CONN_CACHE_HASH(ip,port);

	zassert(pthread_mutex_lock(&glock));
	if (freehead==NULL) {
		conncache_remove(lruhead,1);
	}
	ce = freehead;
	freehead = ce->hashnext;
	ce->ip = ip;
	ce->port = port;
	ce->fd = fd;
	ce->lrunext = NULL;
	ce->lruprev = lrutail;
	*(lrutail) = ce;
	lrutail = &(ce->lrunext);
	ce->hashnext = conncachehash[hash];
	if (ce->hashnext) {
		ce->hashnext->hashprev = &(ce->hashnext);
	}
	ce->hashprev = conncachehash+hash;
	conncachehash[hash] = ce;
	zassert(pthread_mutex_unlock(&glock));
}

int conncache_get(uint32_t ip,uint16_t port) {
	uint32_t hash;
	connentry *ce;
	int fd;

	hash = CONN_CACHE_HASH(ip,port);
	fd = -1;
	zassert(pthread_mutex_lock(&glock));
	for (ce = conncachehash[hash] ; ce!=NULL && fd==-1 ; ce = ce->hashnext) {
		if (ce->ip==ip && ce->port==port && ce->fd>=0) {
			fd = ce->fd;
			conncache_remove(ce,0);
		}
	}
	zassert(pthread_mutex_unlock(&glock));
	return fd;
}

void* conncache_keepalive_thread(void* arg) {
	uint8_t nopbuff[8];
	int i,ka;
	uint32_t p,q;
	connentry *ce;

	p = 0;
	ka = 1;
	while (ka) {
		zassert(pthread_mutex_lock(&glock));
		for (q=p ; q<capacity ; q+=200) {
			ce = conncachetab+q;
			if (ce->fd>=0) {
#ifdef MFSDEBUG
				syslog(LOG_NOTICE,"conncache: pos: %"PRIu32" ; desc: %d ; ip:%08X ; port:%u",p,ce->fd,ce->ip,ce->port);
#endif
				i = universal_read(ce->fd,nopbuff,8);
				if (i<0) {
					if (!ERRNO_ERROR) {
						memset(nopbuff,0,8);
						i = 8;
					}
				}
				if (i!=8) {
					conncache_remove(ce,1);
				} else if ((nopbuff[0]|nopbuff[1]|nopbuff[2]|nopbuff[3]|nopbuff[4]|nopbuff[5]|nopbuff[6]|nopbuff[7])!=0) {
					conncache_remove(ce,1);
				} else {
					memset(nopbuff,0,8);
					i = universal_write(ce->fd,nopbuff,8);
					if (i!=8) {
						conncache_remove(ce,1);
					}
				}
			}
		}
		p++;
		if (p>=200) {
			p=0;
		}
		ka = keep_alive;
		zassert(pthread_mutex_unlock(&glock));
		portable_usleep(2500); // 200 * 2500 = 500000 = 0.5s (send nops every half second)
	}
	return arg;
}

void conncache_term(void) {
	uint32_t p;
	zassert(pthread_mutex_lock(&glock));
	keep_alive = 0;
	zassert(pthread_mutex_unlock(&glock));
	pthread_join(main_thread,NULL);
	zassert(pthread_mutex_lock(&glock));
	// cleanup
	for (p=0 ; p<capacity ; p++) {
		if (conncachetab[p].fd>=0) {
			tcpclose(conncachetab[p].fd);
		}
	}
	free(conncachetab);
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_mutex_destroy(&glock));
}

int conncache_init(uint32_t cap) {
	uint32_t p;

	capacity = cap;
	conncachetab = malloc(sizeof(connentry)*capacity);
	for (p=0 ; p<capacity ; p++) {
		conncachetab[p].fd = -1;
		conncachetab[p].lrunext = NULL;
		conncachetab[p].lruprev = NULL;
		conncachetab[p].hashnext = ((p+1)<capacity)?conncachetab+(p+1):NULL;
		conncachetab[p].hashprev = NULL;
	}
	freehead = conncachetab;
	for (p=0 ; p<CONN_CACHE_HASHSIZE ; p++) {
		conncachehash[p] = NULL;
	}
	lruhead = NULL;
	lrutail = &(lruhead);
	keep_alive = 1;
	if (lwt_minthread_create(&main_thread,0,conncache_keepalive_thread,NULL)<0) {
		return -1;
	}
	return 1;
}
