/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
#include "main.h"

#define CONN_CACHE_HASHSIZE 256
#define CONN_CACHE_HASH(ip,port) (hash32((ip)^((port)<<16))%(CONN_CACHE_HASHSIZE))

typedef struct _connentry {
	uint32_t ip;
	uint16_t port;
	int fd;
	struct _connentry *lrunext,**lruprev;
	struct _connentry *hashnext,**hashprev;
} connentry;

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
	int i;
	uint32_t p;
	connentry *ce;

	for (;;) {
		for (p=0 ; p<capacity ; p++) {
			ce = conncachetab+p;
			zassert(pthread_mutex_lock(&glock));
			if (ce->fd>=0) {
#ifdef MFSDEBUG
				syslog(LOG_NOTICE,"conncache: pos: %"PRIu32" ; desc: %d ; ip:%08X ; port:%u",p,ce->fd,ce->ip,ce->port);
#endif
				i = read(ce->fd,nopbuff,8);
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
					i = write(ce->fd,nopbuff,8);
					if (i!=8) {
						conncache_remove(ce,1);
					}
				}
			}
			zassert(pthread_mutex_unlock(&glock));
		}
		sleep(2);
	}
	return arg;
}

int conncache_init(uint32_t cap) {
	pthread_t kathread;
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

	if (main_minthread_create(&kathread,1,conncache_keepalive_thread,NULL)<0) {
		return -1;
	}
	return 1;
}
