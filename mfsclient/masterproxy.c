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

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <syslog.h>

#include "massert.h"
#include "sockets.h"
#include "mastercomm.h"
#include "datapack.h"
#include "MFSCommunication.h"
#include "negentrycache.h"

#define TOOLTIMEOUTMS 20000
#define QUERYSIZE 50000
#define ANSSIZE 4000000

static int lsock = -1;
static pthread_t proxythread;
#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t tlock;
#endif
static uint8_t terminate;

static uint32_t proxyhost;
static uint16_t proxyport;


void masterproxy_getlocation(uint8_t *masterinfo) {
	const uint8_t *rptr = masterinfo+10;
	if (lsock>=0 && get32bit(&rptr)>=0x00010618) {	// use proxy only when master version is greater than or equal to 1.6.24
		put32bit(&masterinfo,proxyhost);
		put16bit(&masterinfo,proxyport);
	}
}

static void* masterproxy_server(void *args) {
	uint8_t header[8];
	uint8_t *querybuffer;
	uint8_t *ansbuffer;
	uint8_t *wptr;
	const uint8_t *rptr;
	int sock = *((int*)args);
	uint32_t psize,cmd,msgid,asize,acmd;

	free(args);

	querybuffer = malloc(QUERYSIZE);
	ansbuffer = malloc(ANSSIZE);

	for (;;) {
		if (tcptoread(sock,header,8,TOOLTIMEOUTMS)!=8) {
			break;
		}

		rptr = header;
		cmd = get32bit(&rptr);
		psize = get32bit(&rptr);
		if (cmd==CLTOMA_FUSE_REGISTER) {	// special case: register
			// if (psize>QUERYSIZE) {
			if (psize!=73) {
				break;
			}

			if (tcptoread(sock,querybuffer,psize,TOOLTIMEOUTMS)!=(int32_t)(psize)) {
				break;
			}

			if (memcmp(querybuffer,FUSE_REGISTER_BLOB_ACL,64)!=0) {
				break;
			}

			if (querybuffer[64]!=REGISTER_TOOLS) {
				break;
			}

			wptr = ansbuffer;
			put32bit(&wptr,MATOCL_FUSE_REGISTER);
			put32bit(&wptr,1);
			put8bit(&wptr,MFS_STATUS_OK);

			if (tcptowrite(sock,ansbuffer,9,TOOLTIMEOUTMS)!=9) {
				break;
			}
		} else {
			if (psize<4 || psize>QUERYSIZE) {
				break;
			}

			if (tcptoread(sock,querybuffer,psize,TOOLTIMEOUTMS)!=(int32_t)(psize)) {
				break;
			}

			rptr = querybuffer;
			msgid = get32bit(&rptr);

			asize = ANSSIZE-12;
			if (fs_custom(cmd,querybuffer+4,psize-4,&acmd,ansbuffer+12,&asize)!=MFS_STATUS_OK) {
				break;
			}

			if (cmd==CLTOMA_FUSE_SNAPSHOT && acmd==MATOCL_FUSE_SNAPSHOT) {
				negentry_cache_clear();
			}

			wptr = ansbuffer;
			put32bit(&wptr,acmd);
			put32bit(&wptr,asize+4);
			put32bit(&wptr,msgid);

			if (tcptowrite(sock,ansbuffer,asize+12,TOOLTIMEOUTMS)!=(int32_t)(asize+12)) {
				break;
			}
		}
	}
	tcpclose(sock);
	free(ansbuffer);
	free(querybuffer);
	return NULL;
}

static void* masterproxy_acceptor(void *args) {
	pthread_t clientthread;
	pthread_attr_t thattr;
	sigset_t oldset;
	sigset_t newset;
	int sock,res;
	(void)args;

	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	zassert(pthread_attr_setdetachstate(&thattr,PTHREAD_CREATE_DETACHED));

#ifdef HAVE___SYNC_OP_AND_FETCH
	while (__sync_or_and_fetch(&terminate,0)==0) {
#else
	zassert(pthread_mutex_lock(&tlock));
	while (terminate==0) {
		zassert(pthread_mutex_unlock(&tlock));
#endif
		sock = tcptoaccept(lsock,1000);
		if (sock>=0) {
			int *s = malloc(sizeof(int));
			// memory is freed inside pthread routine !!!
			*s = sock;
			tcpnodelay(sock);
			tcpnonblock(sock);
			sigemptyset(&newset);
			sigaddset(&newset, SIGTERM);
			sigaddset(&newset, SIGINT);
			sigaddset(&newset, SIGHUP);
			sigaddset(&newset, SIGQUIT);
			pthread_sigmask(SIG_BLOCK, &newset, &oldset);
			res = pthread_create(&clientthread,&thattr,masterproxy_server,s);
			pthread_sigmask(SIG_SETMASK, &oldset, NULL);
			if (res<0) {
				free(s);
				tcpclose(sock);
			}
		}
#ifdef HAVE___SYNC_OP_AND_FETCH
	}
#else
		zassert(pthread_mutex_lock(&tlock));
	}
	zassert(pthread_mutex_unlock(&tlock));
#endif

	pthread_attr_destroy(&thattr);
	return NULL;
}

void masterproxy_term(void) {
#ifdef HAVE___SYNC_OP_AND_FETCH
	__sync_or_and_fetch(&terminate,1);
#else
	zassert(pthread_mutex_lock(&tlock));
	terminate = 1;
	zassert(pthread_mutex_unlock(&tlock));
#endif
	pthread_join(proxythread,NULL);
#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_destroy(&tlock));
#endif
}

int masterproxy_init(const char *masterproxyip) {
	pthread_attr_t thattr;
	sigset_t oldset;
	sigset_t newset;


	lsock = tcpsocket();
	if (lsock<0) {
		fprintf(stderr,"master proxy module: can't create socket\n");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	// tcpreuseaddr(lsock);
	if (tcpstrlisten(lsock,masterproxyip,0,100)<0) {
		fprintf(stderr,"master proxy module: can't listen on socket\n");
		tcpclose(lsock);
		lsock = -1;
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		syslog(LOG_NOTICE,"master proxy module: can't set accept filter");
	}
	if (tcpgetmyaddr(lsock,&proxyhost,&proxyport)<0) {
		fprintf(stderr,"master proxy module: can't obtain my address and port\n");
		tcpclose(lsock);
		lsock = -1;
		return -1;
	}

	terminate = 0;
#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_init(&tlock,NULL));
#endif
	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	//pthread_create(&proxythread,&thattr,masterproxy_loop,NULL);
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	zassert(pthread_create(&proxythread,&thattr,masterproxy_acceptor,NULL));
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	zassert(pthread_attr_destroy(&thattr));

	return 1;
}
