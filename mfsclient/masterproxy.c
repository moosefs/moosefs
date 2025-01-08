/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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

#include "massert.h"
#include "sockets.h"
#include "mastercomm.h"
#include "datapack.h"
#include "portable.h"
#include "MFSCommunication.h"
#include "negentrycache.h"
#include "mfs_fuse.h"

#define TOOLTIMEOUTPARTMS 10000
#define TOOLTIMEOUTALLMS 30000
#define TOOLSNOPMS 5000
#define AUXBUFFSIZE 65536

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

typedef struct _conn_data {
	int sock;
	int sendnops;
	pthread_mutex_t lock;
} conn_data;

void masterproxy_free_conn_data (conn_data *cd) {
	pthread_mutex_destroy(&(cd->lock));
	tcpclose(cd->sock);
	free(cd);
}

static void* masterproxy_keepalive(void *args) {
	conn_data *cd = (conn_data*)(args);
	int nopcnt;
	uint8_t nopbuff[8];
	uint8_t *wptr;

	wptr = nopbuff;
	put32bit(&wptr,ANTOAN_NOP);
	put32bit(&wptr,0);
	nopcnt = 0;

	for (;;) {
		pthread_mutex_lock(&(cd->lock));
		if (cd->sendnops==255) {
			pthread_mutex_unlock(&(cd->lock));
			masterproxy_free_conn_data(cd);
			return NULL;
		}
		if (cd->sendnops==0) {
			pthread_mutex_unlock(&(cd->lock));
			nopcnt=0;
		} else {
			nopcnt++;
			if (nopcnt>=(TOOLSNOPMS/100)) {
				cd->sendnops = 2;
				pthread_mutex_unlock(&(cd->lock));
				if (tcptowrite(cd->sock,nopbuff,8,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=8) {
					break;
				}
				pthread_mutex_lock(&(cd->lock));
				cd->sendnops = 1;
				nopcnt = 0;
			}
			pthread_mutex_unlock(&(cd->lock));
		}
		portable_usleep(100000);
	}
	return NULL;
}

static void* masterproxy_server(void *args) {
	uint8_t header[8];
	uint8_t *auxbuffer;
	const uint8_t *aptr;
	uint8_t *wptr;
	const uint8_t *rptr;
	conn_data *cd = (conn_data*)(args);
	uint32_t psize,cmd,msgid,asize,acmd;
	// special extra data for snapshots
	uint32_t inode_dst;
	uint8_t name_dst_len;
	char name_dst[256];

	auxbuffer = malloc(AUXBUFFSIZE);

	for (;;) {
		if (tcptoread(cd->sock,header,8,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=8) {
			break;
		}

		rptr = header;
		cmd = get32bit(&rptr);
		psize = get32bit(&rptr);
		if (cmd==CLTOMA_FUSE_REGISTER) {	// special case: register
			// if (psize>AUXBUFFSIZE) {
			if (psize!=73) {
				break;
			}

			if (tcptoread(cd->sock,auxbuffer,psize,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=(int32_t)(psize)) {
				break;
			}

			if (memcmp(auxbuffer,FUSE_REGISTER_BLOB_ACL,64)!=0) {
				break;
			}

			if (auxbuffer[64]!=REGISTER_TOOLS) {
				break;
			}

			wptr = auxbuffer;
			put32bit(&wptr,MATOCL_FUSE_REGISTER);
			put32bit(&wptr,1);
			put8bit(&wptr,MFS_STATUS_OK);

			if (tcptowrite(cd->sock,auxbuffer,9,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=9) {
				break;
			}
		} else {
			if (psize<4 || psize>AUXBUFFSIZE) {
				break;
			}

			if (tcptoread(cd->sock,auxbuffer,psize,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=(int32_t)(psize)) {
				break;
			}

			pthread_mutex_lock(&(cd->lock));
			cd->sendnops = 1;
			pthread_mutex_unlock(&(cd->lock));

			rptr = auxbuffer;
			msgid = get32bit(&rptr);

			inode_dst = 0;
			name_dst_len = 0;
			if (cmd==CLTOMA_FUSE_SNAPSHOT) {
				if (psize>=13) {
					rptr+=4; // ignore src inode
					inode_dst = get32bit(&rptr);
					name_dst_len = get8bit(&rptr);
					if (psize>=13U+(uint32_t)name_dst_len) {
						memcpy(name_dst,rptr,name_dst_len);
						name_dst[name_dst_len]=0;
					} else {
						inode_dst = 0;
					}
				}
			}

			if (fs_custom(cmd,auxbuffer+4,psize-4,&acmd,&aptr,&asize)!=MFS_STATUS_OK) {
				break;
			}

			if (cmd==CLTOMA_FUSE_SNAPSHOT && acmd==MATOCL_FUSE_SNAPSHOT) {
				negentry_cache_clear();
				if (inode_dst>0 && name_dst_len>0) {
					mfs_dentry_invalidate(inode_dst,name_dst_len,name_dst);
				}
			}

			wptr = auxbuffer;
			put32bit(&wptr,acmd);
			put32bit(&wptr,asize+4);
			put32bit(&wptr,msgid);

			// stupid active wait - shuld be replaced by cond, but since it is highly improbable we can leave it
			pthread_mutex_lock(&(cd->lock));
			while (cd->sendnops==2) { // we don't want to send answer when nop packet is being sent
				pthread_mutex_unlock(&(cd->lock));
				portable_usleep(10000);
				pthread_mutex_lock(&(cd->lock));
			}
			cd->sendnops = 0;
			pthread_mutex_unlock(&(cd->lock));

			if (tcptowrite(cd->sock,auxbuffer,12,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=12) {
				break;
			}
			if (tcptowrite(cd->sock,aptr,asize,TOOLTIMEOUTPARTMS,TOOLTIMEOUTALLMS)!=(int32_t)(asize)) {
				break;
			}
		}
	}
	free(auxbuffer);
	pthread_mutex_lock(&(cd->lock));
	cd->sendnops = 255;
	pthread_mutex_unlock(&(cd->lock));

	return NULL;
}

static void* masterproxy_acceptor(void *args) {
	pthread_t clientthread,nopthread;
	pthread_attr_t thattr;
	sigset_t oldset;
	sigset_t newset;
	conn_data *cd;
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
			cd = malloc(sizeof(conn_data));
			cd->sock = sock;
			cd->sendnops = 0;
			pthread_mutex_init(&(cd->lock),NULL);
			// memory is freed inside pthread routine !!!
			tcpnodelay(sock);
			tcpnonblock(sock);
			sigemptyset(&newset);
			sigaddset(&newset, SIGTERM);
			sigaddset(&newset, SIGINT);
			sigaddset(&newset, SIGHUP);
			sigaddset(&newset, SIGQUIT);
			pthread_sigmask(SIG_BLOCK, &newset, &oldset);
			res = pthread_create(&nopthread,&thattr,masterproxy_keepalive,cd);
			if (res<0) {
				masterproxy_free_conn_data(cd);
			} else {
				res = pthread_create(&clientthread,&thattr,masterproxy_server,cd);
				if (res<0) {
					pthread_mutex_lock(&(cd->lock));
					cd->sendnops = 255;
					pthread_mutex_unlock(&(cd->lock));
				}
			}
			pthread_sigmask(SIG_SETMASK, &oldset, NULL);
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
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"master proxy module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	// tcpreuseaddr(lsock);
	if (tcpstrlisten(lsock,masterproxyip,0,100)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"master proxy module: can't listen on socket");
		tcpclose(lsock);
		lsock = -1;
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"master proxy module: can't set accept filter");
	}
	if (tcpgetmyaddr(lsock,&proxyhost,&proxyport)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"master proxy module: can't obtain my address and port");
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
