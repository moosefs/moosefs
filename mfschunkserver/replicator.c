/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <poll.h>
#include <sys/time.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "hddspacemgr.h"
#include "sockets.h"
#include "crc.h"
#include "slogger.h"
#include "datapack.h"
#include "massert.h"
#include "mfsstrerr.h"
#include "clocks.h"

#include "replicator.h"

#define CONNMSECTO 5000
#define SENDMSECTO 5000
#define RECVMSECTO 5000

#define MAX_RECV_PACKET_SIZE (20+MFSBLOCKSIZE)

typedef enum {IDLE,CONNECTING,HEADER,DATA} modetype;

typedef struct _repsrc {
	int sock;
	modetype mode;
	uint8_t hdrbuff[8];
	uint8_t *packet;
	uint8_t *startptr;
	uint32_t bytesleft;

	uint64_t chunkid;
	uint32_t version;
	uint16_t blocks;

	uint32_t ip;
	uint16_t port;

	uint32_t crcsums[4];
} repsrc;

typedef struct _replication {
	uint64_t chunkid;
	uint32_t version;

	uint8_t *xorbuff;

	uint8_t created,opened;
	uint8_t srccnt;
	struct pollfd *fds;
	repsrc *repsources;
} replication;

static uint32_t stats_repl = 0;
static uint64_t stats_bytesin = 0;
static uint64_t stats_bytesout = 0;
static pthread_mutex_t statslock = PTHREAD_MUTEX_INITIALIZER;

void replicator_stats(uint64_t *bin,uint64_t *bout,uint32_t *repl) {
	pthread_mutex_lock(&statslock);
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*repl = stats_repl;
	stats_repl = 0;
	stats_bytesin = 0;
	stats_bytesout = 0;
	pthread_mutex_unlock(&statslock);
}

static inline void replicator_bytesin(uint64_t bytes) {
	zassert(pthread_mutex_lock(&statslock));
	stats_bytesin += bytes;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void replicator_bytesout(uint64_t bytes) {
	zassert(pthread_mutex_lock(&statslock));
	stats_bytesout += bytes;
	zassert(pthread_mutex_unlock(&statslock));
}

static void xordata(uint8_t *dst,const uint8_t *src,uint32_t leng) {
	uint32_t *dst4;
	const uint32_t *src4;
#define XOR_ONE_BYTE (*dst++)^=(*src++)
#define XOR_FOUR_BYTES (*dst4++)^=(*src4++)
	if (((unsigned long)dst&3)==((unsigned long)src&3)) {
		while (leng && ((unsigned long)src & 3)) {
			XOR_ONE_BYTE;
			leng--;
		}
		dst4 = (uint32_t*)dst;
		src4 = (const uint32_t*)src;
		while (leng>=32) {
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			leng-=32;
		}
		while (leng>=4) {
			XOR_FOUR_BYTES;
			leng-=4;
		}
		src = (const uint8_t*)src4;
		dst = (uint8_t*)dst4;
		if (leng) do {
			XOR_ONE_BYTE;
		} while (--leng);
	} else {
		while (leng>=8) {
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			leng-=8;
		}
		if (leng>0) do {
			XOR_ONE_BYTE;
		} while (--leng);
	}
}

static int rep_read(repsrc *rs) {
	int32_t i;
	uint32_t type;
	uint32_t size;
	const uint8_t *ptr;
	while (rs->bytesleft>0) {
		i=read(rs->sock,rs->startptr,rs->bytesleft);
		if (i==0) {
			syslog(LOG_NOTICE,"replicator: connection lost");
			return -1;
		}
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"replicator: read error");
				return -1;
			}
			return 0;
		}
		replicator_bytesin(i);
//		stats_bytesin+=i;
		rs->startptr+=i;
		rs->bytesleft-=i;

		if (rs->bytesleft>0) {
			return 0;
		}

		if (rs->mode==HEADER) {
			ptr = rs->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);
			if (type==ANTOAN_NOP && size==0) { // NOP
				rs->startptr = rs->hdrbuff;
				rs->bytesleft = 8;
				return 0;
			}

			if (rs->packet) {
				free(rs->packet);
			}
			if (size>0) {
				if (size>MAX_RECV_PACKET_SIZE) {
					syslog(LOG_WARNING,"replicator: packet too long (%"PRIu32"/%u) ; command:%"PRIu32,size,MAX_RECV_PACKET_SIZE,type);
					return -1;
				}
				rs->packet = malloc(size);
				passert(rs->packet);
				rs->startptr = rs->packet;
			} else {
				rs->packet = NULL;
			}
			rs->bytesleft = size;
			rs->mode = DATA;
		}
	}
	return 0;
}

static int rep_receive_all_packets(replication *r,uint32_t msecto) {
	uint8_t i,l;
	uint64_t st;
	uint32_t msec;
	st = monotonic_useconds();
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				r->fds[i].events = POLLIN;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {	// finished
			return 0;
		}
		msec = (monotonic_useconds()-st)/1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: receive timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLIN) {
				if (rep_read(r->repsources+i)<0) {
					return -1;
				}
			}
		}
	}
}

static uint8_t* rep_create_packet(repsrc *rs,uint32_t type,uint32_t size) {
	uint8_t *ptr;
	if (rs->packet) {
		free(rs->packet);
	}
	rs->packet = malloc(size+8);
	passert(rs->packet);
	ptr = rs->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	rs->startptr = rs->packet;
	rs->bytesleft = 8+size;
	return ptr;
}

static void rep_no_packet(repsrc *rs) {
	if (rs->packet) {
		free(rs->packet);
	}
	rs->packet=NULL;
	rs->startptr=NULL;
	rs->bytesleft=0;
}

static int rep_write(repsrc *rs) {
	int i;
	i = write(rs->sock,rs->startptr,rs->bytesleft);
	if (i==0) {
		syslog(LOG_NOTICE,"replicator: connection lost");
		return -1;
	}
	if (i<0) {
		if (ERRNO_ERROR) {
			mfs_errlog_silent(LOG_NOTICE,"replicator: write error");
			return -1;
		}
		return 0;
	}
	replicator_bytesout(i);
//	stats_bytesin+=i;
	rs->startptr+=i;
	rs->bytesleft-=i;
	return 0;
}

static int rep_send_all_packets(replication *r,uint32_t msecto) {
	uint8_t i,l;
	uint64_t st;
	uint32_t msec;
	st = monotonic_useconds();
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				r->fds[i].events = POLLOUT;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {	// finished
			return 0;
		}
		msec = (monotonic_useconds()-st)/1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: send timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLOUT) {
				if (rep_write(r->repsources+i)<0) {
					return -1;
				}
			}
		}
	}
}

static int rep_wait_for_connection(replication *r,uint32_t msecto) {
	uint8_t i,l;
	uint64_t st;
	uint32_t msec;
	st = monotonic_useconds();
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].mode==CONNECTING) {
				r->fds[i].events = POLLOUT;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {	// finished
			return 0;
		}
		msec = (monotonic_useconds()-st)/1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: connect timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLOUT) {
				if (tcpgetstatus(r->repsources[i].sock)<0) {
					mfs_errlog_silent(LOG_NOTICE,"replicator: connect error");
					return -1;
				}
				r->repsources[i].mode=IDLE;
			}
		}
	}
}

static void rep_cleanup(replication *r) {
	int i;
	if (r->opened) {
		hdd_close(r->chunkid);
	}
	if (r->created) {
		hdd_delete(r->chunkid,0);
	}
	for (i=0 ; i<r->srccnt ; i++) {
		if (r->repsources[i].sock>=0) {
			tcpclose(r->repsources[i].sock);
		}
		if (r->repsources[i].packet) {
			free(r->repsources[i].packet);
		}
	}
	if (r->fds) {
		free(r->fds);
	}
	if (r->repsources) {
		free(r->repsources);
	}
	if (r->xorbuff) {
		free(r->xorbuff);
	}
}

/* srcs: srccnt * (chunkid:64 version:32 ip:32 port:16) */
uint8_t replicate(uint64_t chunkid,uint32_t version,const uint32_t xormasks[4],uint8_t srccnt,const uint8_t *srcs) {
	replication r;
	uint8_t status,i,j,vbuffs,first;
	uint16_t b,blocks;
	uint32_t xcrc[4],crc;
	uint32_t codeindex,codeword;
	uint8_t *wptr;
	const uint8_t *rptr;
	int s;

	if (srccnt==0) {
		return MFS_ERROR_EINVAL;
	}

//	syslog(LOG_NOTICE,"replication begin (chunkid:%08"PRIX64",version:%04"PRIX32",srccnt:%"PRIu8")",chunkid,version,srccnt);

	pthread_mutex_lock(&statslock);
	stats_repl++;
	pthread_mutex_unlock(&statslock);

// init replication structure
	r.chunkid = chunkid;
	r.version = version;
	r.srccnt = 0;
	r.created = 0;
	r.opened = 0;
	r.fds = malloc(sizeof(struct pollfd)*srccnt);
	passert(r.fds);
	r.repsources = malloc(sizeof(repsrc)*srccnt);
	passert(r.repsources);
	if (srccnt>1) {
		r.xorbuff = malloc(MFSBLOCKSIZE+4);
		passert(r.xorbuff);
	} else {
		r.xorbuff = NULL;
	}
// create chunk
	status = hdd_create(chunkid,0);
	if (status!=MFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_create status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.created = 1;
// init sources
	r.srccnt = srccnt;
	for (i=0 ; i<srccnt ; i++) {
		r.repsources[i].chunkid = get64bit(&srcs);
		r.repsources[i].version = get32bit(&srcs);
		r.repsources[i].ip = get32bit(&srcs);
		r.repsources[i].port = get16bit(&srcs);
		r.repsources[i].sock = -1;
		r.repsources[i].packet = NULL;
	}
// connect
	for (i=0 ; i<srccnt ; i++) {
		s = tcpsocket();
		if (s<0) {
			mfs_errlog_silent(LOG_NOTICE,"replicator: socket error");
			rep_cleanup(&r);
			return MFS_ERROR_CANTCONNECT;
		}
		r.repsources[i].sock = s;
		r.fds[i].fd = s;
		if (tcpnonblock(s)<0) {
			mfs_errlog_silent(LOG_NOTICE,"replicator: nonblock error");
			rep_cleanup(&r);
			return MFS_ERROR_CANTCONNECT;
		}
		s = tcpnumconnect(s,r.repsources[i].ip,r.repsources[i].port);
		if (s<0) {
			mfs_errlog_silent(LOG_NOTICE,"replicator: connect error");
			rep_cleanup(&r);
			return MFS_ERROR_CANTCONNECT;
		}
		if (s==0) {
			r.repsources[i].mode = IDLE;
		} else {
			r.repsources[i].mode = CONNECTING;
		}
	}
	if (rep_wait_for_connection(&r,CONNMSECTO)<0) {
		rep_cleanup(&r);
		return MFS_ERROR_CANTCONNECT;
	}
// disable Nagle
	for (i=0 ; i<srccnt ; i++) {
		tcpnodelay(r.repsources[i].sock);
	}
// open chunk
	status = hdd_open(chunkid,0);
	if (status!=MFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_open status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 1;
// get block numbers
	for (i=0 ; i<srccnt ; i++) {
		wptr = rep_create_packet(r.repsources+i,ANTOCS_GET_CHUNK_BLOCKS,8+4);
		if (wptr==NULL) {
			syslog(LOG_NOTICE,"replicator: out of memory");
			rep_cleanup(&r);
			return MFS_ERROR_OUTOFMEMORY;
		}
		put64bit(&wptr,r.repsources[i].chunkid);
		put32bit(&wptr,r.repsources[i].version);
	}
// send packet
	if (rep_send_all_packets(&r,SENDMSECTO)<0) {
		rep_cleanup(&r);
		return MFS_ERROR_DISCONNECTED;
	}
// receive answers
	for (i=0 ; i<srccnt ; i++) {
		r.repsources[i].mode = HEADER;
		r.repsources[i].startptr = r.repsources[i].hdrbuff;
		r.repsources[i].bytesleft = 8;
	}
	if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
		rep_cleanup(&r);
		return MFS_ERROR_DISCONNECTED;
	}
// get # of blocks
	blocks = 0;
	for (i=0 ; i<srccnt ; i++) {
		uint32_t type,size;
		uint64_t pchid;
		uint32_t pver;
		uint16_t pblocks;
		uint8_t pstatus;
		uint32_t ip;
		rptr = r.repsources[i].hdrbuff;
		type = get32bit(&rptr);
		size = get32bit(&rptr);
		rptr = r.repsources[i].packet;
		ip = r.repsources[i].ip;
		if (rptr==NULL || type!=CSTOAN_CHUNK_BLOCKS || size!=15) {
			syslog(LOG_WARNING,"replicator,get # of blocks: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%u)",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_DISCONNECTED;
		}
		pchid = get64bit(&rptr);
		pver = get32bit(&rptr);
		pblocks = get16bit(&rptr);
		pstatus = get8bit(&rptr);
		if (pchid!=r.repsources[i].chunkid) {
			syslog(LOG_WARNING,"replicator,get # of blocks: got wrong answer (chunk_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%u)",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_WRONGCHUNKID;
		}
		if (pver!=r.repsources[i].version) {
			syslog(LOG_WARNING,"replicator,get # of blocks: got wrong answer (chunk_status:version:%"PRIX32"/%"PRIX32") from (%u.%u.%u.%u:%u)",pver,r.repsources[i].version,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_WRONGVERSION;
		}
		if (pstatus!=MFS_STATUS_OK) {
			syslog(LOG_NOTICE,"replicator,get # of blocks: got status: %s from (%u.%u.%u.%u:%u)",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return pstatus;
		}
		r.repsources[i].blocks = pblocks;
		if (pblocks>blocks) {
			blocks=pblocks;
		}
	}
// create read request
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
			uint32_t leng;
			wptr = rep_create_packet(r.repsources+i,CLTOCS_READ,8+4+4+4);
			if (wptr==NULL) {
				syslog(LOG_NOTICE,"replicator: out of memory");
				rep_cleanup(&r);
				return MFS_ERROR_OUTOFMEMORY;
			}
			leng = r.repsources[i].blocks*MFSBLOCKSIZE;
			put64bit(&wptr,r.repsources[i].chunkid);
			put32bit(&wptr,r.repsources[i].version);
			put32bit(&wptr,0);
			put32bit(&wptr,leng);
		} else {
			rep_no_packet(r.repsources+i);
		}
	}
// send read request
	if (rep_send_all_packets(&r,SENDMSECTO)<0) {
		rep_cleanup(&r);
		return MFS_ERROR_DISCONNECTED;
	}
// receive data and write to hdd
	for (b=0 ; b<blocks ; b++) {
// prepare receive
		for (i=0 ; i<srccnt ; i++) {
			if (b<r.repsources[i].blocks) {
				r.repsources[i].mode = HEADER;
				r.repsources[i].startptr = r.repsources[i].hdrbuff;
				r.repsources[i].bytesleft = 8;
			} else {
				r.repsources[i].mode = IDLE;
				r.repsources[i].bytesleft = 0;
			}
		}
// receive data
		if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
			rep_cleanup(&r);
			return MFS_ERROR_DISCONNECTED;
		}
// check packets
		vbuffs = 0;
		for (i=0 ; i<srccnt ; i++) {
			if (r.repsources[i].mode!=IDLE) {
				uint32_t type,size;
				uint64_t pchid;
				uint16_t pblocknum;
				uint16_t poffset;
				uint32_t psize;
				uint8_t pstatus;
				uint32_t ip;
				rptr = r.repsources[i].hdrbuff;
				type = get32bit(&rptr);
				size = get32bit(&rptr);
				rptr = r.repsources[i].packet;
				ip = r.repsources[i].ip;
				if (rptr==NULL) {
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				if (type==CSTOCL_READ_STATUS && size==9) {
					pchid = get64bit(&rptr);
					pstatus = get8bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (read_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%u)",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGCHUNKID;
					}
					if (pstatus==MFS_STATUS_OK) {	// got status too early or got incorrect packet
						syslog(LOG_WARNING,"replicator,read chunks: got unexpected ok status from (%u.%u.%u.%u:%u)",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					syslog(LOG_NOTICE,"replicator,read chunks: got status: %s from (%u.%u.%u.%u:%u)",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return pstatus;
				} else if (type==CSTOCL_READ_DATA && size==20+MFSBLOCKSIZE) {
					pchid = get64bit(&rptr);
					pblocknum = get16bit(&rptr);
					poffset = get16bit(&rptr);
					psize = get32bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (read_data:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%u)",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGCHUNKID;
					}
					if (pblocknum!=b) {
						syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (read_data:blocknum:%"PRIu16"/%"PRIu16") from (%u.%u.%u.%u:%u)",pblocknum,b,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					if (poffset!=0) {
						syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (read_data:offset:%"PRIu16") from (%u.%u.%u.%u:%u)",poffset,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGOFFSET;
					}
					if (psize!=MFSBLOCKSIZE) {
						syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (read_data:size:%"PRIu32") from (%u.%u.%u.%u:%u)",psize,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGSIZE;
					}
				} else {
					syslog(LOG_WARNING,"replicator,read chunks: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%u)",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				vbuffs++;
			}
		}
// write data
		if (vbuffs==0) {	// no buffers ? - it should never happen
			syslog(LOG_WARNING,"replicator: no data received for block: %"PRIu16,b);
			rep_cleanup(&r);
			return MFS_ERROR_DISCONNECTED;
		} else if (vbuffs==1) { // xor not needed, so just find block and write it
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].mode!=IDLE) {
					rptr = r.repsources[i].packet;
					status = hdd_write(chunkid,0,b,rptr+20,0,MFSBLOCKSIZE,rptr+16);
					if (status!=MFS_STATUS_OK) {
						syslog(LOG_WARNING,"replicator: write status: %s",mfsstrerr(status));
						rep_cleanup(&r);
						return status;
					}
				}
			}
		} else {
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].mode!=IDLE) {
					rptr = r.repsources[i].packet;
					rptr += 16;
					crc = get32bit(&rptr);
					for (j=0 ; j<4 ; j++) {
						r.repsources[i].crcsums[j] = mycrc32(0,rptr+j*MFSBLOCKSIZE/4,MFSBLOCKSIZE/4);
					}
					if (crc != mycrc32_combine(mycrc32_combine(r.repsources[i].crcsums[0],r.repsources[i].crcsums[1],MFSBLOCKSIZE/4),mycrc32_combine(r.repsources[i].crcsums[2],r.repsources[i].crcsums[3],MFSBLOCKSIZE/4),MFSBLOCKSIZE/2)) {
						uint32_t ip;
						ip = r.repsources[i].ip;
						syslog(LOG_WARNING,"replicator: received data with wrong checksum from (%u.%u.%u.%u:%u)",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_CRC;
					}
				}
			}
			crc = mycrc32_zeroblock(0,MFSBLOCKSIZE/4);
			for (codeindex=0 ; codeindex<4 ; codeindex++) {
				codeword = xormasks[codeindex];
				first = 1;
				for (i=0 ; i<srccnt ; i++) {
					for (j=0 ; j<4 ; j++) {
						if (r.repsources[i].mode!=IDLE && (codeword&UINT32_C(0x80000000))) {
							rptr = r.repsources[i].packet;
							rptr += 16;
							if (first) {
								memcpy(r.xorbuff+4+codeindex*MFSBLOCKSIZE/4,rptr+4+j*MFSBLOCKSIZE/4,MFSBLOCKSIZE/4);
								first = 0;
								xcrc[codeindex] = r.repsources[i].crcsums[j];
							} else {
								xordata(r.xorbuff+4+codeindex*MFSBLOCKSIZE/4,rptr+4+j*MFSBLOCKSIZE/4,MFSBLOCKSIZE/4);
								xcrc[codeindex] ^= r.repsources[i].crcsums[j] ^ crc;
							}
						}
						codeword>>=1;
					}
				}
			}
			crc = mycrc32_combine(mycrc32_combine(xcrc[0],xcrc[1],MFSBLOCKSIZE/4),mycrc32_combine(xcrc[2],xcrc[3],MFSBLOCKSIZE/4),MFSBLOCKSIZE/2);
			wptr = r.xorbuff;
			put32bit(&wptr,crc);
/*
			first=1;
			if (vbuffs&1) {
				xcrc = 0;
			} else {
				xcrc = MFSCRCEMPTY; // = mycrc32_zeroblock(0,0x10000);
			}
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].mode!=IDLE) {
					uint32_t ip;
					ip = r.repsources[i].ip;
					rptr = r.repsources[i].packet;
					rptr+=16;	// skip chunkid,blockno,offset and size
					if (first) {
						memcpy(r.xorbuff+4,rptr+4,MFSBLOCKSIZE);
						first=0;
					} else {
						xordata(r.xorbuff+4,rptr+4,MFSBLOCKSIZE);
					}
					crc = get32bit(&rptr);
					if (crc!=mycrc32(0,rptr,MFSBLOCKSIZE)) {
						syslog(LOG_WARNING,"replicator: received data with wrong checksum from (%u.%u.%u.%u:%u)",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_CRC;
					}
					xcrc^=crc;
				}
			}
			wptr = r.xorbuff;
			put32bit(&wptr,xcrc);
*/
			status = hdd_write(chunkid,0,b,r.xorbuff+4,0,MFSBLOCKSIZE,r.xorbuff);
			if (status!=MFS_STATUS_OK) {
				syslog(LOG_WARNING,"replicator: xor write status: %s",mfsstrerr(status));
				rep_cleanup(&r);
				return status;
			}
		}
	}
// receive status
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
//			if (r.repsources[i].packet) {
//				free(r.repsources[i].packet);
//				r.repsources[i].packet=NULL;
//			}
			r.repsources[i].mode = HEADER;
			r.repsources[i].startptr = r.repsources[i].hdrbuff;
			r.repsources[i].bytesleft = 8;
		} else {
			r.repsources[i].mode = IDLE;
			r.repsources[i].bytesleft = 0;
		}
	}
	if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
		rep_cleanup(&r);
		return MFS_ERROR_DISCONNECTED;
	}
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
			uint32_t type,size;
			uint64_t pchid;
			uint8_t pstatus;
			uint32_t ip;
			rptr = r.repsources[i].hdrbuff;
			type = get32bit(&rptr);
			size = get32bit(&rptr);
			rptr = r.repsources[i].packet;
			ip = r.repsources[i].ip;
			if (rptr==NULL || type!=CSTOCL_READ_STATUS || size!=9) {
				syslog(LOG_WARNING,"replicator,check status: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%u)",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
				rep_cleanup(&r);
				return MFS_ERROR_DISCONNECTED;
			}
			pchid = get64bit(&rptr);
			pstatus = get8bit(&rptr);
			if (pchid!=r.repsources[i].chunkid) {
				syslog(LOG_WARNING,"replicator,check status: got wrong answer (read_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%u)",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
				rep_cleanup(&r);
				return MFS_ERROR_WRONGCHUNKID;
			}
			if (pstatus!=MFS_STATUS_OK) {
				syslog(LOG_NOTICE,"replicator,check status: got status: %s from (%u.%u.%u.%u:%u)",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
				rep_cleanup(&r);
				return pstatus;
			}
		}
	}
// close chunk and change version
	status = hdd_close(chunkid);
	if (status!=MFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_close status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 0;
	status = hdd_version(chunkid,0,version);
	if (status!=MFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_version status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.created = 0;
	rep_cleanup(&r);
	return MFS_STATUS_OK;
}
