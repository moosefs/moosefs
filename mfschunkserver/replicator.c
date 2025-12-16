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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
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
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "hddspacemgr.h"
#include "sockets.h"
#include "crc.h"
#include "mfslog.h"
#include "datapack.h"
#include "massert.h"
#include "mfsstrerr.h"
#include "clocks.h"

#include "replicator.h"

#define MAX_REP_TIME_SEC 150
#define PROGRESS_CHECK 30

#define SENDMSECTO 10000
#define RECVMSECTO 10000
#define CONNMAXTRY 10

#define REP_RETRY_CNT 5
#define REP_RETRY_TO 60

#define SRCCNTMAX 8

#define MAX_RECV_PACKET_SIZE (20+MFSBLOCKSIZE)

typedef enum _data_source_state {STATE_IDLE,STATE_CONNECTING,STATE_CONNECTED,STATE_ERROR} data_source_state;

typedef enum _modetypenum {IDLE,CONNECTING,CONNECTED,HEADER,DATA} modetypeenum;

typedef struct _repsrc {
	int sock;
	modetypeenum mode;
	uint8_t hdrbuff[8];
	uint8_t *packet;
	uint8_t *startptr;
	uint32_t bytesleft;

	uint64_t chunkid;
	uint32_t version;
	uint16_t blocks;

	uint32_t ip;
	uint16_t port;

	uint8_t *datapackets[4];

	uint32_t crcsums[4];
} repsrc;

typedef struct _replication {
	uint64_t chunkid;
	uint32_t version;

	uint8_t *xorbuff; // RECOVER only

	uint8_t created,opened;
	uint8_t needsreadrequest;
	uint8_t srccnt;
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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: connection lost (read)");
			return -1;
		}
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: read error");
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
				rs->packet = NULL;
			}
			if (size>0) {
				if (size>MAX_RECV_PACKET_SIZE) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: packet too long (%"PRIu32"/%u) ; command:%"PRIu32,size,MAX_RECV_PACKET_SIZE,type);
					return -1;
				}
				rs->packet = malloc(size);
				passert(rs->packet);
				rs->startptr = rs->packet;
			}
			rs->bytesleft = size;
			rs->mode = DATA;
		}
	}
	return 0;
}

static int rep_receive_all_packets(replication *r,uint32_t msecto) {
	uint8_t i,desc;
	uint64_t st,now;
	uint32_t msec;
	struct pollfd pfd[SRCCNTMAX];
	uint64_t noptime;
	uint8_t nopbuff[8],*wptr;

	if (r->srccnt>SRCCNTMAX) {
		return -1;
	}
	wptr = nopbuff;
	put32bit(&wptr,ANTOAN_NOP);
	put32bit(&wptr,0);
	st = monotonic_useconds();
	noptime = st;
	for (;;) {
		desc = 0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				pfd[desc].fd = r->repsources[i].sock;
				pfd[desc].events = POLLIN;
				pfd[desc].revents = 0;
				desc++;
			}
		}
		if (desc==0) {	// finished
			return 0;
		}
		msec = (monotonic_useconds()-st)/1000;
		if (msec>=msecto) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: receive timed out");
			return -1; // timed out
		}
		msec = msecto-msec;
		if (msec>1000) { // do not wait more than 1s for nop's sake
			msec = 1000;
		}
		if (poll(pfd,desc,msecto-msec)<0) {
			if (errno!=EINTR && ERRNO_ERROR) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: poll error");
				return -1;
			}
			continue;
		}
		desc=0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				if (pfd[desc].revents & POLLERR) {
					tcpgetstatus(pfd[desc].fd); // sets errno
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: socket error (pollerr/receive)");
					return -1;
				}
				if (pfd[desc].revents & POLLIN) {
					if (rep_read(r->repsources+i)<0) {
						return -1;
					}
				}
				desc++;
			}
		}
		now = monotonic_useconds();
		if (now>noptime+1000000) {
			noptime = now;
			for (i=0 ; i<r->srccnt ; i++) {
				if (write(r->repsources[i].sock,nopbuff,8)!=8) { // in such case we always expect at least 8 bytes in output buff - if not then we can safely assume that there are some problems with network connection and return an error
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

static void rep_create_read_request(repsrc *rs,uint32_t offset,uint32_t bsize) {
	uint8_t *ptr;
	ptr = rep_create_packet(rs,CLTOCS_READ,20);
	put64bit(&ptr,rs->chunkid);
	put32bit(&ptr,rs->version);
	put32bit(&ptr,offset);
	put32bit(&ptr,bsize);
}

/*
static void rep_no_packet(repsrc *rs) {
	if (rs->packet) {
		free(rs->packet);
	}
	rs->packet=NULL;
	rs->startptr=NULL;
	rs->bytesleft=0;
}
*/

static int rep_write(repsrc *rs) {
	int i;
	i = write(rs->sock,rs->startptr,rs->bytesleft);
	if (i==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: connection lost (write)");
		return -1;
	}
	if (i<0) {
		if (ERRNO_ERROR) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: write error");
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
	uint8_t i,desc;
	uint64_t st;
	uint32_t msec;
	struct pollfd pfd[SRCCNTMAX];

	if (r->srccnt>SRCCNTMAX) {
		return -1;
	}
	st = monotonic_useconds();
	for (;;) {
		desc = 0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				pfd[desc].fd = r->repsources[i].sock;
				pfd[desc].events = POLLOUT;
				pfd[desc].revents = 0;
				desc++;
			}
		}
		if (desc==0) { // finished
			return 0;
		}
		msec = (monotonic_useconds()-st)/1000;
		if (msec>=msecto) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: send timed out");
			return -1; // timed out
		}
		if (poll(pfd,desc,msecto-msec)<0) {
			if (errno!=EINTR && ERRNO_ERROR) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: poll error");
				return -1;
			}
			continue;
		}
		desc = 0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				if (pfd[desc].revents & POLLERR) {
					tcpgetstatus(pfd[desc].fd); // sets errno
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: socket error (pollerr/send)");
					return -1;
				}
				if (pfd[desc].revents & POLLOUT) {
					if (rep_write(r->repsources+i)<0) {
						return -1;
					}
				}
				desc++;
			}
		}
	}
}

static int rep_concurrent_connect(replication *r) {
	int s;
	int cres;
	data_source_state connect_state;
	struct pollfd pfd[SRCCNTMAX];
	uint8_t desc,finished;
	uint8_t cnt,i;

	if (r->srccnt>SRCCNTMAX) {
		return -1;
	}
	connect_state = STATE_CONNECTING;
	cnt = 0;
	while (cnt<CONNMAXTRY && connect_state==STATE_CONNECTING) {
		uint8_t newconnection;
		newconnection = 0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].mode==IDLE) {
				s = tcpsocket();
				if (s<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: socket error");
					connect_state = STATE_ERROR;
					break;
				}
				if (tcpnonblock(s)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replicator: nonblock error");
					connect_state = STATE_ERROR;
					break;
				}
				// if (srcip) {
				//	if (tcpnumbind(s,srcip,0)<0) {
				//		error
				//		break;
				//	}
				// }
				r->repsources[i].sock = s;
				cres = tcpnumconnect(s,r->repsources[i].ip,r->repsources[i].port);
				if (cres<0) {
					tcpclose(s);
					r->repsources[i].sock = -1;
					r->repsources[i].mode = IDLE; // not needed - left for code readability
					newconnection = 1;
				} else if (cres==0) {
					r->repsources[i].mode = CONNECTED;
				} else {
					r->repsources[i].mode = CONNECTING;
				}
			}
		}
		if (connect_state==STATE_ERROR) {
			break;
		}
		desc = 0;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].mode==CONNECTING) {
				pfd[desc].fd = r->repsources[i].sock;
				pfd[desc].events = POLLOUT;
				pfd[desc].revents = 0;
				desc++;
			}
		}
		if (desc>0) {
			if (poll(pfd,desc,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: poll error: %s",strerr(errno));
				connect_state = STATE_ERROR;
				break;
			}
			finished = 0;
			desc = 0;
			for (i=0 ; i<r->srccnt ; i++) {
				if (r->repsources[i].mode==CONNECTING) {
					if (pfd[desc].revents & (POLLOUT|POLLERR|POLLHUP)) {
						if (tcpgetstatus(r->repsources[i].sock)) {
							tcpclose(r->repsources[i].sock);
							r->repsources[i].sock = -1;
							r->repsources[i].mode = IDLE;
							newconnection = 1;
						} else {
							r->repsources[i].mode = CONNECTED;
						}
						finished = 1;
					}
					desc++;
				}
			}
			if (finished==0) { // timeout on poll
				desc = 0;
				for (i=0 ; i<r->srccnt ; i++) {
					if (r->repsources[i].mode==CONNECTING) {
						tcpclose(r->repsources[i].sock);
						r->repsources[i].sock = -1;
						r->repsources[i].mode = IDLE;
						newconnection = 1;
						desc++;
					}
				}
			}
		}
		connect_state = STATE_CONNECTED;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].mode==CONNECTING || r->repsources[i].mode==IDLE) {
				connect_state = STATE_CONNECTING;
				break;
			}
		}
		if (newconnection) {
			cnt++;
		}
	}
	return (connect_state != STATE_CONNECTED)?-1:0;
}

static int rep_reconnect(replication *r) {
	int i;
	int g;

	for (i=0 ; i<r->srccnt ; i++) {
		if (r->repsources[i].sock>=0) {
			tcpclose(r->repsources[i].sock);
			r->repsources[i].sock = -1;
		}
		if (r->repsources[i].packet) {
			free(r->repsources[i].packet);
			r->repsources[i].packet = NULL;
		}
		for (g=0 ; g<4 ; g++) {
			if (r->repsources[i].datapackets[g]) {
				free(r->repsources[i].datapackets[g]);
				r->repsources[i].datapackets[g] = NULL;
			}
		}
		r->repsources[i].mode = IDLE;
	}

// connect
	if (rep_concurrent_connect(r)<0) {
		return -1;
	}
// disable Nagle
	for (i=0 ; i<r->srccnt ; i++) {
		tcpnodelay(r->repsources[i].sock);
	}
	r->needsreadrequest = 1;
	return 0;
}

static void rep_cleanup(replication *r) {
	int i;
	int g;
	if (r->opened) {
		hdd_close(r->chunkid,0);
	}
	if (r->created) {
		hdd_rep_delete(r->chunkid,0);
	}
	for (i=0 ; i<r->srccnt ; i++) {
		if (r->repsources[i].sock>=0) {
			tcpclose(r->repsources[i].sock);
		}
		if (r->repsources[i].packet) {
			free(r->repsources[i].packet);
		}
		for (g=0 ; g<4 ; g++) {
			if (r->repsources[i].datapackets[g]) {
				free(r->repsources[i].datapackets[g]);
			}
		}
	}
	if (r->repsources) {
		free(r->repsources);
	}
	if (r->xorbuff) {
		free(r->xorbuff);
	}
}

uint8_t replicate(repmodeenum rmode,uint64_t chunkid,uint32_t version,uint8_t partno,uint8_t parts,const uint32_t srcip[MAX_EC_PARTS],const uint16_t srcport[MAX_EC_PARTS],const uint64_t srcchunkid[MAX_EC_PARTS]) {
	uint8_t status,i,nonzero;
	replication r;
	uint8_t srccnt,lastsrccnt,readsrccnt;
	uint8_t bg,blockgroup;
	uint16_t b,blocks;
	uint8_t trycnt;
	double reptotalto;
	double progcheck;
	double start,now;
	uint32_t xcrc,zcrc,bind;
	uint8_t *wptr;
	const uint8_t *rptr;

	start = monotonic_seconds();
	progcheck = start + PROGRESS_CHECK;
	reptotalto = start + MAX_REP_TIME_SEC;

#ifdef MFSDEBUG
		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"rmode: %s ; chunkid: %016"PRIX64" ; version: %08"PRIX32" ; partno: %"PRIu8" ; parts: %"PRIu8,(rmode==SIMPLE)?"SIMPLE":(rmode==SPLIT)?"SPLIT":(rmode==RECOVER)?"RECOVER":(rmode==JOIN)?"JOIN":"???",chunkid,version,partno,parts);
#endif
	if (rmode==SIMPLE || rmode==SPLIT) {
		srccnt = 1;
	} else {
		srccnt = parts;
	}
	if (rmode!=SPLIT) {
		partno = 0;
	} else {
		if (partno>=parts) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (rmode==RECOVER) {
		zcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE);
	} else {
		zcrc = 0;
	}

	if (srccnt==0 || srccnt>MAX_EC_PARTS) {
		return MFS_ERROR_EINVAL;
	}

#ifdef MFSDEBUG
	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"sources: %"PRIu8,srccnt);
	for (i=0 ; i<srccnt ; i++) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"source %"PRIu8": ip:%u.%u.%u.%u ; port:%"PRIu16" ; chunkid:%016"PRIX64,i,srcip[i]>>24,(srcip[i]>>16)&0xFF,(srcip[i]>>8)&0xFF,srcip[i]&0xFF,srcport[i],srcchunkid[i]);
	}
#endif

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"replication begin (chunkid:%08"PRIX64",version:%04"PRIX32",srccnt:%"PRIu8")",chunkid,version,srccnt);

	pthread_mutex_lock(&statslock);
	stats_repl++;
	pthread_mutex_unlock(&statslock);

// init replication structure
	r.chunkid = chunkid;
	r.version = version;
	r.srccnt = 0;
	r.created = 0;
	r.opened = 0;
	r.repsources = malloc(sizeof(repsrc)*srccnt);
	passert(r.repsources);
	if (rmode==RECOVER) {
		r.xorbuff = malloc(MFSBLOCKSIZE+4);
		passert(r.xorbuff);
	} else {
		r.xorbuff = NULL;
	}
// create chunk
	status = hdd_rep_create(chunkid,0);
	if (status!=MFS_STATUS_OK) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: create status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.created = 1;
// init sources
	r.srccnt = srccnt;
	for (i=0 ; i<srccnt ; i++) {
		r.repsources[i].mode = IDLE;
		r.repsources[i].chunkid = srcchunkid[i];
		r.repsources[i].version = version;
		r.repsources[i].ip = srcip[i];
		r.repsources[i].port = srcport[i];
		r.repsources[i].sock = -1;
		r.repsources[i].packet = NULL;
		for (bg=0 ; bg<4 ; bg++) {
			r.repsources[i].datapackets[bg] = NULL;
		}
	}
// connect
	if (rep_concurrent_connect(&r)<0) {
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: open status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 1;
// get block numbers
	for (i=0 ; i<srccnt ; i++) {
		wptr = rep_create_packet(r.repsources+i,ANTOCS_GET_CHUNK_BLOCKS,8+4);
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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,get # of blocks: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%"PRIu16")",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_DISCONNECTED;
		}
		pchid = get64bit(&rptr);
		pver = get32bit(&rptr);
		pblocks = get16bit(&rptr);
		pstatus = get8bit(&rptr);
		if (pchid!=r.repsources[i].chunkid) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,get # of blocks: got wrong answer (chunk_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%"PRIu16")",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_WRONGCHUNKID;
		}
		if (pver!=r.repsources[i].version) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,get # of blocks: got wrong answer (chunk_status:version:%"PRIX32"/%"PRIX32") from (%u.%u.%u.%u:%"PRIu16")",pver,r.repsources[i].version,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			rep_cleanup(&r);
			return MFS_ERROR_WRONGVERSION;
		}
		if (pstatus!=MFS_STATUS_OK) {
			if (pstatus==MFS_ERROR_NOTDONE) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: chunkserver (%u.%u.%u.%u:%"PRIu16") is overloaded",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,get # of blocks: got status: %s from (%u.%u.%u.%u:%"PRIu16")",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
			}
			rep_cleanup(&r);
			return pstatus;
		}
		r.repsources[i].blocks = pblocks;
	}
	switch (rmode) {
		case SIMPLE:
			blocks = r.repsources[0].blocks;
			blockgroup = 1;
			lastsrccnt = srccnt;
			break;
		case SPLIT:
			blocks = (((r.repsources[0].blocks+3)>>2) + parts - 1 - partno) / parts;
			blockgroup = 4;
			lastsrccnt = srccnt;
			break;
		case RECOVER:
			blocks = 0;
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].blocks > blocks) {
					blocks = r.repsources[i].blocks;
				}
			}
			blocks = (blocks+3) >> 2;
			blockgroup = 4;
			lastsrccnt = srccnt;
			break;
		case JOIN:
			blocks = 0;
			lastsrccnt = srccnt;
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].blocks > blocks) {
					blocks = r.repsources[i].blocks;
				}
			}
			blocks = (blocks+3) >> 2;
			blockgroup = 4;
			if (blocks>0) {
				while (lastsrccnt>0 && r.repsources[lastsrccnt-1].blocks <= ((blocks-1)<<2)) {
					lastsrccnt--;
				}
			}
			if (blocks==1) {
				srccnt = lastsrccnt;
			}
			break;
		default:
			/* initialize some variables - silences unnecessary 'may be used uninitialized' warning */
			blocks = 0;
			blockgroup = 0;
			lastsrccnt = 0;
			break;
	}
	r.needsreadrequest = 1;
// receive data and write to hdd
	b = 0;
	trycnt = REP_RETRY_CNT;
	while (b<blocks) {
		now = monotonic_seconds();
		if (now > reptotalto) { // timed out
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: operation timed out after %.2lfs (total timeout)",now-start);
			rep_cleanup(&r);
			return MFS_ERROR_ETIMEDOUT;
		}
		if (now > progcheck) {
			progcheck += PROGRESS_CHECK;
			if ((((now - start) * blocks) / b) > MAX_REP_TIME_SEC) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: operation timed out after %.2lfs (partial timeout, replicated only %u blocks out of %u)",now-start,b,blocks);
				rep_cleanup(&r);
				return MFS_ERROR_ETIMEDOUT;
			}
		}
		if (b+1==blocks) {
			readsrccnt = lastsrccnt;
		} else {
			readsrccnt = srccnt;
		}
// prepare read requests
		switch (rmode) {
			case SIMPLE:
				if (r.needsreadrequest) {
					rep_create_read_request(r.repsources,b*MFSBLOCKSIZE,(blocks-b)*MFSBLOCKSIZE);
				}
				break;
			case SPLIT:
				rep_create_read_request(r.repsources,partno*4*MFSBLOCKSIZE+b*(parts*4*MFSBLOCKSIZE),4*MFSBLOCKSIZE);
				break;
			case RECOVER:
				for (i=0 ; i<readsrccnt ; i++) {
					rep_create_read_request(r.repsources+i,b*4*MFSBLOCKSIZE,4*MFSBLOCKSIZE);
				}
				break;
			case JOIN:
				for (i=0 ; i<readsrccnt ; i++) {
					rep_create_read_request(r.repsources+i,b*4*MFSBLOCKSIZE,4*MFSBLOCKSIZE);
				}
//					for (i=0 ; i<srccnt ; i++) {
//						if (i<lastsrccnt) {
//							rep_create_read_request(r.repsources+i,0,blocks*4*MFSBLOCKSIZE);
//						} else {
//							rep_create_read_request(r.repsources+i,0,(blocks-1)*4*MFSBLOCKSIZE);
//						}
//					}
				break;
		}
		r.needsreadrequest = 0;
// if necessary send requests
		if (rep_send_all_packets(&r,SENDMSECTO)<0) {
			if (trycnt>0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunkid: %016"PRIX64" ; version: %08"PRIX32" ; replication '%s' ; send timeout - reconnect",chunkid,version,(rmode==SIMPLE)?"SIMPLE":(rmode==SPLIT)?"SPLIT":(rmode==RECOVER)?"RECOVER":(rmode==JOIN)?"JOIN":"???");
				if (rep_reconnect(&r)<0) {
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				trycnt--;
				continue;
			} else {
				rep_cleanup(&r);
				return MFS_ERROR_DISCONNECTED;
			}
		}
		for (bg=0 ; bg<blockgroup ; bg++) {
// prepare receive
			for (i=0 ; i<readsrccnt ; i++) {
				r.repsources[i].mode = HEADER;
				r.repsources[i].startptr = r.repsources[i].hdrbuff;
				r.repsources[i].bytesleft = 8;
			}
// receive data
			if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
				if (trycnt>0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunkid: %016"PRIX64" ; version: %08"PRIX32" ; replication '%s' ; receive timeout - reconnect",chunkid,version,(rmode==SIMPLE)?"SIMPLE":(rmode==SPLIT)?"SPLIT":(rmode==RECOVER)?"RECOVER":(rmode==JOIN)?"JOIN":"???");
					if (rep_reconnect(&r)<0) {
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					trycnt--;
					break;
				} else {
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
			}
// check and remember data packets
			for (i=0 ; i<readsrccnt ; i++) {
				uint32_t type,size;
				uint64_t pchid;
				uint16_t pblocknum;
				uint16_t poffset;
				uint16_t expectedblocknum;
				uint32_t psize;
				uint8_t pstatus;
				uint32_t ip;
				rptr = r.repsources[i].hdrbuff;
				type = get32bit(&rptr);
				size = get32bit(&rptr);
				rptr = r.repsources[i].packet;
				ip = r.repsources[i].ip;
				switch (rmode) {
					case SIMPLE:
						expectedblocknum = b;
						break;
					case SPLIT:
						expectedblocknum = ((b * parts) + partno) * 4 + bg;
						break;
					case RECOVER:
					case JOIN:
						expectedblocknum = b*4 + bg;
						break;
					default:
						expectedblocknum = 0xFFFF; // any stupid value - just to silence compiler warnings
				}
				if (rptr==NULL) {
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				if (type==CSTOCL_READ_STATUS && size==9) {
					pchid = get64bit(&rptr);
					pstatus = get8bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (read_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%"PRIu16")",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGCHUNKID;
					}
					if (pstatus==MFS_STATUS_OK) {	// got status too early or got incorrect packet
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got unexpected ok status from (%u.%u.%u.%u:%"PRIu16")",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator,read chunks: got status: %s from (%u.%u.%u.%u:%"PRIu16")",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return pstatus;
				} else if (type==CSTOCL_READ_DATA && size==20+MFSBLOCKSIZE) {
					pchid = get64bit(&rptr);
					pblocknum = get16bit(&rptr);
					poffset = get16bit(&rptr);
					psize = get32bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (read_data:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%"PRIu16")",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGCHUNKID;
					}
					if (pblocknum!=expectedblocknum) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (read_data:blocknum:%"PRIu16"/%"PRIu16") from (%u.%u.%u.%u:%"PRIu16")",pblocknum,b,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					if (poffset!=0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (read_data:offset:%"PRIu16") from (%u.%u.%u.%u:%"PRIu16")",poffset,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGOFFSET;
					}
					if (psize!=MFSBLOCKSIZE) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (read_data:size:%"PRIu32") from (%u.%u.%u.%u:%"PRIu16")",psize,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
						rep_cleanup(&r);
						return MFS_ERROR_WRONGSIZE;
					}
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,read chunks: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%"PRIu16")",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				r.repsources[i].datapackets[bg] = r.repsources[i].packet;
				r.repsources[i].packet = NULL;
			}
		}
		if (r.needsreadrequest) {
			continue;
		}
// if necessary receive statuses
		if (rmode!=SIMPLE || (b+1 == blocks)) { // receive statuses
// prepare receive
			for (i=0 ; i<readsrccnt ; i++) {
				r.repsources[i].mode = HEADER;
				r.repsources[i].startptr = r.repsources[i].hdrbuff;
				r.repsources[i].bytesleft = 8;
			}
// receive data
			if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
				if (trycnt>0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunkid: %016"PRIX64" ; version: %08"PRIX32" ; replication '%s' ; receive status timeout - reconnect",chunkid,version,(rmode==SIMPLE)?"SIMPLE":(rmode==SPLIT)?"SPLIT":(rmode==RECOVER)?"RECOVER":(rmode==JOIN)?"JOIN":"???");
					if (rep_reconnect(&r)<0) {
						rep_cleanup(&r);
						return MFS_ERROR_DISCONNECTED;
					}
					trycnt--;
					continue;
				} else {
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
			}
// check status packets
			for (i=0 ; i<readsrccnt ; i++) {
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
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,check status: got wrong answer (type:0x%08"PRIX32"/size:0x%08"PRIX32") from (%u.%u.%u.%u:%"PRIu16")",type,size,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return MFS_ERROR_DISCONNECTED;
				}
				pchid = get64bit(&rptr);
				pstatus = get8bit(&rptr);
				if (pchid!=r.repsources[i].chunkid) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator,check status: got wrong answer (read_status:chunkid:%"PRIX64"/%"PRIX64") from (%u.%u.%u.%u:%"PRIu16")",pchid,r.repsources[i].chunkid,(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					rep_cleanup(&r);
					return MFS_ERROR_WRONGCHUNKID;
				}
				if (pstatus!=MFS_STATUS_OK) {
					if (pstatus==MFS_ERROR_NOTDONE) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator: chunkserver (%u.%u.%u.%u:%"PRIu16") is overloaded",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					} else {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"replicator,check status: got status: %s from (%u.%u.%u.%u:%"PRIu16")",mfsstrerr(pstatus),(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,r.repsources[i].port);
					}
					rep_cleanup(&r);
					return pstatus;
				}
			}
		}
// write data to chunk
		switch (rmode) {
			case SIMPLE:
				rptr = r.repsources[0].datapackets[0];
				status = hdd_write(chunkid,0,b,rptr+20,0,MFSBLOCKSIZE,rptr+16);
				if (status!=MFS_STATUS_OK) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: write status: %s",mfsstrerr(status));
					rep_cleanup(&r);
					return status;
				}
				break;
			case SPLIT:
				for (bg = 0 ; bg < blockgroup ; bg++) {
					rptr = r.repsources[0].datapackets[bg];
					status = hdd_write(chunkid,0,b*blockgroup+bg,rptr+20,0,MFSBLOCKSIZE,rptr+16);
					if (status!=MFS_STATUS_OK) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: write status: %s",mfsstrerr(status));
						rep_cleanup(&r);
						return status;
					}
				}
				break;
			case RECOVER:
				for (bg = 0 ; bg < blockgroup ; bg++) {
					xcrc = 0;
					for (i=0 ; i<srccnt ; i++) {
						rptr = r.repsources[i].datapackets[bg];
						rptr += 16;
						if (i==0) {
							xcrc = get32bit(&rptr);
							memcpy(r.xorbuff+4,rptr,MFSBLOCKSIZE);
						} else {
							xcrc ^= get32bit(&rptr);
							xordata(r.xorbuff+4,rptr,MFSBLOCKSIZE);
						}
					}
					xcrc ^= zcrc;
					wptr = r.xorbuff;
					put32bit(&wptr,xcrc);
					nonzero = 1;
					if (b+1==blocks && xcrc==zcrc) {
						uint32_t *aptr,oraux;
						rptr = r.xorbuff;
						oraux = 0;
						if ((((unsigned long)(rptr)) & 0x3) == 0 && (MFSBLOCKSIZE & 0x3) == 0) { // should be always true
							aptr = (uint32_t*)(rptr+4);
							for (bind=0 ; bind<MFSBLOCKSIZE/4 ; bind++) {
								oraux |= *aptr;
								aptr++;
							}
						} else {
							for (bind=0 ; bind<MFSBLOCKSIZE ; bind++) {
								oraux |= rptr[4+bind];
							}
						}
						if (oraux==0) {
							nonzero = 0;
						}
					}
					if (nonzero) {
						rptr = r.xorbuff;
						status = hdd_write(chunkid,0,b*blockgroup+bg,rptr+4,0,MFSBLOCKSIZE,rptr);
						if (status!=MFS_STATUS_OK) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: write status: %s",mfsstrerr(status));
							rep_cleanup(&r);
							return status;
						}
					}
				}
				break;
			case JOIN:
				for (i=0 ; i<readsrccnt ; i++) {
					for (bg = 0 ; bg < blockgroup ; bg++) {
						rptr = r.repsources[i].datapackets[bg];
						status = hdd_write(chunkid,0,(b*parts+i)*blockgroup+bg,rptr+20,0,MFSBLOCKSIZE,rptr+16);
						if (status!=MFS_STATUS_OK) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: write status: %s",mfsstrerr(status));
							rep_cleanup(&r);
							return status;
						}
					}
				}
				break;
		}
		for (i=0 ; i<readsrccnt ; i++) {
			for (bg = 0 ; bg < blockgroup ; bg++) {
				if (r.repsources[i].datapackets[bg]!=NULL) {
					free(r.repsources[i].datapackets[bg]);
				}
				r.repsources[i].datapackets[bg] = NULL;
			}
		}
		b++;
		trycnt = REP_RETRY_CNT;
	}

// change version and then close chunk
	status = hdd_rep_setversion(chunkid,version);
	if (status!=MFS_STATUS_OK) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: set version status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	status = hdd_close(chunkid,1);
	if (status!=MFS_STATUS_OK) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"replicator: close status: %s",mfsstrerr(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 0;
	r.created = 0;
	rep_cleanup(&r);
	return MFS_STATUS_OK;
}
