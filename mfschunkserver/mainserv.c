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

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef HAVE_POLL_H
# include <poll.h>
#else
# include <sys/poll.h>
#endif
#include <inttypes.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "cfg.h"
#include "sockets.h"
#include "mfslog.h"
#include "datapack.h"
#include "massert.h"
#include "charts.h"
#include "hddspacemgr.h"
#include "main.h"
#include "lwthread.h"
#include "clocks.h"
#include "portable.h"
#include "mainserv.h"
#ifdef USE_CONNCACHE
#include "conncache.h"
#endif
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "crc.h" // tests only


#define SERV_TIMEOUT 5000

#define NOPS_INTERVAL 1000000

#define SMALL_PACKET_SIZE 12

#define CONNECT_RETRIES 10
#define CONNECT_TIMEOUT(cnt) (((cnt)%2)?(300*(1<<((cnt)>>1))):(200*(1<<((cnt)>>1))))

static uint32_t MaxPacketSize = CSTOCS_MAXPACKETSIZE;

#ifdef HAVE_MMAP
static uint8_t CanUseMmap = 0;

#define myalloc(ptr,size) if (CanUseMmap) { \
	ptr = mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0); \
	passert(ptr); \
} else { \
	ptr = malloc(size); \
	passert(ptr); \
}

#define myunalloc(ptr,size) if (CanUseMmap) { \
	munmap(ptr,size); \
} else { \
	free(ptr); \
}
#else
#define myalloc(ptr,size) { \
	ptr = malloc(size); \
	passert(ptr); \
}
#define myunalloc(ptr,size) free(size)
#endif

// stats_X
#ifndef HAVE___SYNC_FETCH_AND_OP
static pthread_mutex_t statslock = PTHREAD_MUTEX_INITIALIZER;
#endif

static uint64_t stats_bytesin=0;
static uint64_t stats_bytesout=0;
static uint32_t stats_hlopr=0;
static uint32_t stats_hlopw=0;

void mainserv_stats(uint64_t *bin,uint64_t *bout,uint32_t *hlopr,uint32_t *hlopw) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	*bin = __sync_fetch_and_and(&stats_bytesin,0);
	*bout = __sync_fetch_and_and(&stats_bytesout,0);
	*hlopr = __sync_fetch_and_and(&stats_hlopr,0);
	*hlopw = __sync_fetch_and_and(&stats_hlopw,0);
#else
	zassert(pthread_mutex_lock(&statslock));
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*hlopr = stats_hlopr;
	*hlopw = stats_hlopw;
	stats_bytesin = 0;
	stats_bytesout = 0;
	stats_hlopr = 0;
	stats_hlopw = 0;
	zassert(pthread_mutex_unlock(&statslock));
#endif
}

static inline void mainserv_bytesin(uint64_t bytes) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_add(&stats_bytesin,bytes);
#else
	zassert(pthread_mutex_lock(&statslock));
	stats_bytesin += bytes;
	zassert(pthread_mutex_unlock(&statslock));
#endif
}

static inline void mainserv_bytesout(uint64_t bytes) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_add(&stats_bytesout,bytes);
#else
	zassert(pthread_mutex_lock(&statslock));
	stats_bytesout += bytes;
	zassert(pthread_mutex_unlock(&statslock));
#endif
}

static inline int32_t mainserv_toread(int sock,uint8_t *ptr,uint32_t leng,uint32_t timeout) {
	int32_t r;
	r = tcptoread(sock,ptr,leng,timeout,timeout*30);
	if (r>0) {
		mainserv_bytesin(r);
	}
	return r;
}

static inline int32_t mainserv_towrite(int sock,const uint8_t *ptr,uint32_t leng,uint32_t timeout) {
	int32_t r;
	r = tcptowrite(sock,ptr,leng,timeout,timeout*30);
	if (r>0) {
		mainserv_bytesout(r);
	}
	return r;
}

static inline int32_t mainserv_toforward(int sock1,int sock2,uint8_t *ptr,uint32_t leng,uint32_t rskip,uint32_t wskip,uint32_t timeout) {
	int32_t r;
	r = tcptoforward(sock1,sock2,ptr,leng,rskip,wskip,timeout,timeout*30);
	if (r>0) {
		if ((uint32_t)r>rskip) {
			mainserv_bytesin(r-rskip);
		}
		if ((uint32_t)r>wskip) {
			mainserv_bytesout(r-wskip);
		}
	}
	return r;
}

uint8_t* mainserv_create_packet(uint8_t **wptr,uint32_t cmd,uint32_t leng) {
	uint8_t *ptr;
	ptr = malloc(leng+8);
	passert(ptr);
	*wptr = ptr;
	put32bit(wptr,cmd);
	put32bit(wptr,leng);
	return ptr;
}

uint8_t mainserv_send_and_free(const char *packetname,int sock,uint8_t *ptr,uint32_t pleng) {
	uint8_t r;
	uint8_t d;
	r = (mainserv_towrite(sock,ptr,pleng+8,SERV_TIMEOUT)!=(int32_t)(pleng+8))?0:1;
	d = (errno==EPIPE || errno==ECONNRESET)?1:0;
	free(ptr);
	if (r==0) {
		if (d) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"send_and_free: 'send(%s)' disconnected",packetname);
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"send_and_free: 'send(%s)' timed out",packetname);
		}
	}
	return r;
}

int mainserv_connect(uint32_t fwdip,uint16_t fwdport,uint32_t timeout) {
	int fwdsock;
	fwdsock = tcpsocket();
	if (fwdsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"create socket, error");
		return -1;
	}
	if (tcpnonblock(fwdsock)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"set nonblock, error");
		tcpclose(fwdsock);
		return -1;
	}
	if (tcpnumtoconnect(fwdsock,fwdip,fwdport,timeout)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"connect to %u.%u.%u.%u:%u failed, error",(fwdip>>24)&0xFF,(fwdip>>16)&0xFF,(fwdip>>8)&0xFF,fwdip&0xFF,fwdport);
		tcpclose(fwdsock);
		return -1;
	}
	if (tcpnodelay(fwdsock)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,"can't set TCP_NODELAY, error");
	}
	return fwdsock;
}

typedef struct sock_nops {
	int sock;
	uint8_t error;
	uint64_t monotonic_utime;
	uint32_t bytesleft;
	struct sock_nops *next,**prev;
} sock_nops;

static sock_nops *sock_nops_head,**sock_nops_tail;
static pthread_mutex_t sock_nops_lock;
static uint8_t sock_nop_buff[8];

static inline void mainserv_sock_nop_append(sock_nops* sn) {
	if (sn->prev==NULL) {
		sn->next = NULL;
		sn->prev = sock_nops_tail;
		*(sock_nops_tail) = sn;
		sock_nops_tail = &(sn->next);
		sn->monotonic_utime = monotonic_useconds();
	}
}

static inline void mainserv_sock_nop_remove(sock_nops* sn) {
	if (sn->prev!=NULL) {
		if (sn->next) {
			sn->next->prev = sn->prev;
		} else {
			sock_nops_tail = sn->prev;
		}
		*(sn->prev) = sn->next;
		sn->next = NULL;
		sn->prev = NULL;
	}
}

void* mainserv_sock_nop_sender(void* arg) {
	uint64_t monotonic_utime;
	uint64_t sleep_utime;
	uint8_t *wptr;
	int32_t i;
	sock_nops* sn;

	wptr = sock_nop_buff;
	put32bit(&wptr,ANTOAN_NOP);
	put32bit(&wptr,0);

	do {
		zassert(pthread_mutex_lock(&sock_nops_lock));
		if (sock_nops_head==NULL) {
			sleep_utime = NOPS_INTERVAL;
		} else {
			monotonic_utime = monotonic_useconds();
			while (sock_nops_head->monotonic_utime + NOPS_INTERVAL <= monotonic_utime) {
				if (sock_nops_head->error==0) {
					if (sock_nops_head->bytesleft) {
						i = write(sock_nops_head->sock,sock_nop_buff+(8-sock_nops_head->bytesleft),sock_nops_head->bytesleft);
					} else {
						sock_nops_head->bytesleft = 8;
						i = write(sock_nops_head->sock,sock_nop_buff,8);
					}
					if (i<0) {
						if (ERRNO_ERROR) {
							sock_nops_head->error = 1;
							sock_nops_head->bytesleft = 0;
						}
					}
					if (i>0) {
						sock_nops_head->bytesleft -= i;
					}
				}
				sn = sock_nops_head;
				mainserv_sock_nop_remove(sn);
				mainserv_sock_nop_append(sn);
			}
			if (monotonic_utime - sock_nops_head->monotonic_utime < NOPS_INTERVAL) {
				sleep_utime = NOPS_INTERVAL - (monotonic_utime - sock_nops_head->monotonic_utime);
			} else {
				sleep_utime = 0;
			}
		}
		zassert(pthread_mutex_unlock(&sock_nops_lock));
		if (sleep_utime>0) {
			portable_usleep(sleep_utime);
		}
	} while (1);
	return arg;
}

static inline void mainserv_sock_nop_init(sock_nops *sn,int sock) {
	sn->sock = sock;
	sn->error = 0;
	sn->bytesleft = 0;
	sn->next = NULL;
	sn->prev = NULL;
}

static inline void mainserv_sock_nop_add(sock_nops* sn) {
	zassert(pthread_mutex_lock(&sock_nops_lock));
	mainserv_sock_nop_append(sn);
	zassert(pthread_mutex_unlock(&sock_nops_lock));
}

static inline void mainserv_sock_nop_del(sock_nops* sn) {
	zassert(pthread_mutex_lock(&sock_nops_lock));
	mainserv_sock_nop_remove(sn);
	zassert(pthread_mutex_unlock(&sock_nops_lock));
	if (sn->bytesleft>0 && sn->bytesleft<8) {
		if (mainserv_towrite(sn->sock,sock_nop_buff+(8-sn->bytesleft),sn->bytesleft,SERV_TIMEOUT)!=(int32_t)sn->bytesleft) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"nop_tail: 'send(nop)' timed out");
			sn->error = 1;
		}
	}
	sn->bytesleft = 0;
}

uint8_t mainserv_read(int sock,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t offset;
	uint32_t size;
	uint16_t blocknum;
	uint16_t blockoffset;
	uint32_t blocksize;
	uint8_t *packet,*wptr;
	uint8_t status;
	uint8_t ret;
	uint8_t protover;
	int32_t i;
	uint32_t rcvd;
	const uint8_t *rptr;
	uint8_t hdr[8];
	uint32_t cmd,leng;
	sock_nops sn;

	if (length!=20 && length!=21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_READ - wrong size (%"PRIu32"/20|21)",length);
		return 0;
	}
	if (length==21) {
		protover = get8bit(&data);
		if (protover) {
			mainserv_sock_nop_init(&sn,sock);
		}
	} else {
		protover = 0;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	offset = get32bit(&data);
	size = get32bit(&data);
	if (size==0) {
		packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&wptr,chunkid);
		put8bit(&wptr,MFS_STATUS_OK);	// no bytes to read - just return MFS_STATUS_OK
		return mainserv_send_and_free("read status",sock,packet,8+1);
	}
	if (size>MFSCHUNKSIZE) {
		packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&wptr,chunkid);
		put8bit(&wptr,MFS_ERROR_WRONGSIZE);
		return mainserv_send_and_free("read status",sock,packet,8+1);
	}
	if (offset>=MFSCHUNKSIZE || offset+size>MFSCHUNKSIZE) {
		packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&wptr,chunkid);
		put8bit(&wptr,MFS_ERROR_WRONGOFFSET);
		return mainserv_send_and_free("read status",sock,packet,8+1);
	}
	if (protover) {
		mainserv_sock_nop_add(&sn);
	}
	status = hdd_open(chunkid,version);
	if (protover) {
		mainserv_sock_nop_del(&sn);
		if (sn.error) {
			if (status==MFS_STATUS_OK) {
				hdd_close(chunkid,0);
			}
			return 0;
		}
	}
	if (status!=MFS_STATUS_OK) {
		packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&wptr,chunkid);
		put8bit(&wptr,status);
		return mainserv_send_and_free("read status",sock,packet,8+1);
	}
	if (protover) {
		mainserv_sock_nop_add(&sn);
	}
	hdd_precache_data(chunkid,offset,size);
	if (protover) {
		mainserv_sock_nop_del(&sn);
		if (sn.error) {
			if (status==MFS_STATUS_OK) {
				hdd_close(chunkid,0);
			}
			return 0;
		}
	}
	rcvd = 0;
	while (size>0) {
		blocknum = (offset)>>MFSBLOCKBITS;
		blockoffset = (offset)&MFSBLOCKMASK;
		if (((offset+size-1)>>MFSBLOCKBITS) == blocknum) {	// last block
			blocksize = size;
		} else {
			blocksize = MFSBLOCKSIZE-blockoffset;
		}
		packet = mainserv_create_packet(&wptr,CSTOCL_READ_DATA,8+2+2+4+4+blocksize);
		put64bit(&wptr,chunkid);
		put16bit(&wptr,blocknum);
		put16bit(&wptr,blockoffset);
		put32bit(&wptr,blocksize);
		if (protover) {
			mainserv_sock_nop_add(&sn);
		}
		status = hdd_read(chunkid,version,blocknum,wptr+4,blockoffset,blocksize,wptr);
		if (protover) {
			mainserv_sock_nop_del(&sn);
			if (sn.error) {
				hdd_close(chunkid,0);
				return 0;
			}
		}
		if (status!=MFS_STATUS_OK) {
			free(packet);
			hdd_close(chunkid,0);
			packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
			put64bit(&wptr,chunkid);
			put8bit(&wptr,status);
			ret = mainserv_send_and_free("read status",sock,packet,8+1);
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_add(&stats_hlopr,1);
#else
			zassert(pthread_mutex_lock(&statslock));
			stats_hlopr++;
			zassert(pthread_mutex_unlock(&statslock));
#endif
			return ret;
		}
		if (mainserv_send_and_free("read data",sock,packet,8+2+2+4+4+blocksize)==0) {
			hdd_close(chunkid,0);
			return 0;
		}
		offset += blocksize;
		size -= blocksize;
		i = read(sock,hdr+rcvd,(8-rcvd));
		if (i<0) { // error or nothing to read
			if (ERRNO_ERROR) {
				hdd_close(chunkid,0);
				return 0;
			}
		} else if (i==0) { // hup
			hdd_close(chunkid,0);
			return 0;
		} else {
			rcvd += i;
			if (rcvd==8) {
				rptr = hdr;
				cmd = get32bit(&rptr);
				leng = get32bit(&rptr);
				if (cmd==ANTOAN_NOP && leng==0) { // received nop
					rcvd=0;
				} else { // received garbage
					hdd_close(chunkid,0);
					return 0;
				}
			}
		}
	}
	hdd_close(chunkid,0);
	packet = mainserv_create_packet(&wptr,CSTOCL_READ_STATUS,8+1);
	put64bit(&wptr,chunkid);
	put8bit(&wptr,MFS_STATUS_OK);	// no bytes to read - just return MFS_STATUS_OK
	ret = mainserv_send_and_free("read status",sock,packet,8+1);
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_add(&stats_hlopr,1);
#else
	zassert(pthread_mutex_lock(&statslock));
	stats_hlopr++;
	zassert(pthread_mutex_unlock(&statslock));
#endif
	return ret;
}

typedef struct write_job {
	uint64_t chunkid;
	uint32_t writeid;
	uint16_t blocknum;
	uint16_t offset;
	uint32_t size;
	const uint8_t *crcptr;
	const uint8_t *buff;
	uint8_t hddstatus;
	uint8_t netstatus;
	uint8_t ack;
	struct write_job *next;
	uint32_t structsize;
	uint8_t data[1];
} write_job;

typedef struct write_xchg {
	uint64_t chunkid;
	uint32_t version;
	int pipe[2];
	write_job *head,*hddhead,*nethead,**tail;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	uint8_t condwaiting;
	uint8_t term;
} write_xchg;

void* mainserv_write_thread(void* arg) {
	write_xchg *wrdata = (write_xchg*)arg;
	write_job *wrjob;
	uint64_t gchunkid;
	uint32_t gversion;
	uint8_t status;
	while (1) {
		zassert(pthread_mutex_lock(&(wrdata->lock)));
		while (wrdata->hddhead==NULL && wrdata->term==0) {
			wrdata->condwaiting=1;
			zassert(pthread_cond_wait(&(wrdata->cond),&(wrdata->lock)));
		}
		if (wrdata->term) {
			zassert(pthread_mutex_unlock(&(wrdata->lock)));
			return NULL;
		}
		wrjob = wrdata->hddhead;
		gchunkid = wrdata->chunkid;
		gversion = wrdata->version;
		zassert(pthread_mutex_unlock(&(wrdata->lock)));
		status = hdd_write(gchunkid,gversion,wrjob->blocknum,wrjob->buff,wrjob->offset,wrjob->size,wrjob->crcptr);
		zassert(pthread_mutex_lock(&(wrdata->lock)));
		wrjob->hddstatus = status;
		wrjob->ack |= 1;
		wrdata->hddhead = wrjob->next;
		zassert(pthread_mutex_unlock(&(wrdata->lock)));
		if (write(wrdata->pipe[1],"*",1)!=1) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"pipe write error");
		}
		if (status!=MFS_STATUS_OK) {
			break;
		}
	}
	return NULL;
}

uint8_t mainserv_write_middle(int sock,int fwdsock,uint64_t gchunkid,uint32_t gversion,uint8_t protover,sock_nops *sn,sock_nops *fsn) {
	pthread_t wrthread;
	write_xchg wrdata;
	write_job *wrjob;
	uint8_t *packet,*wptr;
	const uint8_t *rptr;
	struct pollfd pfd[3];
	uint8_t hdr[8+SMALL_PACKET_SIZE];
	uint8_t *pdata;
	uint32_t pdataleng;
	uint32_t cmd,leng;
	uint64_t chunkid;
	uint32_t writeid;
	uint8_t status;
	uint8_t gotlast;

	wrdata.chunkid = gchunkid;
	wrdata.version = gversion;
	if (pipe(wrdata.pipe)<0) {
		return 0;
	}
	pfd[0].fd = sock;
	pfd[0].events = POLLIN;
	pfd[1].fd = fwdsock;
	pfd[1].events = POLLIN;
	pfd[2].fd = wrdata.pipe[0];
	pfd[2].events = POLLIN;
	wrdata.head = NULL;
	wrdata.tail = &(wrdata.head);
	wrdata.hddhead = NULL;
	wrdata.nethead = NULL;
	zassert(pthread_mutex_init(&(wrdata.lock),NULL));
	zassert(pthread_cond_init(&(wrdata.cond),NULL));
	wrdata.condwaiting = 0;
	wrdata.term = 0;
	lwt_minthread_create(&wrthread,0,mainserv_write_thread,&wrdata);

	wrjob = NULL;
	pdataleng = 4096;
	myalloc(pdata,pdataleng);
	gotlast = 0;

	while (1) {
		if (poll(pfd,3,SERV_TIMEOUT)<0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'poll' timed out");
			break;
		}
		if (pfd[0].revents & POLLERR) {
			tcpgetstatus(pfd[0].fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"write_middle: 'poll(prev)' returned POLLERR");
		}
		if (pfd[1].revents & POLLERR) {
			tcpgetstatus(pfd[1].fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"write_middle: 'poll(next)' returned POLLERR");
		}
		if (pfd[2].revents & POLLERR) {
			tcpgetstatus(pfd[2].fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"write_middle: 'poll(pipe)' returned POLLERR");
		}
		if ((pfd[0].revents & POLLERR) || (pfd[1].revents & POLLERR) || (pfd[2].revents & POLLERR)) {
			break;
		}
		if (((pfd[0].revents & (POLLIN|POLLHUP))==POLLHUP) || ((pfd[1].revents & (POLLIN|POLLHUP))==POLLHUP) || ((pfd[2].revents & (POLLIN|POLLHUP))==POLLHUP)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'poll' returned POLLHUP");
			break;
		}
		if (pfd[0].revents & POLLIN) {
			if (mainserv_toread(sock,hdr,8,SERV_TIMEOUT)!=8) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'receive(header prev)' timed out");
				break;
			}
			rptr = hdr;
			cmd = get32bit(&rptr);
			leng = get32bit(&rptr);
			if (protover) {
				mainserv_sock_nop_del(fsn);
			}
			if (cmd==CLTOCS_WRITE_DATA) {
				if (leng>0) {
					if (leng > MaxPacketSize) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MaxPacketSize,cmd);
						break;
					}
					myalloc(wrjob,offsetof(write_job,data)+leng+8);
					wrjob->structsize = offsetof(write_job,data)+leng+8;
					memcpy(wrjob->data,hdr,8);
					if (mainserv_toforward(sock,fwdsock,wrjob->data,leng+8,8,0,SERV_TIMEOUT)!=(int32_t)(leng+8)) {
						myunalloc(wrjob,wrjob->structsize);
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'forward(write data)' timed out");
						break;
					}
				} else {
					break;
				}
			} else {
				if (leng>0) {
					if (leng > SMALL_PACKET_SIZE) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"packet too long (%"PRIu32"/12) ; command:%"PRIu32,leng,cmd);
						break;
					}
					if (mainserv_toforward(sock,fwdsock,hdr,leng+8,8,0,SERV_TIMEOUT)!=(int32_t)(leng+8)) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'forward(%s)' timed out",(cmd==CLTOCS_WRITE_FINISH)?"write finish":(cmd==ANTOAN_NOP)?"nop":"???");
						break;
					}
				} else {
					if (mainserv_towrite(fwdsock,hdr,8,SERV_TIMEOUT)!=8) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'send(%s)' timed out",(cmd==CLTOCS_WRITE_FINISH)?"write finish":(cmd==ANTOAN_NOP)?"nop":"???");
						break;
					}
				}
			}
			if (protover) {
				mainserv_sock_nop_add(fsn);
			}
			if (cmd==CLTOCS_WRITE_FINISH) {
				if (leng<12) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE_FINISH - wrong size (%"PRIu32"/12)",leng);
					break;
				}
				rptr = hdr+8;
				if (gchunkid!=get64bit(&rptr)) {
					packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
					put64bit(&wptr,gchunkid);
					put32bit(&wptr,0);
					put8bit(&wptr,MFS_ERROR_WRONGCHUNKID);
					if (protover) {
						mainserv_sock_nop_del(sn);
					}
					mainserv_send_and_free("write status",sock,packet,8+4+1);
					break;
				}
				if (gversion!=get32bit(&rptr)) {
					packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
					put64bit(&wptr,gchunkid);
					put32bit(&wptr,0);
					put8bit(&wptr,MFS_ERROR_WRONGCHUNKID);
					if (protover) {
						mainserv_sock_nop_del(sn);
					}
					mainserv_send_and_free("write status",sock,packet,8+4+1);
					break;
				}
				gotlast = (wrdata.head==NULL)?2:1;
				break;
			} else if (cmd==CLTOCS_WRITE_DATA) {
//				uint32_t crc;
//				const uint8_t *crcptr;
				if (leng<8+4+2+2+4+4) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+size)",leng);
					myunalloc(wrjob,wrjob->structsize);
					break;
				}
				rptr = wrjob->data+8;
				wrjob->chunkid = get64bit(&rptr);
				wrjob->writeid = get32bit(&rptr);
				wrjob->blocknum = get16bit(&rptr);
				wrjob->offset = get16bit(&rptr);
				wrjob->size = get32bit(&rptr);
//				crcptr = rptr;
//				crc = get32bit(&crcptr);
				if (leng!=8+4+2+2+4+4+wrjob->size) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+%"PRIu32")",leng,wrjob->size);
					myunalloc(wrjob,wrjob->structsize);
					break;
				}
				if (gchunkid!=wrjob->chunkid) {
					packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
					put64bit(&wptr,gchunkid);
					put32bit(&wptr,0);
					put8bit(&wptr,MFS_ERROR_WRONGCHUNKID);
					if (protover) {
						mainserv_sock_nop_del(sn);
					}
					mainserv_send_and_free("write status",sock,packet,8+4+1);
					myunalloc(wrjob,wrjob->structsize);
					break;
				}
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunkid: %016"PRIX64", version: %08"PRIX32", blocknum: %"PRIu16", offset: %"PRIu16", size: %"PRIu32", crc: %08"PRIX32":%08"PRIX32,gchunkid,gversion,wrjob->blocknum,wrjob->offset,wrjob->size,crc,mycrc32(0,rptr+4,wrjob->size));
/*
				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunkid: %016X, version: %08X, blocknum: %"PRIu16", offset: %"PRIu16", size: %"PRIu32", crc: %08"PRIX32":%08"PRIX32,chunkid,version,blocknum,offset,size,crc,mycrc32(0,rptr+4,size));
				if (crc!=mycrc32(0,rptr+4,size)) {
					uint32_t xxx,sl;
					char buff[200];
					for (xxx=0 ; xxx<size ; xxx++) {
						if ((xxx&0x3F)==0) {
							sl = snprintf(buff,200,"%05X: ",xxx);
						}
						sl += snprintf(buff+sl,200-sl,"%02X",rptr[xxx+4]);
						if ((xxx&0x3F)==0x1F) {
							buff[sl]=0;
							mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hexdump: %s",buff);
							sl=0;
						}
					}
					if (sl>0) {
						buff[sl]=0;
						mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hexdump: %s",buff);
					}
				}
*/
				wrjob->crcptr = rptr;
				wrjob->buff = rptr+4;
				wrjob->ack = 0;
				wrjob->hddstatus = 0xFF;
				wrjob->netstatus = 0xFF;
				wrjob->next = NULL;
				zassert(pthread_mutex_lock(&(wrdata.lock)));
				*(wrdata.tail) = wrjob;
				wrdata.tail = &(wrjob->next);
				if (wrdata.hddhead==NULL) {
					wrdata.hddhead = wrjob;
				}
				if (wrdata.nethead==NULL) {
					wrdata.nethead = wrjob;
				}
				if (wrdata.condwaiting) {
					zassert(pthread_cond_signal(&(wrdata.cond)));
					wrdata.condwaiting=0;
				}
				zassert(pthread_mutex_unlock(&(wrdata.lock)));
			} else if (cmd!=ANTOAN_NOP) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"received unrecognized packet !!!");
				break;
			}
		}
		if (pfd[1].revents & POLLIN) {
			if (mainserv_toread(fwdsock,pdata,8,SERV_TIMEOUT)!=8) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'receive(header next)' timed out");
				break;
			}
			rptr = pdata;
			cmd = get32bit(&rptr);
			leng = get32bit(&rptr);
			if (leng>0) {
				if (leng > MaxPacketSize) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MaxPacketSize,cmd);
					break;
				}
				if (pdataleng<leng) {
					if (pdata) {
						myunalloc(pdata,pdataleng);
					}
					pdataleng = leng;
					myalloc(pdata,pdataleng);
				}
				if (mainserv_toread(fwdsock,pdata,leng,SERV_TIMEOUT)!=(int32_t)leng) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_middle: 'receive(%s)' timed out",(cmd==CSTOCL_WRITE_STATUS)?"write status":(cmd==ANTOAN_NOP)?"nop":"???");
					break;
				}
			}
			if (cmd==CSTOCL_WRITE_STATUS) {
				if (leng!=8+4+1) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOCL_WRITE_STATUS - wrong size (%"PRIu32"/13)",leng);
					break;
				}
				rptr = pdata;
				chunkid = get64bit(&rptr);
				writeid = get32bit(&rptr);
				status = get8bit(&rptr);
				zassert(pthread_mutex_lock(&(wrdata.lock)));
				if (writeid==0) {
					// add new element to wrdata.head
					// myalloc(wrjob,offsetof(write_job,data)); <- this is correct version of this code, but compiler makers "know" better how to make code.
					// wrjob->structsize = offsetof(write_job,data);
					myalloc(wrjob,sizeof(write_job));
					wrjob->structsize = sizeof(write_job);
					wrjob->chunkid = chunkid;
					wrjob->writeid = 0;
					wrjob->ack = 3;
					wrjob->hddstatus = MFS_STATUS_OK;
					wrjob->netstatus = status;
					wrjob->next = wrdata.head;
					wrdata.head = wrjob;
					if (wrjob->next==NULL) {
						wrdata.tail = &(wrjob->next);
					}
				} else {
					wrjob = wrdata.nethead;
					if (wrjob==NULL) {
						zassert(pthread_mutex_unlock(&(wrdata.lock)));
						break;
					}
					if (chunkid!=wrjob->chunkid) {	// wrong chunkid
						zassert(pthread_mutex_unlock(&(wrdata.lock)));
						break;
					}
					if (writeid!=wrjob->writeid) {	// wrong writeid
						zassert(pthread_mutex_unlock(&(wrdata.lock)));
						break;
					}
					wrjob->netstatus = status;
					wrjob->ack|=2;
					wrdata.nethead = wrjob->next;
				}
				zassert(pthread_mutex_unlock(&(wrdata.lock)));
			}
		}
		if (pfd[2].revents & POLLIN) {
			if (read(wrdata.pipe[0],&status,1)!=1) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"read pipe error");
			}
		}
		status = MFS_STATUS_OK;
		while (status==MFS_STATUS_OK) {
			uint8_t exitloop;
			zassert(pthread_mutex_lock(&(wrdata.lock)));
			if (wrdata.head==NULL) {
				zassert(pthread_mutex_unlock(&(wrdata.lock)));
				break;
			}
			exitloop = 1;
			wrjob = wrdata.head;
			if (wrjob->ack&1 && wrjob->hddstatus!=MFS_STATUS_OK) {
				status = wrjob->hddstatus;
			} else if (wrjob->ack&2 && wrjob->netstatus!=MFS_STATUS_OK) {
				status = wrjob->netstatus;
			} else if (wrjob->ack==3) {
				status = MFS_STATUS_OK;
				exitloop = 0;
			}
			if (exitloop) {
				zassert(pthread_mutex_unlock(&(wrdata.lock)));
				break;
			}
			chunkid = wrjob->chunkid;
			writeid = wrjob->writeid;
			wrdata.head = wrjob->next;
			if (wrdata.head==NULL) {
				wrdata.tail = &(wrdata.head);
			}
			myunalloc(wrjob,wrjob->structsize);
			zassert(pthread_mutex_unlock(&(wrdata.lock)));
			packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&wptr,chunkid);
			put32bit(&wptr,writeid);
			put8bit(&wptr,status);
			if (protover) {
				mainserv_sock_nop_del(sn);
			}
			if (mainserv_send_and_free("write status",sock,packet,8+4+1)==0) {
				status = MFS_ERROR_DISCONNECTED; // any error
			}
			if (protover) {
				mainserv_sock_nop_add(sn);
			}
		}
		if (sn->error || fsn->error) {
			status = MFS_ERROR_DISCONNECTED; // any error
		}
		if (status!=MFS_STATUS_OK) {
			break;
		}
		if (pfd[1].revents & POLLHUP) {
			packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&wptr,gchunkid);
			put32bit(&wptr,0);
			put8bit(&wptr,MFS_ERROR_DISCONNECTED);
			if (protover) {
				mainserv_sock_nop_del(sn);
			}
			mainserv_send_and_free("write status",sock,packet,8+4+1);
			break;
		}
		if (pfd[0].revents & POLLHUP) {
			break;
		}
	}
	zassert(pthread_mutex_lock(&(wrdata.lock)));
	wrdata.term = 1;
	if (wrdata.condwaiting) {
		zassert(pthread_cond_signal(&(wrdata.cond)));
		wrdata.condwaiting=0;
	}
	zassert(pthread_mutex_unlock(&(wrdata.lock)));
	zassert(pthread_join(wrthread,NULL));
	zassert(pthread_mutex_destroy(&(wrdata.lock)));
	zassert(pthread_cond_destroy(&(wrdata.cond)));
	while ((wrjob=wrdata.head)) {
		wrdata.head = wrjob->next;
//		if (wrdata.head==NULL) {
//			wrdata.tail = &(wrdata.head);
//		}
		myunalloc(wrjob,wrjob->structsize);
	}
	if (pdata) {
		myunalloc(pdata,pdataleng);
	}
	close(wrdata.pipe[0]);
	close(wrdata.pipe[1]);
	return gotlast;
}

uint8_t mainserv_write_last(int sock,uint64_t gchunkid,uint32_t gversion,uint8_t protover,sock_nops *sn) {
	uint8_t *packet,*wptr;
	const uint8_t *rptr;
	uint8_t *pdata;
	uint32_t pdataleng;
	uint32_t cmd,leng;
	uint64_t chunkid;
	uint32_t version;
	uint32_t writeid;
	uint32_t size;
	uint16_t blocknum;
	uint16_t offset;
	uint8_t rstat;
	uint8_t status;

	status = 0; // make gcc happy
	writeid = 0;
	packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
	put64bit(&wptr,gchunkid);
	put32bit(&wptr,0);
	put8bit(&wptr,MFS_STATUS_OK);
	if (mainserv_send_and_free("write status",sock,packet,8+4+1)==0) {
		return 0;
	}

	pdataleng = 65536+4096;
	myalloc(pdata,pdataleng);
	rstat = 0;

	while (1) {
		if (mainserv_toread(sock,pdata,8,SERV_TIMEOUT)!=8) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_last: 'receive(header prev)' timed out");
			break;
		}
		rptr = pdata;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
		if (leng>0) {
			if (leng > MaxPacketSize) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MaxPacketSize,cmd);
				break;
			}
			if (pdataleng<leng+8) {
				if (pdata) {
					myunalloc(pdata,pdataleng);
				}
				pdataleng = leng+8;
				pdataleng += 0xFFF;
				pdataleng &= ~0xFFF;
				myalloc(pdata,pdataleng);
			}
			if (mainserv_toread(sock,pdata+8,leng,SERV_TIMEOUT)!=(int32_t)leng) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"write_last: 'receive(%s)' timed out",(cmd==CLTOCS_WRITE_FINISH)?"write finish":(cmd==CLTOCS_WRITE_DATA)?"write data":(cmd==ANTOAN_NOP)?"nop":"???");
				break;
			}
		}
		if (cmd==CLTOCS_WRITE_FINISH) {
			rptr = pdata+8;
			chunkid = get64bit(&rptr);
			version = get32bit(&rptr);
			if (gchunkid!=chunkid || gversion!=version) {
				rstat = 1;
				status = MFS_ERROR_WRONGCHUNKID;
				break;
			}
			rstat = 1;
			status = MFS_STATUS_OK;
			break;
		}
		if (cmd==CLTOCS_WRITE_DATA) {
//			uint32_t crc;
//			const uint8_t *crcptr;
			if (leng<8+4+2+2+4+4) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+size)",leng);
				break;
			}
			rptr = pdata+8;
			chunkid = get64bit(&rptr);
			writeid = get32bit(&rptr);
			blocknum = get16bit(&rptr);
			offset = get16bit(&rptr);
			size = get32bit(&rptr);
//				crcptr = rptr;
//				crc = get32bit(&crcptr);
			if (leng!=8+4+2+2+4+4+size) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+%"PRIu32")",leng,size);
				break;
			}
			if (gchunkid!=chunkid) {
				rstat = 1;
				status = MFS_ERROR_WRONGCHUNKID;
				break;
			}
/*
			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunkid: %016X, version: %08X, blocknum: %"PRIu16", offset: %"PRIu16", size: %"PRIu32", crc: %08"PRIX32":%08"PRIX32,chunkid,version,blocknum,offset,size,crc,mycrc32(0,rptr+4,size));
			if (crc!=mycrc32(0,rptr+4,size)) {
				uint32_t xxx,sl;
				char buff[200];
				for (xxx=0 ; xxx<size ; xxx++) {
					if ((xxx&0x3F)==0) {
						sl = snprintf(buff,200,"%05X: ",xxx);
					}
					sl += snprintf(buff+sl,200-sl,"%02X",rptr[xxx+4]);
					if ((xxx&0x3F)==0x1F) {
						buff[sl]=0;
						mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hexdump: %s",buff);
						sl=0;
					}
				}
				if (sl>0) {
					buff[sl]=0;
					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hexdump: %s",buff);
				}
			}
*/
			status = hdd_write(gchunkid,gversion,blocknum,rptr+4,offset,size,rptr);
			if (status!=MFS_STATUS_OK) {
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_write error: %s",mfsstrerr(status));
				rstat = 1;
				break;
			}
			packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&wptr,chunkid);
			put32bit(&wptr,writeid);
			put8bit(&wptr,MFS_STATUS_OK);
			if (protover) {
				mainserv_sock_nop_del(sn);
			}
			if (mainserv_send_and_free("write status",sock,packet,8+4+1)==0) {
				break;
			}
			if (protover) {
				mainserv_sock_nop_add(sn);
			}
		}
	}
	if (pdata) {
		myunalloc(pdata,pdataleng);
	}
	if (rstat>0) {
		if (status==MFS_STATUS_OK) {
			return 1;
		} else {
			packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&wptr,gchunkid);
			put32bit(&wptr,writeid);
			put8bit(&wptr,status);
			if (protover) {
				mainserv_sock_nop_del(sn);
			}
			return mainserv_send_and_free("write status",sock,packet,8+4+1);
		}
	} else {
		return 0;
	}
}

uint8_t mainserv_write(int sock,const uint8_t *data,uint32_t length) {
	uint8_t *packet,*wptr;

	uint8_t status;
	int fwdsock;
	uint32_t fwdip;
	uint16_t fwdport;
	uint8_t protover;
	uint64_t gchunkid;
	uint32_t gversion;
	uint32_t i;
	uint8_t ret;
	sock_nops sn,fsn;

	fwdport = 0; // make old compilers happy
	fwdip = 0; // make old compilers happy
	if (length&1) {
		if (length<13 || ((length-13)%6)!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE - wrong size (%"PRIu32"/13+N*6)",length);
			return 0;
		}
		protover = get8bit(&data);
	} else {
		if (length<12 || ((length-12)%6)!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOCS_WRITE - wrong size (%"PRIu32"/12+N*6)",length);
			return 0;
		}
		protover = 0;
	}
	if (protover) {
		mainserv_sock_nop_init(&sn,sock);
	}
	gchunkid = get64bit(&data);
	gversion = get32bit(&data);
	if (length>((protover)?13:12)) { // write and forward data
		fwdip = get32bit(&data);
		fwdport = get16bit(&data);
		fwdsock = -1;
		if (protover) {
			mainserv_sock_nop_add(&sn);
		}
		for (i=0 ; i<CONNECT_RETRIES && fwdsock<0 ; i++) {
#ifdef USE_CONNCACHE
			if (i==0) {
				fwdsock = conncache_get(fwdip,fwdport);
			}
			if (fwdsock<0) {
				fwdsock = mainserv_connect(fwdip,fwdport,CONNECT_TIMEOUT(i));
			}
#else
			fwdsock = mainserv_connect(fwdip,fwdport,CONNECT_TIMEOUT(i));
#endif
			if (fwdsock>=0) {
				packet = mainserv_create_packet(&wptr,CLTOCS_WRITE,length-6);
				if (protover) {
					put8bit(&wptr,protover);
				}
				put64bit(&wptr,gchunkid);
				put32bit(&wptr,gversion);
				if (protover) {
					memcpy(wptr,data,length-13-6);
				} else {
					memcpy(wptr,data,length-12-6);
				}
				if (mainserv_send_and_free("write init",fwdsock,packet,length-6)) {
					break;
				}
				tcpclose(fwdsock);
				fwdsock=-1;
			}
		}
		if (fwdsock<0) {
			packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&wptr,gchunkid);
			put32bit(&wptr,0);
			put8bit(&wptr,MFS_ERROR_CANTCONNECT);
			if (protover) {
				mainserv_sock_nop_del(&sn);
			}
			return mainserv_send_and_free("write status",sock,packet,8+4+1);
		}
	} else { // last in chain
		fwdsock=-1;
	}
	if (protover) {
		if (fwdsock>=0) {
			mainserv_sock_nop_init(&fsn,fwdsock);
			mainserv_sock_nop_add(&fsn);
		}
	}
	status = hdd_open(gchunkid,gversion);
	if (status!=MFS_STATUS_OK) {
		if (fwdsock>=0) {
			tcpclose(fwdsock);
		}
		packet = mainserv_create_packet(&wptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&wptr,gchunkid);
		put32bit(&wptr,0);
		put8bit(&wptr,status);
		if (protover) {
			mainserv_sock_nop_del(&sn);
			if (fwdsock>=0) {
				mainserv_sock_nop_del(&fsn);
			}
		}
		return mainserv_send_and_free("write status",sock,packet,8+4+1);
	}
	if (fwdsock>=0) {
		ret = mainserv_write_middle(sock,fwdsock,gchunkid,gversion,protover,&sn,&fsn);
		if (protover) {
			mainserv_sock_nop_del(&fsn);
		}
#ifdef USE_CONNCACHE
		if (ret<2 || protover==0) {
			tcpclose(fwdsock);
		} else {
			conncache_insert(fwdip,fwdport,fwdsock);
		}
#else
		tcpclose(fwdsock);
#endif
	} else {
		ret = mainserv_write_last(sock,gchunkid,gversion,protover,&sn);
	}
	hdd_close(gchunkid,0); // TODO: add flag to control fsync here
	if (protover) {
		mainserv_sock_nop_del(&sn);
	}
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_add(&stats_hlopw,1);
#else
	zassert(pthread_mutex_lock(&statslock));
	stats_hlopw++;
	zassert(pthread_mutex_unlock(&statslock));
#endif
	return ret;
}

void mainserv_term(void) {
	/* to do: terminate thread */
	conncache_term();
}

int mainserv_init(void) {
	pthread_t rnthread;
#ifdef HAVE_MMAP
	CanUseMmap = cfg_getuint8("CAN_USE_MMAP",0);
#else
	if (cfg_getuint8("CAN_USE_MMAP",0)!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mmap is not supported in your OS - ignoring CAN_USE_MMAP option");
	}
#endif
	if (conncache_init(250)<0) {
		return -1;
	}
	main_destruct_register(mainserv_term);
	sock_nops_head = NULL;
	sock_nops_tail = &sock_nops_head;
	if (pthread_mutex_init(&sock_nops_lock,NULL)<0) {
		return -1;
	}
	if (lwt_minthread_create(&rnthread,1,mainserv_sock_nop_sender,NULL)<0) {
		return -1;
	}
	return 1;
}
