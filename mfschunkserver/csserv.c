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

#include "config.h"

#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "csserv.h"
#include "cfg.h"
#include "main.h"
#include "clocks.h"
#include "sockets.h"
#include "hddspacemgr.h"
#include "masterconn.h"
#include "charts.h"
#include "slogger.h"
#include "bgjobs.h"
#include "massert.h"

// connection timeout in seconds
#define CSSERV_TIMEOUT 5

#define MaxPacketSize CSTOCS_MAXPACKETSIZE

//csserventry.mode
enum {HEADER,DATA};

//csserventry.state
enum {IDLE,READ,WRITE,CLOSE};

struct csserventry;

enum {IJ_GET_CHUNK_BLOCKS,IJ_GET_CHUNK_CHECKSUM,IJ_GET_CHUNK_CHECKSUM_TAB};

typedef struct idlejob {
	uint32_t jobid;
	uint8_t op;
	uint64_t chunkid;
	uint32_t version;
	struct csserventry *eptr;
	struct idlejob *next,**prev;
	uint8_t buff[1];
} idlejob;

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct csserventry {
	uint8_t state;
	uint8_t mode;

	int sock;
	int32_t pdescpos;
	double lastread,lastwrite;
	uint32_t activity;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint32_t jobid;

	struct idlejob *idlejobs;

	struct csserventry *next;
} csserventry;

static csserventry *csservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

static uint32_t mylistenip;
static uint16_t mylistenport;

static uint64_t stats_bytesin=0;
static uint64_t stats_bytesout=0;

// from config
static char *ListenHost;
static char *ListenPort;

void csserv_stats(uint64_t *bin,uint64_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

uint8_t* csserv_create_packet(csserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket = malloc(sizeof(packetstruct));
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	psize = size+8;
	outpacket->packet=malloc(psize);
#ifndef __clang_analyzer__
	passert(outpacket->packet);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

void csserv_get_version(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;
	static const char vstring[] = VERSSTR;
	if (length!=0 && length!=4) {
		syslog(LOG_NOTICE,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->state = CLOSE;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = csserv_create_packet(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = csserv_create_packet(eptr,ANTOAN_VERSION,4+strlen(vstring));
	}
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	memcpy(ptr,vstring,strlen(vstring));
}

void csserv_get_config(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	uint32_t vleng;
	char *val;
	uint8_t *ptr;

	if (length<5) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32")",length);
		eptr->state = CLOSE;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->state = CLOSE;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	val = cfg_getstr(name,"");
	vleng = strlen(val);
	if (vleng>255) {
		vleng=255;
	}
	ptr = csserv_create_packet(eptr,ANTOAN_CONFIG_VALUE,5+vleng);
	put32bit(&ptr,msgid);
	put8bit(&ptr,vleng);
	memcpy(ptr,val,vleng);
}

void csserv_iothread_finished(uint8_t status,void *e) {
	csserventry *eptr = (csserventry*)e;
	if (status==0) {
		eptr->state = CLOSE;
	} else {
		eptr->state = IDLE;
	}
	eptr->jobid = 0;
	if (eptr->inputpacket.packet) {
		free(eptr->inputpacket.packet);
	}
	eptr->inputpacket.packet=NULL;
}

void csserv_read_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;

	eptr->state = READ;
	eptr->jobid = job_serv_read(csserv_iothread_finished,eptr,eptr->sock,data,length);
	if (eptr->jobid==0) { // not done - queue full
		if (length!=20 && length!=21) {
			syslog(LOG_NOTICE,"CLTOCS_READ - wrong size (%"PRIu32"/20|21)",length);
			eptr->state = CLOSE;
			return;
		}
		if (length==21) {
			data++; // skip proto version
		}
		ptr = csserv_create_packet(eptr,CSTOCL_READ_STATUS,8+1);
		memcpy(ptr,data,8); // copy chunkid directly from source packet
		ptr+=8;
		put8bit(&ptr,MFS_ERROR_NOTDONE);
		eptr->state = IDLE;
	}
}

void csserv_write_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;

	eptr->state = WRITE;
	eptr->jobid = job_serv_write(csserv_iothread_finished,eptr,eptr->sock,data,length);
	if (eptr->jobid==0) { // not done - queue full
		if (length&1) {
			if (length<13 || ((length-13)%6)!=0) {
				syslog(LOG_NOTICE,"CLTOCS_WRITE - wrong size (%"PRIu32"/13+N*6)",length);
				eptr->state = CLOSE;
				return;
			}
			data++; // skip proto version
		} else {
			if (length<12 || ((length-12)%6)!=0) {
				syslog(LOG_NOTICE,"CLTOCS_WRITE - wrong size (%"PRIu32"/12+N*6)",length);
				eptr->state = CLOSE;
				return;
			}
		}
		ptr = csserv_create_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		memcpy(ptr,data,8); // copy chunkid directly from source packet
		ptr+=8;
		put32bit(&ptr,0);
		put8bit(&ptr,MFS_ERROR_NOTDONE);
		eptr->state = IDLE;
	}
}

/* IDLE operations */

void csserv_idlejob_finished(uint8_t status,void *ijp) {
	idlejob *ij = (idlejob*)ijp;
	csserventry *eptr = ij->eptr;
	uint8_t *ptr;

	if (eptr) {
		switch (ij->op) {
			case IJ_GET_CHUNK_BLOCKS:
				ptr = csserv_create_packet(eptr,CSTOAN_CHUNK_BLOCKS,8+4+2+1);
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				if (status==MFS_STATUS_OK) {
					memcpy(ptr,ij->buff,2);
					ptr+=2;
				} else {
					put16bit(&ptr,0);
				}
				put8bit(&ptr,status);
				break;
			case IJ_GET_CHUNK_CHECKSUM:
				if (status!=MFS_STATUS_OK) {
					ptr = csserv_create_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+1);
				} else {
					ptr = csserv_create_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+4);
				}
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				if (status!=MFS_STATUS_OK) {
					put8bit(&ptr,status);
				} else {
					memcpy(ptr,ij->buff,4);
				}
				break;
			case IJ_GET_CHUNK_CHECKSUM_TAB:
				if (status!=MFS_STATUS_OK) {
					ptr = csserv_create_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+1);
				} else {
					ptr = csserv_create_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+4096);
				}
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				if (status!=MFS_STATUS_OK) {
					put8bit(&ptr,status);
				} else {
					memcpy(ptr,ij->buff,4096);
				}
				break;
		}
		*(ij->prev) = ij->next;
		if (ij->next) {
			ij->next->prev = ij->prev;
		}
	}
	free(ij);
}

void csserv_get_chunk_blocks(csserventry *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_BLOCKS - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+2);
	ij->op = IJ_GET_CHUNK_BLOCKS;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->eptr = eptr;
	ij->next = eptr->idlejobs;
	ij->prev = &(eptr->idlejobs);
	eptr->idlejobs = ij;
	ij->jobid = job_get_chunk_blocks(csserv_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}

void csserv_get_chunk_checksum(csserventry *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_CHECKSUM - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+4);
	ij->op = IJ_GET_CHUNK_CHECKSUM;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->eptr = eptr;
	ij->next = eptr->idlejobs;
	ij->prev = &(eptr->idlejobs);
	eptr->idlejobs = ij;
	ij->jobid = job_get_chunk_checksum(csserv_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}

void csserv_get_chunk_checksum_tab(csserventry *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_CHECKSUM_TAB - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+4096);
	ij->op = IJ_GET_CHUNK_CHECKSUM_TAB;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->eptr = eptr;
	ij->next = eptr->idlejobs;
	ij->prev = &(eptr->idlejobs);
	eptr->idlejobs = ij;
	ij->jobid = job_get_chunk_checksum_tab(csserv_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}

void csserv_hdd_list(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t l;
	uint8_t *ptr;

	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_size();	// lock
	ptr = csserv_create_packet(eptr,CSTOCL_HDD_LIST,l);
	hdd_diskinfo_data(ptr);	// unlock
}

void csserv_chart(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	uint16_t w,h;

	if (length!=4 && length!=8) {
		syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%"PRIu32"/4|8)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	if (length==8) {
		w = get16bit(&data);
		h = get16bit(&data);
	} else {
		w = 0;
		h = 0;
	}
	l = charts_make_png(chartid,w,h);
	ptr = csserv_create_packet(eptr,ANTOCL_CHART,l);
	if (l>0) {
		charts_get_png(ptr);
	}
}

void csserv_chart_data(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	uint32_t maxentries;

	if (length!=4 && length!=8) {
		syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%"PRIu32"/4|8)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	if (length==8) {
		maxentries = get32bit(&data);
	} else {
		maxentries = UINT32_C(0xFFFFFFFF);
	}
	l = charts_makedata(NULL,chartid,maxentries);
	ptr = csserv_create_packet(eptr,ANTOCL_CHART_DATA,l);
	if (l>0) {
		charts_makedata(ptr,chartid,maxentries);
	}
}

void csserv_monotonic_data(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t l;
	uint32_t dil;

	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOAN_MONOTONIC_DATA - wrong size (%"PRIu32"/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = charts_monotonic_data(NULL);
	dil = hdd_diskinfo_monotonic_size();
	ptr = csserv_create_packet(eptr,ANTOCL_MONOTONIC_DATA,l+dil);
	if (l>0) {
		charts_monotonic_data(ptr);
		ptr += l;
	}
	hdd_diskinfo_monotonic_data(ptr);
}

void csserv_module_info(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;

	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOAN_MODULE_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->state = CLOSE;
		return;
	}
	(void)data;
	ptr = csserv_create_packet(eptr,ANTOCL_MODULE_INFO,21);
	put8bit(&ptr,MODULE_TYPE_CHUNKSERVER);
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put16bit(&ptr,masterconn_getcsid());
	put64bit(&ptr,masterconn_getmetaid());
	put32bit(&ptr,masterconn_getmasterip());
	put16bit(&ptr,masterconn_getmasterport());
}

void csserv_close(csserventry *eptr) {
	idlejob *ij,*nij;

	if (eptr->jobid>0 && (eptr->state==READ || eptr->state==WRITE)) {
		job_pool_disable_job(eptr->jobid);
		job_pool_change_callback(eptr->jobid,NULL,NULL);
	}

	for (ij=eptr->idlejobs ; ij ; ij=nij) {
		nij = ij->next;
		job_pool_disable_job(ij->jobid);
		ij->next = NULL;
		ij->prev = NULL;
		ij->eptr = NULL;
	}
}

void csserv_gotpacket(csserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
//	syslog(LOG_NOTICE,"packet %u:%u",type,length);
	if (type==ANTOAN_NOP) {
		return;
	}
	if (type==ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type==ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
	if (eptr->state==IDLE) {
		switch (type) {
		case ANTOAN_GET_VERSION:
			csserv_get_version(eptr,data,length);
			break;
		case ANTOAN_GET_CONFIG:
			csserv_get_config(eptr,data,length);
			break;
		case CLTOCS_READ:
			csserv_read_init(eptr,data,length);
			break;
		case CLTOCS_WRITE:
			csserv_write_init(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_BLOCKS:
			csserv_get_chunk_blocks(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_CHECKSUM:
			csserv_get_chunk_checksum(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_CHECKSUM_TAB:
			csserv_get_chunk_checksum_tab(eptr,data,length);
			break;
		case CLTOCS_HDD_LIST:
			csserv_hdd_list(eptr,data,length);
			break;
		case CLTOAN_CHART:
			csserv_chart(eptr,data,length);
			break;
		case CLTOAN_CHART_DATA:
			csserv_chart_data(eptr,data,length);
			break;
		case CLTOAN_MONOTONIC_DATA:
			csserv_monotonic_data(eptr,data,length);
			break;
		case CLTOAN_MODULE_INFO:
			csserv_module_info(eptr,data,length);
			break;
		case CLTOCS_WRITE_DATA:
		case CLTOCS_WRITE_FINISH:
			eptr->state = CLOSE; // silently ignore those packets
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->state = CLOSE;
		}
	} else {
		syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
		eptr->state = CLOSE;
	}
}

void csserv_wantexit(void) {
	syslog(LOG_NOTICE,"closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);
	lsock = -1;
}

void csserv_term(void) {
	csserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;

	eptr = csservhead;
	while (eptr) {
		tcpclose(eptr->sock);
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		pptr = eptr->outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			paptr = pptr;
			pptr = pptr->next;
			free(paptr);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	csservhead=NULL;
	free(ListenHost);
	free(ListenPort);
}

void csserv_read(csserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;

	if (eptr->mode == HEADER) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(read) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"(read) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin+=i;
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		ptr = eptr->hdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		if (size>0) {
			if (size>MaxPacketSize) {
				syslog(LOG_WARNING,"(read) packet too long (%"PRIu32"/%u) ; command:%"PRIu32,size,MaxPacketSize,type);
				eptr->state = CLOSE;
				return;
			}
			eptr->inputpacket.packet = malloc(size);
			passert(eptr->inputpacket.packet);
			eptr->inputpacket.startptr = eptr->inputpacket.packet;
		}
		eptr->inputpacket.bytesleft = size;
		eptr->mode = DATA;
	}
	if (eptr->mode == DATA) {
		if (eptr->inputpacket.bytesleft>0) {
			i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
			if (i==0) {
//				syslog(LOG_NOTICE,"(read) connection closed");
				eptr->state = CLOSE;
				return;
			}
			if (i<0) {
				if (ERRNO_ERROR) {
					mfs_errlog_silent(LOG_NOTICE,"(read) read error");
					eptr->state = CLOSE;
				}
				return;
			}
			stats_bytesin+=i;
			eptr->inputpacket.startptr+=i;
			eptr->inputpacket.bytesleft-=i;

			if (eptr->inputpacket.bytesleft>0) {
				return;
			}
		}
		ptr = eptr->hdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		eptr->mode = HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		csserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->state != READ && eptr->state != WRITE) {
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void csserv_write(csserventry *eptr) {
	packetstruct *pack;
	int32_t i;
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(write) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"(write) write error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesout+=i;
		pack->startptr+=i;
		pack->bytesleft-=i;
		if (pack->bytesleft>0) {
			return;
		}
		free(pack->packet);
		eptr->outputhead = pack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(pack);
	}
}

void csserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	csserventry *eptr;

	if (lsock>=0) {
		pdesc[pos].fd = lsock;
		pdesc[pos].events = POLLIN;
		lsockpdescpos = pos;
		pos++;
	} else {
		lsockpdescpos = 0;
	}
	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		eptr->pdescpos = -1;
		if (eptr->state==IDLE) {
			pdesc[pos].events = POLLIN;
			if (eptr->outputhead!=NULL) {
				pdesc[pos].events |= POLLOUT;
			}
			eptr->pdescpos = pos;
			pdesc[pos].fd = eptr->sock;
			pos++;
		}
	}
	*ndesc = pos;
}

void csserv_serve(struct pollfd *pdesc) {
	double now;
	csserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	now = monotonic_seconds();

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN) && lsock>=0) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(csserventry));
			passert(eptr);
			eptr->next = csservhead;
			csservhead = eptr;
			eptr->state = IDLE;
			eptr->mode = HEADER;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			eptr->jobid = 0;

			eptr->idlejobs = NULL;
		}
	}

	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP))) {
			eptr->state = CLOSE;
		}
		if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLIN) && eptr->state==IDLE) {
			eptr->lastread = now;
			csserv_read(eptr);
		}
		if (eptr->state==IDLE && eptr->lastwrite+(CSSERV_TIMEOUT/3.0)<now && eptr->outputhead==NULL) {
			csserv_create_packet(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->state==IDLE) {
			eptr->lastwrite = now;
			csserv_write(eptr);
		}
		if (eptr->state==IDLE && eptr->lastread+CSSERV_TIMEOUT<now) {
//			syslog(LOG_NOTICE,"csserv: connection timed out");
			eptr->state = CLOSE;
		}
	}

	kptr = &csservhead;
	while ((eptr=*kptr)) {
		if (eptr->state == CLOSE) {
			tcpclose(eptr->sock);
			csserv_close(eptr);
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
//			wptr = eptr->todolist;
//			while (wptr) {
//				waptr = wptr;
//				wptr = wptr->next;
//				free(waptr);
//			}
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				free(paptr);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

uint32_t csserv_getlistenip() {
	return mylistenip;
}

uint16_t csserv_getlistenport() {
	return mylistenport;
}

void csserv_reload(void) {
	char *newListenHost,*newListenPort;
	uint32_t newmylistenip;
	uint16_t newmylistenport;
	int newlsock;

	if (lsock<0) { // this is exiting stage - ignore reload
		return ;
	}
//	ThreadedServer = 1-ThreadedServer;

	newListenHost = cfg_getstr("CSSERV_LISTEN_HOST","*");
	newListenPort = cfg_getstr("CSSERV_LISTEN_PORT",DEFAULT_CS_DATA_PORT);
	if (strcmp(newListenHost,ListenHost)==0 && strcmp(newListenPort,ListenPort)==0) {
		free(newListenHost);
		free(newListenPort);
		mfs_arg_syslog(LOG_NOTICE,"main server module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"main server module: socket address has changed, but can't create new socket");
		free(newListenHost);
		free(newListenPort);
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	tcpresolve(newListenHost,newListenPort,&newmylistenip,&newmylistenport,1);
	if (tcpnumlisten(newlsock,newmylistenip,newmylistenport,100)<0) {
		mfs_arg_errlog(LOG_ERR,"main server module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(newListenHost);
		free(newListenPort);
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main server module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"main server module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(ListenHost);
	free(ListenPort);
	ListenHost = newListenHost;
	ListenPort = newListenPort;
	tcpclose(lsock);
	lsock = newlsock;
	mylistenip = newmylistenip;
	mylistenport = newmylistenport;
	masterconn_forcereconnect();
}

int csserv_init(void) {
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST","*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT",DEFAULT_CS_DATA_PORT);

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"main server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	tcpresolve(ListenHost,ListenPort,&mylistenip,&mylistenport,1);
	if (tcpnumlisten(lsock,mylistenip,mylistenport,100)<0) {
		mfs_errlog(LOG_ERR,"main server module: can't listen on socket");
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main server module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"main server module: listen on %s:%s",ListenHost,ListenPort);

	csservhead = NULL;
	main_wantexit_register(csserv_wantexit);
	main_reload_register(csserv_reload);
	main_destruct_register(csserv_term);
	main_poll_register(csserv_desc,csserv_serve);

	return 0;
}
