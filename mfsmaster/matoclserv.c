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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <sys/resource.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"

#include "datapack.h"
#include "matoclserv.h"
#include "matocsserv.h"
#include "matomlserv.h"
#include "sessions.h"
#include "csdb.h"
#include "chunks.h"
#include "filesystem.h"
#include "openfiles.h"
#include "metadata.h"
#include "random.h"
#include "exports.h"
#include "datacachemgr.h"
#include "charts.h"
#include "chartsdata.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "slogger.h"
#include "massert.h"
#include "clocks.h"
#include "missinglog.h"

#define MaxPacketSize CLTOMA_MAXPACKETSIZE

// matoclserventry.mode
enum {KILL,DATA,FINISH};
// chunklis.type
enum {FUSE_WRITE,FUSE_TRUNCATE};

// #define SESSION_STATS 16

/* CACHENOTIFY
// hash size should be at least 1.5 * 10000 * # of connected mounts
// it also should be the prime number
// const 10000 is defined in mfsmount/dircache.c file as DIRS_REMOVE_THRESHOLD_MAX
// current const is calculated as nextprime(1.5 * 10000 * 500) and is enough for up to about 500 mounts
#define DIRINODE_HASH_SIZE 7500013
*/

struct matoclserventry;

/* CACHENOTIFY
// directories in external caches
typedef struct dirincache {
	struct matoclserventry *eptr;
	uint32_t dirinode;
	struct dirincache *nextnode,**prevnode;
	struct dirincache *nextcu,**prevcu;
} dirincache;

static dirincache **dirinodehash;
*/

// locked chunks
typedef struct chunklist {
	uint64_t chunkid;
	uint64_t fleng;		// file length
	uint32_t qid;		// queryid for answer
	uint32_t inode;		// inode
	uint32_t uid;
	uint32_t gid;
	uint32_t auid;
	uint32_t agid;
	uint8_t type;
	struct chunklist *next;
} chunklist;

// opened files
/*
typedef struct filelist {
	uint32_t inode;
	struct filelist *next;
} filelist;

typedef struct session {
	uint32_t sessionid;
	char *info;
	uint32_t peerip;
	uint8_t newsession;
	uint8_t sesflags;
	uint8_t mingoal;
	uint8_t maxgoal;
	uint32_t mintrashtime;
	uint32_t maxtrashtime;
	uint32_t rootuid;
	uint32_t rootgid;
	uint32_t mapalluid;
	uint32_t mapallgid;
	uint32_t rootinode;
	uint32_t disconnected;	// 0 = connected ; other = disconnection timestamp
	uint32_t nsocks;	// >0 - connected (number of active connections) ; 0 - not connected
	uint32_t currentopstats[SESSION_STATS];
	uint32_t lasthouropstats[SESSION_STATS];
	filelist *openedfiles;
	struct session *next;
} session;
*/

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t data[1];
} out_packetstruct;

typedef struct in_packetstruct {
	struct in_packetstruct *next;
	uint32_t type,leng;
	uint8_t data[1];
} in_packetstruct;

typedef struct matoclserventry {
	uint8_t registered;
	uint8_t mode;				//0 - not active, 1 - read header, 2 - read packet
/* CACHENOTIFY
	uint8_t notifications;
*/
	int sock;				//socket number
	int32_t pdescpos;
	double lastread,lastwrite;		//time of last activity
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;
	uint32_t version;
	uint32_t peerip;

	uint8_t passwordrnd[32];
//	session *sesdata;
	void *sesdata;
	chunklist *chunkdelayedops;
/* CACHENOTIFY
	dirincache *cacheddirs;
*/
//	filelist *openedfiles;

	struct matoclserventry *next;
} matoclserventry;

//static session *sessionshead=NULL;
static matoclserventry *matoclservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;
static int starting;

// from config
static char *ListenHost;
static char *ListenPort;
//static uint32_t SessionSustainTime;
//static uint32_t Timeout;

static uint32_t stats_prcvd = 0;
static uint32_t stats_psent = 0;
static uint64_t stats_brcvd = 0;
static uint64_t stats_bsent = 0;

void matoclserv_stats(uint64_t stats[5]) {
	stats[0] = stats_prcvd;
	stats[1] = stats_psent;
	stats[2] = stats_brcvd;
	stats[3] = stats_bsent;
	stats_prcvd = 0;
	stats_psent = 0;
	stats_brcvd = 0;
	stats_bsent = 0;
}

/* CACHENOTIFY
// cache notification routines

static inline void matoclserv_dircache_init(void) {
	dirinodehash = (dirincache**)malloc(sizeof(dirincache*)*DIRINODE_HASH_SIZE);
	passert(dirinodehash);
}

static inline void matoclserv_dircache_remove_entry(dirincache *dc) {
	*(dc->prevnode) = dc->nextnode;
	if (dc->nextnode) {
		dc->nextnode->prevnode = dc->prevnode;
	}
	*(dc->prevcu) = dc->nextcu;
	if (dc->nextcu) {
		dc->nextcu->prevcu = dc->prevcu;
	}
	free(dc);
}

static inline void matoclserv_notify_add_dir(matoclserventry *eptr,uint32_t inode) {
	uint32_t hash = (inode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;

	dc = (dirincache*)malloc(sizeof(dirincache));
	passert(dc);
	dc->eptr = eptr;
	dc->dirinode = inode;
	// by inode
	dc->nextnode = dirinodehash[hash];
	dc->prevnode = (dirinodehash+hash);
	if (dirinodehash[hash]) {
		dirinodehash[hash]->prevnode = &(dc->nextnode);
	}
	dirinodehash[hash] = dc;
	// by eptr
	dc->nextcu = eptr->cacheddirs;
	dc->prevcu = &(eptr->cacheddirs);
	if (eptr->cacheddirs) {
		eptr->cacheddirs->prevcu = &(dc->nextcu);
	}
	eptr->cacheddirs = dc;

//	syslog(LOG_NOTICE,"rcvd from: '%s' ; add inode: %"PRIu32,eptr->sesdata->info,inode);
}

static inline void matoclserv_notify_remove_dir(matoclserventry *eptr,uint32_t inode) {
	uint32_t hash = (inode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc,*ndc;

	for (dc=dirinodehash[hash] ; dc ; dc=ndc) {
		ndc = dc->nextnode;
		if (dc->eptr==eptr && dc->dirinode==inode) {
			matoclserv_dircache_remove_entry(dc);
		}
	}
//	syslog(LOG_NOTICE,"rcvd from: '%s' ; remove inode: %"PRIu32,eptr->sesdata->info,inode);
}

static inline void matoclserv_notify_disconnected(matoclserventry *eptr) {
	while (eptr->cacheddirs) {
		matoclserv_dircache_remove_entry(eptr->cacheddirs);
	}
}

static inline void matoclserv_show_notification_dirs(void) {
	uint32_t hash;
	dirincache *dc;

	for (hash=0 ; hash<DIRINODE_HASH_SIZE ; hash++) {
		for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
			syslog(LOG_NOTICE,"session: %u ; dir inode: %u",dc->eptr->sesdata->sessionid,dc->dirinode);
		}
	}
}
*/

/* new registration procedure */
/*
session* matoclserv_new_session(uint8_t newsession,uint8_t nonewid) {
	session *asesdata;
	asesdata = (session*)malloc(sizeof(session));
	passert(asesdata);
	if (newsession==0 && nonewid) {
		asesdata->sessionid = 0;
	} else {
		asesdata->sessionid = fs_newsessionid();
	}
	asesdata->info = NULL;
	asesdata->peerip = 0;
	asesdata->sesflags = 0;
	asesdata->rootuid = 0;
	asesdata->rootgid = 0;
	asesdata->mapalluid = 0;
	asesdata->mapallgid = 0;
	asesdata->newsession = newsession;
	asesdata->rootinode = MFS_ROOT_ID;
	asesdata->openedfiles = NULL;
	asesdata->disconnected = 0;
	asesdata->nsocks = 1;
	memset(asesdata->currentopstats,0,4*SESSION_STATS);
	memset(asesdata->lasthouropstats,0,4*SESSION_STATS);
	asesdata->next = sessionshead;
	sessionshead = asesdata;
	return asesdata;
}

void matoclserv_attach_session(session* sesdata) {
//	syslog(LOG_NOTICE,"found: %u ; before ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
	if (sesdata->newsession>=2) {
		sesdata->newsession-=2;
	}
	sesdata->nsocks++;
//	syslog(LOG_NOTICE,"found: %u ; after ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
	sesdata->disconnected = 0;
}

session* matoclserv_find_session(uint32_t sessionid) {
	session *asesdata;
	if (sessionid==0) {
		return NULL;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->sessionid==sessionid) {
			return asesdata;
		}
	}
	return NULL;
}

void matoclserv_close_session(uint32_t sessionid) {
	session *asesdata;
	if (sessionid==0) {
		return;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->sessionid==sessionid) {
//			syslog(LOG_NOTICE,"close: %u ; before ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
			if (asesdata->nsocks==1 && asesdata->newsession<2) {
				asesdata->newsession+=2;
			}
//			syslog(LOG_NOTICE,"close: %u ; after ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
		}
	}
	return;
}

void matoclserv_store_sessions() {
	session *asesdata;
	uint32_t ileng;
	uint8_t fsesrecord[43+SESSION_STATS*8];	// 4+4+4+4+1+1+1+4+4+4+4+4+4+SESSION_STATS*4+SESSION_STATS*4
	uint8_t *ptr;
	int i;
	FILE *fd;

	fd = fopen("sessions.mfs.tmp","w");
	if (fd==NULL) {
		mfs_errlog_silent(LOG_WARNING,"can't store sessions, open error");
		return;
	}
	memcpy(fsesrecord,MFSSIGNATURE "S \001\006\004",8);
	ptr = fsesrecord+8;
	put16bit(&ptr,SESSION_STATS);
	if (fwrite(fsesrecord,10,1,fd)!=1) {
		syslog(LOG_WARNING,"can't store sessions, fwrite error");
		fclose(fd);
		return;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->newsession==1) {
			ptr = fsesrecord;
			if (asesdata->info) {
				ileng = strlen(asesdata->info);
			} else {
				ileng = 0;
			}
			put32bit(&ptr,asesdata->sessionid);
			put32bit(&ptr,ileng);
			put32bit(&ptr,asesdata->peerip);
			put32bit(&ptr,asesdata->rootinode);
			put8bit(&ptr,asesdata->sesflags);
			put8bit(&ptr,asesdata->mingoal);
			put8bit(&ptr,asesdata->maxgoal);
			put32bit(&ptr,asesdata->mintrashtime);
			put32bit(&ptr,asesdata->maxtrashtime);
			put32bit(&ptr,asesdata->rootuid);
			put32bit(&ptr,asesdata->rootgid);
			put32bit(&ptr,asesdata->mapalluid);
			put32bit(&ptr,asesdata->mapallgid);
			for (i=0 ; i<SESSION_STATS ; i++) {
				put32bit(&ptr,asesdata->currentopstats[i]);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				put32bit(&ptr,asesdata->lasthouropstats[i]);
			}
			if (fwrite(fsesrecord,(43+SESSION_STATS*8),1,fd)!=1) {
				syslog(LOG_WARNING,"can't store sessions, fwrite error");
				fclose(fd);
				return;
			}
			if (ileng>0) {
				if (fwrite(asesdata->info,ileng,1,fd)!=1) {
					syslog(LOG_WARNING,"can't store sessions, fwrite error");
					fclose(fd);
					return;
				}
			}
		}
	}
	if (fclose(fd)!=0) {
		mfs_errlog_silent(LOG_WARNING,"can't store sessions, fclose error");
		return;
	}
	if (rename("sessions.mfs.tmp","sessions.mfs")<0) {
		mfs_errlog_silent(LOG_WARNING,"can't store sessions, rename error");
	}
}

int matoclserv_load_sessions() {
	session *asesdata;
	uint32_t ileng;
//	uint8_t fsesrecord[33+SESSION_STATS*8];	// 4+4+4+4+1+4+4+4+4+SESSION_STATS*4+SESSION_STATS*4
	uint8_t hdr[8];
	uint8_t *fsesrecord;
	const uint8_t *ptr;
	uint8_t mapalldata;
	uint8_t goaltrashdata;
	uint32_t i,statsinfile;
	int r;
	FILE *fd;

	fd = fopen("sessions.mfs","r");
	if (fd==NULL) {
		mfs_errlog_silent(LOG_WARNING,"can't load sessions, fopen error");
		if (errno==ENOENT) {	// it's ok if file does not exist
			return 0;
		} else {
			return -1;
		}
	}
	if (fread(hdr,8,1,fd)!=1) {
		syslog(LOG_WARNING,"can't load sessions, fread error");
		fclose(fd);
		return -1;
	}
	if (memcmp(hdr,MFSSIGNATURE "S 1.5",8)==0) {
		mapalldata = 0;
		goaltrashdata = 0;
		statsinfile = 16;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\001",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		statsinfile = 16;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\002",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		statsinfile = 21;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\003",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		if (fread(hdr,2,1,fd)!=1) {
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\004",8)==0) {
		mapalldata = 1;
		goaltrashdata = 1;
		if (fread(hdr,2,1,fd)!=1) {
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else {
		syslog(LOG_WARNING,"can't load sessions, bad header");
		fclose(fd);
		return -1;
	}

	if (mapalldata==0) {
		fsesrecord = malloc(25+statsinfile*8);
	} else if (goaltrashdata==0) {
		fsesrecord = malloc(33+statsinfile*8);
	} else {
		fsesrecord = malloc(43+statsinfile*8);
	}
	passert(fsesrecord);

	while (!feof(fd)) {
		if (mapalldata==0) {
			r = fread(fsesrecord,25+statsinfile*8,1,fd);
		} else if (goaltrashdata==0) {
			r = fread(fsesrecord,33+statsinfile*8,1,fd);
		} else {
			r = fread(fsesrecord,43+statsinfile*8,1,fd);
		}
		if (r==1) {
			ptr = fsesrecord;
			asesdata = (session*)malloc(sizeof(session));
			passert(asesdata);
			asesdata->sessionid = get32bit(&ptr);
			ileng = get32bit(&ptr);
			asesdata->peerip = get32bit(&ptr);
			asesdata->rootinode = get32bit(&ptr);
			asesdata->sesflags = get8bit(&ptr);
			if (goaltrashdata) {
				asesdata->mingoal = get8bit(&ptr);
				asesdata->maxgoal = get8bit(&ptr);
				asesdata->mintrashtime = get32bit(&ptr);
				asesdata->maxtrashtime = get32bit(&ptr);
			} else { // set defaults (no limits)
				asesdata->mingoal = 1;
				asesdata->maxgoal = 9;
				asesdata->mintrashtime = 0;
				asesdata->maxtrashtime = UINT32_C(0xFFFFFFFF);
			}
			asesdata->rootuid = get32bit(&ptr);
			asesdata->rootgid = get32bit(&ptr);
			if (mapalldata) {
				asesdata->mapalluid = get32bit(&ptr);
				asesdata->mapallgid = get32bit(&ptr);
			} else {
				asesdata->mapalluid = 0;
				asesdata->mapallgid = 0;
			}
			asesdata->info = NULL;
			asesdata->newsession = 1;
			asesdata->openedfiles = NULL;
			asesdata->disconnected = main_time();
			asesdata->nsocks = 0;
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->currentopstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (statsinfile>SESSION_STATS) {
				ptr+=4*(statsinfile-SESSION_STATS);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->lasthouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (ileng>0) {
				asesdata->info = malloc(ileng+1);
				passert(asesdata->info);
				if (fread(asesdata->info,ileng,1,fd)!=1) {
					free(asesdata->info);
					free(asesdata);
					free(fsesrecord);
					syslog(LOG_WARNING,"can't load sessions, fread error");
					fclose(fd);
					return -1;
				}
				asesdata->info[ileng]=0;
			}
			asesdata->next = sessionshead;
			sessionshead = asesdata;
		}
		if (ferror(fd)) {
			free(fsesrecord);
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
	}
	free(fsesrecord);
	syslog(LOG_NOTICE,"sessions have been loaded");
	fclose(fd);
	return 1;
}
*/
/* old registration procedure */
/*
session* matoclserv_get_session(uint32_t sessionid) {
	// if sessionid==0 - create new record with next id
	session *asesdata;

	if (sessionid>0) {
		for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
			if (asesdata->sessionid==sessionid) {
				asesdata->nsocks++;
				asesdata->disconnected = 0;
				return asesdata;
			}
		}
	}
	asesdata = (session*)malloc(sizeof(session));
	passert(asesdata);
	if (sessionid==0) {
		asesdata->sessionid = fs_newsessionid();
	} else {
		asesdata->sessionid = sessionid;
	}
	asesdata->openedfiles = NULL;
	asesdata->disconnected = 0;
	asesdata->nsocks = 1;
	memset(asesdata->currentopstats,0,4*SESSION_STATS);
	memset(asesdata->lasthouropstats,0,4*SESSION_STATS);
	asesdata->next = sessionshead;
	sessionshead = asesdata;
	return asesdata;
}
*/

#if 0
int matoclserv_insert_openfile(session* cr,uint32_t inode) {
	filelist *ofptr,**ofpptr;
	int status;

	ofpptr = &(cr->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return STATUS_OK;	// file already acquired - nothing to do
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	status = fs_acquire(inode,cr->sessionid);
	if (status==STATUS_OK) {
		ofptr = (filelist*)malloc(sizeof(filelist));
		passert(ofptr);
		ofptr->inode = inode;
		ofptr->next = *ofpptr;
		*ofpptr = ofptr;
	}
	return status;
}

void matoclserv_init_sessions(uint32_t sessionid,uint32_t inode) {
	session *asesdata;
	filelist *ofptr,**ofpptr;

	for (asesdata = sessionshead ; asesdata && asesdata->sessionid!=sessionid; asesdata=asesdata->next) ;
	if (asesdata==NULL) {
		asesdata = (session*)malloc(sizeof(session));
		passert(asesdata);
		asesdata->sessionid = sessionid;
/* session created by filesystem - only for old clients (pre 1.5.13) */
		asesdata->info = NULL;
		asesdata->peerip = 0;
		asesdata->sesflags = 0;
		asesdata->mingoal = 1;
		asesdata->maxgoal = 9;
		asesdata->mintrashtime = 0;
		asesdata->maxtrashtime = UINT32_C(0xFFFFFFFF);
		asesdata->rootuid = 0;
		asesdata->rootgid = 0;
		asesdata->mapalluid = 0;
		asesdata->mapallgid = 0;
		asesdata->newsession = 0;
		asesdata->rootinode = MFS_ROOT_ID;
		asesdata->openedfiles = NULL;
		asesdata->disconnected = main_time();
		asesdata->nsocks = 0;
		memset(asesdata->currentopstats,0,4*SESSION_STATS);
		memset(asesdata->lasthouropstats,0,4*SESSION_STATS);
		asesdata->next = sessionshead;
		sessionshead = asesdata;
	}

	ofpptr = &(asesdata->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return;
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	ofptr = (filelist*)malloc(sizeof(filelist));
	passert(ofptr);
	ofptr->inode = inode;
	ofptr->next = *ofpptr;
	*ofpptr = ofptr;
}
#endif

uint8_t* matoclserv_createpacket(matoclserventry *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket=malloc(offsetof(out_packetstruct,data)+psize);
	passert(outpacket);
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

/*
int matoclserv_open_check(matoclserventry *eptr,uint32_t fid) {
	filelist *fl;
	for (fl=eptr->openedfiles ; fl ; fl=fl->next) {
		if (fl->fid==fid) {
			return 0;
		}
	}
	return -1;
}
*/

void matoclserv_chunk_status(uint64_t chunkid,uint8_t status) {
	uint32_t qid,inode,uid,gid,auid,agid;
	uint64_t fleng;
	uint8_t type,attr[35];
	uint32_t version;
//	uint8_t rstat;
//	uint32_t ip;
//	uint16_t port;
	uint8_t *ptr;
	uint8_t count;
	uint8_t cs_data[100*10];
	chunklist *cl,**acl;
	matoclserventry *eptr,*eaptr;

	eptr=NULL;
	qid=0;
	fleng=0;
	type=0;
	inode=0;
	uid=0;
	gid=0;
	auid=0;
	agid=0;
	for (eaptr = matoclservhead ; eaptr && eptr==NULL ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL) {
			acl = &(eaptr->chunkdelayedops);
			while (*acl && eptr==NULL) {
				cl = *acl;
				if (cl->chunkid==chunkid) {
					eptr = eaptr;
					qid = cl->qid;
					fleng = cl->fleng;
					type = cl->type;
					inode = cl->inode;
					uid = cl->uid;
					gid = cl->gid;
					auid = cl->auid;
					agid = cl->agid;

					*acl = cl->next;
					free(cl);
				} else {
					acl = &(cl->next);
				}
			}
		}
	}

	if (!eptr) {
		syslog(LOG_WARNING,"got chunk status, but don't want it");
		return;
	}
	if (status==STATUS_OK) {
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
	}
	switch (type) {
	case FUSE_WRITE:
		if (status==STATUS_OK) {
			if (eptr->version>=VERSION2INT(1,7,32)) {
				status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data);
			} else {
				status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data);
			}
			//syslog(LOG_NOTICE,"get version for chunk %"PRIu64" -> %"PRIu32,chunkid,version);
		}
		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,qid);
			put8bit(&ptr,status);
			fs_writeend(0,0,chunkid);	// ignore status - just do it.
			return;
		}
		if (eptr->version>=VERSION2INT(1,7,32)) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*10);
		} else {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		}
		put32bit(&ptr,qid);
		if (eptr->version>=VERSION2INT(1,7,32)) {
			put8bit(&ptr,1);
		}
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		if (eptr->version>=VERSION2INT(1,7,32)) {
			memcpy(ptr,cs_data,count*10);
		} else {
			memcpy(ptr,cs_data,count*6);
		}
//		for (i=0 ; i<count ; i++) {
//			if (matocsserv_getlocation(sptr[i],&ip,&port)<0) {
//				put32bit(&ptr,0);
//				put16bit(&ptr,0);
//			} else {
//				put32bit(&ptr,ip);
//				put16bit(&ptr,port);
//			}
//		}
		return;
	case FUSE_TRUNCATE:
		fs_end_setlength(chunkid);

		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,5);
			put32bit(&ptr,qid);
			put8bit(&ptr,status);
			return;
		}
		fs_do_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gid,auid,agid,fleng,attr);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,39);
		put32bit(&ptr,qid);
		memcpy(ptr,attr,35);
		return;
	default:
		syslog(LOG_WARNING,"got chunk status, but operation type is unknown");
	}
}

void matoclserv_cserv_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_CSERV_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_CSERV_LIST,csdb_servlist_size());
	csdb_servlist_data(ptr);
}

void matoclserv_cserv_command(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t ip;
	uint16_t port;
	uint8_t cmd,status;
	uint8_t *ptr;
	if (length!=6 && length!=7) {
		syslog(LOG_NOTICE,"CLTOMA_CSSERV_COMMAND - wrong size (%"PRIu32"/6|7)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==7) {
		cmd = get8bit(&data);
	} else {
		cmd = MFS_CSSERV_COMMAND_REMOVE;
	}
	ip = get32bit(&data);
	port = get16bit(&data);
	if (cmd==MFS_CSSERV_COMMAND_REMOVE) {
		status = csdb_remove_server(ip,port);
	} else if (cmd==MFS_CSSERV_COMMAND_BACKTOWORK) {
		status = csdb_back_to_work(ip,port);
	} else if (cmd==MFS_CSSERV_COMMAND_MAINTENANCEON) {
		status = csdb_maintenance(ip,port,1);
	} else if (cmd==MFS_CSSERV_COMMAND_MAINTENANCEOFF) {
		status = csdb_maintenance(ip,port,0);
	} else {
		status = ERROR_EINVAL;
	}
	if (length==6) {
		matoclserv_createpacket(eptr,MATOCL_CSSERV_COMMAND,0);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_CSSERV_COMMAND,1);
		put8bit(&ptr,status);
	}
}

void matoclserv_session_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
//	matoclserventry *eaptr;
	uint32_t size; //,sessionid;
//	uint16_t statscnt;
	uint8_t vmode;
	(void)data;
	if (length!=0 && length!=1) {
		syslog(LOG_NOTICE,"CLTOMA_SESSION_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		vmode = 0;
	} else {
		vmode = get8bit(&data);
	}
	size = sessions_datasize(vmode);
/*
	size = 2;
	for (eaptr = matoclservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->sesdata && eaptr->registered>0 && eaptr->registered<100) {
			size += 12+sessions_datasize(eaptr->sesdata,vmode);
		}
	}
*/
	ptr = matoclserv_createpacket(eptr,MATOCL_SESSION_LIST,size);
	sessions_datafill(ptr,vmode);
/*
	statscnt = sessions_get_statscnt();
	put16bit(&ptr,statscnt);
	for (eaptr = matoclservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->sesdata && eaptr->registered>0 && eaptr->registered<100) {
			sessionid = sessions_get_id(eaptr->sesdata);
//			tcpgetpeer(eaptr->sock,&ip,NULL);
			put32bit(&ptr,sessionid);
			put32bit(&ptr,eaptr->peerip);
			put32bit(&ptr,eaptr->version);
			size = sessions_datafill(ptr,eaptr->sesdata,vmode);
			ptr += size;
		}
	}
*/
}

void matoclserv_session_command(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t sessionid;
	uint8_t cmd,status;
	uint8_t *ptr;
	if (length!=5) {
		syslog(LOG_NOTICE,"CLTOMA_SESSION_COMMAND - wrong size (%"PRIu32"/5)",length);
		eptr->mode = KILL;
		return;
	}
	cmd = get8bit(&data);
	sessionid = get32bit(&data);
	if (cmd==MFS_SESSION_COMMAND_REMOVE) {
		status = sessions_force_remove(sessionid);
	} else {
		status = ERROR_EINVAL;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_SESSION_COMMAND,1);
	put8bit(&ptr,status);
}

void matoclserv_chart(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	uint16_t w,h;

	if (length!=4 && length!=8) {
		syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%"PRIu32"/4|8)",length);
		eptr->mode = KILL;
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
	ptr = matoclserv_createpacket(eptr,ANTOCL_CHART,l);
	if (l>0) {
		charts_get_png(ptr);
	}
}

void matoclserv_chart_data(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint32_t maxentries;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4 && length!=8) {
		syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%"PRIu32"/4|8)",length);
		eptr->mode = KILL;
		return;
	}
	chartid = get32bit(&data);
	if (length==8) {
		maxentries = get32bit(&data);
	} else {
		maxentries = UINT32_C(0xFFFFFFFF);
	}
	l = charts_datasize(chartid,maxentries);
	ptr = matoclserv_createpacket(eptr,ANTOCL_CHART_DATA,l);
	if (l>0) {
		charts_makedata(ptr,chartid,maxentries);
	}
}

void matoclserv_get_version(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;
	static const char vstring[] = VERSSTR;
	if (length!=0 && length!=4) {
		syslog(LOG_NOTICE,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matoclserv_createpacket(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_createpacket(eptr,ANTOAN_VERSION,4+strlen(vstring));
	}
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	memcpy(ptr,vstring,strlen(vstring));
}

void matoclserv_module_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;

	if (length!=0 && length!=4) {
		syslog(LOG_NOTICE,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matoclserv_createpacket(eptr,ANTOCL_MODULE_INFO,25);
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_createpacket(eptr,ANTOCL_MODULE_INFO,21);
	}
	put8bit(&ptr,MODULE_TYPE_MASTER);
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put16bit(&ptr,0);
	put64bit(&ptr,meta_get_fileid());
	put32bit(&ptr,0);
	put16bit(&ptr,0);
}

void matoclserv_mass_resolve_paths(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	static uint32_t *inodetab = NULL;
	static uint32_t *psizetab = NULL;
	static uint32_t tabsleng = 0;
	uint32_t i,j;
	uint32_t totalsize;
	uint32_t psize;
	uint8_t *ptr;
	uint8_t status;
	if ((length%4)!=0) {
		syslog(LOG_NOTICE,"CLTOMA_MASS_RESOLVE_PATHS - wrong size (%"PRIu32"/N*4)",length);
		eptr->mode = KILL;
		return;
	}
	length>>=2;
	if (length>tabsleng) {
		if (inodetab) {
			free(inodetab);
		}
		if (psizetab) {
			free(psizetab);
		}
		tabsleng = ((length+0xFF)&0xFFFFFF00);
		inodetab = malloc(sizeof(uint32_t)*tabsleng);
		passert(inodetab);
		psizetab = malloc(sizeof(uint32_t)*tabsleng);
		passert(psizetab);
	}
	j = 0;
	totalsize = 0;
	while (length>0) {
		i = get32bit(&data);
		if (i>0) {
			status = fs_get_paths_size(MFS_ROOT_ID,i,&psize);
			if (status==STATUS_OK) {
				inodetab[j] = i;
				psizetab[j] = psize;
				j++;
				totalsize += 8 + psize;
			}
		}
		length--;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_MASS_RESOLVE_PATHS,totalsize);
	for (i=0 ; i<j ; i++) {
		put32bit(&ptr,inodetab[i]);
		put32bit(&ptr,psizetab[i]);
		fs_get_paths_data(MFS_ROOT_ID,inodetab[i],ptr);
		ptr+=psizetab[i];
	}
}

void matoclserv_missing_chunks(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_MISSING_CHUNKS - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	(void)data;
	ptr = matoclserv_createpacket(eptr,MATOCL_MISSING_CHUNKS,missing_log_getdata(NULL));
	missing_log_getdata(ptr);
}

void matoclserv_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,trspace,respace;
	uint64_t memusage,syscpu,usercpu;
	uint32_t trnodes,renodes,inodes,dnodes,fnodes;
	uint32_t chunks,chunkcopies,tdcopies;
	uint32_t lsstore,lstime;
	uint8_t lsstat;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	meta_info(&lsstore,&lstime,&lsstat);
	fs_info(&totalspace,&availspace,&trspace,&trnodes,&respace,&renodes,&inodes,&dnodes,&fnodes);
	chunk_info(&chunks,&chunkcopies,&tdcopies);
	chartsdata_resusage(&memusage,&syscpu,&usercpu);
	ptr = matoclserv_createpacket(eptr,MATOCL_INFO,121);
	/* put32bit(&buff,VERSION): */
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put64bit(&ptr,memusage);
	put64bit(&ptr,syscpu);
	put64bit(&ptr,usercpu);
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	put64bit(&ptr,trspace);
	put32bit(&ptr,trnodes);
	put64bit(&ptr,respace);
	put32bit(&ptr,renodes);
	put32bit(&ptr,inodes);
	put32bit(&ptr,dnodes);
	put32bit(&ptr,fnodes);
	put32bit(&ptr,chunks);
	put32bit(&ptr,chunkcopies);
	put32bit(&ptr,tdcopies);
	put32bit(&ptr,lsstore);
	put32bit(&ptr,lstime);
	put8bit(&ptr,lsstat);
	put8bit(&ptr,0xFF);
	put8bit(&ptr,0xFF);
	put8bit(&ptr,0xFF);
	put8bit(&ptr,0xFF);
	put32bit(&ptr,0);
	put32bit(&ptr,0);
	put64bit(&ptr,meta_version());
}

void matoclserv_memory_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t allocated[8];
	uint64_t used[8];
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_MEMORY_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_MEMORY_INFO,176);
	chunk_get_memusage(allocated,used);
	put64bit(&ptr,allocated[0]);
	put64bit(&ptr,used[0]);
	put64bit(&ptr,allocated[1]);
	put64bit(&ptr,used[1]);
	put64bit(&ptr,allocated[2]);
	put64bit(&ptr,used[2]);
	fs_get_memusage(allocated,used);
	put64bit(&ptr,allocated[0]);
	put64bit(&ptr,used[0]);
	put64bit(&ptr,allocated[1]);
	put64bit(&ptr,used[1]);
	put64bit(&ptr,allocated[2]);
	put64bit(&ptr,used[2]);
	put64bit(&ptr,allocated[3]);
	put64bit(&ptr,used[3]);
	put64bit(&ptr,allocated[4]);
	put64bit(&ptr,used[4]);
	put64bit(&ptr,allocated[5]);
	put64bit(&ptr,used[5]);
	put64bit(&ptr,allocated[6]);
	put64bit(&ptr,used[6]);
	put64bit(&ptr,allocated[7]);
	put64bit(&ptr,used[7]);
}

void matoclserv_fstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng;
	char *msgbuff;
	uint8_t *ptr;
	(void)data;
	if (length!=0 && length!=1) {
		syslog(LOG_NOTICE,"CLTOMA_FSTEST_INFO - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	fs_test_getdata(&loopstart,&loopend,&files,&ugfiles,&mfiles,&mtfiles,&msfiles,&chunks,&ugchunks,&mchunks,&msgbuff,&msgbuffleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FSTEST_INFO,msgbuffleng+((length==1)?44:36));
	put32bit(&ptr,loopstart);
	put32bit(&ptr,loopend);
	put32bit(&ptr,files);
	put32bit(&ptr,ugfiles);
	put32bit(&ptr,mfiles);
	if (length==1) {
		put32bit(&ptr,mtfiles);
		put32bit(&ptr,msfiles);
	}
	put32bit(&ptr,chunks);
	put32bit(&ptr,ugchunks);
	put32bit(&ptr,mchunks);
	put32bit(&ptr,msgbuffleng);
	if (msgbuffleng>0) {
		memcpy(ptr,msgbuff,msgbuffleng);
	}
}

void matoclserv_chunkstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_CHUNKSTEST_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_CHUNKSTEST_INFO,60);
	chunk_store_info(ptr);
}

void matoclserv_chunks_matrix(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length>1) {
		syslog(LOG_NOTICE,"CLTOMA_CHUNKS_MATRIX - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		uint8_t matrixid;
		matrixid = get8bit(&data);
		ptr = matoclserv_createpacket(eptr,MATOCL_CHUNKS_MATRIX,484);
		chunk_store_chunkcounters(ptr,matrixid);
	} else {
		uint8_t progressstatus;
		ptr = matoclserv_createpacket(eptr,MATOCL_CHUNKS_MATRIX,969);
		progressstatus = chunk_counters_in_progress();
//		syslog(LOG_NOTICE,"progressstatus: %u",progressstatus);
		put8bit(&ptr,progressstatus);
		chunk_store_chunkcounters(ptr,0);
		chunk_store_chunkcounters(ptr+484,1);
	}
}

void matoclserv_quota_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_QUOTA_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_QUOTA_INFO,fs_getquotainfo_size());
	fs_getquotainfo_data(ptr);
}

void matoclserv_exports_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t vmode;
	if (length!=0 && length!=1) {
		syslog(LOG_NOTICE,"CLTOMA_EXPORTS_INFO - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		vmode = 0;
	} else {
		vmode = get8bit(&data);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_EXPORTS_INFO,exports_info_size(vmode));
	exports_info_data(vmode,ptr);
}

void matoclserv_mlog_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_MLOG_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_MLOG_LIST,matomlserv_mloglist_size());
	matomlserv_mloglist_data(ptr);
}


/* CACHENOTIFY
void matoclserv_notify_attr(uint32_t dirinode,uint32_t inode,const uint8_t attr[35]) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//			syslog(LOG_NOTICE,"send to: '%s' ; attrs of inode: %"PRIu32,dc->eptr->sesdata->info,inode);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_ATTR,43);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,inode);
			memcpy(ptr,attr,35);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[16]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_link(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t inode,const uint8_t attr[35],uint32_t ts) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//			{
//				char strname[256];
//				memcpy(strname,name,nleng);
//				strname[nleng]=0;
//				syslog(LOG_NOTICE,"send to: '%s' ; new link (%"PRIu32",%s)->%"PRIu32,dc->eptr->sesdata->info,dirinode,strname,inode);
//			}
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_LINK,52+nleng);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,ts);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			put8bit(&ptr,nleng);
			memcpy(ptr,name,nleng);
			ptr+=nleng;
			put32bit(&ptr,inode);
			memcpy(ptr,attr,35);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[17]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_unlink(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t ts) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//			{
//				char strname[256];
//				memcpy(strname,name,nleng);
//				strname[nleng]=0;
//				syslog(LOG_NOTICE,"send to: '%s' ; remove link (%"PRIu32",%s)",dc->eptr->sesdata->info,dirinode,strname);
//			}
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_UNLINK,13+nleng);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,ts);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			put8bit(&ptr,nleng);
			memcpy(ptr,name,nleng);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[18]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_remove(uint32_t dirinode) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//			syslog(LOG_NOTICE,"send to: '%s' ; removed inode: %"PRIu32,dc->eptr->sesdata->info,dirinode);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_REMOVE,8);
			stats_notify++;
			put32bit(&ptr,0);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[19]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_parent(uint32_t dirinode,uint32_t parent) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode && dirinode!=dc->eptr->sesdata->rootinode) {
//			syslog(LOG_NOTICE,"send to: '%s' ; new parent: %"PRIu32"->%"PRIu32,dc->eptr->sesdata->info,dirinode,parent);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_PARENT,12);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,dirinode);
			if (parent==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,parent);
			}
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[20]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}
*/

void matoclserv_fuse_register(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	uint8_t *wptr;
	uint32_t sessionid;
	uint8_t status;

	if (length<64) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong size (%"PRIu32"/<64)",length);
		eptr->mode = KILL;
		return;
	}
	if (memcmp(data,FUSE_REGISTER_BLOB_ACL,64)==0) {
		uint32_t rootinode;
		uint8_t sesflags;
		uint8_t mingoal,maxgoal;
		uint32_t mintrashtime,maxtrashtime;
		uint32_t rootuid,rootgid;
		uint32_t mapalluid,mapallgid;
		uint32_t ileng,pleng;
		uint8_t i,rcode;
		const uint8_t *path;
		const char *info;

		if (length<65) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong size (%"PRIu32"/<65)",length);
			eptr->mode = KILL;
			return;
		}

		rptr = data+64;
		rcode = get8bit(&rptr);

		if ((eptr->registered==0 && rcode==REGISTER_CLOSESESSION) || (eptr->registered && rcode!=REGISTER_CLOSESESSION)) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%d) for registered status (%d)",rcode,eptr->registered);
			eptr->mode = KILL;
			return;
		}

//		printf("rcode: %d\n",rcode);
//
		switch (rcode) {
		case REGISTER_GETRANDOM:
			if (length!=65) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.1 - wrong size (%"PRIu32"/65)",length);
				eptr->mode = KILL;
				return;
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,32);
			for (i=0 ; i<32 ; i++) {
				eptr->passwordrnd[i]=rndu8();
			}
			memcpy(wptr,eptr->passwordrnd,32);
			return;
		case REGISTER_NEWSESSION:
			if (length<77) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/>=77)",length);
				eptr->mode = KILL;
				return;
			}
			if (starting) {
				eptr->mode = KILL;
				return;
			}

			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length<77+ileng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/>=77+ileng(%"PRIu32"))",length,ileng);
				eptr->mode = KILL;
				return;
			}
			info = (const char*)rptr;
			rptr+=ileng;
			pleng = get32bit(&rptr);
			if (length!=77+ileng+pleng && length!=77+16+ileng+pleng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/77+ileng(%"PRIu32")+pleng(%"PRIu32")[+16])",length,ileng,pleng);
				eptr->mode = KILL;
				return;
			}
			path = rptr;
			rptr+=pleng;
			if (pleng>0 && rptr[-1]!=0) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - received path without ending zero");
				eptr->mode = KILL;
				return;
			}
			if (pleng==0) {
				path = (const uint8_t*)"";
			}
			if (length==77+16+ileng+pleng) {
				status = exports_check(eptr->peerip,eptr->version,0,path,eptr->passwordrnd,rptr,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			} else {
				status = exports_check(eptr->peerip,eptr->version,0,path,NULL,NULL,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			}
			if (status==STATUS_OK) {
				status = fs_getrootinode(&rootinode,path);
			}
			if (status==STATUS_OK) {
				eptr->sesdata = sessions_new_session(rootinode,sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashtime,maxtrashtime,eptr->peerip,info,ileng);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==STATUS_OK)?((eptr->version>=VERSION2INT(1,6,26))?35:(eptr->version>=VERSION2INT(1,6,21))?25:(eptr->version>=VERSION2INT(1,6,1))?21:13):1);
			if (status!=STATUS_OK) {
				put8bit(&wptr,status);
				eptr->sesdata = NULL;
				return;
			}
			sessionid = sessions_get_id(eptr->sesdata);
			if (eptr->version==VERSION2INT(1,6,21)) {
				put32bit(&wptr,0);
			} else if (eptr->version>=VERSION2INT(1,6,22)) {
				put16bit(&wptr,VERSMAJ);
				put8bit(&wptr,VERSMID);
				put8bit(&wptr,VERSMIN);
			}
			put32bit(&wptr,sessionid);
			put8bit(&wptr,sesflags);
			put32bit(&wptr,rootuid);
			put32bit(&wptr,rootgid);
			if (eptr->version>=VERSION2INT(1,6,1)) {
				put32bit(&wptr,mapalluid);
				put32bit(&wptr,mapallgid);
			}
			if (eptr->version>=VERSION2INT(1,6,26)) {
				put8bit(&wptr,mingoal);
				put8bit(&wptr,maxgoal);
				put32bit(&wptr,mintrashtime);
				put32bit(&wptr,maxtrashtime);
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = 1;
			syslog(LOG_NOTICE,"created new sessionid:%"PRIu32,sessionid);
			return;
		case REGISTER_NEWMETASESSION:
			if (length<73) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%"PRIu32"/>=73)",length);
				eptr->mode = KILL;
				return;
			}
			if (starting) {
				eptr->mode = KILL;
				return;
			}

			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length!=73+ileng && length!=73+16+ileng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%"PRIu32"/73+ileng(%"PRIu32")[+16])",length,ileng);
				eptr->mode = KILL;
				return;
			}
			info = (const char*)rptr;
			rptr+=ileng;
			if (length==73+16+ileng) {
				status = exports_check(eptr->peerip,eptr->version,1,NULL,eptr->passwordrnd,rptr,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			} else {
				status = exports_check(eptr->peerip,eptr->version,1,NULL,NULL,NULL,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			}
			if (status==STATUS_OK) {
				eptr->sesdata = sessions_new_session(0,sesflags,0,0,0,0,mingoal,maxgoal,mintrashtime,maxtrashtime,eptr->peerip,info,ileng);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==STATUS_OK)?((eptr->version>=VERSION2INT(1,6,26))?19:(eptr->version>=VERSION2INT(1,6,21))?9:5):1);
			if (status!=STATUS_OK) {
				put8bit(&wptr,status);
				eptr->sesdata = NULL;
				return;
			}
			sessionid = sessions_get_id(eptr->sesdata);
			if (eptr->version>=VERSION2INT(1,6,21)) {
				put16bit(&wptr,VERSMAJ);
				put8bit(&wptr,VERSMID);
				put8bit(&wptr,VERSMIN);
			}
			put32bit(&wptr,sessionid);
			put8bit(&wptr,sesflags);
			if (eptr->version>=VERSION2INT(1,6,26)) {
				put8bit(&wptr,mingoal);
				put8bit(&wptr,maxgoal);
				put32bit(&wptr,mintrashtime);
				put32bit(&wptr,maxtrashtime);
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = 1;
			return;
		case REGISTER_RECONNECT:
		case REGISTER_TOOLS:
			if (length<73) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - wrong size (%"PRIu32"/73)",rcode,length);
				eptr->mode = KILL;
				return;
			}

			sessionid = get32bit(&rptr);
			if (starting) {
				eptr->mode = KILL;
				return;
			}

			eptr->version = get32bit(&rptr);
			eptr->sesdata = sessions_find_session(sessionid);
			if (eptr->sesdata==NULL || sessions_get_peerip(eptr->sesdata)==0) { // no such session or session created by entries in metadata
				status = ERROR_BADSESSIONID;
			} else {
				if ((sessions_get_sesflags(eptr->sesdata)&SESFLAG_DYNAMICIP)==0 && eptr->peerip!=sessions_get_peerip(eptr->sesdata)) {
					status = ERROR_EACCES;
				} else {
					status = STATUS_OK;
				}
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
			put8bit(&wptr,status);
			if (status!=STATUS_OK) {
				eptr->sesdata = NULL;
				return;
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = (rcode==3)?1:100;
			return;
		case REGISTER_CLOSESESSION:
			if (length<69) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.6 - wrong size (%"PRIu32"/69)",length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			sessions_close_session(sessionid);
			if (eptr->version>=VERSION2INT(1,7,29)) {
				wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
				put8bit(&wptr,0);
			}
			eptr->mode = FINISH;
			return;
		}
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%"PRIu8")",rcode);
		eptr->mode = KILL;
		return;
	} else {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong register blob");
		eptr->mode = KILL;
		return;
	}
}

void matoclserv_fuse_sustained_inodes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	static uint32_t *inodetab = NULL;
	static uint32_t inodetabsize = 0;
	uint32_t i,j;

	if ((length&0x3)!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SUSTAINED_INODES - wrong size (%"PRIu32"/N*4)",length);
		eptr->mode = KILL;
		return;
	}

	if (eptr->sesdata==NULL) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SUSTAINED_INODES - session doesn't exist");
		eptr->mode = KILL;
		return;
	}
	length>>=2;
	if (length>inodetabsize) {
		if (inodetab) {
			free(inodetab);
		}
		inodetabsize = ((length+0xFF)&0xFFFFFF00);
		inodetab = malloc(sizeof(uint32_t)*inodetabsize);
		passert(inodetab);
	}
	rptr = data;
	j = 0;
	while (length>0) {
		i = get32bit(&rptr);
		if (i>0) {
			inodetab[j] = i;
			j++;
		}
		length--;
	}
	of_sync(sessions_get_id(eptr->sesdata),inodetab,j);
//	sessions_sync_open_files(eptr->sesdata,data,length>>2);
}

uint32_t* matoclserv_gid_storage(uint32_t gids) {
	static uint32_t *gid=NULL;
	static uint32_t gidleng=0;
	if (gids==0) {
		if (gid!=NULL) {
			free(gid);
		}
		gidleng=0;
		return NULL;
	} else {
		if (gidleng<gids) {
			gidleng = (gids+255)&UINT32_C(0xFFFFFF00);
			if (gid!=NULL) {
				free(gid);
			}
			gid = malloc(sizeof(uint32_t)*gidleng);
			passert(gid);
		}
		return gid;
	}
}

/*
static inline void matoclserv_ugid_remap(matoclserventry *eptr,uint32_t *auid,uint32_t *agid) {
	if (*auid==0) {
		*auid = eptr->sesdata->rootuid;
		if (agid) {
			*agid = eptr->sesdata->rootgid;
		}
	} else if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_MAPALL) {
		*auid = eptr->sesdata->mapalluid;
		if (agid) {
			*agid = eptr->sesdata->mapallgid;
		}
	}
}
*/
/*
static inline void matoclserv_ugid_attr_remap(matoclserventry *eptr,uint8_t attr[35],uint32_t auid,uint32_t agid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t fuid,fgid;
	if (auid!=0 && (sessions_get_sesflags(eptr->sesdata)&SESFLAG_MAPALL)) {
		rptr = attr+3;
		fuid = get32bit(&rptr);
		fgid = get32bit(&rptr);
		fuid = (fuid==eptr->sesdata->mapalluid)?auid:0;
		fgid = (fgid==eptr->sesdata->mapallgid)?agid:0;
		wptr = attr+3;
		put32bit(&wptr,fuid);
		put32bit(&wptr,fgid);
	}
}
*/
void matoclserv_fuse_statfs(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,trashspace,sustainedspace;
	uint32_t msgid,inodes;
	uint8_t *ptr;
	if (length!=4) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_STATFS - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	fs_statfs(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),&totalspace,&availspace,&trashspace,&sustainedspace,&inodes);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_STATFS,40);
	put32bit(&ptr,msgid);
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	put64bit(&ptr,trashspace);
	put64bit(&ptr,sustainedspace);
	put32bit(&ptr,inodes);
	sessions_inc_stats(eptr->sesdata,0);
}

void matoclserv_fuse_access(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t *gid;
	uint32_t i;
	uint32_t inode,uid,gids;
	uint16_t modemask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if ((length&1)==1) {
		if (length!=17) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/17)",length);
			eptr->mode = KILL;
			return;
		}
		msgid = get32bit(&data);
		inode = get32bit(&data);
		uid = get32bit(&data);
		gid = matoclserv_gid_storage(1);
		gid[0] = get32bit(&data);
		gids = 1;
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
		modemask = get8bit(&data);
	} else {
		if (length<18) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/18+4*N)",length);
			eptr->mode = KILL;
			return;
		}
		msgid = get32bit(&data);
		inode = get32bit(&data);
		uid = get32bit(&data);
		gids = get32bit(&data);
		if (length!=18+gids*4) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/18+4*N)",length);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
		modemask = get16bit(&data);
	}
	status = fs_access(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,modemask);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_ACCESS,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_lookup(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	auid = uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		gids = get32bit(&data);
		if (length!=17U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	status = fs_lookup(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid,auid,agid,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LOOKUP,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,3);
}

void matoclserv_fuse_getattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint8_t opened;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8 && length!=16 && length!=17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETATTR - wrong size (%"PRIu32"/8|16|17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==17) {
		opened = get8bit(&data);
	} else {
		opened = 0;
	}
	if (length>=16) {
		auid = uid = get32bit(&data);
		agid = gid = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,&gid);
	} else {
		auid = uid = 12345;
		agid = gid = 12345;
	}
	status = fs_getattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gid,auid,agid,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,1);
}

void matoclserv_fuse_setattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t opened;
	uint16_t setmask;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t sugidclearmode;
	uint16_t attrmode;
	uint32_t attruid,attrgid,attratime,attrmtime;
	if (length!=35 && length!=36 && length<37) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETATTR - wrong size (%"PRIu32"/35|36|37+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length>=37) {
		opened = get8bit(&data);
	} else {
		opened = 0;
	}
	auid = uid = get32bit(&data);
	if (length<=37) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=37+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_SETATTR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	setmask = get8bit(&data);
	attrmode = get16bit(&data);
	attruid = get32bit(&data);
	attrgid = get32bit(&data);
	attratime = get32bit(&data);
	attrmtime = get32bit(&data);
	if (length>=36) {
		sugidclearmode = get8bit(&data);
	} else {
		sugidclearmode = SUGID_CLEAR_MODE_ALWAYS; // this is safest option
	}
	if (setmask&(SET_GOAL_FLAG|SET_LENGTH_FLAG|SET_OPENED_FLAG)) {
		status = ERROR_EINVAL;
	} else {
		status = fs_setattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,auid,agid,setmask,attrmode,attruid,attrgid,attratime,attrmtime,sugidclearmode,attr);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,2);
}

void matoclserv_fuse_truncate(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t opened;
	uint8_t status;
	uint64_t attrlength;
	chunklist *cl;
	uint64_t chunkid;
	if (length!=24 && length<25) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_TRUNCATE - wrong size (%"PRIu32"/24|25+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0;
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length>=25) {
		opened = get8bit(&data);
	}
	auid = uid = get32bit(&data);
	if (length<=25) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
		if (length==24) {
			if (uid==0 && gid[0]!=0) {	// stupid "opened" patch for old clients
				opened = 1;
			}
		}
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		gids = get32bit(&data);
		if (length!=25+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_TRUNCATE - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	attrlength = get64bit(&data);
	status = fs_try_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,auid,agid,attrlength,attr,&chunkid);
	if (status==ERROR_DELAYED) {
		cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		cl->chunkid = chunkid;
		cl->qid = msgid;
		cl->inode = inode;
		cl->uid = uid;
		cl->gid = gid[0];
		cl->auid = auid;
		cl->agid = agid;
		cl->fleng = attrlength;
		cl->type = FUSE_TRUNCATE;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
		sessions_inc_stats(eptr->sesdata,2);
		return;
	}
	if (status==STATUS_OK) {
		status = fs_do_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gid[0],auid,agid,attrlength,attr);
	}
	if (status==STATUS_OK) {
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,2);
}

void matoclserv_fuse_readlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_READLINK - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_readlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&pleng,&path);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READLINK,(status!=STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
	sessions_inc_stats(eptr->sesdata,7);
}

void matoclserv_fuse_symlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t nleng;
	const uint8_t *name,*path;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint32_t pleng;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<21) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<21U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	pleng = get32bit(&data);
	if (length<21U+nleng+pleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8":pleng=%"PRIu32")",length,nleng,pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	auid = uid = get32bit(&data);
	if (length==21U+nleng+pleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=21U+nleng+pleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8":pleng=%"PRIu32":gids=%"PRIu32")",length,nleng,pleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_symlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,pleng,path,uid,gids,gid,auid,agid,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SYMLINK,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,6);
}

void matoclserv_fuse_mknod(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid,rdev;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint8_t type;
	uint16_t mode,cumask;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<24) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=24U+nleng && length<26U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	type = get8bit(&data);
	mode = get16bit(&data);
	if (length>=26U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	auid = uid = get32bit(&data);
	if (length<=26U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=26U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	rdev = get32bit(&data);
	status = fs_mknod(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,type,mode,cumask,uid,gids,gid,auid,agid,rdev,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_MKNOD,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,8);
}

void matoclserv_fuse_mkdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint16_t mode,cumask;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t copysgid;
	if (length<19) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=19U+nleng && length!=20U+nleng && length<22U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	mode = get16bit(&data);
	if (length>=22U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	auid = uid = get32bit(&data);
	if (length<=22U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=22U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (length>20U+nleng) {
		copysgid = get8bit(&data);
	} else {
		copysgid = 0; // by default do not copy sgid bit
	}
	status = fs_mkdir(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,mode,cumask,uid,gids,gid,auid,agid,copysgid,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_MKDIR,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,4);
}

void matoclserv_fuse_unlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=17U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_unlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_UNLINK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,9);
}

void matoclserv_fuse_rmdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=17U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_rmdir(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RMDIR,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,5);
}

void matoclserv_fuse_rename(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,inode_dst;
	uint8_t nleng_src,nleng_dst;
	const uint8_t *name_src,*name_dst;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<22) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode_src = get32bit(&data);
	nleng_src = get8bit(&data);
	if (length<22U+nleng_src) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8")",length,nleng_src);
		eptr->mode = KILL;
		return;
	}
	name_src = data;
	data += nleng_src;
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length<22U+nleng_src+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8":nleng_dst=%"PRIu8")",length,nleng_src,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	auid = uid = get32bit(&data);
	if (length==22U+nleng_src+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=22U+nleng_src+nleng_dst+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_src,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_rename(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode_src,nleng_src,name_src,inode_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,&inode,attr);
	if (eptr->version>=VERSION2INT(1,6,21) && status==STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,43);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=VERSION2INT(1,6,21) && status==STATUS_OK) {
		put32bit(&ptr,inode);
		memcpy(ptr,attr,35);
	} else {
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,10);
}

void matoclserv_fuse_link(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<21) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length<21U+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32":nleng_dst=%"PRIu8")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	auid = uid = get32bit(&data);
	if (length==21U+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=21U+nleng_dst+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_link(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,inode_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LINK,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,11);
}

void matoclserv_fuse_readdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t flags;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	uint32_t maxentries;
	uint64_t nedgeid;
	void *c1,*c2;
	if (length!=16 && length!=17 && length<29) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_READDIR - wrong size (%"PRIu32"/16|17|29+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	auid = uid = get32bit(&data);
	if (length<=29) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=29+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_READDIR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (length>=17) {
		flags = get8bit(&data);
	} else {
		flags = 0;
	}
	if (length>=29) {
		maxentries = get32bit(&data);
		nedgeid = get64bit(&data);
	} else {
		maxentries = 0xFFFFFFFF;
		nedgeid = 0;
	}
	status = fs_readdir_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,flags,maxentries,nedgeid,&c1,&c2,&dleng);
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READDIR,5);
	} else if (length>=29) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READDIR,12+dleng);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READDIR,4+dleng);
	}
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		if (length>=29) {
			put64bit(&ptr,nedgeid);
		}
		fs_readdir_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),uid,gid[0],auid,agid,flags,maxentries,&nedgeid,c1,c2,ptr);
/* CACHENOTIFY
		if (flags&GETDIR_FLAG_ADDTOCACHE) {
			if (inode==MFS_ROOT_ID) {
				matoclserv_notify_add_dir(eptr,sessions_get_rootinode(eptr->sesdata));
			} else {
				matoclserv_notify_add_dir(eptr,inode);
			}
		}
*/
	}
	sessions_inc_stats(eptr->sesdata,12);
}

/* CACHENOTIFY
void matoclserv_fuse_dir_removed(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	if (length%4!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_DIR_REMOVED - wrong size (%"PRIu32"/N*4)",length);
		eptr->mode = KILL;
		return;
	}
	if (get32bit(&data)) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_DIR_REMOVED - wrong msgid");
		eptr->mode = KILL;
		return;
	}
	length-=4;
	while (length) {
		inode = get32bit(&data);
		length-=4;
		if (inode==MFS_ROOT_ID) {
			matoclserv_notify_remove_dir(eptr,sessions_get_rootinode(eptr->sesdata));
		} else {
			matoclserv_notify_remove_dir(eptr,inode);
		}
	}
}
*/

void matoclserv_fuse_open(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t flags;
	uint8_t sesflags;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	int allowcache;
	if (length<17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_OPEN - wrong size (%"PRIu32"/17+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==17) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		auid = uid = get32bit(&data);
		agid = gid[0] = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		auid = uid = get32bit(&data);
		gids = get32bit(&data);
		if (length!=17+gids*4) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_OPEN - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	flags = get8bit(&data);
	sesflags = sessions_get_sesflags(eptr->sesdata);
	status = fs_opencheck(sessions_get_rootinode(eptr->sesdata),sesflags,inode,uid,gids,gid,auid,agid,flags,attr);
	if (status==STATUS_OK) {
		of_openfile(sessions_get_id(eptr->sesdata),inode);
	}
	if (eptr->version>=VERSION2INT(1,6,9) && status==STATUS_OK) {
		allowcache = dcm_open(inode,sessions_get_id(eptr->sesdata));
		if (allowcache==0) {
			if (sesflags&SESFLAG_ATTRBIT) {
				attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
			} else {
				attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
			}
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,39);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=VERSION2INT(1,6,9) && status==STATUS_OK) {
		memcpy(ptr,attr,35);
	} else {
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,13);
}

void matoclserv_fuse_create(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint16_t mode,cumask;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t sesflags;
	int allowcache;
	if (length<19) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=19U+nleng && length<21U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	mode = get16bit(&data);
	if (length>=21U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	auid = uid = get32bit(&data);
	if (length<=21U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=21U+nleng+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	sesflags = sessions_get_sesflags(eptr->sesdata);
	status = fs_mknod(sessions_get_rootinode(eptr->sesdata),sesflags,inode,nleng,name,TYPE_FILE,mode,cumask,uid,gids,gid,auid,agid,0,&newinode,attr);
	if (status==STATUS_OK) {
		of_openfile(sessions_get_id(eptr->sesdata),newinode);
		allowcache = dcm_open(newinode,sessions_get_id(eptr->sesdata));
		if (allowcache==0) {
			if (sesflags&SESFLAG_ATTRBIT) {
				attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
			} else {
				attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
			}
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CREATE,43);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CREATE,5);
	}
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	sessions_inc_stats(eptr->sesdata,8);
	sessions_inc_stats(eptr->sesdata,13);
}

void matoclserv_fuse_read_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
//	uint32_t ip;
//	uint16_t port;
	uint8_t count;
	uint8_t cs_data[100*10];
	uint32_t msgid;
	if (length!=12) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_READ_CHUNK - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
//	if (matoclserv_open_check(eptr,inode)<0) {
//		status = ERROR_NOTOPENED;
//	} else {
		status = fs_readchunk(inode,indx,&chunkid,&fleng);
//	}
	if (status==STATUS_OK) {
		if (chunkid>0) {
			if (eptr->version>=VERSION2INT(1,7,32)) {
				status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data);
			} else {
				status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data);
			}
		} else {
			version = 0;
			count = 0;
		}
	}
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READ_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return;
	}
	dcm_access(inode,sessions_get_id(eptr->sesdata));
	if (eptr->version>=VERSION2INT(1,7,32)) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READ_CHUNK,25+count*10);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READ_CHUNK,24+count*6);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=VERSION2INT(1,7,32)) {
		put8bit(&ptr,1);
	}
	put64bit(&ptr,fleng);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (eptr->version>=VERSION2INT(1,7,32)) {
		memcpy(ptr,cs_data,count*10);
	} else {
		memcpy(ptr,cs_data,count*6);
	}
	sessions_inc_stats(eptr->sesdata,14);
}

void matoclserv_fuse_write_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t fleng;
	uint64_t chunkid;
	uint32_t msgid;
	uint8_t opflag;
	chunklist *cl;
	uint32_t version;
	uint8_t count;
	uint8_t cs_data[100*10];

	if (length!=12) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_WRITE_CHUNK - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_READONLY) {
		status = ERROR_EROFS;
	} else {
		status = fs_writechunk(inode,indx,&chunkid,&fleng,&opflag);
	}
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return;
	}
	if (opflag) {	// wait for operation end
		cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		cl->inode = inode;
		cl->chunkid = chunkid;
		cl->qid = msgid;
		cl->fleng = fleng;
		cl->type = FUSE_WRITE;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
	} else {	// return status immediately
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
		if (eptr->version>=VERSION2INT(1,7,32)) {
			status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data);
		} else {
			status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data);
		}
		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			fs_writeend(0,0,chunkid);	// ignore status - just do it.
			return;
		}
		if (eptr->version>=VERSION2INT(1,7,32)) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*10);
		} else {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		}
		put32bit(&ptr,msgid);
		if (eptr->version>=VERSION2INT(1,7,32)) {
			put8bit(&ptr,1);
		}
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		if (eptr->version>=VERSION2INT(1,7,32)) {
			memcpy(ptr,cs_data,count*10);
		} else {
			memcpy(ptr,cs_data,count*6);
		}
	}
	sessions_inc_stats(eptr->sesdata,15);
}

void matoclserv_fuse_write_chunk_end(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t msgid;
	uint32_t inode;
	uint64_t fleng;
	uint64_t chunkid;
	uint8_t status;
//	chunklist *cl,**acl;
	if (length!=24) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_WRITE_CHUNK_END - wrong size (%"PRIu32"/24)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	chunkid = get64bit(&data);
	inode = get32bit(&data);
	fleng = get64bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_READONLY) {
		status = ERROR_EROFS;
	} else {
		status = fs_writeend(inode,fleng,chunkid);
	}
	dcm_modify(inode,sessions_get_id(eptr->sesdata));
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK_END,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_repair(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint32_t chunksnotchanged,chunkserased,chunksrepaired;
	uint8_t *ptr;
	uint8_t status;
	if (length<16) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REPAIR - wrong size (%"PRIu32"/16+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	if (length==16) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=16+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REPAIR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_repair(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,&chunksnotchanged,&chunkserased,&chunksrepaired);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REPAIR,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=0) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,chunksnotchanged);
		put32bit(&ptr,chunkserased);
		put32bit(&ptr,chunksrepaired);
	}
}

void matoclserv_fuse_check(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t i,chunkcount[11];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_CHECK - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_checkfile(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,chunkcount);
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		if (eptr->version>=VERSION2INT(1,6,23)) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,48);
			put32bit(&ptr,msgid);
			for (i=0 ; i<11 ; i++) {
				put32bit(&ptr,chunkcount[i]);
			}
		} else {
			uint8_t j;
			j=0;
			for (i=0 ; i<11 ; i++) {
				if (chunkcount[i]>0) {
					j++;
				}
			}
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,4+3*j);
			put32bit(&ptr,msgid);
			for (i=0 ; i<11 ; i++) {
				if (chunkcount[i]>0) {
					put8bit(&ptr,i);
					if (chunkcount[i]<=65535) {
						put16bit(&ptr,chunkcount[i]);
					} else {
						put16bit(&ptr,65535);
					}
				}
			}
		}
	}
}


void matoclserv_fuse_gettrashtime(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t gmode;
	void *fptr,*dptr;
	uint32_t fnodes,dnodes;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASHTIME - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_gettrashtime_prepare(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,&fptr,&dptr,&fnodes,&dnodes);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASHTIME,(status!=STATUS_OK)?5:12+8*(fnodes+dnodes));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,fnodes);
		put32bit(&ptr,dnodes);
		fs_gettrashtime_store(fptr,dptr,ptr);
	}
}

void matoclserv_fuse_settrashtime(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,trashtime;
	uint32_t msgid;
	uint8_t smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHTIME - wrong size (%"PRIu32"/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	trashtime = get32bit(&data);
	smode = get8bit(&data);
	status = sessions_check_trashtime(eptr->sesdata,smode&SMODE_TMASK,trashtime);
	if (status==STATUS_OK) {
		status = fs_settrashtime(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,trashtime,smode,&changed,&notchanged,&notpermitted);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETTRASHTIME,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
}

void matoclserv_fuse_getgoal(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t fgtab[10],dgtab[10];
	uint8_t i,fn,dn,gmode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETGOAL - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_getgoal(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,fgtab,dgtab);
	fn=0;
	dn=0;
	if (status==STATUS_OK) {
		for (i=1 ; i<10 ; i++) {
			if (fgtab[i]) {
				fn++;
			}
			if (dgtab[i]) {
				dn++;
			}
		}
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETGOAL,(status!=STATUS_OK)?5:6+5*(fn+dn));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fn);
		put8bit(&ptr,dn);
		for (i=1 ; i<10 ; i++) {
			if (fgtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,fgtab[i]);
			}
		}
		for (i=1 ; i<10 ; i++) {
			if (dgtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,dgtab[i]);
			}
		}
	}
}

void matoclserv_fuse_setgoal(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t goal,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETGOAL - wrong size (%"PRIu32"/14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	goal = get8bit(&data);
	smode = get8bit(&data);
	status = sessions_check_goal(eptr->sesdata,smode&SMODE_TMASK,goal);
	if (goal<1 || goal>9) {
		status = ERROR_EINVAL;
	}
	if (status==STATUS_OK) {
		status = fs_setgoal(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,goal,smode,&changed,&notchanged,&notpermitted);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETGOAL,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
}

void matoclserv_fuse_geteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t feattrtab[16],deattrtab[16];
	uint8_t i,fn,dn,gmode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETEATTR - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_geteattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,feattrtab,deattrtab);
	fn=0;
	dn=0;
	if (status==STATUS_OK) {
		for (i=0 ; i<16 ; i++) {
			if (feattrtab[i]) {
				fn++;
			}
			if (deattrtab[i]) {
				dn++;
			}
		}
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETEATTR,(status!=STATUS_OK)?5:6+5*(fn+dn));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fn);
		put8bit(&ptr,dn);
		for (i=0 ; i<16 ; i++) {
			if (feattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,feattrtab[i]);
			}
		}
		for (i=0 ; i<16 ; i++) {
			if (deattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,deattrtab[i]);
			}
		}
	}
}

void matoclserv_fuse_seteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t eattr,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETEATTR - wrong size (%"PRIu32"/14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	eattr = get8bit(&data);
	smode = get8bit(&data);
	status = fs_seteattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,eattr,smode,&changed,&notchanged,&notpermitted);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETEATTR,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
}

void matoclserv_fuse_parents(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t pcount;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_PARENTS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_parents_count(sessions_get_rootinode(eptr->sesdata),inode,&pcount);
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PARENTS,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PARENTS,4+4*pcount);
		put32bit(&ptr,msgid);
		fs_get_parents_data(sessions_get_rootinode(eptr->sesdata),inode,ptr);
	}
}

void matoclserv_fuse_paths(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t psize;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_PATHS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_paths_size(sessions_get_rootinode(eptr->sesdata),inode,&psize);
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PATHS,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PATHS,4+psize);
		put32bit(&ptr,msgid);
		fs_get_paths_data(sessions_get_rootinode(eptr->sesdata),inode,ptr);
	}
}

void matoclserv_fuse_getxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint8_t opened;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	uint8_t anleng;
	const uint8_t *attrname;
	if (length<19) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0; // makes gcc happy
	gid = NULL; // makes gcc happy
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (eptr->version<VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	}
	anleng = get8bit(&data);
	attrname = data;
	data+=anleng;
	if (length<19U+anleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	mode = get8bit(&data);
	if (eptr->version>=VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		if (length==19U+anleng) {
			gids = 1;
			gid = matoclserv_gid_storage(gids);
			gid[0] = get32bit(&data);
		} else {
			gids = get32bit(&data);
			if (length!=19U+anleng+4*gids) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":gids=%"PRIu32")",length,anleng,gids);
				eptr->mode = KILL;
				return;
			}
			gid = matoclserv_gid_storage(gids);
			for (i=0 ; i<gids ; i++) {
				gid[i] = get32bit(&data);
			}
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (mode!=MFS_XATTR_GETA_DATA && mode!=MFS_XATTR_LENGTH_ONLY) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,ERROR_EINVAL);
	} else if (anleng==0) {
		void *xanode;
		uint32_t xasize;
		status = fs_listxattr_leng(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,&xanode,&xasize);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=STATUS_OK)?5:8+((mode==MFS_XATTR_GETA_DATA)?xasize:0));
		put32bit(&ptr,msgid);
		if (status!=STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,xasize);
			if (mode==MFS_XATTR_GETA_DATA && xasize>0) {
				fs_listxattr_data(xanode,ptr);
			}
		}
	} else {
		uint8_t *attrvalue;
		uint32_t avleng;
		status = fs_getxattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,anleng,attrname,&avleng,&attrvalue);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=STATUS_OK)?5:8+((mode==MFS_XATTR_GETA_DATA)?avleng:0));
		put32bit(&ptr,msgid);
		if (status!=STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,avleng);
			if (mode==MFS_XATTR_GETA_DATA && avleng>0) {
				memcpy(ptr,attrvalue,avleng);
			}
		}
	}
}

void matoclserv_fuse_setxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	const uint8_t *attrname,*attrvalue;
	uint8_t opened;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	if (length<23) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0; // makes gcc happy
	gid = NULL; // makes gcc happy
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (eptr->version<VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	}
	anleng = get8bit(&data);
	if (length<23U+anleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	attrname = data;
	data += anleng;
	avleng = get32bit(&data);
	if (length<23U+anleng+avleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":avleng=%"PRIu32")",length,anleng,avleng);
		eptr->mode = KILL;
		return;
	}
	attrvalue = data;
	data += avleng;
	mode = get8bit(&data);
	if (eptr->version>=VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		if (length==23U+anleng+avleng) {
			gids = 1;
			gid = matoclserv_gid_storage(gids);
			gid[0] = get32bit(&data);
		} else {
			gids = get32bit(&data);
			if (length!=23U+anleng+avleng+4*gids) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":avleng=%"PRIu32":gids=%"PRIu32")",length,anleng,avleng,gids);
				eptr->mode = KILL;
				return;
			}
			gid = matoclserv_gid_storage(gids);
			for (i=0 ; i<gids ; i++) {
				gid[i] = get32bit(&data);
			}
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_setxattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,anleng,attrname,avleng,attrvalue,mode);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETXATTR,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_getacl(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint8_t opened;
	uint32_t msgid;
	uint8_t acltype;
	uint8_t *ptr;
	uint8_t status;
	void *c;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint32_t aclleng;
	if (length<18) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETACL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	acltype = get8bit(&data);
	opened = get8bit(&data);
	uid = get32bit(&data);
	if (length==18) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=18+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_GETACL - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_getacl_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,acltype,&c,&aclleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETACL,(status!=STATUS_OK)?5:16+aclleng);
	if (status!=STATUS_OK) {
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		fs_getacl_data(c,&userperm,&groupperm,&otherperm,&mask,&namedusers,&namedgroups,ptr+16);
		put32bit(&ptr,msgid);
		put16bit(&ptr,userperm);
		put16bit(&ptr,groupperm);
		put16bit(&ptr,otherperm);
		put16bit(&ptr,mask);
		put16bit(&ptr,namedusers);
		put16bit(&ptr,namedgroups);
	}
}

void matoclserv_fuse_setacl(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t uid;
	uint32_t msgid;
	uint8_t acltype;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint8_t *ptr;
	uint8_t status;
	if (length<25) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETACL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	acltype = get8bit(&data);
	userperm = get16bit(&data);
	groupperm = get16bit(&data);
	otherperm = get16bit(&data);
	mask = get16bit(&data);
	namedusers = get16bit(&data);
	namedgroups = get16bit(&data);
	if (length!=(namedusers+namedgroups)*6U+25U) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETACL - wrong size (%"PRIu32":namedusers=%"PRIu16":namedgroups=%"PRIu16")",length,namedusers,namedgroups);
		eptr->mode = KILL;
		return;
	}
//	uid = get32bit(&data);
//	gid = get32bit(&data);
//	sessions_ugid_remap(eptr->sesdata,&uid,&gid);
	status = fs_setacl(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,data);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETACL,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

/*
void matoclserv_fuse_setfilechunks(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,indx;
	uint32_t msgid;
	uint64_t leng;
	uint8_t *ptr;
	uint8_t status;
	if (length<28U || ((length-28)%8)!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETFILECHUNKS - wrong size (%"PRIu32"/28+N*8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	leng = get64bit(&data);
	indx = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,&gid);
	status = fs_setfilechunks(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gid,leng,indx,(length-28)/8,data);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETFILECHUNKS,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}
*/

void matoclserv_fuse_append(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<20) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_APPEND - wrong size (%"PRIu32"/20)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_src = get32bit(&data);
	uid = get32bit(&data);
	if (length==20) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=20+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_APPEND - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_append(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,inode_src,uid,gids,gid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_APPEND,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_snapshot(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint8_t smode;
	uint16_t requmask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<22) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=22U+nleng_dst && length<24U+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32":nleng_dst=%"PRIu8")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	if (length<=24U+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=24U+nleng_dst+4*gids) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	smode = get8bit(&data);
	if (length>=24U+nleng_dst) {
		requmask = get16bit(&data);
	} else {
		smode &= ~SNAPSHOT_MODE_CPLIKE_ATTR;
		requmask = 0;
	}
	status = fs_snapshot(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,inode_dst,nleng_dst,name_dst,uid,gids,gid,smode,requmask);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SNAPSHOT,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_quotacontrol(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t flags,del;
	uint32_t sinodes,hinodes,curinodes;
	uint64_t slength,ssize,srealsize,hlength,hsize,hrealsize,curlength,cursize,currealsize;
	uint32_t msgid,inode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=65 && length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_QUOTACONTROL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	flags = get8bit(&data);
	if (length==65) {
		sinodes = get32bit(&data);
		slength = get64bit(&data);
		ssize = get64bit(&data);
		srealsize = get64bit(&data);
		hinodes = get32bit(&data);
		hlength = get64bit(&data);
		hsize = get64bit(&data);
		hrealsize = get64bit(&data);
		del=0;
	} else {
		del=1;
	}
	if (flags && sessions_is_root_remapped(eptr->sesdata)) {
		status = ERROR_EACCES;
	} else {
		status = fs_quotacontrol(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,del,&flags,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize,&curinodes,&curlength,&cursize,&currealsize);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_QUOTACONTROL,(status!=STATUS_OK)?5:89);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,flags);
		put32bit(&ptr,sinodes);
		put64bit(&ptr,slength);
		put64bit(&ptr,ssize);
		put64bit(&ptr,srealsize);
		put32bit(&ptr,hinodes);
		put64bit(&ptr,hlength);
		put64bit(&ptr,hsize);
		put64bit(&ptr,hrealsize);
		put32bit(&ptr,curinodes);
		put64bit(&ptr,curlength);
		put64bit(&ptr,cursize);
		put64bit(&ptr,currealsize);
	}
}

/*
void matoclserv_fuse_eattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t mode,eattr,fneattr;
	uint32_t msgid,inode,uid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_EATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	mode = get8bit(&data);
	eattr = get8bit(&data);
	status = fs_eattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,mode,&eattr,&fneattr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_EATTR,(status!=STATUS_OK)?5:6);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,eattr);
		put8bit(&ptr,fneattr);
	}
}
*/

void matoclserv_fuse_getdirstats_old(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=STATUS_OK)?5:60);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put32bit(&ptr,chunks);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_getdirstats(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=STATUS_OK)?5:44);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,chunks);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_gettrash(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	if (length!=4) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASH - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readtrash_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASH,(status!=STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readtrash_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),ptr);
	}
}

void matoclserv_fuse_getdetachedattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t dtype;
	uint8_t *ptr;
	uint8_t status;
	if (length<8 || length>9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDETACHEDATTR - wrong size (%"PRIu32"/8,9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==9) {
		dtype = get8bit(&data);
	} else {
		dtype = DTYPE_UNKNOWN;
	}
	status = fs_getdetachedattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,attr,dtype);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDETACHEDATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
}

void matoclserv_fuse_gettrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	const uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASHPATH - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_gettrashpath(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&pleng,&path);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASHPATH,(status!=STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
}

void matoclserv_fuse_settrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	const uint8_t *path;
	uint32_t pleng;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<12) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%"PRIu32"/>=12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	pleng = get32bit(&data);
	if (length!=12+pleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%"PRIu32"/%"PRIu32")",length,12+pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_settrashpath(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,pleng,path);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETTRASHPATH,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_undel(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNDEL - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_undel(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_UNDEL,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_purge(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_PURGE - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_purge(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PURGE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}


void matoclserv_fuse_getsustained(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	if (length!=4) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETSUSTAINED - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readsustained_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETSUSTAINED,(status!=STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readsustained_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),ptr);
	}
}


/*
void matocl_session_timedout(session *sesdata) {
	filelist *fl,*afl;
	fl=sesdata->openedfiles;
	while (fl) {
		afl = fl;
		fl=fl->next;
		fs_release(afl->inode,sesdata->sessionid);
		free(afl);
	}
	sesdata->openedfiles=NULL;
	if (sesdata->info) {
		free(sesdata->info);
	}
}

void matocl_session_check(void) {
	session **sesdata,*asesdata;
	uint32_t now;

	now = main_time();
	sesdata = &(sessionshead);
	while ((asesdata=*sesdata)) {
		// syslog(LOG_NOTICE,"session: %u ; nsocks: %u ; state: %u ; disconnected: %u",asesdata->sessionid,asesdata->nsocks,asesdata->newsession,asesdata->disconnected);
		if (asesdata->nsocks==0 && ((asesdata->newsession>1 && asesdata->disconnected<now) || (asesdata->newsession==1 && asesdata->disconnected+SessionSustainTime<now) || (asesdata->newsession==0 && asesdata->disconnected+120<now))) {
			syslog(LOG_NOTICE,"remove session: %u",asesdata->sessionid);
			matocl_session_timedout(asesdata);
			*sesdata = asesdata->next;
			free(asesdata);
		} else {
			sesdata = &(asesdata->next);
		}
	}
//	matoclserv_show_notification_dirs();
}

void matocl_session_statsmove(void) {
	session *sesdata;
	for (sesdata = sessionshead ; sesdata ; sesdata=sesdata->next) {
		memcpy(sesdata->lasthouropstats,sesdata->currentopstats,4*SESSION_STATS);
		memset(sesdata->currentopstats,0,4*SESSION_STATS);
	}
	matoclserv_store_sessions();
}
*/
void matocl_beforedisconnect(matoclserventry *eptr) {
	chunklist *cl,*acl;
// unlock locked chunks
	cl=eptr->chunkdelayedops;
	while (cl) {
		acl = cl;
		cl=cl->next;
		if (acl->type == FUSE_TRUNCATE) {
			fs_end_setlength(acl->chunkid);
		}
		free(acl);
	}
	eptr->chunkdelayedops=NULL;
	sessions_disconnection(eptr->sesdata);
/*
	if (eptr->sesdata) {

		if (eptr->sesdata->nsocks>0) {
			eptr->sesdata->nsocks--;
		}
		if (eptr->sesdata->nsocks==0) {
			eptr->sesdata->disconnected = main_time();
		}
	}
*/
/* CACHENOTIFY
	matoclserv_notify_disconnected(eptr);
*/
}

void matoclserv_gotpacket(matoclserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	if (type==ANTOAN_NOP) {
		return;
	}
	if (type==ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type==ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
//	printf("AQQ\n");
	if (eptr->registered==0) {	// unregistered clients - beware that in this context sesdata is NULL
		switch (type) {
			case ANTOAN_GET_VERSION:
				matoclserv_get_version(eptr,data,length);
				break;
			case CLTOMA_FUSE_REGISTER:
//				printf("REGISTER\n");
				matoclserv_fuse_register(eptr,data,length);
				break;
			case CLTOMA_CSERV_LIST:
				matoclserv_cserv_list(eptr,data,length);
				break;
			case CLTOMA_SESSION_LIST:
				matoclserv_session_list(eptr,data,length);
				break;
			case CLTOAN_CHART:
				matoclserv_chart(eptr,data,length);
				break;
			case CLTOAN_CHART_DATA:
				matoclserv_chart_data(eptr,data,length);
				break;
			case CLTOMA_INFO:
				matoclserv_info(eptr,data,length);
				break;
			case CLTOMA_FSTEST_INFO:
				matoclserv_fstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKSTEST_INFO:
				matoclserv_chunkstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKS_MATRIX:
				matoclserv_chunks_matrix(eptr,data,length);
				break;
			case CLTOMA_QUOTA_INFO:
				matoclserv_quota_info(eptr,data,length);
				break;
			case CLTOMA_EXPORTS_INFO:
				matoclserv_exports_info(eptr,data,length);
				break;
			case CLTOMA_MLOG_LIST:
				matoclserv_mlog_list(eptr,data,length);
				break;
			case CLTOMA_CSSERV_COMMAND:
				matoclserv_cserv_command(eptr,data,length);
				break;
			case CLTOMA_SESSION_COMMAND:
				matoclserv_session_command(eptr,data,length);
				break;
			case CLTOMA_MEMORY_INFO:
				matoclserv_memory_info(eptr,data,length);
				break;
			case CLTOAN_MODULE_INFO:
				matoclserv_module_info(eptr,data,length);
				break;
			case CLTOMA_MASS_RESOLVE_PATHS:
				matoclserv_mass_resolve_paths(eptr,data,length);
				break;
			case CLTOMA_MISSING_CHUNKS:
				matoclserv_missing_chunks(eptr,data,length);
				break;
			default:
				syslog(LOG_NOTICE,"main master server module: got unknown message from unregistered (type:%"PRIu32")",type);
				eptr->mode=KILL;
		}
	} else if (eptr->registered<100) {	// mounts and new tools
		if (eptr->sesdata==NULL) {
			syslog(LOG_ERR,"registered connection without sesdata !!!");
			eptr->mode=KILL;
			return;
		}
		switch (type) {
			case ANTOAN_GET_VERSION:
				matoclserv_get_version(eptr,data,length);
				break;
			case CLTOMA_FUSE_REGISTER:
				matoclserv_fuse_register(eptr,data,length);
				break;
			case CLTOMA_FUSE_SUSTAINED_INODES:
				matoclserv_fuse_sustained_inodes(eptr,data,length);
				break;
			case CLTOMA_FUSE_STATFS:
				matoclserv_fuse_statfs(eptr,data,length);
				break;
			case CLTOMA_FUSE_ACCESS:
				matoclserv_fuse_access(eptr,data,length);
				break;
			case CLTOMA_FUSE_LOOKUP:
				matoclserv_fuse_lookup(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETATTR:
				matoclserv_fuse_getattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETATTR:
				matoclserv_fuse_setattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_READLINK:
				matoclserv_fuse_readlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_SYMLINK:
				matoclserv_fuse_symlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_MKNOD:
				matoclserv_fuse_mknod(eptr,data,length);
				break;
			case CLTOMA_FUSE_MKDIR:
				matoclserv_fuse_mkdir(eptr,data,length);
				break;
			case CLTOMA_FUSE_UNLINK:
				matoclserv_fuse_unlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_RMDIR:
				matoclserv_fuse_rmdir(eptr,data,length);
				break;
			case CLTOMA_FUSE_RENAME:
				matoclserv_fuse_rename(eptr,data,length);
				break;
			case CLTOMA_FUSE_LINK:
				matoclserv_fuse_link(eptr,data,length);
				break;
			case CLTOMA_FUSE_READDIR:
				matoclserv_fuse_readdir(eptr,data,length);
				break;
/* CACHENOTIFY
			case CLTOMA_FUSE_DIR_REMOVED:
				matoclserv_fuse_dir_removed(eptr,data,length);
				break;
*/
			case CLTOMA_FUSE_OPEN:
				matoclserv_fuse_open(eptr,data,length);
				break;
			case CLTOMA_FUSE_CREATE:
				matoclserv_fuse_create(eptr,data,length);
				break;
			case CLTOMA_FUSE_READ_CHUNK:
				matoclserv_fuse_read_chunk(eptr,data,length);
				break;
			case CLTOMA_FUSE_WRITE_CHUNK:
				matoclserv_fuse_write_chunk(eptr,data,length);
				break;
			case CLTOMA_FUSE_WRITE_CHUNK_END:
				matoclserv_fuse_write_chunk_end(eptr,data,length);
				break;
// fuse - meta
			case CLTOMA_FUSE_GETTRASH:
				matoclserv_fuse_gettrash(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETDETACHEDATTR:
				matoclserv_fuse_getdetachedattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETTRASHPATH:
				matoclserv_fuse_gettrashpath(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETTRASHPATH:
				matoclserv_fuse_settrashpath(eptr,data,length);
				break;
			case CLTOMA_FUSE_UNDEL:
				matoclserv_fuse_undel(eptr,data,length);
				break;
			case CLTOMA_FUSE_PURGE:
				matoclserv_fuse_purge(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETSUSTAINED:
				matoclserv_fuse_getsustained(eptr,data,length);
				break;
			case CLTOMA_FUSE_CHECK:
				matoclserv_fuse_check(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETTRASHTIME:
				matoclserv_fuse_gettrashtime(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETTRASHTIME:
				matoclserv_fuse_settrashtime(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETGOAL:
				matoclserv_fuse_getgoal(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETGOAL:
				matoclserv_fuse_setgoal(eptr,data,length);
				break;
			case CLTOMA_FUSE_APPEND:
				matoclserv_fuse_append(eptr,data,length);
				break;
//			case CLTOMA_FUSE_SETFILECHUNKS:
//				matoclserv_fuse_setfilechunks(eptr,data,length);
//				break;
			case CLTOMA_FUSE_GETDIRSTATS:
				matoclserv_fuse_getdirstats_old(eptr,data,length);
				break;
			case CLTOMA_FUSE_TRUNCATE:
				matoclserv_fuse_truncate(eptr,data,length);
				break;
			case CLTOMA_FUSE_REPAIR:
				matoclserv_fuse_repair(eptr,data,length);
				break;
			case CLTOMA_FUSE_SNAPSHOT:
				matoclserv_fuse_snapshot(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETEATTR:
				matoclserv_fuse_geteattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETEATTR:
				matoclserv_fuse_seteattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_PARENTS:
				matoclserv_fuse_parents(eptr,data,length);
				break;
			case CLTOMA_FUSE_PATHS:
				matoclserv_fuse_paths(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETXATTR:
				matoclserv_fuse_getxattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETXATTR:
				matoclserv_fuse_setxattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETACL:
				matoclserv_fuse_getacl(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETACL:
				matoclserv_fuse_setacl(eptr,data,length);
				break;
			case CLTOMA_FUSE_QUOTACONTROL:
				matoclserv_fuse_quotacontrol(eptr,data,length);
				break;

/* for tools - also should be available for registered clients */
			case CLTOMA_CSERV_LIST:
				matoclserv_cserv_list(eptr,data,length);
				break;
			case CLTOMA_SESSION_LIST:
				matoclserv_session_list(eptr,data,length);
				break;
			case CLTOAN_CHART:
				matoclserv_chart(eptr,data,length);
				break;
			case CLTOAN_CHART_DATA:
				matoclserv_chart_data(eptr,data,length);
				break;
			case CLTOMA_INFO:
				matoclserv_info(eptr,data,length);
				break;
			case CLTOMA_FSTEST_INFO:
				matoclserv_fstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKSTEST_INFO:
				matoclserv_chunkstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKS_MATRIX:
				matoclserv_chunks_matrix(eptr,data,length);
				break;
			case CLTOMA_QUOTA_INFO:
				matoclserv_quota_info(eptr,data,length);
				break;
			case CLTOMA_EXPORTS_INFO:
				matoclserv_exports_info(eptr,data,length);
				break;
			case CLTOMA_MLOG_LIST:
				matoclserv_mlog_list(eptr,data,length);
				break;
			case CLTOMA_CSSERV_COMMAND:
				matoclserv_cserv_command(eptr,data,length);
				break;
			case CLTOMA_SESSION_COMMAND:
				matoclserv_session_command(eptr,data,length);
				break;
			case CLTOMA_MEMORY_INFO:
				matoclserv_memory_info(eptr,data,length);
				break;
			case CLTOAN_MODULE_INFO:
				matoclserv_module_info(eptr,data,length);
				break;
			case CLTOMA_MASS_RESOLVE_PATHS:
				matoclserv_mass_resolve_paths(eptr,data,length);
				break;
			case CLTOMA_MISSING_CHUNKS:
				matoclserv_missing_chunks(eptr,data,length);
				break;
			default:
				syslog(LOG_NOTICE,"main master server module: got unknown message from mfsmount (type:%"PRIu32")",type);
				eptr->mode=KILL;
		}
	} else {	// old mfstools
		if (eptr->sesdata==NULL) {
			syslog(LOG_ERR,"registered connection (tools) without sesdata !!!");
			eptr->mode=KILL;
			return;
		}
		switch (type) {
// extra (external tools)
			case ANTOAN_GET_VERSION:
				matoclserv_get_version(eptr,data,length);
				break;
			case CLTOMA_FUSE_REGISTER:
				matoclserv_fuse_register(eptr,data,length);
				break;
			case CLTOMA_FUSE_READ_CHUNK:	// used in mfsfileinfo
				matoclserv_fuse_read_chunk(eptr,data,length);
				break;
			case CLTOMA_FUSE_CHECK:
				matoclserv_fuse_check(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETTRASHTIME:
				matoclserv_fuse_gettrashtime(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETTRASHTIME:
				matoclserv_fuse_settrashtime(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETGOAL:
				matoclserv_fuse_getgoal(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETGOAL:
				matoclserv_fuse_setgoal(eptr,data,length);
				break;
			case CLTOMA_FUSE_APPEND:
				matoclserv_fuse_append(eptr,data,length);
				break;
//			case CLTOMA_FUSE_SETFILECHUNKS:
//				matoclserv_fuse_setfilechunks(eptr,data,length);
//				break;
			case CLTOMA_FUSE_GETDIRSTATS:
				matoclserv_fuse_getdirstats(eptr,data,length);
				break;
			case CLTOMA_FUSE_TRUNCATE:
				matoclserv_fuse_truncate(eptr,data,length);
				break;
			case CLTOMA_FUSE_REPAIR:
				matoclserv_fuse_repair(eptr,data,length);
				break;
			case CLTOMA_FUSE_SNAPSHOT:
				matoclserv_fuse_snapshot(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETEATTR:
				matoclserv_fuse_geteattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETEATTR:
				matoclserv_fuse_seteattr(eptr,data,length);
				break;
/* do not use in version before 1.7.x */
			case CLTOMA_FUSE_QUOTACONTROL:
				matoclserv_fuse_quotacontrol(eptr,data,length);
				break;
/* ------ */
			default:
				syslog(LOG_NOTICE,"main master server module: got unknown message from mfstools (type:%"PRIu32")",type);
				eptr->mode=KILL;
		}
	}
}

void matoclserv_read(matoclserventry *eptr,double now) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup,errmsg;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;

	if (eptr == NULL) {
		if (readbuff != NULL) {
			free(readbuff);
		}
		readbuff = NULL;
		readbuffsize = 0;
		return;
	}

	if (readbuffsize==0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		passert(readbuff);
	}

	rbleng = 0;
	err = 0;
	hup = 0;
	errmsg = 0;
	for (;;) {
		i = read(eptr->sock,readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
#ifdef ECONNRESET
				if (errno!=ECONNRESET || eptr->registered<100) {
#endif
					errmsg = 1;
#ifdef ECONNRESET
				}
#endif
			}
			break;
		} else {
			stats_brcvd += i;
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = realloc(readbuff,readbuffsize);
				passert(readbuff);
			} else {
				break;
			}
		}
	}

	if (rbleng>0) {
		eptr->lastread = now;
	}

	rbpos = 0;
	while (rbpos<rbleng) {
		if ((rbleng-rbpos)>=eptr->input_bytesleft) {
			memcpy(eptr->input_startptr,readbuff+rbpos,eptr->input_bytesleft);
			i = eptr->input_bytesleft;
		} else {
			memcpy(eptr->input_startptr,readbuff+rbpos,rbleng-rbpos);
			i = rbleng-rbpos;
		}
		rbpos += i;
		eptr->input_startptr+=i;
		eptr->input_bytesleft-=i;

		if (eptr->input_bytesleft>0) {
			break;
		}

		if (eptr->input_packet == NULL) {
			ptr = eptr->input_hdr;
			type = get32bit(&ptr);
			leng = get32bit(&ptr);

			if (leng>MaxPacketSize) {
				syslog(LOG_WARNING,"main master server module: packet too long (%"PRIu32"/%u)",leng,MaxPacketSize);
				eptr->input_end = 1;
				return;
			}

			stats_prcvd++;
			eptr->input_packet = malloc(offsetof(in_packetstruct,data)+leng);
			passert(eptr->input_packet);
			eptr->input_packet->next = NULL;
			eptr->input_packet->type = type;
			eptr->input_packet->leng = leng;

			eptr->input_startptr = eptr->input_packet->data;
			eptr->input_bytesleft = leng;
		}

		if (eptr->input_bytesleft>0) {
			continue;
		}

		if (eptr->input_packet != NULL) {
			*(eptr->inputtail) = eptr->input_packet;
			eptr->inputtail = &(eptr->input_packet->next);
			eptr->input_packet = NULL;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
		}
	}

	if (hup) {
		if (eptr->registered>0 && eptr->registered<100) {	// show this message only for standard, registered clients
			syslog(LOG_NOTICE,"connection with client(ip:%u.%u.%u.%u) has been closed by peer",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
		}
		eptr->input_end = 1;
	} else if (err) {
		if (errmsg) {
			mfs_arg_errlog_silent(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) read error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
		}
		eptr->input_end = 1;
	}
}

void matoclserv_parse(matoclserventry *eptr) {
	in_packetstruct *ipack;
	uint64_t starttime;
	uint64_t currtime;

	starttime = monotonic_useconds();
	currtime = starttime;
	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL && starttime+10000>currtime) {
		matoclserv_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
		eptr->inputhead = ipack->next;
		free(ipack);
		if (eptr->inputhead==NULL) {
			eptr->inputtail = &(eptr->inputhead);
		} else {
			currtime = monotonic_useconds();
		}
	}
	if (eptr->mode==DATA && eptr->inputhead==NULL && eptr->input_end) {
		eptr->mode = KILL;
	}
}

void matoclserv_write(matoclserventry *eptr,double now) {
	out_packetstruct *opack;
	int32_t i;
#ifdef HAVE_WRITEV
	struct iovec iovtab[100];
	uint32_t iovdata;
	uint32_t leng;
	uint32_t left;

	for (;;) {
		leng = 0;
		for (iovdata=0,opack=eptr->outputhead ; iovdata<100 && opack!=NULL ; iovdata++,opack=opack->next) {
			iovtab[iovdata].iov_base = opack->startptr;
			iovtab[iovdata].iov_len = opack->bytesleft;
			leng += opack->bytesleft;
		}
		if (iovdata==0) {
			return;
		}
		i = writev(eptr->sock,iovtab,iovdata);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_arg_errlog_silent(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		stats_bsent+=i;
		left = i;
		while (left>0 && eptr->outputhead!=NULL) {
			opack = eptr->outputhead;
			if (opack->bytesleft>left) {
				opack->startptr+=left;
				opack->bytesleft-=left;
				left = 0;
			} else {
				left -= opack->bytesleft;
				eptr->outputhead = opack->next;
				if (eptr->outputhead==NULL) {
					eptr->outputtail = &(eptr->outputhead);
				}
				free(opack);
				stats_psent++;
			}
		}
		if ((uint32_t)i < leng) {
			return;
		}
	}
#else
	for (;;) {
		opack = eptr->outputhead;
		if (opack==NULL) {
			return;
		}
		i=write(eptr->sock,opack->startptr,opack->bytesleft);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_arg_errlog_silent(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		opack->startptr+=i;
		opack->bytesleft-=i;
		stats_bsent+=i;
		if (opack->bytesleft>0) {
			return;
		}
		stats_psent++;
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}

void matoclserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	matoclserventry *eptr;

	pdesc[pos].fd = lsock;
		pdesc[pos].events = POLLIN;
		lsockpdescpos = pos;
		pos++;
//		FD_SET(lsock,rset);
//		max = lsock;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;
//		i=eptr->sock;
		pdesc[pos].events |= POLLIN;
//			FD_SET(i,rset);
//			if (i>max) {
//				max=i;
//			}
		if (eptr->outputhead!=NULL) {
			pdesc[pos].events |= POLLOUT;
//			FD_SET(i,wset);
//			if (i>max) {
//				max=i;
//			}
		}
		pos++;
	}
	*ndesc = pos;
//	return max;
}

void matoclserv_disconnection_loop(void) {
	matoclserventry *eptr,**kptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	kptr = &matoclservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matocl_beforedisconnect(eptr);
			tcpclose(eptr->sock);
			if (eptr->input_packet) {
				free(eptr->input_packet);
			}
			ipptr = eptr->inputhead;
			while (ipptr) {
				ipaptr = ipptr;
				ipptr = ipptr->next;
				free(ipaptr);
			}
			opptr = eptr->outputhead;
			while (opptr) {
				opaptr = opptr;
				opptr = opptr->next;
				free(opaptr);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matoclserv_serve(struct pollfd *pdesc) {
	double now;
	matoclserventry *eptr;
	int ns;
	static double lastaction = 0.0;
	double timeoutadd;

	now = monotonic_seconds();
// timeout fix
	if (lastaction>0.0) {
		timeoutadd = now-lastaction;
		if (timeoutadd>1.0) {
			for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
				eptr->lastread += timeoutadd;
			}
		}
	}
	lastaction = now;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
//	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"main master server module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matoclserventry));
			passert(eptr);
			eptr->next = matoclservhead;
			matoclservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->registered = 0;
/* CACHENOTIFY
			eptr->notifications = 0;
*/
			eptr->version = 0;
			eptr->mode = DATA;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
			eptr->input_end = 0;
			eptr->input_packet = NULL;
			eptr->inputhead = NULL;
			eptr->inputtail = &(eptr->inputhead);
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			eptr->chunkdelayedops = NULL;
			eptr->sesdata = NULL;
/* CACHENOTIFY
			eptr->cacheddirs = NULL;
*/
			memset(eptr->passwordrnd,0,32);
//			eptr->openedfiles = NULL;
		}
	}

// read
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((pdesc[eptr->pdescpos].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode!=KILL) {
				matoclserv_read(eptr,now);
			}
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->input_end = 1;
			}
		}
		matoclserv_parse(eptr);
	}

// write
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->lastwrite+1.0<now && eptr->registered<100 && eptr->outputhead==NULL) {
			uint8_t *ptr = matoclserv_createpacket(eptr,ANTOAN_NOP,4);	// 4 byte length because of 'msgid'
			*((uint32_t*)ptr) = 0;
		}
		if (eptr->pdescpos>=0) {
/* CACHENOTIFY
			if (eptr->notifications) {
				if (eptr->version>=VERSION2INT(1,6,22)) {
					uint8_t *ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_NOTIFY_END,4);	// transaction end
					*((uint32_t*)ptr) = 0;
				}
				eptr->notifications = 0;
			}
*/
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				matoclserv_write(eptr,now);
			}
		}
		if (eptr->lastread+10.0<now) {
			eptr->mode = KILL;
		}
		if (eptr->mode==FINISH && eptr->outputhead==NULL) {
			eptr->mode = KILL;
		}
	}

	matoclserv_disconnection_loop();
}

void matoclserv_keep_alive(void) {
	double now;
	matoclserventry *eptr;

	now = monotonic_seconds();
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode == DATA && eptr->input_end==0) {
			matoclserv_read(eptr,now);
		}
	}
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->lastwrite+1.0<now && eptr->registered<100 && eptr->outputhead==NULL) {
			uint8_t *ptr = matoclserv_createpacket(eptr,ANTOAN_NOP,4);	// 4 byte length because of 'msgid'
			*((uint32_t*)ptr) = 0;
		}
		if (eptr->mode == DATA && eptr->outputhead) {
			matoclserv_write(eptr,now);
		}
	}
}

void matoclserv_term(void) {
	matoclserventry *eptr,*eaptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;
	chunklist *cl,*cln;
//	session *ss,*ssn;
//	filelist *of,*ofn;

	syslog(LOG_NOTICE,"main master server module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matoclservhead;
	while (eptr) {
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
		for (cl = eptr->chunkdelayedops ; cl ; cl = cln) {
			cln = cl->next;
			free(cl);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matoclservhead=NULL;

	matoclserv_read(NULL,0.0); // free internal read buffer
	matoclserv_gid_storage(0); // free supplementary groups buffer

	free(ListenHost);
	free(ListenPort);
}

int matoclserv_no_more_pending_jobs(void) {
	matoclserventry *eptr;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
		if (eptr->chunkdelayedops!=NULL) {
			return 0;
		}
	}
	return 1;
}

void matoclserv_disconnect_all(void) {
	matoclserventry *eptr;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		eptr->mode = KILL;
	}
	matoclserv_disconnection_loop();
}

void matoclserv_start_cond_check(void) {
	if (starting) {
// very simple condition checking if all chunkservers have been connected
// in the future master will know his chunkservers list and then this condition will be changed
		if (chunk_get_missing_count()<100) {
			starting=0;
		} else {
			starting--;
		}
	}
}

/*
int matoclserv_sessionsinit(void) {
	fprintf(stderr,"loading sessions ... ");
	fflush(stderr);
	sessionshead = NULL;
	switch (matoclserv_load_sessions()) {
		case 0:	// no file
			fprintf(stderr,"file not found\n");
			fprintf(stderr,"if it is not fresh installation then you have to restart all active mounts !!!\n");
			matoclserv_store_sessions();
			break;
		case 1: // file loaded
			fprintf(stderr,"ok\n");
			fprintf(stderr,"sessions file has been loaded\n");
			break;
		default:
			fprintf(stderr,"error\n");
			fprintf(stderr,"due to missing sessions you have to restart all active mounts !!!\n");
			break;
	}
	SessionSustainTime = cfg_getuint32("SESSION_SUSTAIN_TIME",86400);
	if (SessionSustainTime>7*86400) {
		SessionSustainTime=7*86400;
		mfs_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too big (more than week) - setting this value to one week");
	}
	if (SessionSustainTime<60) {
		SessionSustainTime=60;
		mfs_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too low (less than minute) - setting this value to one minute");
	}
	return 0;
}
*/

void matoclserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;
/*
	SessionSustainTime = cfg_getuint32("SESSION_SUSTAIN_TIME",86400);
	if (SessionSustainTime>7*86400) {
		SessionSustainTime=7*86400;
		mfs_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too big (more than week) - setting this value to one week");
	}
	if (SessionSustainTime<60) {
		SessionSustainTime=60;
		mfs_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too low (less than minute) - setting this value to one minute");
	}
*/
	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_HOST"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT);
	} else {
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*"); // deprecated option
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT); // deprecated option
	}
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,"main master server module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"main master server module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"main master server module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"main master server module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int matoclserv_init(void) {
	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_HOST"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT);
	} else {
		fprintf(stderr,"change MATOCU_LISTEN_* option names to MATOCL_LISTEN_* !!!\n");
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*"); // deprecated option
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT); // deprecated option
	}

	starting = 12;
	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"main master server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"main master server module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"main master server module: listen on %s:%s",ListenHost,ListenPort);

	matoclservhead = NULL;
/* CACHENOTIFY
	matoclserv_dircache_init();
*/

	main_time_register(10,0,matoclserv_start_cond_check);
//	main_time_register(10,0,matocl_session_check);
//	main_time_register(3600,0,matocl_session_statsmove);
	main_reload_register(matoclserv_reload);
	main_destruct_register(matoclserv_term);
	main_poll_register(matoclserv_desc,matoclserv_serve);
	main_keepalive_register(matoclserv_keep_alive);
//	main_wantexit_register(matoclserv_wantexit);
//	main_canexit_register(matoclserv_canexit);
	return 0;
}
