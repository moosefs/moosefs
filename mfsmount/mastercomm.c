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

#define MMAP_ALLOC 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/poll.h>
#include <syslog.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <pthread.h>
#ifdef MMAP_ALLOC
#include <sys/types.h>
#include <sys/mman.h>
#endif
#include <pwd.h>
#include <grp.h>

#include "MFSCommunication.h"
#include "stats.h"
#include "sockets.h"
#include "strerr.h"
#include "md5.h"
#include "datapack.h"
// #include "dircache.h"

#define CONNECT_TIMEOUT 2000

typedef struct _threc {
	pthread_t thid;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *obuff;
	uint32_t obuffsize;
	uint32_t odataleng;
	uint8_t *ibuff;
	uint32_t ibuffsize;
	uint32_t idataleng;

	uint8_t sent;		// packet was sent
	uint8_t status;		// receive status
	uint8_t rcvd;		// packet was received
	uint8_t waiting;	// thread is waiting for answer

	uint32_t rcvd_cmd;

	uint32_t packetid;	// thread number
	struct _threc *next;
} threc;

/*
typedef struct _threc {
	pthread_t thid;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *buff;
	uint32_t buffsize;
	uint8_t sent;
	uint8_t status;
	uint8_t release;	// cond variable
	uint8_t waiting;
	uint32_t size;
	uint32_t cmd;
	uint32_t packetid;
	struct _threc *next;
} threc;
*/


typedef struct _acquired_file {
	uint32_t inode;
	uint32_t cnt;
	struct _acquired_file *next;
} acquired_file;


#define DEFAULT_OUTPUT_BUFFSIZE 0x1000
#define DEFAULT_INPUT_BUFFSIZE 0x10000

#define RECEIVE_TIMEOUT 10

static threc *threchead=NULL;

static acquired_file *afhead=NULL;

static int fd;
static int disconnect;
static int donotsendsustainedinodes;
static time_t lastwrite;
static int sessionlost;

static uint32_t maxretries;

static pthread_t rpthid,npthid;
static pthread_mutex_t fdlock,reclock,aflock;

static uint32_t sessionid;
static uint32_t masterversion;

static char masterstrip[17];
static uint32_t masterip=0;
static uint16_t masterport=0;
static char srcstrip[17];
static uint32_t srcip=0;

static uint8_t fterm;

void fs_getmasterlocation(uint8_t loc[14]) {
	put32bit(&loc,masterip);
	put16bit(&loc,masterport);
	put32bit(&loc,sessionid);
	put32bit(&loc,masterversion);
}

uint32_t master_version(void) {
	return masterversion;
}

uint32_t fs_getsrcip() {
	return srcip;
}

enum {
	MASTER_CONNECTS = 0,
	MASTER_BYTESSENT,
	MASTER_BYTESRCVD,
	MASTER_PACKETSSENT,
	MASTER_PACKETSRCVD,
	STATNODES
};

static void *statsptr[STATNODES];

struct connect_args_t {
	char *bindhostname;
	char *masterhostname;
	char *masterportname;
	uint8_t meta;
	uint8_t clearpassword;
	char *info;
	char *subfolder;
	uint8_t *passworddigest;
};

static struct connect_args_t connect_args;

void master_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"master",0,0);
	statsptr[MASTER_PACKETSRCVD] = stats_get_subnode(s,"packets_received",0,1);
	statsptr[MASTER_PACKETSSENT] = stats_get_subnode(s,"packets_sent",0,1);
	statsptr[MASTER_BYTESRCVD] = stats_get_subnode(s,"bytes_received",0,1);
	statsptr[MASTER_BYTESSENT] = stats_get_subnode(s,"bytes_sent",0,1);
	statsptr[MASTER_CONNECTS] = stats_get_subnode(s,"reconnects",0,1);
}

void master_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_counter_inc(statsptr[id]);
	}
}

void master_stats_add(uint8_t id,uint64_t s) {
	if (id<STATNODES) {
		stats_counter_add(statsptr[id],s);
	}
}

const char* errtab[]={ERROR_STRINGS};

static inline const char* mfs_strerror(uint8_t status) {
	if (status>ERROR_MAX) {
		status=ERROR_MAX;
	}
	return errtab[status];
}
/*
void fs_lock_acnt(void) {
	pthread_mutex_lock(&aflock);
}

void fs_unlock_acnt(void) {
	pthread_mutex_unlock(&aflock);
}

uint32_t fs_get_acnt(uint32_t inode) {
	acquired_file *afptr;
	for (afptr=afhead ; afptr ; afptr=afptr->next) {
		if (afptr->inode==inode) {
			return (afptr->cnt);
		}
	}
	return 0;
}
*/


// attributes of inode have changed
// #define MATOCL_FUSE_NOTIFY_ATTR 491
// // msgid:32 N*[ inode:32 attr:35B ]
//
// // new entry has been added
// #define MATOCL_FUSE_NOTIFY_LINK 492
// // msgid:32 N*[ parent:32 name:NAME inode:32 attr:35B ]
//
// // entry has been deleted
// #define MATOCL_FUSE_NOTIFY_UNLINK 493
// // msgid:32 N*[ parent:32 name:NAME ]
//
// // whole directory needs to be removed
// #define MATOCL_FUSE_NOTIFY_REMOVE 494
// // msgid:32 N*[ inode:32 ]
//

/*
void fs_notify_attr(const uint8_t *buff,uint32_t size) {
	uint32_t inode;
	while (size>=39) {
		inode = get32bit(&buff);
		dir_cache_attr(inode,buff);
		buff += 35;
		size -= 39;
	}
}

void fs_notify_link(const uint8_t *buff,uint32_t size) {
	uint32_t parent,inode;
	uint32_t ts;
	const uint8_t *name;
	uint8_t nleng;
	ts = get32bit(&buff);
	size-=4;
	while (size>=44) {
		parent = get32bit(&buff);
		nleng = get8bit(&buff);
		if (size<44U+nleng) {
			return;
		}
		name = buff;
		buff += nleng;
		inode = get32bit(&buff);
		dir_cache_link(parent,nleng,name,inode,buff,ts);
		buff += 35;
		size -= 44U+nleng;
	}
}

void fs_notify_unlink(const uint8_t *buff,uint32_t size) {
	uint32_t ts;
	uint32_t parent;
	uint8_t nleng;
	ts = get32bit(&buff);
	size-=4;
	while (size>=5) {
		parent = get32bit(&buff);
		nleng = get8bit(&buff);
		if (size<5U+nleng) {
			return;
		}
		dir_cache_unlink(parent,nleng,buff,ts);
		buff += nleng;
		size -= 5U+nleng;
	}
}

void fs_notify_remove(const uint8_t *buff,uint32_t size) {
	uint32_t inode;
	while (size>=4) {
		inode = get32bit(&buff);
		dir_cache_remove(inode);
		size -= 4;
	}
}

void fs_notify_parent(const uint8_t *buff,uint32_t size) {
	uint32_t inode,parent;
	while (size>=8) {
		inode = get32bit(&buff);
		parent = get32bit(&buff);
		dir_cache_parent(inode,parent);
		size -= 8;
	}
}

void fs_notify_sendremoved(uint32_t cnt,uint32_t *inodes) {
	static uint8_t *notify_buff=NULL;
	static uint32_t notify_buff_size=0;
	uint32_t size;
	uint8_t *ptr;

	if (cnt==0) {
		return;
	}

	size = 12+4*cnt;

	if (size>DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (notify_buff) {
			munmap(notify_buff,notify_buff_size);
		}
		notify_buff = mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (notify_buff) {
			free(notify_buff);
		}
		notify_buff = malloc(size);
#endif
		notify_buff_size = size;
	} else if (notify_buff_size!=DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (notify_buff) {
			munmap(notify_buff,notify_buff_size);
		}
		notify_buff = mmap(NULL,DEFAULT_OUTPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (notify_buff) {
			free(notify_buff);
		}
		notify_buff = malloc(DEFAULT_OUTPUT_BUFFSIZE);
#endif
		notify_buff_size = DEFAULT_OUTPUT_BUFFSIZE;
	}
	if (notify_buff==NULL) {
		notify_buff_size = 0;
		disconnect=1;
		return;
	}

	ptr = notify_buff;

	put32bit(&ptr,CLTOMA_FUSE_DIR_REMOVED);
	put32bit(&ptr,size-8);
	put32bit(&ptr,0);

	while (cnt) {
		put32bit(&ptr,*inodes);
		inodes++;
		cnt--;
	}

	pthread_mutex_lock(&fdlock);
	if (sessionlost || fd==-1 || masterversion<VERSION2INT(1,6,21)) {
		pthread_mutex_unlock(&fdlock);
		return;
	}
	if (tcptowrite(fd,notify_buff,size,1000)!=(int32_t)size) {
		syslog(LOG_WARNING,"tcp send error: %s",strerr(errno));
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		return;
	}
	master_stats_add(MASTER_BYTESSENT,size);
	master_stats_inc(MASTER_PACKETSSENT);
	lastwrite = time(NULL);
	pthread_mutex_unlock(&fdlock);
}
*/
void fs_inc_acnt(uint32_t inode) {
	acquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode==inode) {
			afptr->cnt++;
			pthread_mutex_unlock(&aflock);
			return;
		}
		if (afptr->inode>inode) {
			break;
		}
		afpptr = &(afptr->next);
	}
	afptr = (acquired_file*)malloc(sizeof(acquired_file));
	afptr->inode = inode;
	afptr->cnt = 1;
	afptr->next = *afpptr;
	*afpptr = afptr;
	pthread_mutex_unlock(&aflock);
}

void fs_dec_acnt(uint32_t inode) {
	acquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode == inode) {
			if (afptr->cnt<=1) {
				*afpptr = afptr->next;
				free(afptr);
			} else {
				afptr->cnt--;
			}
			pthread_mutex_unlock(&aflock);
			return;
		}
		afpptr = &(afptr->next);
	}
	pthread_mutex_unlock(&aflock);
}

threc* fs_get_my_threc() {
	pthread_t mythid = pthread_self();
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (pthread_equal(rec->thid,mythid)) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	rec = malloc(sizeof(threc));
	rec->thid = mythid;
/*
#ifdef MMAP_ALLOC
	rec->obuff = mmap(NULL,DEFAULT_OUTPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
	rec->ibuff = mmap(NULL,DEFAULT_INPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
	rec->obuff = malloc(DEFAULT_OUTPUT_BUFFSIZE);
	rec->ibuff = malloc(DEFAULT_INPUT_BUFFSIZE);
#endif
	if (rec->obuff==NULL || rec->ibuff==NULL) {
#ifdef MMAP_ALLOC
		if (rec->obuff) {
			munmap(rec->obuff,DEFAULT_OUTPUT_BUFFSIZE);
		}
		if (rec->ibuff) {
			munmap(rec->ibuff,DEFAULT_INPUT_BUFFSIZE);
		}
#else
		if (rec->obuff) {
			free(rec->obuff);
		}
		if (rec->ibuff) {
			free(rec->ibuff);
		}
#endif
		free(rec);
		pthread_mutex_unlock(&reclock);
		return NULL;
	}
*/
	pthread_mutex_init(&(rec->mutex),NULL);
	pthread_cond_init(&(rec->cond),NULL);
	rec->obuff = NULL;
	rec->ibuff = NULL;
	rec->obuffsize = 0;//DEFAULT_OUTPUT_BUFFSIZE;
	rec->ibuffsize = 0;//DEFAULT_INPUT_BUFFSIZE;
	rec->odataleng = 0;
	rec->idataleng = 0;
	rec->sent = 0;
	rec->status = 0;
	rec->rcvd = 0;
	rec->waiting = 0;
	rec->rcvd_cmd = 0;
	if (threchead==NULL) {
		rec->packetid = 1;
	} else {
		rec->packetid = threchead->packetid+1;
	}
	rec->next = threchead;
	//syslog(LOG_NOTICE,"mastercomm: create new threc (%"PRIu32")",rec->packetid);
	threchead = rec;
	pthread_mutex_unlock(&reclock);
	return rec;
}

threc* fs_get_threc_by_id(uint32_t packetid) {
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (rec->packetid==packetid) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	pthread_mutex_unlock(&reclock);
	return NULL;
}

void fs_output_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->obuff) {
			munmap((void*)(rec->obuff),rec->obuffsize);
		}
		rec->obuff = (void*)mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(size);
#endif
		rec->obuffsize = size;
	} else if (rec->obuffsize!=DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->obuff) {
			munmap((void*)(rec->obuff),rec->obuffsize);
		}
		rec->obuff = (void*)mmap(NULL,DEFAULT_OUTPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(DEFAULT_OUTPUT_BUFFSIZE);
#endif
		rec->obuffsize = DEFAULT_OUTPUT_BUFFSIZE;
	}
	if (rec->obuff==NULL) {
		rec->obuffsize = 0;
	}
}

void fs_input_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_INPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->ibuff) {
			munmap((void*)(rec->ibuff),rec->ibuffsize);
		}
		rec->ibuff = (void*)mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->ibuff) {
			free(rec->ibuff);
		}
		rec->ibuff = malloc(size);
#endif
		rec->ibuffsize = size;
	} else if (rec->ibuffsize!=DEFAULT_INPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->ibuff) {
			munmap((void*)(rec->ibuff),rec->ibuffsize);
		}
		rec->ibuff = (void*)mmap(NULL,DEFAULT_INPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->ibuff) {
			free(rec->ibuff);
		}
		rec->ibuff = malloc(DEFAULT_INPUT_BUFFSIZE);
#endif
		rec->ibuffsize = DEFAULT_INPUT_BUFFSIZE;
	}
	if (rec->ibuff==NULL) {
		rec->ibuffsize = 0;
	}
}

uint8_t* fs_createpacket(threc *rec,uint32_t cmd,uint32_t size) {
	uint8_t *ptr;
	uint32_t hdrsize = size+4;
	pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
	fs_output_buffer_init(rec,size+12);
	if (rec->obuff==NULL) {
		return NULL;
	}
	ptr = rec->obuff;
	put32bit(&ptr,cmd);
	put32bit(&ptr,hdrsize);
	put32bit(&ptr,rec->packetid);
	rec->odataleng = size+12;
	pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
	return ptr;
}

const uint8_t* fs_sendandreceive(threc *rec,uint32_t expected_cmd,uint32_t *answer_leng) {
	uint32_t cnt;
	static uint8_t notsup = ERROR_ENOTSUP;
//	uint32_t size = rec->size;

	for (cnt=0 ; cnt<maxretries ; cnt++) {
		pthread_mutex_lock(&fdlock);
		if (sessionlost) {
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - sending ...",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000)!=(int32_t)(rec->odataleng)) {
			syslog(LOG_WARNING,"tcp send error: %s",strerr(errno));
			disconnect = 1;
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_unlock(&fdlock);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = time(NULL);
		pthread_mutex_unlock(&fdlock);
		// syslog(LOG_NOTICE,"master: lock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->rcvd==0) {
			rec->waiting = 1;
			pthread_cond_wait(&(rec->cond),&(rec->mutex));
			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// syslog(LOG_NOTICE,"master: unlocked: %"PRIu32,rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %"PRIu32" ; reccmd: %"PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			pthread_mutex_unlock(&(rec->mutex));
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		if (rec->rcvd_cmd==ANTOAN_UNKNOWN_COMMAND || rec->rcvd_cmd==ANTOAN_BAD_COMMAND_SIZE) {
			pthread_mutex_unlock(&(rec->mutex));
			*answer_leng = 1; // simulate error
			return &notsup; // return ERROR_ENOTSUP in this case
		}
		if (rec->rcvd_cmd!=expected_cmd) {
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		pthread_mutex_unlock(&(rec->mutex));
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - received",rec->packetid);
		return rec->ibuff;
	}
	return NULL;
}

const uint8_t* fs_sendandreceive_any(threc *rec,uint32_t *received_cmd,uint32_t *answer_leng) {
	uint32_t cnt;
//	uint32_t size = rec->size;

	for (cnt=0 ; cnt<maxretries ; cnt++) {
		pthread_mutex_lock(&fdlock);
		if (sessionlost) {
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - sending ...",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000)!=(int32_t)(rec->odataleng)) {
			syslog(LOG_WARNING,"tcp send error: %s",strerr(errno));
			disconnect = 1;
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_unlock(&fdlock);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = time(NULL);
		pthread_mutex_unlock(&fdlock);
		// syslog(LOG_NOTICE,"master: lock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->rcvd==0) {
			rec->waiting = 1;
			pthread_cond_wait(&(rec->cond),&(rec->mutex));
			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// syslog(LOG_NOTICE,"master: unlocked: %"PRIu32,rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %"PRIu32" ; reccmd: %"PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			pthread_mutex_unlock(&(rec->mutex));
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		*received_cmd = rec->rcvd_cmd;
		pthread_mutex_unlock(&(rec->mutex));
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - received",rec->packetid);
		return rec->ibuff;
	}
	return NULL;
}

//static inline const uint8_t* fs_sendandreceive(threc *rec,uint32_t expected_cmd,uint32_t *answer_leng) {
//	uint32_t *rcmd;
//	const uint8_t *rptr;
//	rptr = fs_commwithmaster(rec,&rcmd,answer_leng);
//	if (
//}

/*
int fs_direct_connect() {
	int rfd;
	rfd = tcpsocket();
	if (tcpnumconnect(rfd,masterip,masterport)<0) {
		tcpclose(rfd);
		return -1;
	}
	master_stats_inc(MASTER_TCONNECTS);
	return rfd;
}

void fs_direct_close(int rfd) {
	tcpclose(rfd);
}

int fs_direct_write(int rfd,const uint8_t *buff,uint32_t size) {
	int rsize = tcptowrite(rfd,buff,size,60000);
	if (rsize==(int)size) {
		master_stats_add(MASTER_BYTESSENT,size);
	}
	return rsize;
}

int fs_direct_read(int rfd,uint8_t *buff,uint32_t size) {
	int rsize = tcptoread(rfd,buff,size,60000);
	if (rsize>0) {
		master_stats_add(MASTER_BYTESRCVD,rsize);
	}
	return rsize;
}
*/

int fs_resolve(uint8_t oninit,const char *bindhostname,const char *masterhostname,const char *masterportname) {
	if (bindhostname) {
		if (tcpresolve(bindhostname,NULL,&srcip,NULL,1)<0) {
			if (oninit) {
				fprintf(stderr,"can't resolve source hostname (%s)\n",bindhostname);
			} else {
				syslog(LOG_WARNING,"can't resolve source hostname (%s)",bindhostname);
			}
			return -1;
		}
	} else {
		srcip=0;
	}
	snprintf(srcstrip,17,"%"PRIu32".%"PRIu32".%"PRIu32".%"PRIu32,(srcip>>24)&0xFF,(srcip>>16)&0xFF,(srcip>>8)&0xFF,srcip&0xFF);
	srcstrip[16]=0;

	if (tcpresolve(masterhostname,masterportname,&masterip,&masterport,0)<0) {
		if (oninit) {
			fprintf(stderr,"can't resolve master hostname and/or portname (%s:%s)\n",masterhostname,masterportname);
		} else {
			syslog(LOG_WARNING,"can't resolve master hostname and/or portname (%s:%s)",masterhostname,masterportname);
		}
		return -1;
	}
	snprintf(masterstrip,17,"%"PRIu32".%"PRIu32".%"PRIu32".%"PRIu32,(masterip>>24)&0xFF,(masterip>>16)&0xFF,(masterip>>8)&0xFF,masterip&0xFF);
	masterstrip[16]=0;

	return 0;
}

// int fs_connect(uint8_t oninit,const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t *sesflags,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid,uint8_t *mingoal,uint8_t *maxgoal,uint32_t *mintrashtime,uint32_t *maxtrashtime) {
int fs_connect(uint8_t oninit,struct connect_args_t *cargs) {
	uint32_t i,j;
	uint8_t *wptr,*regbuff;
	md5ctx ctx;
	uint8_t digest[16];
	const uint8_t *rptr;
	uint32_t newmasterip;
	uint8_t havepassword;
	uint32_t pleng,ileng;
	uint8_t sesflags;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint8_t mingoal,maxgoal;
	uint32_t mintrashtime,maxtrashtime;
	const char *sesflagposstrtab[]={SESFLAG_POS_STRINGS};
	const char *sesflagnegstrtab[]={SESFLAG_NEG_STRINGS};
	struct passwd pwd,*pw;
	struct group grp,*gr;
	char pwdgrpbuff[16384];
	static uint32_t trycnt=0;

	if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
		return -1;
	}

	havepassword=(cargs->passworddigest==NULL)?0:1;
	ileng=strlen(cargs->info)+1;
	if (cargs->meta) {
		pleng=0;
		regbuff = malloc(8+64+9+ileng+16);
	} else {
		pleng=strlen(cargs->subfolder)+1;
		regbuff = malloc(8+64+13+pleng+ileng+16);
	}

	do {
		fd = tcpsocket();
		if (fd<0) {
			free(regbuff);
			return -1;
		}
		if (tcpnodelay(fd)<0) {
			if (oninit) {
				fprintf(stderr,"can't set TCP_NODELAY\n");
			} else {
				syslog(LOG_WARNING,"can't set TCP_NODELAY");
			}
		}
		if (srcip>0) {
			if (tcpnumbind(fd,srcip,0)<0) {
				if (oninit) {
					fprintf(stderr,"can't bind socket to given ip (\"%s\")\n",srcstrip);
				} else {
					syslog(LOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
		}
		if (tcpnumtoconnect(fd,masterip,masterport,CONNECT_TIMEOUT)<0) {
			tcpclose(fd);
			fd=-1;
			if (oninit) {
				if (trycnt<10) {
					trycnt++;
					if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
						free(regbuff);
						return -1;
					}
					i=4;
					continue;
				} else {
					fprintf(stderr,"can't connect to mfsmaster (\"%s\":\"%"PRIu16"\")\n",masterstrip,masterport);
					free(regbuff);
					return -1;
				}
			} else {
				syslog(LOG_WARNING,"can't connect to mfsmaster (\"%s\":\"%"PRIu16"\")",masterstrip,masterport);
				free(regbuff);
				return -1;
			}
		}
		if (havepassword) {
			wptr = regbuff;
			put32bit(&wptr,CLTOMA_FUSE_REGISTER);
			put32bit(&wptr,65);
			memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
			wptr+=64;
			put8bit(&wptr,REGISTER_GETRANDOM);
			if (tcptowrite(fd,regbuff,8+65,1000)!=8+65) {
				if (oninit) {
					fprintf(stderr,"error sending data to mfsmaster\n");
				} else {
					syslog(LOG_WARNING,"error sending data to mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			if (tcptoread(fd,regbuff,8,1000)!=8) {
				if (oninit) {
					fprintf(stderr,"error receiving data from mfsmaster\n");
				} else {
					syslog(LOG_WARNING,"error receiving data from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			rptr = regbuff;
			i = get32bit(&rptr);
			if (i!=MATOCL_FUSE_REGISTER) {
				if (oninit) {
					fprintf(stderr,"got incorrect answer from mfsmaster\n");
				} else {
					syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			i = get32bit(&rptr);
			if (i!=32) {
				if (oninit) {
					fprintf(stderr,"got incorrect answer from mfsmaster\n");
				} else {
					syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			if (tcptoread(fd,regbuff,32,1000)!=32) {
				if (oninit) {
					fprintf(stderr,"error receiving data from mfsmaster\n");
				} else {
					syslog(LOG_WARNING,"error receiving data from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			md5_init(&ctx);
			md5_update(&ctx,regbuff,16);
			md5_update(&ctx,cargs->passworddigest,16);
			md5_update(&ctx,regbuff+16,16);
			md5_final(digest,&ctx);
		}
		wptr = regbuff;
		put32bit(&wptr,CLTOMA_FUSE_REGISTER);
		if (cargs->meta) {
			if (havepassword) {
				put32bit(&wptr,64+9+ileng+16);
			} else {
				put32bit(&wptr,64+9+ileng);
			}
		} else {
			if (havepassword) {
				put32bit(&wptr,64+13+ileng+pleng+16);
			} else {
				put32bit(&wptr,64+13+ileng+pleng);
			}
		}
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,(cargs->meta)?REGISTER_NEWMETASESSION:REGISTER_NEWSESSION);
		put16bit(&wptr,VERSMAJ);
		put8bit(&wptr,VERSMID);
		put8bit(&wptr,VERSMIN);
		put32bit(&wptr,ileng);
		memcpy(wptr,cargs->info,ileng);
		wptr+=ileng;
		if (!cargs->meta) {
			put32bit(&wptr,pleng);
			memcpy(wptr,cargs->subfolder,pleng);
		}
		if (havepassword) {
			memcpy(wptr+pleng,digest,16);
		}
		if (tcptowrite(fd,regbuff,8+64+(cargs->meta?9:13)+ileng+pleng+(havepassword?16:0),1000)!=(int32_t)(8+64+(cargs->meta?9:13)+ileng+pleng+(havepassword?16:0))) {
			if (oninit) {
				fprintf(stderr,"error sending data to mfsmaster: %s\n",strerr(errno));
			} else {
				syslog(LOG_WARNING,"error sending data to mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,8,1000)!=8) {
			if (oninit) {
				fprintf(stderr,"error receiving data from mfsmaster: %s\n",strerr(errno));
			} else {
				syslog(LOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i!=MATOCL_FUSE_REGISTER) {
			if (oninit) {
				fprintf(stderr,"got incorrect answer from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		i = get32bit(&rptr);
		if (!(i==1 || i==4 || (cargs->meta && (i==5 || i==9 || i==19)) || (cargs->meta==0 && (i==13 || i==21 || i==25 || i==35)))) {
			if (oninit) {
				fprintf(stderr,"got incorrect answer from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,i,1000)!=(int32_t)i) {
			if (oninit) {
				fprintf(stderr,"error receiving data from mfsmaster: %s\n",strerr(errno));
			} else {
				syslog(LOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		if (i==1) {
			if (oninit) {
				fprintf(stderr,"mfsmaster register error: %s\n",mfs_strerror(rptr[0]));
			} else {
				syslog(LOG_WARNING,"mfsmaster register error: %s",mfs_strerror(rptr[0]));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (i==4) {
			// redirect
			newmasterip = get32bit(&rptr);
			if (newmasterip==0 || newmasterip==masterip) {
				if (trycnt<10) {
					trycnt++;
					if (oninit) {
						fprintf(stderr,"mfsmaster %s - this is ELECT waitng for being LEADER, waiting a moment and retrying\n",masterstrip);
					} else {
						syslog(LOG_WARNING,"mfsmaster %s - this is ELECT waitng for being LEADER, waiting a moment and retrying",masterstrip);
					}
					tcpclose(fd);
					fd = -1;
					if (oninit) {
						sleep(2);
					} else {
						free(regbuff);
						return -1;
					}
				} else {
					trycnt=0;
					if (oninit) {
						fprintf(stderr,"mfsmaster %s - this is ELECT waitng for being LEADER, waiting a moment and retrying using different IP\n",masterstrip);
					} else {
						syslog(LOG_WARNING,"mfsmaster %s - this is ELECT waitng for being LEADER, waiting a moment and retrying using different IP",masterstrip);
					}
					if (oninit) {
						if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
							free(regbuff);
							return -1;
						}
						sleep(2);
					} else {
						free(regbuff);
						return -1;
					}
				}
			} else {
				char newmasterstrip[17];
				snprintf(newmasterstrip,17,"%"PRIu32".%"PRIu32".%"PRIu32".%"PRIu32,(newmasterip>>24)&0xFF,(newmasterip>>16)&0xFF,(newmasterip>>8)&0xFF,newmasterip&0xFF);
				newmasterstrip[16]=0;
				if (oninit) {
					fprintf(stderr,"mfsmaster %s - found leader: %s\n",masterstrip,newmasterstrip);
				} else {
					syslog(LOG_WARNING,"mfsmaster %s - found leader: %s",masterstrip,newmasterstrip);
				}
				masterip = newmasterip;
				strcpy(masterstrip,newmasterstrip);
				tcpclose(fd);
				fd = -1;
			}
		}
	} while (i==4);
	if (i==9 || i==19 || i==25 || i==35) {
		masterversion = get32bit(&rptr);
//		dir_cache_master_switch((masterversion<VERSION2INT(1,6,21))?0:(masterversion<VERSION2INT(1,6,22))?1:2);
	} else {
		masterversion = 0;
//		dir_cache_master_switch(0);
	}
	sessionid = get32bit(&rptr);
	sesflags = get8bit(&rptr);
	if (!cargs->meta) {
		rootuid = get32bit(&rptr);
		rootgid = get32bit(&rptr);
		if (i>=21) {
			mapalluid = get32bit(&rptr);
			mapallgid = get32bit(&rptr);
		} else {
			mapalluid = 0;
			mapallgid = 0;
		}
	} else {
		rootuid = 0;
		rootgid = 0;
		mapalluid = 0;
		mapallgid = 0;
	}
	if (i==19 || i==35) {
		mingoal = get8bit(&rptr);
		maxgoal = get8bit(&rptr);
		mintrashtime = get32bit(&rptr);
		maxtrashtime = get32bit(&rptr);
	} else {
		mingoal = 0;
		maxgoal = 0;
		mintrashtime = 0;
		maxtrashtime = 0;
	}
	free(regbuff);
	lastwrite=time(NULL);
	if (oninit==0) {
		syslog(LOG_NOTICE,"registered to master with new session");
	}
	if (cargs->clearpassword && cargs->passworddigest!=NULL) {
		memset(cargs->passworddigest,0,16);
		free(cargs->passworddigest);
		cargs->passworddigest = NULL;
	}
	if (oninit==1) {
		fprintf(stderr,"mfsmaster accepted connection with parameters: ");
		j=0;
		for (i=0 ; i<8 ; i++) {
			if (sesflags&(1<<i)) {
				fprintf(stderr,"%s%s",j?",":"",sesflagposstrtab[i]);
				j=1;
			} else if (sesflagnegstrtab[i]) {
				fprintf(stderr,"%s%s",j?",":"",sesflagnegstrtab[i]);
				j=1;
			}
		}
		if (j==0) {
			fprintf(stderr,"-");
		}
		if (!cargs->meta) {
			fprintf(stderr," ; root mapped to ");
			getpwuid_r(rootuid,&pwd,pwdgrpbuff,16384,&pw);
	//		pw = getpwuid(rootuid);
			if (pw) {
				fprintf(stderr,"%s:",pw->pw_name);
			} else {
				fprintf(stderr,"%"PRIu32":",rootuid);
			}
			getgrgid_r(rootgid,&grp,pwdgrpbuff,16384,&gr);
	//		gr = getgrgid(rootgid);
			if (gr) {
				fprintf(stderr,"%s",gr->gr_name);
			} else {
				fprintf(stderr,"%"PRIu32,rootgid);
			}
			if (sesflags&SESFLAG_MAPALL) {
				fprintf(stderr," ; users mapped to ");
				pw = getpwuid(mapalluid);
				if (pw) {
					fprintf(stderr,"%s:",pw->pw_name);
				} else {
					fprintf(stderr,"%"PRIu32":",mapalluid);
				}
				gr = getgrgid(mapallgid);
				if (gr) {
					fprintf(stderr,"%s",gr->gr_name);
				} else {
					fprintf(stderr,"%"PRIu32,mapallgid);
				}
			}
		}
		if (mingoal>0 && maxgoal>0) {
			if (mingoal>1 || maxgoal<9) {
				fprintf(stderr," ; setgoal limited to (%u:%u)",mingoal,maxgoal);
			}
			if (mintrashtime>0 || maxtrashtime<UINT32_C(0xFFFFFFFF)) {
				fprintf(stderr," ; settrashtime limited to (");
				if (mintrashtime>0) {
					if (mintrashtime>604800) {
						fprintf(stderr,"%uw",mintrashtime/604800);
						mintrashtime %= 604800;
					}
					if (mintrashtime>86400) {
						fprintf(stderr,"%ud",mintrashtime/86400);
						mintrashtime %= 86400;
					}
					if (mintrashtime>3600) {
						fprintf(stderr,"%uh",mintrashtime/3600);
						mintrashtime %= 3600;
					}
					if (mintrashtime>60) {
						fprintf(stderr,"%um",mintrashtime/60);
						mintrashtime %= 60;
					}
					if (mintrashtime>0) {
						fprintf(stderr,"%us",mintrashtime);
					}
				} else {
					fprintf(stderr,"0s");
				}
				fprintf(stderr,":");
				if (maxtrashtime>0) {
					if (maxtrashtime>604800) {
						fprintf(stderr,"%uw",maxtrashtime/604800);
						maxtrashtime %= 604800;
					}
					if (maxtrashtime>86400) {
						fprintf(stderr,"%ud",maxtrashtime/86400);
						maxtrashtime %= 86400;
					}
					if (maxtrashtime>3600) {
						fprintf(stderr,"%uh",maxtrashtime/3600);
						maxtrashtime %= 3600;
					}
					if (maxtrashtime>60) {
						fprintf(stderr,"%um",maxtrashtime/60);
						maxtrashtime %= 60;
					}
					if (maxtrashtime>0) {
						fprintf(stderr,"%us",maxtrashtime);
					}
				} else {
					fprintf(stderr,"0s");
				}
				fprintf(stderr,")");
			}
		}
		fprintf(stderr,"\n");
	}
	return 0;
}

void fs_reconnect() {
	uint32_t newmasterip;
	uint32_t i;
	uint8_t *wptr,regbuff[8+64+9];
	const uint8_t *rptr;

	if (sessionid==0) {
		syslog(LOG_WARNING,"can't register: session not created");
		return;
	}

	snprintf(masterstrip,17,"%"PRIu32".%"PRIu32".%"PRIu32".%"PRIu32,(masterip>>24)&0xFF,(masterip>>16)&0xFF,(masterip>>8)&0xFF,masterip&0xFF);
	masterstrip[16]=0;

	do {
		fd = tcpsocket();
		if (fd<0) {
			return;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
		}
		if (srcip>0) {
			if (tcpnumbind(fd,srcip,0)<0) {
				syslog(LOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
				tcpclose(fd);
				fd=-1;
				return;
			}
		}
		if (tcpnumtoconnect(fd,masterip,masterport,CONNECT_TIMEOUT)<0) {
			syslog(LOG_WARNING,"can't connect to master (\"%s\":\"%"PRIu16"\")",masterstrip,masterport);
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_inc(MASTER_CONNECTS);
		wptr = regbuff;
		put32bit(&wptr,CLTOMA_FUSE_REGISTER);
		put32bit(&wptr,73);
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,REGISTER_RECONNECT);
		put32bit(&wptr,sessionid);
		put16bit(&wptr,VERSMAJ);
		put8bit(&wptr,VERSMID);
		put8bit(&wptr,VERSMIN);
		if (tcptowrite(fd,regbuff,8+64+9,1000)!=8+64+9) {
			syslog(LOG_WARNING,"master: register error (write: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESSENT,16+64);
		master_stats_inc(MASTER_PACKETSSENT);
		if (tcptoread(fd,regbuff,8,1000)!=8) {
			syslog(LOG_WARNING,"master: register error (read header: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESRCVD,8);
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i!=MATOCL_FUSE_REGISTER) {
			syslog(LOG_WARNING,"master: register error (bad answer: %"PRIu32")",i);
			tcpclose(fd);
			fd=-1;
			return;
		}
		i = get32bit(&rptr);
		if (i!=1 && i!=4) {
			syslog(LOG_WARNING,"master: register error (bad length: %"PRIu32")",i);
			tcpclose(fd);
			fd=-1;
			return;
		}
		if (tcptoread(fd,regbuff,i,1000)!=(int32_t)i) {
			syslog(LOG_WARNING,"master: register error (read data: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESRCVD,i);
		master_stats_inc(MASTER_PACKETSRCVD);
		rptr = regbuff;
		if (i==4) {
			// redirect
			newmasterip = get32bit(&rptr);
			if (newmasterip==0) {
				syslog(LOG_WARNING,"mfsmaster %s - doesn't know his leader, waiting a moment and retrying using different IP",masterstrip);
				tcpclose(fd);
				fd = -1;
				return;
			} else {
				if (newmasterip==masterip) { // this is ELECT
					syslog(LOG_WARNING,"mfsmaster %s - this is ELECT waitng for being LEADER, waiting a moment and retrying",masterstrip);
					tcpclose(fd);
					fd = -1;
					return;
				} else {
					masterip = newmasterip;
					snprintf(masterstrip,17,"%"PRIu32".%"PRIu32".%"PRIu32".%"PRIu32,(masterip>>24)&0xFF,(masterip>>16)&0xFF,(masterip>>8)&0xFF,masterip&0xFF);
					masterstrip[16]=0;
					syslog(LOG_WARNING,"mfsmaster found leader: %s",masterstrip);
					tcpclose(fd);
					fd = -1;
				}
			}
		}
	} while (i==4);
	if (rptr[0]!=0) {
		sessionlost=1;
		syslog(LOG_WARNING,"master: register status: %s",mfs_strerror(rptr[0]));
		tcpclose(fd);
		fd=-1;
		return;
	}
	lastwrite=time(NULL);
	syslog(LOG_NOTICE,"registered to master");
}

void fs_close_session(void) {
	uint8_t *wptr,regbuff[8+64+5];

	if (sessionid==0) {
		return;
	}

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,69);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_CLOSESESSION);
	put32bit(&wptr,sessionid);
	if (tcptowrite(fd,regbuff,8+64+5,1000)!=8+64+5) {
		syslog(LOG_WARNING,"master: close session error (write: %s)",strerr(errno));
	}
	if (masterversion>=VERSION2INT(1,7,29)) {
		if (tcptoread(fd,regbuff,9,500)!=9) {
			syslog(LOG_WARNING,"master: close session error (read: %s)",strerr(errno));
		}
	}
}

void fs_send_open_inodes(void) {
	uint8_t *ptr,*inodespacket;
	int32_t inodesleng;
	acquired_file *afptr;

	pthread_mutex_lock(&aflock);
	//inodesleng=24;
	inodesleng=8;
	for (afptr=afhead ; afptr ; afptr=afptr->next) {
		//syslog(LOG_NOTICE,"sustained inode: %"PRIu32,afptr->inode);
		inodesleng+=4;
	}
	inodespacket = malloc(inodesleng);
	ptr = inodespacket;
	put32bit(&ptr,CLTOMA_FUSE_SUSTAINED_INODES);
	put32bit(&ptr,inodesleng-8);
	//put32bit(&ptr,inodesleng-24);
	//put64bit(&ptr,0);	// readbytes
	//put64bit(&ptr,0);	// writebytes
	// readbytes = 0;
	// writebytes = 0;
	for (afptr=afhead ; afptr ; afptr=afptr->next) {
		put32bit(&ptr,afptr->inode);
	}
	if (tcptowrite(fd,inodespacket,inodesleng,1000)!=inodesleng) {
		disconnect=1;
	} else {
		master_stats_add(MASTER_BYTESSENT,inodesleng);
		master_stats_inc(MASTER_PACKETSSENT);
	}
	free(inodespacket);
	pthread_mutex_unlock(&aflock);
}

void* fs_nop_thread(void *arg) {
	uint8_t *ptr,hdr[12];
	int now;
	int inodeswritecnt=0;
	(void)arg;
	for (;;) {
		now = time(NULL);
		pthread_mutex_lock(&fdlock);
		if (fterm==2 && donotsendsustainedinodes==0) {
			if (fd>=0) {
				fs_send_open_inodes();
				fs_close_session();
				tcpclose(fd);
				fd = -1;
			}
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (disconnect==0 && fd>=0) {
			if (lastwrite+2<now) {	// NOP
				ptr = hdr;
				put32bit(&ptr,ANTOAN_NOP);
				put32bit(&ptr,4);
				put32bit(&ptr,0);
				if (tcptowrite(fd,hdr,12,1000)!=12) {
					disconnect=1;
				} else {
					master_stats_add(MASTER_BYTESSENT,12);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				lastwrite=now;
			}
			if (inodeswritecnt<=0 || inodeswritecnt>60) {
				inodeswritecnt=60;
			} else {
				inodeswritecnt--;
			}
			if (inodeswritecnt==0) {	// HELD INODES
				if (donotsendsustainedinodes) {
					inodeswritecnt=1;
				} else {
					fs_send_open_inodes();
				}
			}
		}
		pthread_mutex_unlock(&fdlock);
		sleep(1);
	}
}

void* fs_receive_thread(void *arg) {
	const uint8_t *ptr;
	uint8_t hdr[12];
	threc *rec;
	uint32_t cmd,size,packetid;
	uint32_t rcvd;
//	static uint8_t *notify_buff=NULL;
//	static uint32_t notify_buff_size=0;
	int r;

	(void)arg;
	for (;;) {
		pthread_mutex_lock(&fdlock);
		if (fterm) {
			fterm=2;
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (disconnect) {
//			dir_cache_remove_all();
			tcpclose(fd);
			fd=-1;
			disconnect=0;
			// send to any threc status error and unlock them
			pthread_mutex_lock(&reclock);
			for (rec=threchead ; rec ; rec=rec->next) {
				pthread_mutex_lock(&(rec->mutex));
				if (rec->sent) {
					rec->status = 1;
					rec->rcvd = 1;
					if (rec->waiting) {
						pthread_cond_signal(&(rec->cond));
					}
				}
				pthread_mutex_unlock(&(rec->mutex));
			}
			pthread_mutex_unlock(&reclock);
		}
		if (fd==-1 && sessionid!=0) {
			fs_reconnect();		// try to register using the same session id
		}
		if (fd==-1) {	// still not connected
			if (sessionlost) {	// if previous session is lost then try to register as a new session
				if (fs_connect(0,&connect_args)==0) {
					sessionlost=0;
				}
			} else {	// if other problem occured then try to resolve hostname and portname then try to reconnect using the same session id
				if (fs_resolve(0,connect_args.bindhostname,connect_args.masterhostname,connect_args.masterportname)==0) {
					fs_reconnect();
				}
			}
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			sleep(2);	// reconnect every 2 seconds
			continue;
		}
		pthread_mutex_unlock(&fdlock);
		r = tcptoread(fd,hdr,12,RECEIVE_TIMEOUT*1000);	// read timeout
		// syslog(LOG_NOTICE,"master: header size: %d",r);
		if (r==0) {
			syslog(LOG_WARNING,"master: connection lost (header)");
			disconnect=1;
			continue;
		}
		if (r!=12) {
			syslog(LOG_WARNING,"master: tcp recv error: %s (header)",strerr(errno));
			disconnect=1;
			continue;
		}
		master_stats_add(MASTER_BYTESRCVD,12);
		master_stats_inc(MASTER_PACKETSRCVD);

		ptr = hdr;
		cmd = get32bit(&ptr);
		size = get32bit(&ptr);
		packetid = get32bit(&ptr);
		if (size<4) {
			syslog(LOG_WARNING,"master: packet too small");
			disconnect=1;
			continue;
		}
		size -= 4;
		if (packetid==0) {
			if (cmd==ANTOAN_NOP && size==0) {
				// syslog(LOG_NOTICE,"master: got nop");
				continue;
			}
			if (cmd==ANTOAN_UNKNOWN_COMMAND || cmd==ANTOAN_BAD_COMMAND_SIZE) { // just ignore these packets with packetid==0
				continue;
			}
/*
			if (cmd==MATOCL_FUSE_NOTIFY_END && size==0) {
				dir_cache_transaction_end();
				continue;
			}
			if (cmd==MATOCL_FUSE_NOTIFY_ATTR || cmd==MATOCL_FUSE_NOTIFY_LINK || cmd==MATOCL_FUSE_NOTIFY_UNLINK || cmd==MATOCL_FUSE_NOTIFY_REMOVE || cmd==MATOCL_FUSE_NOTIFY_PARENT) {
				if (size>DEFAULT_INPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
					if (notify_buff) {
						munmap(notify_buff,notify_buff_size);
					}
					notify_buff = mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
					if (notify_buff) {
						free(notify_buff);
					}
					notify_buff = malloc(size);
#endif
					notify_buff_size = size;
				} else if (notify_buff_size!=DEFAULT_INPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
					if (notify_buff) {
						munmap(notify_buff,notify_buff_size);
					}
					notify_buff = mmap(NULL,DEFAULT_INPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
					if (notify_buff) {
						free(notify_buff);
					}
					notify_buff = malloc(DEFAULT_INPUT_BUFFSIZE);
#endif
					notify_buff_size = DEFAULT_INPUT_BUFFSIZE;
				}
				if (notify_buff==NULL) {
					notify_buff_size = 0;
					disconnect=1;
					continue;
				}
				if (size>0) {
					r = tcptoread(fd,notify_buff,size,1000);
					// syslog(LOG_NOTICE,"master: data size: %d",r);
					if (r==0) {
						syslog(LOG_WARNING,"master: connection lost (2)");
						disconnect=1;
						continue;
					}
					if (r!=(int32_t)(size)) {
						syslog(LOG_WARNING,"master: tcp recv error: %s (2)",strerr(errno));
						disconnect=1;
						continue;
					}
					master_stats_add(MASTER_BYTESRCVD,size);
				}
				switch (cmd) {
					case MATOCL_FUSE_NOTIFY_ATTR:
						fs_notify_attr(notify_buff,size);
						break;
					case MATOCL_FUSE_NOTIFY_LINK:
						fs_notify_link(notify_buff,size);
						break;
					case MATOCL_FUSE_NOTIFY_UNLINK:
						fs_notify_unlink(notify_buff,size);
						break;
					case MATOCL_FUSE_NOTIFY_REMOVE:
						fs_notify_remove(notify_buff,size);
						break;
					case MATOCL_FUSE_NOTIFY_PARENT:
						fs_notify_parent(notify_buff,size);
						break;
				}
				continue;
			}
*/
		}
		rec = fs_get_threc_by_id(packetid);
		if (rec==NULL) {
			syslog(LOG_WARNING,"master: got unexpected queryid");
			disconnect=1;
			continue;
		}
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		fs_input_buffer_init(rec,size);
		if (rec->ibuff==NULL) {
			pthread_mutex_unlock(&(rec->mutex));
			disconnect=1;
			continue;
		}
		// syslog(LOG_NOTICE,"master: expected data size: %"PRIu32,size);
		rcvd = 0;
		while (size-rcvd>65536) {
			r = tcptoread(fd,rec->ibuff+rcvd,65536,RECEIVE_TIMEOUT*1000);
			// syslog(LOG_NOTICE,"master: data size: %d",r);
			if (r==0) {
				syslog(LOG_WARNING,"master: connection lost (data)");
				pthread_mutex_unlock(&(rec->mutex));
				disconnect=1;
				break;
			}
			if (r!=65536) {
				syslog(LOG_WARNING,"master: tcp recv error: %s (data)",strerr(errno));
				pthread_mutex_unlock(&(rec->mutex));
				disconnect=1;
				break;
			}
			master_stats_add(MASTER_BYTESRCVD,65536);
			rcvd += 65536;
		}
		if (disconnect) {
			continue;
		}
		if (size-rcvd>0) {
			r = tcptoread(fd,rec->ibuff+rcvd,size-rcvd,RECEIVE_TIMEOUT*1000);
			// syslog(LOG_NOTICE,"master: data size: %d",r);
			if (r==0) {
				syslog(LOG_WARNING,"master: connection lost (data)");
				pthread_mutex_unlock(&(rec->mutex));
				disconnect=1;
				continue;
			}
			if (r!=(int32_t)(size-rcvd)) {
				syslog(LOG_WARNING,"master: tcp recv error: %s (data)",strerr(errno));
				pthread_mutex_unlock(&(rec->mutex));
				disconnect=1;
				continue;
			}
			master_stats_add(MASTER_BYTESRCVD,size-rcvd);
		}
		rec->sent = 0;
		rec->status = 0;
		rec->idataleng = size;
		rec->rcvd_cmd = cmd;
		// syslog(LOG_NOTICE,"master: unlock: %"PRIu32,rec->packetid);
		rec->rcvd = 1;
		if (rec->waiting) {
			pthread_cond_signal(&(rec->cond));
		}
		pthread_mutex_unlock(&(rec->mutex));
	}
}

// called before fork
int fs_init_master_connection(const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t donotrememberpassword,uint8_t bgregister) {
	master_statsptr_init();

	fd = -1;
	sessionlost = bgregister;
	sessionid = 0;
	disconnect = 0;
	donotsendsustainedinodes = 0;

	if (bindhostname) {
		connect_args.bindhostname = strdup(bindhostname);
	} else {
		connect_args.bindhostname = NULL;
	}
	connect_args.masterhostname = strdup(masterhostname);
	connect_args.masterportname = strdup(masterportname);
	connect_args.meta = meta;
	connect_args.clearpassword = donotrememberpassword;
	connect_args.info = strdup(info);
	connect_args.subfolder = strdup(subfolder);
	if (passworddigest==NULL) {
		connect_args.passworddigest = NULL;
	} else {
		connect_args.passworddigest = malloc(16);
		memcpy(connect_args.passworddigest,passworddigest,16);
	}

	if (bgregister) {
		return 1;
	}
	return fs_connect(1,&connect_args);
}

// called after fork
void fs_init_threads(uint32_t retries) {
	pthread_attr_t thattr;
	maxretries = retries;
	fterm = 0;
	pthread_mutex_init(&reclock,NULL);
	pthread_mutex_init(&fdlock,NULL);
	pthread_mutex_init(&aflock,NULL);
	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_create(&rpthid,&thattr,fs_receive_thread,NULL);
	pthread_create(&npthid,&thattr,fs_nop_thread,NULL);
	pthread_attr_destroy(&thattr);
}

void fs_term(void) {
	threc *tr,*trn;
	acquired_file *af,*afn;

	pthread_mutex_lock(&fdlock);
	fterm = 1;
	pthread_mutex_unlock(&fdlock);
	pthread_join(npthid,NULL);
	pthread_join(rpthid,NULL);
	pthread_mutex_destroy(&aflock);
	pthread_mutex_destroy(&fdlock);
	pthread_mutex_destroy(&reclock);
	for (tr = threchead ; tr ; tr = trn) {
		trn = tr->next;
		if (tr->obuff) {
#ifdef MMAP_ALLOC
			munmap((void*)(tr->obuff),tr->obuffsize);
#else
			free(tr->obuff);
#endif
		}
		if (tr->ibuff) {
#ifdef MMAP_ALLOC
			munmap((void*)(tr->ibuff),tr->ibuffsize);
#else
			free(tr->ibuff);
#endif
		}
		pthread_mutex_destroy(&(tr->mutex));
		pthread_cond_destroy(&(tr->cond));
		free(tr);
	}
	for (af = afhead ; af ; af = afn) {
		afn = af->next;
		free(af);
	}
	if (fd>=0) {
		tcpclose(fd);
	}
	if (connect_args.bindhostname) {
		free(connect_args.bindhostname);
	}
	free(connect_args.masterhostname);
	free(connect_args.masterportname);
	free(connect_args.info);
	free(connect_args.subfolder);
	if (connect_args.passworddigest) {
		free(connect_args.passworddigest);
	}
}

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *sustainedspace,uint32_t *inodes) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_STATFS,0);
	if (wptr==NULL) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*sustainedspace = 0;
		*inodes = 0;
		return;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_STATFS,&i);
	if (rptr==NULL || i!=36) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*sustainedspace = 0;
		*inodes = 0;
	} else {
		*totalspace = get64bit(&rptr);
		*availspace = get64bit(&rptr);
		*trashspace = get64bit(&rptr);
		*sustainedspace = get64bit(&rptr);
		*inodes = get32bit(&rptr);
	}
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t modemask) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0) || gids==0) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_ACCESS,13);
		if (wptr==NULL) {
			return ERROR_IO;
		}
		put32bit(&wptr,inode);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
		put8bit(&wptr,modemask);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_ACCESS,14+4*gids);
		if (wptr==NULL) {
			return ERROR_IO;
		}
		put32bit(&wptr,inode);
		put32bit(&wptr,uid);
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
		put16bit(&wptr,modemask);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_ACCESS,&i);
	if (!rptr || i!=1) {
		ret = ERROR_IO;
	} else {
		ret = rptr[0];
	}
	return ret;
}

uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+4*gids+nleng);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_LOOKUP,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,6,28)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETATTR,12);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETATTR,13);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
	}
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_setattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,6,25)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,31);
		packetver = 0;
	} else if (masterversion<VERSION2INT(1,6,28)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,32);
		packetver = 1;
	} else if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,33);
		packetver = 2;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,33+gids*4);
		packetver = 3;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver>=2) {
		put8bit(&wptr,opened);
	}
	put32bit(&wptr,uid);
	if (packetver<=2) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put8bit(&wptr,setmask);
	put16bit(&wptr,attrmode);
	put32bit(&wptr,attruid);
	put32bit(&wptr,attrgid);
	put32bit(&wptr,attratime);
	put32bit(&wptr,attrmtime);
	if (packetver>=1) {
		put8bit(&wptr,sugidclearmode);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_truncate(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t attrlength,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_TRUNCATE,21);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_TRUNCATE,21+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put64bit(&wptr,attrlength);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_TRUNCATE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_readlink(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READLINK,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			//*path = malloc(pleng);
			//memcpy(*path,ptr,pleng);
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	pleng = strlen((const char *)path)+1;
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SYMLINK,pleng+nleng+17);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SYMLINK,pleng+nleng+17+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,pleng);
	memcpy(wptr,path,pleng);
	wptr+=pleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SYMLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

static inline uint8_t fsnodes_type_back_convert(uint8_t type) {
	switch (type) {
		case TYPE_FILE:
			return DISP_TYPE_FILE;
		case TYPE_DIRECTORY:
			return DISP_TYPE_DIRECTORY;
		case TYPE_SYMLINK:
			return DISP_TYPE_SYMLINK;
		case TYPE_FIFO:
			return DISP_TYPE_FIFO;
		case TYPE_BLOCKDEV:
			return DISP_TYPE_BLOCKDEV;
		case TYPE_CHARDEV:
			return DISP_TYPE_CHARDEV;
		case TYPE_SOCKET:
			return DISP_TYPE_SOCKET;
		case TYPE_TRASH:
			return DISP_TYPE_TRASH;
		case TYPE_SUSTAINED:
			return DISP_TYPE_SUSTAINED;
	}
	return type;
}

uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();

	if (masterversion<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKNOD,20+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKNOD,22+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	if (masterversion<VERSION2INT(1,7,32)) {
		type = fsnodes_type_back_convert(type);
	}
	put8bit(&wptr,type);
	put16bit(&wptr,mode);
	if (packetver>=1) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put32bit(&wptr,rdev);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKNOD,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t copysgid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,6,25)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,15+nleng);
		packetver = 0;
	} else if (masterversion<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,16+nleng);
		packetver = 1;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,18+nleng+4*gids);
		packetver = 2;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	if (packetver>=2) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver<2) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	if (packetver>=1) {
		put8bit(&wptr,copysgid);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_UNLINK,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_UNLINK,13+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RMDIR,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RMDIR,13+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RMDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,18+nleng_src+nleng_dst);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,18+nleng_src+nleng_dst+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent_src);
	put8bit(&wptr,nleng_src);
	memcpy(wptr,name_src,nleng_src);
	wptr+=nleng_src;
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RENAME,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
		*inode = 0;
		memset(attr,0,35);
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LINK,17+nleng_dst);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LINK,17+nleng_dst+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode_src);
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_LINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}
/*
uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,12);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}
*/

uint8_t fs_readdir(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t wantattr,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	uint8_t flags;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,13);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,25+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	flags = 0;
	if (wantattr) {
		flags |= GETDIR_FLAG_WITHATTR;
	}
	if (addtocache) {
		flags |= GETDIR_FLAG_ADDTOCACHE;
	}
	put8bit(&wptr,flags);
	if (packetver>=1) {
		put32bit(&wptr,0xFFFFFFFFU);
		put64bit(&wptr,0);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (packetver>=1) { // skip 'read-next' handler
			rptr+=8;
			i-=8;
		}
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

/*
uint8_t fs_check(uint32_t inode,uint8_t dbuff[22]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint16_t cbuff[11];
	uint8_t copies;
	uint16_t chunks;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_CHECK,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_CHECK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i%3!=0) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		for (copies=0 ; copies<11 ; copies++) {
			cbuff[copies]=0;
		}
		while (i>0) {
			copies = get8bit(&rptr);
			chunks = get16bit(&rptr);
			if (copies<10) {
				cbuff[copies]+=chunks;
			} else {
				cbuff[10]+=chunks;
			}
			i-=3;
		}
		wptr = dbuff;
		for (copies=0 ; copies<11 ; copies++) {
			chunks = cbuff[copies];
			put16bit(&wptr,chunks);
		}
		ret = STATUS_OK;
	}
	return ret;
}
*/
// FUSE - I/O

uint8_t fs_create(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,7,25)) {
		return ERROR_ENOTSUP;
	}
	if (masterversion<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_CREATE,15+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_CREATE,17+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	if (packetver>=1) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	pthread_mutex_lock(&fdlock);
	donotsendsustainedinodes = 1;
	pthread_mutex_unlock(&fdlock);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_CREATE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		fs_inc_acnt((*inode = get32bit(&rptr)));
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	pthread_mutex_lock(&fdlock);
	donotsendsustainedinodes = 0;
	pthread_mutex_unlock(&fdlock);
	return ret;
}

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0) || gids==0) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_OPEN,13);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_OPEN,13+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) { // should be always true) 
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
	}
	put8bit(&wptr,flags);
	fs_inc_acnt(inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_OPEN,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		if (attr) {
			memset(attr,0,35);
		}
		ret = rptr[0];
	} else if (i==35) {
		if (attr) {
			memcpy(attr,rptr,35);
		}
		ret = STATUS_OK;
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	if (ret) {	// release on error
		fs_dec_acnt(inode);
	}
	return ret;
}

void fs_release(uint32_t inode) {
	fs_dec_acnt(inode);
}

// release - decrease acquire cnt - if reach 0 send CLTOMA_FUSE_RELEASE
/*
uint8_t fs_release(uint32_t inode) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	ptr = fs_createpacket(rec,CLTOMA_FUSE_RELEASE,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&ptr,inode);
	ptr = fs_sendandreceive(rec,MATOCL_FUSE_RELEASE,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
*/

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();

	*csdata = NULL;
	*csdatasize = 0;

	wptr = fs_createpacket(rec,CLTOMA_FUSE_READ_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READ_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (i&1) {
			*csdataver = get8bit(&rptr);
			if (i<21 || ((i-21)%10)!=0) {
				ret = ERROR_IO;
			} else {
				*csdatasize = i-21;
				ret = STATUS_OK;
			}
		} else {
			*csdataver = 0;
			if (i<20 || ((i-20)%6)!=0) {
				ret = ERROR_IO;
			} else {
				*csdatasize = i-20;
				ret = STATUS_OK;
			}
		}
		if (ret!=STATUS_OK) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
		} else {
			*length = get64bit(&rptr);
			*chunkid = get64bit(&rptr);
			*version = get32bit(&rptr);
			*csdata = rptr;
		}
	}
	return ret;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();

	*csdata = NULL;
	*csdatasize = 0;

	wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (i&1) {
			*csdataver = get8bit(&rptr);
			if (i<21 || ((i-21)%10)!=0) {
				ret = ERROR_IO;
			} else {
				*csdatasize = i-21;
				ret = STATUS_OK;
			}
		} else {
			*csdataver = 0;
			if (i<20 || ((i-20)%6)!=0) {
				ret = ERROR_IO;
			} else {
				*csdatasize = i-20;
				ret = STATUS_OK;
			}
		}
		if (ret!=STATUS_OK) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
		} else {
			*length = get64bit(&rptr);
			*chunkid = get64bit(&rptr);
			*version = get32bit(&rptr);
			*csdata = rptr;
		}
	}
	return ret;
}

uint8_t fs_writeend(uint64_t chunkid,uint32_t inode,uint64_t length) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,20);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put64bit(&wptr,chunkid);
	put32bit(&wptr,inode);
	put64bit(&wptr,length);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK_END,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}


// FUSE - META


uint8_t fs_getsustained(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETSUSTAINED,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETSUSTAINED,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrash(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASH,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETDETACHEDATTR,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETDETACHEDATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASHPATH,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	pleng = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETTRASHPATH,pleng+8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,pleng);
	memcpy(wptr,path,pleng);
//	ptr+=pleng;
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_undel(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_UNDEL,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNDEL,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_purge(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_PURGE,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_PURGE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_getacl(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t acltype,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *maskperm,uint16_t *namedusers,uint16_t *namedgroups,const uint8_t **namedacls,uint32_t *namedaclssize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	*namedacls = NULL;
	*namedaclssize = 0;
	if (masterversion<VERSION2INT(2,0,0)) {
		return ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETACL,14+4*gids);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,acltype);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	if (gids>0) {
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
	} else {
		put32bit(&wptr,0xFFFFFFFFU);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETACL,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<12) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*userperm = get16bit(&rptr);
		*groupperm = get16bit(&rptr);
		*otherperm = get16bit(&rptr);
		*maskperm = get16bit(&rptr);
		*namedusers = get16bit(&rptr);
		*namedgroups = get16bit(&rptr);
		*namedacls = rptr;
		*namedaclssize = i-12;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_setacl(uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t maskperm,uint16_t namedusers,uint16_t namedgroups,uint8_t *namedacls,uint32_t namedaclssize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(2,0,0)) {
		return ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETACL,21+namedaclssize);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put8bit(&wptr,acltype);
	put16bit(&wptr,userperm);
	put16bit(&wptr,groupperm);
	put16bit(&wptr,otherperm);
	put16bit(&wptr,maskperm);
	put16bit(&wptr,namedusers);
	put16bit(&wptr,namedgroups);
	memcpy(wptr,namedacls,namedaclssize);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETACL,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	*vbuff = NULL;
	*vleng = 0;
	if (masterversion<VERSION2INT(1,7,0)) {
		return ERROR_ENOTSUP;
	}
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*vleng = get32bit(&rptr);
		*vbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*vleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,7,0)) {
		return ERROR_ENOTSUP;
	}
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,0);
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		*dleng = get32bit(&rptr);
		*dbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*dleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,7,0)) {
		return ERROR_ENOTSUP;
	}
	if (mode>=MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+vleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+vleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,vleng);
	memcpy(wptr,value,vleng);
	wptr+=vleng;
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (masterversion<VERSION2INT(1,7,0)) {
		return ERROR_ENOTSUP;
	}
	if (masterversion<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,0);
	put8bit(&wptr,MFS_XATTR_REMOVE);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_custom(uint32_t qcmd,const uint8_t *query,uint32_t queryleng,uint32_t *acmd,uint8_t *answer,uint32_t *answerleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,qcmd,queryleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	memcpy(wptr,query,queryleng);
	rptr = fs_sendandreceive_any(rec,acmd,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else {
		if (*answerleng<i) {
			ret = ERROR_EINVAL;
		} else {
			*answerleng = i;
			memcpy(answer,rptr,i);
			ret = STATUS_OK;
		}
	}
	return ret;
}

/*
uint8_t fs_append(uint32_t inode,uint32_t ainode,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_APPEND,16);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,ainode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_APPEND,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
*/
