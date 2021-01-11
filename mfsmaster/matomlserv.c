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

#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"

#include "datapack.h"
#include "matomlserv.h"
#include "changelog.h"
#include "metadata.h"
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "sizestr.h"
#include "sockets.h"
#include "slogger.h"
#include "massert.h"
#include "clocks.h"
#include "mfsalloc.h"

#define MaxPacketSize ANTOMA_MAXPACKETSIZE


#define OLD_CHANGES_GROUP_COUNT 10000

// matomlserventry.mode
enum{KILL,DATA,CLOSE};

// matomlserventry.clienttype
enum{UNKNOWN,METALOGGER};

// matomlserventry.logstate
enum{NONE,DELAYED,SYNC};

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

typedef struct matomlserventry {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	double lastread,lastwrite;
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;

	uint16_t timeout;
	uint64_t next_log_version;

	char *servstrip;		// human readable version of servip
	uint32_t version;
	uint32_t servip;
	uint8_t clienttype;
	uint8_t logstate;


	int upload_meta_fd;
	int upload_chain1_fd;
	int upload_chain2_fd;

	struct matomlserventry *next;
} matomlserventry;

static matomlserventry *matomlservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

/*
typedef struct old_changes_entry {
	uint64_t version;
	uint32_t length;
	uint8_t *data;
} old_changes_entry;

typedef struct old_changes_block {
	old_changes_entry old_changes_block [OLD_CHANGES_BLOCK_SIZE];
	uint32_t entries;
	uint32_t mintimestamp;
	uint64_t minversion;
	struct old_changes_block *next;
} old_changes_block;

static old_changes_block *old_changes_head=NULL;
static old_changes_block *old_changes_current=NULL;
*/

// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t listenip;
static uint16_t listenport;


static uint32_t BackMetaCopies;


// static uint16_t ChangelogSecondsToRemember;

/*
void matomlserv_old_changes_free_block(old_changes_block *oc) {
	uint32_t i;
	for (i=0 ; i<oc->entries ; i++) {
		free(oc->old_changes_block[i].data);
	}
	free(oc);
}

void matomlserv_store_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {\
	old_changes_block *oc;
	old_changes_entry *oce;
	uint32_t ts;
	if (ChangelogSecondsToRemember==0) {
		while (old_changes_head) {
			oc = old_changes_head->next;
			matomlserv_old_changes_free_block(old_changes_head);
			old_changes_head = oc;
		}
		return;
	}
	if (old_changes_current==NULL || old_changes_head==NULL || old_changes_current->entries>=OLD_CHANGES_BLOCK_SIZE) {
		oc = malloc(sizeof(old_changes_block));
		passert(oc);
		ts = main_time();
		oc->entries = 0;
		oc->minversion = version;
		oc->mintimestamp = ts;
		oc->next = NULL;
		if (old_changes_current==NULL || old_changes_head==NULL) {
			old_changes_head = old_changes_current = oc;
		} else {
			old_changes_current->next = oc;
			old_changes_current = oc;
		}
		while (old_changes_head && old_changes_head->next && old_changes_head->next->mintimestamp+ChangelogSecondsToRemember<ts) {
			oc = old_changes_head->next;
			matomlserv_old_changes_free_block(old_changes_head);
			old_changes_head = oc;
		}
	}
	oc = old_changes_current;
	oce = oc->old_changes_block + oc->entries;
	oce->version = version;
	oce->length = logstrsize;
	oce->data = malloc(logstrsize);
	passert(oce->data);
	memcpy(oce->data,logstr,logstrsize);
	oc->entries++;
}
*/


static inline const char* matomlserv_clientname(matomlserventry *eptr) {
	switch (eptr->clienttype) {
		case METALOGGER:
			switch (eptr->logstate) {
				case DELAYED:
					return "METALOGGER-DELAYED";
				case SYNC:
					return "METALOGGER-SYNC";
				default:
					return "METALOGGER";
			}
	}
	return "UNKNOWN";
}

uint32_t matomlserv_mloglist_size(void) {
	matomlserventry *eptr;
	uint32_t i;
	i=0;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->mode!=CLOSE && eptr->clienttype==METALOGGER) {
			i++;
		}
	}
	return i*(4+4);
}

void matomlserv_mloglist_data(uint8_t *ptr) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->mode!=CLOSE && eptr->clienttype==METALOGGER) {
			put32bit(&ptr,eptr->version);
			put32bit(&ptr,eptr->servip);
		}
	}
}


void matomlserv_status(void) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA) {
			return;
		}
	}
	syslog(LOG_WARNING,"no metaloggers connected !!!");
}

char* matomlserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = malloc(l);
	passert(optr);
	snprintf(optr,l,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matomlserv_createpacket(matomlserventry *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
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

void matomlserv_send_old_change(void *veptr,uint64_t version,uint8_t *data,uint32_t length) {
	matomlserventry *eptr = (matomlserventry *)veptr;
	uint8_t *pdata;

	pdata = matomlserv_createpacket(eptr,MATOAN_METACHANGES_LOG,9+length);
	put8bit(&pdata,0xFF);
	put64bit(&pdata,version);
	memcpy(pdata,data,length);
}

/*
void matomlserv_send_old_changes(matomlserventry *eptr,uint64_t version) {
	uint64_t minver = changelog_get_minversion();
	if (minver==0) {
		// syslog(LOG_WARNING,"meta logger wants old changes, but storage is disabled");
		return;
	}
	if (version<minver) {
		syslog(LOG_WARNING,"meta logger wants changes since version: %"PRIu64", but minimal version in storage is: %"PRIu64,version,minver);
		return;
	}
	changelog_get_old_changes(version,matomlserv_send_old_change,eptr);
}
*/
/*
void matomlserv_send_old_changes(matomlserventry *eptr,uint64_t version) {
	old_changes_block *oc;
	old_changes_entry *oce;
	uint8_t *data;
	uint8_t start=0;
	uint32_t i;
	if (old_changes_head==NULL) {
		// syslog(LOG_WARNING,"meta logger wants old changes, but storage is disabled");
		return;
	}
	if (old_changes_head->minversion>version) {
		syslog(LOG_WARNING,"meta logger wants changes since version: %"PRIu64", but minimal version in storage is: %"PRIu64,version,old_changes_head->minversion);
		return;
	}
	for (oc=old_changes_head ; oc ; oc=oc->next) {
		if (oc->minversion<=version && (oc->next==NULL || oc->next->minversion>version)) {
			start=1;
		}
		if (start) {
			for (i=0 ; i<oc->entries ; i++) {
				oce = oc->old_changes_block + i;
				if (version>=oce->version) {
					data = matomlserv_createpacket(eptr,MATOAN_METACHANGES_LOG,9+oce->length);
					put8bit(&data,0xFF);
					put64bit(&data,oce->version);
					memcpy(data,oce->data,oce->length);
				}
			}
		}
	}
}
*/


void matomlserv_get_version(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
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
		ptr = matomlserv_createpacket(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = matomlserv_createpacket(eptr,ANTOAN_VERSION,4+strlen(vstring));
	}
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	memcpy(ptr,vstring,strlen(vstring));
}

void matomlserv_get_config(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	uint32_t vleng;
	char *val;
	uint8_t *ptr;

	if (length<5) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	val = cfg_getstr(name,"");
	vleng = strlen(val);
	if (vleng>255) {
		vleng=255;
	}
	ptr = matomlserv_createpacket(eptr,ANTOAN_CONFIG_VALUE,5+vleng);
	put32bit(&ptr,msgid);
	put8bit(&ptr,vleng);
	memcpy(ptr,val,vleng);
}

void matomlserv_register(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t rversion;
	uint64_t req_minversion,chlog_minversion;
	uint32_t n;

	if (eptr->version>0) {
		syslog(LOG_WARNING,"got register message from registered metalogger !!!");
		eptr->mode = KILL;
		return;
	}
	if (length<1) {
		syslog(LOG_NOTICE,"ANTOMA_REGISTER - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	} else {
		rversion = get8bit(&data);
		if (rversion==1) {
			eptr->clienttype = METALOGGER;
			if (length!=7) {
				syslog(LOG_NOTICE,"ANTOMA_REGISTER (logger 1) - wrong size (%"PRIu32"/7)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->timeout = get16bit(&data);
			eptr->logstate = SYNC;
		} else if (rversion==2) {
			eptr->clienttype = METALOGGER;
			if (length!=7+8) {
				syslog(LOG_NOTICE,"ANTOMA_REGISTER (logger 2) - wrong size (%"PRIu32"/15)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->timeout = get16bit(&data);
			req_minversion = get64bit(&data);
			chlog_minversion = changelog_get_minversion();
			if (chlog_minversion>0 && chlog_minversion<=req_minversion) {
						n = changelog_get_old_changes(req_minversion,matomlserv_send_old_change,eptr,OLD_CHANGES_GROUP_COUNT);
						if (n<OLD_CHANGES_GROUP_COUNT) {
							eptr->logstate = SYNC;
						} else {
							eptr->next_log_version = req_minversion+n;
							eptr->logstate = DELAYED;
						}
					} else {
						eptr->logstate = SYNC; // desync
					}
		} else if (rversion==3 || rversion==4) { // just ignore PRO components
			eptr->mode = KILL;
			return;
		} else {
			syslog(LOG_NOTICE,"ANTOMA_REGISTER - wrong version (%"PRIu8"/1)",rversion);
			eptr->mode = KILL;
			return;
		}
		if (eptr->timeout<10) {
			syslog(LOG_NOTICE,"ANTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
			if (eptr->timeout<3) {
				eptr->timeout=3;
			}
//			eptr->mode = KILL;
			return;
		}
	}
}


void matomlserv_download_start(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t filenum;
	uint64_t size;
	uint8_t *ptr;
	if (length!=1) {
		syslog(LOG_NOTICE,"ANTOMA_DOWNLOAD_START - wrong size (%"PRIu32"/1)",length);
		eptr->mode = KILL;
		return;
	}
	filenum = get8bit(&data);
	if (filenum==1 || filenum==2) {
		if (eptr->upload_meta_fd>=0) {
			close(eptr->upload_meta_fd);
			eptr->upload_meta_fd=-1;
		}
		if (eptr->upload_chain1_fd>=0) {
			close(eptr->upload_chain1_fd);
			eptr->upload_chain1_fd=-1;
		}
		if (eptr->upload_chain2_fd>=0) {
			close(eptr->upload_chain2_fd);
			eptr->upload_chain2_fd=-1;
		}
	}
	if (filenum==1) {
		eptr->upload_meta_fd = open("metadata.mfs.back",O_RDONLY);
		eptr->upload_chain1_fd = open("changelog.0.mfs",O_RDONLY);
		eptr->upload_chain2_fd = open("changelog.1.mfs",O_RDONLY);
	} else if (filenum==2) {
		eptr->upload_meta_fd = open("sessions.mfs",O_RDONLY);
	} else if (filenum==11) {
		if (eptr->upload_meta_fd>=0) {
			close(eptr->upload_meta_fd);
		}
		eptr->upload_meta_fd = eptr->upload_chain1_fd;
		eptr->upload_chain1_fd = -1;
	} else if (filenum==12) {
		if (eptr->upload_meta_fd>=0) {
			close(eptr->upload_meta_fd);
		}
		eptr->upload_meta_fd = eptr->upload_chain2_fd;
		eptr->upload_chain2_fd = -1;
	} else {
		eptr->mode = KILL;
		return;
	}
	if (eptr->upload_meta_fd<0) {
		if (filenum==11 || filenum==12) {
			ptr = matomlserv_createpacket(eptr,MATOAN_DOWNLOAD_INFO,8);
			put64bit(&ptr,0);
			return;
		} else {
			ptr = matomlserv_createpacket(eptr,MATOAN_DOWNLOAD_INFO,1);
			put8bit(&ptr,0xff);	// error
			return;
		}
	}
	size = lseek(eptr->upload_meta_fd,0,SEEK_END);
	ptr = matomlserv_createpacket(eptr,MATOAN_DOWNLOAD_INFO,8);
	put64bit(&ptr,size);	// ok
}

void matomlserv_download_request(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;

	if (length!=12) {
		syslog(LOG_NOTICE,"ANTOMA_DOWNLOAD_REQUEST - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	if (eptr->upload_meta_fd<0) {
		syslog(LOG_NOTICE,"ANTOMA_DOWNLOAD_REQUEST - file not opened");
		eptr->mode = KILL;
		return;
	}
	offset = get64bit(&data);
	leng = get32bit(&data);
	ptr = matomlserv_createpacket(eptr,MATOAN_DOWNLOAD_DATA,16+leng);
	put64bit(&ptr,offset);
	put32bit(&ptr,leng);
#ifdef HAVE_PREAD
	ret = pread(eptr->upload_meta_fd,ptr+4,leng,offset);
#else /* HAVE_PWRITE */
	lseek(eptr->upload_meta_fd,offset,SEEK_SET);
	ret = read(eptr->upload_meta_fd,ptr+4,leng);
#endif /* HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		mfs_errlog_silent(LOG_NOTICE,"error reading metafile");
		eptr->mode = KILL;
		return;
	}
	crc = mycrc32(0,ptr+4,leng);
	put32bit(&ptr,crc);
}

void matomlserv_download_end(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"ANTOMA_DOWNLOAD_END - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (eptr->upload_meta_fd>=0) {
		close(eptr->upload_meta_fd);
		eptr->upload_meta_fd=-1;
	}
}


void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	matomlserventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0 && eptr->clienttype==METALOGGER && eptr->logstate==SYNC) {
			data = matomlserv_createpacket(eptr,MATOAN_METACHANGES_LOG,9+logstrsize);
			put8bit(&data,0xFF);
			put64bit(&data,version);
			memcpy(data,logstr,logstrsize);
		}
	}
}

void matomlserv_broadcast_logrotate(void) {
	matomlserventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0 && eptr->clienttype==METALOGGER) {
			data = matomlserv_createpacket(eptr,MATOAN_METACHANGES_LOG,1);
			put8bit(&data,0x55);
		}
	}
}

void matomlserv_beforeclose(matomlserventry *eptr) {
	if (eptr->upload_meta_fd>=0) {
		close(eptr->upload_meta_fd);
		eptr->upload_meta_fd=-1;
	}
	if (eptr->upload_chain1_fd>=0) {
		close(eptr->upload_chain1_fd);
		eptr->upload_chain1_fd=-1;
	}
	if (eptr->upload_chain2_fd>=0) {
		close(eptr->upload_chain2_fd);
		eptr->upload_chain2_fd=-1;
	}
}

void matomlserv_gotpacket(matomlserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case ANTOAN_GET_VERSION:
			matomlserv_get_version(eptr,data,length);
			break;
		case ANTOAN_GET_CONFIG:
			matomlserv_get_config(eptr,data,length);
			break;
		case ANTOMA_REGISTER:
			matomlserv_register(eptr,data,length);
			break;
		case ANTOMA_DOWNLOAD_START:
			matomlserv_download_start(eptr,data,length);
			break;
		case ANTOMA_DOWNLOAD_REQUEST:
			matomlserv_download_request(eptr,data,length);
			break;
		case ANTOMA_DOWNLOAD_END:
			matomlserv_download_end(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"master control module: got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
	}
}

void matomlserv_read(matomlserventry *eptr,double now) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup;
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
	for (;;) {
		i = read(eptr->sock,readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
			}
			break;
		} else {
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = mfsrealloc(readbuff,readbuffsize);
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
				syslog(LOG_WARNING,"ML(%s) packet too long (%"PRIu32"/%u) ; command:%"PRIu32,eptr->servstrip,leng,MaxPacketSize,type);
				eptr->input_end = 1;
				return;
			}

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
		syslog(LOG_NOTICE,"connection with %s(%s) has been closed by peer",matomlserv_clientname(eptr),eptr->servstrip);
		eptr->input_end = 1;
	} else if (err) {
		mfs_arg_errlog_silent(LOG_NOTICE,"read from ML(%s) error",eptr->servstrip);
		eptr->input_end = 1;
	}
}

void matomlserv_parse(matomlserventry *eptr) {
	in_packetstruct *ipack;
	uint64_t starttime;
	uint64_t currtime;

	starttime = monotonic_useconds();
	currtime = starttime;
	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL && starttime+10000>currtime) {
		matomlserv_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
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

void matomlserv_write(matomlserventry *eptr,double now) {
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
				mfs_arg_errlog_silent(LOG_NOTICE,"write to ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
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
				mfs_arg_errlog_silent(LOG_NOTICE,"write to ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		opack->startptr+=i;
		opack->bytesleft-=i;
		if (opack->bytesleft>0) {
			return;
		}
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}

void matomlserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	int events;
	matomlserventry *eptr;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		events = 0;
		if (eptr->input_end==0) {
			events |= POLLIN;
		}
		if (eptr->outputhead!=NULL) {
			events |= POLLOUT;
		}
		if (events) {
			pdesc[pos].fd = eptr->sock;
			pdesc[pos].events = POLLIN;
			eptr->pdescpos = pos;
			pos++;
		} else {
			eptr->pdescpos = -1;
		}
	}
	*ndesc = pos;
}

void matomlserv_disconnection_loop(void) {
	matomlserventry *eptr,**kptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	kptr = &matomlservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode==KILL || eptr->mode==CLOSE) {
			matomlserv_beforeclose(eptr);
			if (eptr->mode==KILL) {
				tcpclose(eptr->sock);
			} else {
				close(eptr->sock);
			}
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
			if (eptr->servstrip) {
				free(eptr->servstrip);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matomlserv_serve(struct pollfd *pdesc) {
	double now;
	matomlserventry *eptr;
	int ns;
	static double lastaction = 0.0;
	double timeoutadd;
	uint32_t n;

	now = monotonic_seconds();
// timeout fix
	if (lastaction>0.0) {
		timeoutadd = now-lastaction;
		if (timeoutadd>1.0) {
			for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
				eptr->lastread += timeoutadd;
			}
		}
	}
	lastaction = now;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"Master<->ML socket: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matomlserventry));
			passert(eptr);
			eptr->next = matomlservhead;
			matomlservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
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
			eptr->timeout = 10;

			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
			eptr->servstrip = matomlserv_makestrip(eptr->servip);
			eptr->version = 0;
			eptr->clienttype = UNKNOWN;
			eptr->logstate = NONE;
			eptr->upload_meta_fd = -1;
			eptr->upload_chain1_fd = -1;
			eptr->upload_chain2_fd = -1;
		}
	}

// read
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((pdesc[eptr->pdescpos].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode!=KILL) {
				matomlserv_read(eptr,now);
			}
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->input_end = 1;
			}
		}
		matomlserv_parse(eptr);
	}

// write
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if ((eptr->lastwrite+(eptr->timeout/3.0))<now && eptr->outputhead==NULL && eptr->clienttype!=UNKNOWN) {
			matomlserv_createpacket(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				matomlserv_write(eptr,now);
			}
		}
		if ((eptr->lastread+eptr->timeout)<now) {
			eptr->mode = KILL;
		}
		if (eptr->logstate==DELAYED && eptr->outputhead==NULL) {
			n = changelog_get_old_changes(eptr->next_log_version,matomlserv_send_old_change,eptr,OLD_CHANGES_GROUP_COUNT);
			if (n<OLD_CHANGES_GROUP_COUNT) {
				eptr->logstate=SYNC;
			} else {
				eptr->next_log_version += n;
			}
		}
	}
	matomlserv_disconnection_loop();
}

void matomlserv_keep_alive(void) {
	double now;
	matomlserventry *eptr;

	now = monotonic_seconds();
// read
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode == DATA && eptr->input_end==0) {
			matomlserv_read(eptr,now);
		}
	}
// write
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if ((eptr->lastwrite+(eptr->timeout/3.0))<now && eptr->outputhead==NULL && eptr->clienttype!=UNKNOWN) {
			matomlserv_createpacket(eptr,ANTOAN_NOP,0);
		}
		if (eptr->mode == DATA && eptr->outputhead) {
			matomlserv_write(eptr,now);
		}
	}
}

void matomlserv_close_lsock(void) { // after fork
	if (lsock>=0) {
		close(lsock);
	}
}

void matomlserv_term(void) {
	matomlserventry *eptr,*eaptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;
	syslog(LOG_INFO,"master control module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matomlservhead;
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
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matomlservhead=NULL;

	matomlserv_read(NULL,0.0); // free internal read buffer

	free(ListenHost);
	free(ListenPort);
}

int matomlserv_no_more_pending_jobs(void) {
	matomlserventry *eptr;
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
	}
	return 1;
}

void matomlserv_disconnect_all(void) {
	matomlserventry *eptr;
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		eptr->mode = KILL;
	}
	matomlserv_disconnection_loop();
}

uint16_t matomlserv_getport(void) {
	return listenport;
}

void matomlserv_reload_common(void) {
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",1);
	if (BackMetaCopies>99) {
		BackMetaCopies=99;
	}
}

void matomlserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	uint32_t oldlistenip;
	uint16_t oldlistenport;
	int newlsock;

	matomlserv_reload_common();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	oldlistenip = listenip;
	oldlistenport = listenport;

	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT",DEFAULT_MASTER_CONTROL_PORT);
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"master <-> metaloggers module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> metaloggers module: socket address has changed, but can't be resolved (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpnumlisten(newlsock,listenip,listenport,100)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> metaloggers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;

//	ChangelogSecondsToRemember = cfg_getuint16("MATOAN_LOG_PRESERVE_SECONDS",600);
//	if (ChangelogSecondsToRemember>3600) {
//		syslog(LOG_WARNING,"Number of seconds of change logs to be preserved in master is too big (%"PRIu16") - decreasing to 3600 seconds",ChangelogSecondsToRemember);
//		ChangelogSecondsToRemember=3600;
//	}
}

int matomlserv_init(void) {
	matomlserv_reload_common();

	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT",DEFAULT_MASTER_CONTROL_PORT);

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"master <-> metaloggers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> metaloggers module: can't resolve %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpnumlisten(lsock,listenip,listenport,100)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> metaloggers module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: listen on %s:%s",ListenHost,ListenPort);

	matomlservhead = NULL;
//	ChangelogSecondsToRemember = cfg_getuint16("MATOAN_LOG_PRESERVE_SECONDS",600);
//	if (ChangelogSecondsToRemember>3600) {
//		syslog(LOG_WARNING,"Number of seconds of change logs to be preserved in master is too big (%"PRIu16") - decreasing to 3600 seconds",ChangelogSecondsToRemember);
//		ChangelogSecondsToRemember=3600;
//	}
	main_reload_register(matomlserv_reload);
	main_destruct_register(matomlserv_term);
	main_poll_register(matomlserv_desc,matomlserv_serve);
	main_keepalive_register(matomlserv_keep_alive);
	main_time_register(3600,0,matomlserv_status);
	return 0;
}
