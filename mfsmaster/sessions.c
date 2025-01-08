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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "MFSCommunication.h"

#include "sockets.h"
#include "sessions.h"
#include "storageclass.h"
#include "bio.h"
#include "filesystem.h"
#include "openfiles.h"
#include "changelog.h"
#include "metadata.h"
#include "datapack.h"
#include "cfg.h"
#include "main.h"
#include "mfslog.h"
#include "massert.h"

static const char* opname[SESSION_STATS]={SES_OP_STRINGS};

#define SESSION_HASHSIZE 256
#define SESSION_HASH(sessionid) ((sessionid)%(SESSION_HASHSIZE))

// opened files
/*
typedef struct filelist {
	uint32_t inode;
	struct filelist *next;
} filelist;
*/

typedef struct session {
	uint32_t sessionid;
	uint64_t exportscsum;	// session valid for given "exports" file
	uint8_t *info;
	uint32_t ileng;
	uint32_t peerip;
	uint8_t closed;
	uint8_t sesflags;
	uint16_t umaskval;
	uint16_t sclassgroups;
	uint32_t mintrashretention;
	uint32_t maxtrashretention;
	uint32_t rootuid;
	uint32_t rootgid;
	uint32_t mapalluid;
	uint32_t mapallgid;
	uint32_t disables;
	uint32_t rootinode;
	uint32_t disconnected;	// 0 = connected ; other = disconnection timestamp
	uint32_t nsocks;	// >0 - connected (number of active connections) ; 0 - not connected
	uint32_t infopeerip;	// peer ip for info
	uint32_t infoversion;	// version for info
	uint32_t chouropstats[SESSION_STATS];
	uint32_t lhouropstats[SESSION_STATS];
	uint32_t cminopstats[SESSION_STATS];
	uint32_t lminopstats[SESSION_STATS];
//	filelist *openedfiles;
	struct session *next;
} session;

static session *sessionshashtab[SESSION_HASHSIZE];
// static session *sessionshead=NULL;
static uint32_t nextsessionid;

static uint32_t SessionSustainTime;


void sessions_attach_session(void* vsesdata,uint32_t peerip,uint32_t version) {
	session *sesdata = (session*)vsesdata;
	sesdata->closed = 0;
	sesdata->nsocks++;
	sesdata->infopeerip = peerip;
	sesdata->infoversion = version;
	if (sesdata->disconnected!=0) {
		sesdata->disconnected = 0;
		changelog("%"PRIu32"|SESCONNECTED(%"PRIu32")",main_time(),sesdata->sessionid);
	}
}

void sessions_close_session(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	if (sesdata) {
		if (sesdata->nsocks==1) {
			sesdata->closed = 1;
		}
	}
	return;
}

void sessions_disconnection(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	if (sesdata) {
		if (sesdata->nsocks>0) {
			sesdata->nsocks--;
		}
		if (sesdata->nsocks==0) {
			sesdata->disconnected = main_time();
			changelog("%"PRIu32"|SESDISCONNECTED(%"PRIu32")",main_time(),sesdata->sessionid);
		}
	}
}

void* sessions_find_session(uint32_t sessionid) {
	session *asesdata;
	if (sessionid==0 || sessionid>=UINT32_C(0x80000000)) {
		return NULL;
	}
	for (asesdata = sessionshashtab[SESSION_HASH(sessionid)] ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->sessionid==sessionid) {
			return asesdata;
		}
	}
	return NULL;
}

static inline void sessions_clean_session(session *sesdata) {
	if (sesdata->sessionid>0 && sesdata->sessionid<UINT32_C(0x80000000)) {
		of_session_removed(sesdata->sessionid);
	}
	if (sesdata->info) {
		free(sesdata->info);
	}
}

uint8_t sessions_store(bio *fd) {
	session *asesdata;
	uint8_t fsesrecord[61]; // 4+4+4+4+1+1+1+4+4+4+4+4+4
	uint8_t *ptr;
	uint32_t hpos;
	if (fd==NULL) {
		return 0x16;
	}
	ptr = fsesrecord;
	put32bit(&ptr,nextsessionid);
	put16bit(&ptr,0); // do not store stats - statisitcs shouldn't be a part of metadata
	if (bio_write(fd,fsesrecord,6)!=6) {
		return 0xFF;
	}
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (asesdata = sessionshashtab[hpos] ; asesdata ; asesdata=asesdata->next) {
			if (asesdata->closed==0) {
				ptr = fsesrecord;
				put32bit(&ptr,asesdata->sessionid);
				put64bit(&ptr,asesdata->exportscsum);
				put32bit(&ptr,asesdata->ileng);
				put32bit(&ptr,asesdata->peerip);
				put32bit(&ptr,asesdata->rootinode);
				put8bit(&ptr,asesdata->sesflags);
				put16bit(&ptr,asesdata->umaskval);
				put16bit(&ptr,asesdata->sclassgroups);
				put32bit(&ptr,asesdata->mintrashretention);
				put32bit(&ptr,asesdata->maxtrashretention);
				put32bit(&ptr,asesdata->rootuid);
				put32bit(&ptr,asesdata->rootgid);
				put32bit(&ptr,asesdata->mapalluid);
				put32bit(&ptr,asesdata->mapallgid);
				put32bit(&ptr,asesdata->disables);
				put32bit(&ptr,asesdata->disconnected);
				if (bio_write(fd,fsesrecord,61)!=61) {
					return 0xFF;
				}
				if (asesdata->ileng>0) {
					if (bio_write(fd,asesdata->info,asesdata->ileng)!=asesdata->ileng) {
						return 0xFF;
					}
				}
			}
		}
	}
	memset(fsesrecord,0,61);
	if (bio_write(fd,fsesrecord,61)!=61) {
		return 0xFF;
	}
	return 0;
}

int sessions_load(bio *fd,uint8_t mver) {
	session *asesdata;
	uint8_t hdr[8];
	uint8_t *fsesrecord;
	const uint8_t *ptr;
	uint16_t statsinfile;
	uint32_t recsize;
	uint32_t i,sessionid,hpos;

	if (mver<0x12) {
		if (bio_read(fd,hdr,8)!=8) {
			return -1;
		}

		ptr = hdr;
		switch (get16bit(&ptr)) {
			case 1:
				mver = 0x10;
				break;
			case 2:
				mver = 0x11;
				break;
			default:
				return -1;
		}
	} else {
		if (bio_read(fd,hdr,6)!=6) {
			return -1;
		}

		ptr = hdr;
	}
	nextsessionid = get32bit(&ptr);
	statsinfile = get16bit(&ptr);

	if (mver<0x11) {
		recsize = 43+statsinfile*8;
	} else if (mver<0x13) {
		recsize = 47+statsinfile*8;
	} else if (mver<0x14) {
		recsize = 55+statsinfile*8;
	} else if (mver<0x15) {
		recsize = 57+statsinfile*8;
	} else {
		recsize = 61+statsinfile*8;
	}
	fsesrecord = malloc(recsize);
	passert(fsesrecord);

	while(1) {
		if (bio_read(fd,fsesrecord,recsize)==recsize) {
			ptr = fsesrecord;
			sessionid = get32bit(&ptr);
			if (sessionid==0) {
				free(fsesrecord);
				return 0;
			}
			asesdata = (session*)malloc(sizeof(session));
			passert(asesdata);
			asesdata->sessionid = sessionid;
			if (mver>=0x13) {
				asesdata->exportscsum = get64bit(&ptr);
			} else {
				asesdata->exportscsum = 0;
			}
			asesdata->ileng = get32bit(&ptr);
			asesdata->peerip = get32bit(&ptr);
			asesdata->rootinode = get32bit(&ptr);
			asesdata->sesflags = get8bit(&ptr);
			if (mver>=0x14) {
				asesdata->umaskval = get16bit(&ptr);
			} else {
				asesdata->umaskval = 0;
			}
			if (mver>=0x16) {
				asesdata->sclassgroups = get16bit(&ptr);
			} else {
				uint8_t g,mingoal,maxgoal;
				mingoal = get8bit(&ptr);
				maxgoal = get8bit(&ptr);
				asesdata->sclassgroups = 1;
				for (g=mingoal ; g<=maxgoal ; g++) {
					asesdata->sclassgroups |= (1<<g);
				}
			}
			asesdata->mintrashretention = get32bit(&ptr);
			asesdata->maxtrashretention = get32bit(&ptr);
			asesdata->rootuid = get32bit(&ptr);
			asesdata->rootgid = get32bit(&ptr);
			asesdata->mapalluid = get32bit(&ptr);
			asesdata->mapallgid = get32bit(&ptr);
			if (mver>=0x15) {
				asesdata->disables = get32bit(&ptr);
			} else {
				asesdata->disables = 0;
			}
			if (mver>=0x11) {
				asesdata->disconnected = get32bit(&ptr);
			} else {
				asesdata->disconnected = main_time();
			}
			asesdata->info = NULL;
			asesdata->closed = 0;
//			asesdata->openedfiles = NULL;
			asesdata->nsocks = 0;
			asesdata->infopeerip = asesdata->peerip;
			asesdata->infoversion = 0;
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->chouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (statsinfile>SESSION_STATS) {
				ptr+=4*(statsinfile-SESSION_STATS);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->lhouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (asesdata->ileng>0) {
				asesdata->info = malloc(asesdata->ileng+1);
				passert(asesdata->info);
				if (bio_read(fd,asesdata->info,asesdata->ileng)!=asesdata->ileng) {
					free(asesdata->info);
					free(asesdata);
					free(fsesrecord);
					return -1;
				}
				asesdata->info[asesdata->ileng]=0;
			}
			hpos = SESSION_HASH(sessionid);
			asesdata->next = sessionshashtab[hpos];
			sessionshashtab[hpos] = asesdata;
		} else {
			free(fsesrecord);
			return -1;
		}
	}
}

/* import from old metadata */
void sessions_set_nextsessionid(uint32_t nsi) {
	nextsessionid = nsi;
}
/*
uint32_t sessions_getnextsessionid(void) {
	return nextsessionid;
}
*/
int sessions_import_data(void) {
	session *asesdata;
//	uint8_t fsesrecord[33+SESSION_STATS*8];	// 4+4+4+4+1+4+4+4+4+SESSION_STATS*4+SESSION_STATS*4
	uint8_t hdr[8];
	uint8_t *fsesrecord;
	const uint8_t *ptr;
	uint8_t mapalldata;
	uint8_t goaltrashdata;
	uint32_t i,statsinfile;
	uint32_t hpos;
	int r;
	FILE *fd;

	fd = fopen("sessions.mfs","r");
	if (fd==NULL) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't load sessions, fopen error");
		if (errno==ENOENT) {	// it's ok if file does not exist
			nextsessionid = 1;
			return 0;
		} else {
			return -1;
		}
	}
	if (fread(hdr,8,1,fd)!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, fread error");
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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\004",8)==0) {
		mapalldata = 1;
		goaltrashdata = 1;
		if (fread(hdr,2,1,fd)!=1) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, bad header");
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
			asesdata->ileng = get32bit(&ptr);
			asesdata->peerip = get32bit(&ptr);
			asesdata->rootinode = get32bit(&ptr);
			asesdata->sesflags = get8bit(&ptr);
			asesdata->umaskval = 0;
			if (goaltrashdata) {
				uint8_t g,mingoal,maxgoal;
				mingoal = get8bit(&ptr);
				maxgoal = get8bit(&ptr);
				asesdata->sclassgroups = 1;
				for (g=mingoal ; g<=maxgoal ; g++) {
					asesdata->sclassgroups |= (1<<g);
				}
				asesdata->mintrashretention = get32bit(&ptr);
				asesdata->maxtrashretention = get32bit(&ptr);
			} else { // set defaults (no limits)
				asesdata->sclassgroups = 0x03FF; // all groups from 0 to 9
				asesdata->mintrashretention = 0;
				asesdata->maxtrashretention = UINT32_C(0xFFFFFFFF);
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
			asesdata->disables = 0;
			asesdata->info = NULL;
			asesdata->closed = 0;
//			asesdata->openedfiles = NULL;
			asesdata->disconnected = main_time();
			asesdata->nsocks = 0;
			asesdata->infopeerip = 0;
			asesdata->infoversion = 0;
			asesdata->exportscsum = 0;
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->chouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (statsinfile>SESSION_STATS) {
				ptr+=4*(statsinfile-SESSION_STATS);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->lhouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (asesdata->ileng>0) {
				asesdata->info = malloc(asesdata->ileng+1);
				passert(asesdata->info);
				if (fread(asesdata->info,asesdata->ileng,1,fd)!=1) {
					free(asesdata->info);
					free(asesdata);
					free(fsesrecord);
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, fread error");
					fclose(fd);
					return -1;
				}
				asesdata->info[asesdata->ileng]=0;
			}
			hpos = SESSION_HASH(asesdata->sessionid);
			asesdata->next = sessionshashtab[hpos];
			sessionshashtab[hpos] = asesdata;
		}
		if (ferror(fd)) {
			free(fsesrecord);
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
	}
	free(fsesrecord);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"sessions have been loaded");
	fclose(fd);
	return 1;
}

void sessions_import(void) {
	fprintf(stderr,"loading sessions ... ");
	fflush(stderr);
	switch (sessions_import_data()) {
		case 0:	// no file
			fprintf(stderr,"file not found\n");
			fprintf(stderr,"if it is not fresh installation then you have to restart all active mounts !!!\n");
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
}

static inline uint32_t sessions_data_session(uint8_t vmode,uint8_t *ptr,uint32_t now,session *sesdata) {
	uint32_t size;
	uint32_t pleng,i;

	if (vmode==0xFF) {
		size = 16 + sesdata->ileng;
		if (ptr!=NULL) {
			put32bit(&ptr,sesdata->sessionid);
			if (sesdata->infopeerip==0 && sesdata->peerip!=0) {
				put32bit(&ptr,sesdata->peerip);
			} else {
				put32bit(&ptr,sesdata->infopeerip);
			}
			if (sesdata->nsocks>0) {
				put32bit(&ptr,UINT32_C(0xFFFFFFFF));
			} else {
				if (sesdata->closed || sesdata->disconnected+SessionSustainTime<now) {
					put32bit(&ptr,0);
				} else {
					put32bit(&ptr,sesdata->disconnected+SessionSustainTime-now);
				}
			}
			put32bit(&ptr,sesdata->ileng);
			if (sesdata->ileng>0) {
				memcpy(ptr,sesdata->info,sesdata->ileng);
				ptr+=sesdata->ileng;
			}
		}
	} else {
		if (vmode<2) {
			if (sesdata->nsocks>0) {
				size = 37+SESSION_STATS*8+(vmode?10:0);
				size += sesdata->ileng;
				if (sesdata->rootinode==0) {
					size += 1;
				} else {
					size += fs_getdirpath_size(sesdata->rootinode);
				}
				if (ptr!=NULL) {
					put32bit(&ptr,sesdata->sessionid);
					put32bit(&ptr,sesdata->infopeerip);
					put32bit(&ptr,sesdata->infoversion);
					put32bit(&ptr,sesdata->ileng);
					if (sesdata->ileng>0) {
						memcpy(ptr,sesdata->info,sesdata->ileng);
						ptr+=sesdata->ileng;
					}
					if (sesdata->rootinode==0) { // meta
						pleng = 1;
						put32bit(&ptr,pleng);
						put8bit(&ptr,'.');
					} else {
						pleng = fs_getdirpath_size(sesdata->rootinode);
						put32bit(&ptr,pleng);
						if (pleng>0) {
							fs_getdirpath_data(sesdata->rootinode,ptr,pleng);
							ptr+=pleng;
						}
					}
					put8bit(&ptr,sesdata->sesflags);
					put32bit(&ptr,sesdata->rootuid);
					put32bit(&ptr,sesdata->rootgid);
					put32bit(&ptr,sesdata->mapalluid);
					put32bit(&ptr,sesdata->mapallgid);
					if (vmode) {
						put8bit(&ptr,0); // mingoal
						put8bit(&ptr,0); // maxgoal
						put32bit(&ptr,sesdata->mintrashretention);
						put32bit(&ptr,sesdata->maxtrashretention);
					}
					for (i=0 ; i<SESSION_STATS ; i++) {
						put32bit(&ptr,sesdata->chouropstats[i]);
					}
					for (i=0 ; i<SESSION_STATS ; i++) {
						put32bit(&ptr,sesdata->lhouropstats[i]);
					}
				}
			} else {
				size = 0;
			}
		} else {
			if (vmode<3) {
				size = 56+SESSION_STATS*8;
			} else if (vmode<4) {
				size = 58+SESSION_STATS*8;
			} else { // vmode 5,6
				size = 62+SESSION_STATS*8;
			}
			size += sesdata->ileng;
			if (sesdata->rootinode==0) {
				size += 1;
			} else {
				size += fs_getdirpath_size(sesdata->rootinode);
			}
			if (ptr!=NULL) {
				put32bit(&ptr,sesdata->sessionid);
				if (sesdata->infopeerip==0 && sesdata->peerip!=0) {
					put32bit(&ptr,sesdata->peerip);
				} else {
					put32bit(&ptr,sesdata->infopeerip);
				}
				put32bit(&ptr,sesdata->infoversion);
				put32bit(&ptr,of_noofopenedfiles(sesdata->sessionid));
				if (sesdata->nsocks>255) {
					put8bit(&ptr,255);
				} else {
					put8bit(&ptr,sesdata->nsocks);
				}
				if (sesdata->nsocks>0) {
					put32bit(&ptr,UINT32_C(0xFFFFFFFF));
				} else {
					if (sesdata->closed || sesdata->disconnected+SessionSustainTime<now) {
						put32bit(&ptr,0);
					} else {
						put32bit(&ptr,sesdata->disconnected+SessionSustainTime-now);
					}
				}
				put32bit(&ptr,sesdata->ileng);
				if (sesdata->ileng>0) {
					memcpy(ptr,sesdata->info,sesdata->ileng);
					ptr+=sesdata->ileng;
				}
				if (sesdata->rootinode==0) { // meta
					pleng = 1;
					put32bit(&ptr,pleng);
					put8bit(&ptr,'.');
				} else {
					pleng = fs_getdirpath_size(sesdata->rootinode);
					put32bit(&ptr,pleng);
					if (pleng>0) {
						fs_getdirpath_data(sesdata->rootinode,ptr,pleng);
						ptr+=pleng;
					}
				}
				put8bit(&ptr,sesdata->sesflags);
				if (vmode>=3) {
					put16bit(&ptr,sesdata->umaskval);
				}
				put32bit(&ptr,sesdata->rootuid);
				put32bit(&ptr,sesdata->rootgid);
				put32bit(&ptr,sesdata->mapalluid);
				put32bit(&ptr,sesdata->mapallgid);
				if (vmode>=5) {
					put16bit(&ptr,sesdata->sclassgroups);
				} else {
					put8bit(&ptr,0); // mingoal
					put8bit(&ptr,0); // maxgoal
				}
				put32bit(&ptr,sesdata->mintrashretention);
				put32bit(&ptr,sesdata->maxtrashretention);
				if (vmode>=4) {
					put32bit(&ptr,sesdata->disables);
				}
				for (i=0 ; i<SESSION_STATS ; i++) {
					put32bit(&ptr,sesdata->chouropstats[i]);
				}
				for (i=0 ; i<SESSION_STATS ; i++) {
					put32bit(&ptr,sesdata->lhouropstats[i]);
				}
			}
		}
	}
	return size;
}

uint32_t sessions_datasize(uint8_t vmode) {
	session *sesdata;
	uint32_t hpos;	
	uint32_t size;

	size = (vmode==0xFF)?0:2;
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (sesdata = sessionshashtab[hpos] ; sesdata != NULL ; sesdata = sesdata->next) {
			size += sessions_data_session(vmode,NULL,0,sesdata);
		}
	}
	return size;
}

void sessions_datafill(uint8_t *ptr,uint8_t vmode) {
	session *sesdata;
	uint32_t now;
	uint32_t hpos;

	now = main_time();
	if (vmode!=0xFF) {
		put16bit(&ptr,SESSION_STATS);
	}
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (sesdata = sessionshashtab[hpos] ; sesdata ; sesdata=sesdata->next) {
			ptr += sessions_data_session(vmode,ptr,now,sesdata);
		}
	}
}

static inline void* sessions_create_session(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	session *sesdata;
	uint32_t hpos;

	sesdata = (session*)malloc(sizeof(session));
	passert(sesdata);
	nextsessionid &= UINT32_C(0x7FFFFFFF);
	sesdata->sessionid = nextsessionid++;
	if (nextsessionid >= UINT32_C(0x80000000)) {
		nextsessionid = 1;
	}
	sesdata->exportscsum = exportscsum;
	sesdata->rootinode = rootinode;
	sesdata->sesflags = (sesflags&(~SESFLAG_METARESTORE));
	sesdata->umaskval = umaskval;
	sesdata->rootuid = rootuid;
	sesdata->rootgid = rootgid;
	sesdata->mapalluid = mapalluid;
	sesdata->mapallgid = mapallgid;
	sesdata->sclassgroups = sclassgroups;
	sesdata->mintrashretention = mintrashretention;
	sesdata->maxtrashretention = maxtrashretention;
	sesdata->disables = disables;
	sesdata->peerip = peerip;
	while (ileng>0 && info[ileng-1]==0) {
		ileng--;
	}
	if (ileng>0) {
		sesdata->info = malloc(ileng+1);
		passert(sesdata->info);
		memcpy(sesdata->info,info,ileng);
		sesdata->info[ileng]=0;
		sesdata->ileng = ileng;
	} else {
		sesdata->info = NULL;
		sesdata->ileng = 0;
	}
	sesdata->closed = 0;
//	sesdata->openedfiles = NULL;
	sesdata->disconnected = 0;
	sesdata->nsocks = 0;
	sesdata->infopeerip = 0;
	sesdata->infoversion = 0;
	memset(sesdata->chouropstats,0,4*SESSION_STATS);
	memset(sesdata->lhouropstats,0,4*SESSION_STATS);
	memset(sesdata->cminopstats,0,4*SESSION_STATS);
	memset(sesdata->lminopstats,0,4*SESSION_STATS);
	hpos = SESSION_HASH(sesdata->sessionid);
	sesdata->next = sessionshashtab[hpos];
	sessionshashtab[hpos] = sesdata;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%" PRIu32 "|SESADD(#%"PRIu64",%"PRIu32",%"PRIu8",0%03"PRIo16",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",0x%04"PRIX16",%"PRIu32",%"PRIu32",0x%08"PRIX32",%"PRIu32",%s):%"PRIu32,main_time(),exportscsum,rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,changelog_escape_name(ileng,(uint8_t*)info),sesdata->sessionid);
	} else {
		meta_version_inc();
	}
	return sesdata;
}

static inline uint8_t sessions_not_changed(session *sesdata,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	if (sesdata->rootinode!=rootinode) {
		return 0;
	}
	if (sesdata->exportscsum!=exportscsum) {
		return 0;
	}
	if (sesdata->sesflags!=sesflags) {
		return 0;
	}
	if (sesdata->umaskval!=umaskval) {
		return 0;
	}
	if (sesdata->rootuid!=rootuid || sesdata->rootgid!=rootgid) {
		return 0;
	}
	if (sesdata->mapalluid!=mapalluid || sesdata->mapallgid!=mapallgid) {
		return 0;
	}
	if (sesdata->sclassgroups!=sclassgroups) {
		return 0;
	}
	if (sesdata->mintrashretention!=mintrashretention || sesdata->maxtrashretention!=maxtrashretention) {
		return 0;
	}
	if (sesdata->disables!=disables) {
		return 0;
	}
	if (sesdata->peerip!=peerip) {
		return 0;
	}
	if (sesdata->ileng!=ileng) {
		return 0;
	}
	if ((sesdata->info!=NULL && info==NULL) || (sesdata->info==NULL && info!=NULL)) {
		return 0;
	}
	if (memcmp(sesdata->info,info,ileng)!=0) {
		return 0;
	}
	return 1;
}

static inline uint32_t sessions_change_session(session *sesdata,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	if (sessions_not_changed(sesdata,exportscsum,rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,info,ileng)) {
		return sesdata->sessionid;
	}
	sesdata->rootinode = rootinode;
	sesdata->exportscsum = exportscsum;
	sesdata->sesflags = (sesflags&(~SESFLAG_METARESTORE));
	sesdata->umaskval = umaskval;
	sesdata->rootuid = rootuid;
	sesdata->rootgid = rootgid;
	sesdata->mapalluid = mapalluid;
	sesdata->mapallgid = mapallgid;
	sesdata->sclassgroups = sclassgroups;
	sesdata->mintrashretention = mintrashretention;
	sesdata->maxtrashretention = maxtrashretention;
	sesdata->disables = disables;
	sesdata->peerip = peerip;
	if (sesdata->info!=NULL) {
		free(sesdata->info);
	}
	while (ileng>0 && info[ileng-1]==0) {
		ileng--;
	}
	if (ileng>0) {
		sesdata->info = malloc(ileng+1);
		passert(sesdata->info);
		memcpy(sesdata->info,info,ileng);
		sesdata->info[ileng]=0;
		sesdata->ileng = ileng;
	} else {
		sesdata->info = NULL;
		sesdata->ileng = 0;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%" PRIu32 "|SESCHANGED(%"PRIu32",#%"PRIu64",%"PRIu32",%"PRIu8",0%03"PRIo16",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",0x%04"PRIX16",%"PRIu32",%"PRIu32",0x%08"PRIX32",%"PRIu32",%s)",main_time(),sesdata->sessionid,exportscsum,rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,changelog_escape_name(ileng,info));
	} else {
		meta_version_inc();
	}
	return sesdata->sessionid;
}

void* sessions_new_session(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	session *sesdata;
	sesdata = sessions_create_session(exportscsum,rootinode,sesflags&(~SESFLAG_METARESTORE),umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,info,ileng);
	return sesdata;
}

uint8_t sessions_mr_sesadd(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng,uint32_t sessionid) {
	session *sesdata;
	sesdata = sessions_create_session(exportscsum,rootinode,sesflags|SESFLAG_METARESTORE,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,info,ileng);
	if (sesdata->sessionid!=sessionid) {
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint32_t sessions_chg_session(void *vsesdata,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	session *sesdata = (session*)vsesdata;
	return sessions_change_session(sesdata,exportscsum,rootinode,sesflags&(~SESFLAG_METARESTORE),umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,info,ileng);
}

uint8_t sessions_mr_seschanged(uint32_t sessionid,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng) {
	session *sesdata;
	sesdata = sessions_find_session(sessionid);
	if (sesdata==NULL) {
		return MFS_ERROR_MISMATCH;
	}
	sessions_change_session(sesdata,exportscsum,rootinode,sesflags|SESFLAG_METARESTORE,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peerip,info,ileng);
	return MFS_STATUS_OK;
}

uint8_t sessions_mr_sesdel(uint32_t sessionid) {
	session **sesdata,*asesdata;
	uint8_t status = MFS_ERROR_BADSESSIONID;
	uint32_t hpos;

	hpos = SESSION_HASH(sessionid);
	sesdata = sessionshashtab + hpos;
	while ((asesdata=*sesdata)) {
		if (asesdata->sessionid==sessionid) {
			sessions_clean_session(asesdata);
			*sesdata = asesdata->next;
			free(asesdata);
			status = MFS_STATUS_OK;
		} else {
			sesdata = &(asesdata->next);
		}
	}
	if (status==MFS_STATUS_OK) {
		meta_version_inc();
	}
	return status;
}

uint8_t sessions_mr_connected(uint32_t sessionid) {
	session *sesdata;

	for (sesdata = sessionshashtab[SESSION_HASH(sessionid)] ; sesdata ; sesdata = sesdata->next) {
		if (sesdata->sessionid == sessionid) {
			sesdata->disconnected = 0;
			meta_version_inc();
			return MFS_STATUS_OK;
		}
	}
	return MFS_ERROR_NOTFOUND;
}

uint8_t sessions_mr_disconnected(uint32_t sessionid,uint32_t disctime) {
	session *sesdata;

	for (sesdata = sessionshashtab[SESSION_HASH(sessionid)] ; sesdata ; sesdata = sesdata->next) {
		if (sesdata->sessionid == sessionid) {
			sesdata->disconnected = disctime;
			meta_version_inc();
			return MFS_STATUS_OK;
		}
	}
	return MFS_ERROR_NOTFOUND;
}

uint8_t sessions_mr_session(uint32_t sessionid) {
	if (sessionid!=nextsessionid) {
		return MFS_ERROR_MISMATCH;
	}
	nextsessionid++;
	meta_version_inc();
	return MFS_STATUS_OK;
}

void sessions_new(void) {
	nextsessionid=1;
}

uint32_t sessions_get_id(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->sessionid;
}

uint64_t sessions_get_exportscsum(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->exportscsum;
}

uint32_t sessions_get_peerip(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->peerip;
}

uint32_t sessions_get_rootinode(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->rootinode;
}

uint32_t sessions_get_sesflags(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	uint32_t sesflags;
	sesflags = sesdata->sesflags;
	if (sesdata->infoversion>=VERSION2INT(1,7,32)) {
		sesflags |= SESFLAG_ATTRBIT;
	}
	return sesflags;
}

uint16_t sessions_get_umask(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->umaskval;
}

uint32_t sessions_get_disables(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return sesdata->disables;
}

uint8_t sessions_is_root_remapped(void *vsesdata) {
	session *sesdata = (session*)vsesdata;
	return (sesdata->rootuid!=0)?1:0;
}

uint8_t sessions_check_sclass(void *vsesdata,uint8_t smode,uint8_t sclassid) {
	session *sesdata = (session*)vsesdata;
	switch (smode) {
		case SMODE_EXCHANGE:
		case SMODE_SET:
			if (((1<<sclass_get_export_group(sclassid))&sesdata->sclassgroups)==0) {
				return MFS_ERROR_EPERM;
			}
			break;
		case SMODE_INCREASE:
			return MFS_ERROR_EPERM;
		case SMODE_DECREASE:
			return MFS_ERROR_EPERM;
	}
	return MFS_STATUS_OK;
}

uint8_t sessions_check_trashretention(void *vsesdata,uint8_t smode,uint32_t trashretention) {
	session *sesdata = (session*)vsesdata;
	switch (smode) {
		case SMODE_EXCHANGE:
		case SMODE_SET:
			if (trashretention<sesdata->mintrashretention || trashretention>sesdata->maxtrashretention) {
				return MFS_ERROR_EPERM;
			}
			break;
		case SMODE_INCREASE:
			if (trashretention>sesdata->maxtrashretention) {
				return MFS_ERROR_EPERM;
			}
			break;
		case SMODE_DECREASE:
			if (trashretention<sesdata->mintrashretention) {
				return MFS_ERROR_EPERM;
			}
			break;
	}
	return MFS_STATUS_OK;
}

void sessions_inc_stats(void *vsesdata,uint8_t statid) {
	session *sesdata = (session*)vsesdata;
	if (sesdata && statid<SESSION_STATS) {
		sesdata->chouropstats[statid]++;
		sesdata->cminopstats[statid]++;
	}
}

void sessions_add_stats(void *vsesdata,uint8_t statid,uint64_t value) {
	session *sesdata = (session*)vsesdata;
	if (sesdata && statid<SESSION_STATS) {
		sesdata->chouropstats[statid]+=value;
		sesdata->cminopstats[statid]+=value;
	}
}

void sessions_ugid_remap(void *vsesdata,uint32_t *auid,uint32_t *agid) {
	session *sesdata = (session*)vsesdata;
	if (*auid==0) {
		*auid = sesdata->rootuid;
		if (agid) {
			*agid = sesdata->rootgid;
		}
	} else if (sesdata->sesflags&SESFLAG_MAPALL) {
		*auid = sesdata->mapalluid;
		if (agid) {
			*agid = sesdata->mapallgid;
		}
	}
}

void sessions_check(void) {
	session **sesdata,*asesdata;
	uint32_t now;
	uint32_t hpos;

	now = main_time();
	if (main_start_time()+120>now) {
		return;
	}
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		sesdata = sessionshashtab + hpos;
		while ((asesdata=*sesdata)) {
			if (asesdata->nsocks==0 && (asesdata->closed || asesdata->disconnected+SessionSustainTime<now)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"remove session: %u",asesdata->sessionid);
				sessions_clean_session(asesdata);
				changelog("%"PRIu32"|SESDEL(%"PRIu32")",main_time(),asesdata->sessionid);
				*sesdata = asesdata->next;
				free(asesdata);
			} else {
				sesdata = &(asesdata->next);
			}
		}
	}
}

uint8_t sessions_force_remove(uint32_t sessionid) {
	session **sesdata,*asesdata;
	uint32_t hpos;

	hpos = SESSION_HASH(sessionid);
	sesdata = sessionshashtab + hpos;
	while ((asesdata=*sesdata)) {
		if (asesdata->sessionid==sessionid) {
			if (asesdata->nsocks==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"remove session: %u",asesdata->sessionid);
				sessions_clean_session(asesdata);
				changelog("%"PRIu32"|SESDEL(%"PRIu32")",main_time(),asesdata->sessionid);
				*sesdata = asesdata->next;
				free(asesdata);
				return MFS_STATUS_OK;
			} else {
				return MFS_ERROR_ACTIVE;
			}
		} else {
			sesdata = &(asesdata->next);
		}
	}
	return MFS_ERROR_NOTFOUND;
}

void sessions_statsmove(void) {
	session *sesdata;
	uint32_t hpos;
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (sesdata = sessionshashtab[hpos] ; sesdata ; sesdata=sesdata->next) {
			memcpy(sesdata->lhouropstats,sesdata->chouropstats,4*SESSION_STATS);
			memset(sesdata->chouropstats,0,4*SESSION_STATS);
		}
	}
}

void sessions_infostats_shift(void) {
	session *sesdata;
	uint32_t hpos;
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (sesdata = sessionshashtab[hpos] ; sesdata ; sesdata=sesdata->next) {
			memcpy(sesdata->lminopstats,sesdata->cminopstats,4*SESSION_STATS);
			memset(sesdata->cminopstats,0,4*SESSION_STATS);
		}
	}
}

static void sessions_info_session(FILE *fd,session *sesdata) {
	uint32_t i,c;
	uint32_t ip;
	char strip[STRIPSIZE];
	char *iptr;
	if (sesdata->infopeerip==0 && sesdata->peerip!=0) {
		ip = sesdata->peerip;
	} else {
		ip = sesdata->infopeerip;
	}
	univmakestrip(strip,ip);
	if (sesdata->info==NULL) {
		iptr = "<NULL>";
	} else {
		iptr = (char*)(sesdata->info);
	}
	for (i=0 ; i<SESSION_STATS ; i++) {
		c = sesdata->lminopstats[i];
		if (c>0) {
			fprintf(fd,"session ip:%s mount_point:%s operation:%s count:%"PRIu32"\n",strip,iptr,opname[i],c);
		}
	}
}

void sessions_info(FILE *fd) {
	session *sesdata;
	uint32_t hpos;

	fprintf(fd,"[session ops]\n");
	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (sesdata = sessionshashtab[hpos] ; sesdata != NULL ; sesdata=sesdata->next) {
			sessions_info_session(fd,sesdata);
		}
	}
	fprintf(fd,"\n");
}

void sessions_cleanup(void) {
	session *ss,*ssn;
	uint32_t hpos;
//	filelist *of,*ofn;

	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		for (ss = sessionshashtab[hpos] ; ss ; ss = ssn) {
			ssn = ss->next;
			if (ss->info) {
				free(ss->info);
			}
			free(ss);
		}
		sessionshashtab[hpos] = NULL;
	}
}

void sessions_reload(void) {
	SessionSustainTime = cfg_getsperiod("SESSION_SUSTAIN_TIME","1d");
	if (SessionSustainTime>7*86400) {
		SessionSustainTime=7*86400;
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"SESSION_SUSTAIN_TIME too big (more than week) - setting this value to one week");
	}
	if (SessionSustainTime<60) {
		SessionSustainTime=60;
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"SESSION_SUSTAIN_TIME too low (less than minute) - setting this value to one minute");
	}
}

int sessions_init(void) {
	uint32_t hpos;

	for (hpos = 0 ; hpos < SESSION_HASHSIZE ; hpos++) {
		sessionshashtab[hpos] = NULL;
	}
	sessions_reload();
	main_time_register(10,0,sessions_check);
	main_time_register(3600,0,sessions_statsmove);
	main_time_register(60,0,sessions_infostats_shift);
	main_info_register(sessions_info);
	main_reload_register(sessions_reload);
	return 0;
}
