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

#include <stdio.h>
#include <stdarg.h>
#include <syslog.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "changelog.h"
#include "metadata.h"
#include "massert.h"
#include "bgsaver.h"
#include "main.h"
#include "slogger.h"
#include "matomlserv.h"
#include "cfg.h"

#define MAXLOGLINESIZE 200000U
#define MAXLOGNUMBER 1000U
static uint32_t BackLogsNumber;
static FILE *currentfd;


#define OLD_CHANGES_BLOCK_SIZE 5000

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

static uint16_t ChangelogSecondsToRemember;

static uint8_t ChangelogSaveMode;

#define SAVEMODE_BACKGROUND 0
#define SAVEMODE_ASYNC 1
#define SAVEMODE_SYNC 2

static inline void changelog_old_changes_free_block(old_changes_block *oc) {
	uint32_t i;
	for (i=0 ; i<oc->entries ; i++) {
		free(oc->old_changes_block[i].data);
	}
	free(oc);
}

static inline void changelog_store_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	old_changes_block *oc;
	old_changes_entry *oce;
	uint32_t ts;

	matomlserv_broadcast_logstring(version,logstr,logstrsize);
//	matomaserv_broadcast_logstring(version,(uint8_t*)printbuff,leng);

	if (ChangelogSecondsToRemember==0) {
		while (old_changes_head) {
			oc = old_changes_head->next;
			changelog_old_changes_free_block(old_changes_head);
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
			changelog_old_changes_free_block(old_changes_head);
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

uint32_t changelog_get_old_changes(uint64_t version,void (*sendfn)(void *,uint64_t,uint8_t *,uint32_t),void *userdata,uint32_t limit) {
	old_changes_block *oc;
	old_changes_entry *oce;
	uint8_t start=0;
	uint32_t i,j;

	j=0;
	for (oc=old_changes_head ; oc ; oc=oc->next) {
		if (oc->minversion<=version && (oc->next==NULL || oc->next->minversion>version)) {
			start=1;
		}
		if (start) {
			for (i=0 ; i<oc->entries ; i++) {
				oce = oc->old_changes_block + i;
				if (version<=oce->version) {
					if (j<limit) {
						sendfn(userdata,oce->version,oce->data,oce->length);
						j++;
					} else {
						return j;
					}
				}
			}
		}
	}
	return j;
}

uint64_t changelog_get_minversion(void) {
	if (old_changes_head==NULL) {
		return meta_version();
	}
	return old_changes_head->minversion;
}

void changelog_rotate() {
	if (ChangelogSaveMode==0) {
		bgsaver_rotatelog();
	} else {
		char logname1[100],logname2[100];
		uint32_t i;
		if (currentfd) {
			if (ChangelogSaveMode==2) {
				fsync(fileno(currentfd));
			}
			fclose(currentfd);
			currentfd=NULL;
		}
		if (BackLogsNumber>0) {
			for (i=BackLogsNumber ; i>0 ; i--) {
				snprintf(logname1,100,"changelog.%"PRIu32".mfs",i);
				snprintf(logname2,100,"changelog.%"PRIu32".mfs",i-1);
				rename(logname2,logname1);
			}
		} else {
			unlink("changelog.0.mfs");
		}
	}
	matomlserv_broadcast_logrotate();
}

void changelog_mr(uint64_t version,const char *data) {
	if (ChangelogSaveMode==0) {
		bgsaver_changelog(version,data);
	} else {
		if (currentfd==NULL) {
			currentfd = fopen("changelog.0.mfs","a");
			if (!currentfd) {
				syslog(LOG_NOTICE,"lost MFS change %"PRIu64": %s",version,data);
			}
		}

		if (currentfd) {
			fprintf(currentfd,"%"PRIu64": %s\n",version,data);
			fflush(currentfd);
			if (ChangelogSaveMode==2) {
				fsync(fileno(currentfd));
			}
		}
	}
}

void changelog(const char *format,...) {
	static char printbuff[MAXLOGLINESIZE];
	va_list ap;
	uint32_t leng;

	uint64_t version = meta_version_inc();

	va_start(ap,format);
	leng = vsnprintf(printbuff,MAXLOGLINESIZE,format,ap);
	va_end(ap);
	if (leng>=MAXLOGLINESIZE) {
		printbuff[MAXLOGLINESIZE-1]='\0';
		leng=MAXLOGLINESIZE;
	} else {
		leng++;
	}

	changelog_mr(version,printbuff);
	changelog_store_logstring(version,(uint8_t*)printbuff,leng);
}

char* changelog_generate_gids(uint32_t gids,uint32_t *gid) {
	static char *gidstr = NULL;
	static uint32_t gidstr_size = 0;
	uint32_t i,l;

	i = ((gids/32)+1)*32;
	i *= 11;
	i += 10;
	if (i>gidstr_size || gidstr==NULL) {
		if (gidstr!=NULL) {
			free(gidstr);
		}
		gidstr = malloc(i);
		passert(gidstr);
		gidstr_size = i;
	}
	l = 0;
	gidstr[l++] = '[';
	for (i=0 ; i<gids ; i++) {
		if (l<gidstr_size) {
			l += snprintf(gidstr+l,gidstr_size-l,"%"PRIu32,gid[i]);
		}
		if (l<gidstr_size) {
			gidstr[l++] = (i+1<gids)?',':']';
		}
	}
	if (l<gidstr_size) {
		gidstr[l++]='\0';
	}
	return gidstr;
}

char* changelog_escape_name(uint32_t nleng,const uint8_t *name) {
	static char *escname[2]={NULL,NULL};
	static uint32_t escnamesize[2]={0,0};
	static uint8_t buffid=0;
	char *currescname=NULL;
	uint32_t i;
	uint8_t c;
	buffid = 1-buffid;
	i = nleng;
	i = i*3+1;
	if (i>escnamesize[buffid] || i==0) {
		escnamesize[buffid] = ((i/1000)+1)*1000;
		if (escname[buffid]!=NULL) {
			free(escname[buffid]);
		}
		escname[buffid] = malloc(escnamesize[buffid]);
		passert(escname[buffid]);
	}
	i = 0;
	currescname = escname[buffid];
	passert(currescname);
	while (nleng>0) {
		c = *name;
		if (c<32 || c>=127 || c==',' || c=='%' || c=='(' || c==')') {
			currescname[i++]='%';
			currescname[i++]="0123456789ABCDEF"[(c>>4)&0xF];
			currescname[i++]="0123456789ABCDEF"[c&0xF];
		} else {
			currescname[i++]=c;
		}
		name++;
		nleng--;
	}
	currescname[i]=0;
	return currescname;
}


void changelog_reload(void) {
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	if (BackLogsNumber>MAXLOGNUMBER) {
		mfs_syslog(LOG_WARNING,"BACK_LOGS value too big !!!");
		BackLogsNumber = MAXLOGNUMBER;
	}
	ChangelogSecondsToRemember = cfg_getuint16("CHANGELOG_PRESERVE_SECONDS",1800);
	if (ChangelogSecondsToRemember>15000) {
		mfs_arg_syslog(LOG_WARNING,"Number of seconds of change logs to be preserved in master is too big (%"PRIu16") - decreasing to 15000 seconds",ChangelogSecondsToRemember);
		ChangelogSecondsToRemember=15000;
	}
	ChangelogSaveMode = cfg_getuint8("CHANGELOG_SAVE_MODE",0);
	if (ChangelogSaveMode>2) {
		mfs_syslog(LOG_WARNING,"CHANGELOG_SAVE_MODE - wrong value - using 0 (write in background)");
		ChangelogSaveMode = 0;
	}
}

int changelog_init(void) {
	changelog_reload();
	main_reload_register(changelog_reload);
	currentfd = NULL;
	return 0;
}

uint64_t changelog_findfirstversion(const char *fname) {
	uint8_t buff[50];
	int32_t s,p;
	uint64_t fv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	s = read(fd,buff,50);
	close(fd);
	if (s<=0) {
		return 0;
	}
	fv = 0;
	p = 0;
	while (p<s && buff[p]>='0' && buff[p]<='9') {
		fv *= 10;
		fv += buff[p]-'0';
		p++;
	}
	if (p>=s || buff[p]!=':') {
		return 0;
	}
	return fv;
}

uint64_t changelog_findlastversion(const char *fname) {
	struct stat st;
	uint8_t buff[32800]; // 32800 = 32768 + 32
	uint64_t size;
	uint32_t buffpos;
	uint64_t lastnewline,lv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	fstat(fd,&st);
	size = st.st_size;
	memset(buff,0,32);
	lastnewline = 0;
	while (size>0 && size+200000>(uint64_t)(st.st_size)) {
		if (size>32768) {
			memcpy(buff+32768,buff,32);
			size-=32768;
			lseek(fd,size,SEEK_SET);
			if (read(fd,buff,32768)!=32768) {
				close(fd);
				return 0;
			}
			buffpos = 32768;
		} else {
			memmove(buff+size,buff,32);
			lseek(fd,0,SEEK_SET);
			if (read(fd,buff,size)!=(ssize_t)size) {
				close(fd);
				return 0;
			}
			buffpos = size;
			size = 0;
		}
		// size = position in file of first byte in buff
		// buffpos = position of last byte in buff to search
		while (buffpos>0) {
			buffpos--;
			if (buff[buffpos]=='\n' || (size + buffpos)==0) {
				if (lastnewline==0) {
					lastnewline = size + buffpos;
				} else {
					if (lastnewline+1 != (uint64_t)(st.st_size)) { // garbage at the end of file
						close(fd);
						return 0;
					}
					if ((size + buffpos)>0) {
						buffpos++;
					}
					lv = 0;
					while (buffpos<32800 && buff[buffpos]>='0' && buff[buffpos]<='9') {
						lv *= 10;
						lv += buff[buffpos]-'0';
						buffpos++;
					}
					if (buffpos==32800 || buff[buffpos]!=':') {
						lv = 0;
					}
					close(fd);
					return lv;
				}
			}
		}
	}
	close(fd);
	return 0;
}

int changelog_checkname(const char *fname) {
	const char *ptr = fname;
	if (strncmp(ptr,"changelog.",10)==0) {
		ptr+=10;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml.",13)==0) {
		ptr+=13;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml_back.",18)==0) {
		ptr+=18;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	}
	return 0;
}
