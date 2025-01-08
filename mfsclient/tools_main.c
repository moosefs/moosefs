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
#include <limits.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>

#include "labelparser.h"
#include "datapack.h"
#include "strerr.h"
#include "mfsstrerr.h"

// tools modules:
#include "tools_main.h"
#include "tools_trashadmin.h"

#define COMM_PREFIX "mfs"
#define COMM_PREFIX_LEN 3


#define INODE_VALUE_MASK        0x1FFFFFFF
#define INODE_TYPE_MASK         0x60000000
#define INODE_TYPE_TRASH        0x20000000
#define INODE_TYPE_SUSTAINED    0x40000000
#define INODE_TYPE_SPECIAL      0x00000000


//--------------------
//	Global Variables:
//--------------------
const char id[]="@(#) version: " VERSSTR ", written by Jakub Kruszona-Zawadzki";
static int current_master = -1;
static uint32_t current_masterip = 0;
static uint16_t current_masterport = 0;
static uint16_t current_mastercuid = 0;
static uint32_t current_masterversion = 0;
static uint64_t current_masterprocessid = 0;


static uint8_t  reqmasterversion = ~0;
//static uint8_t	humode=0;
//static uint8_t	numbermode=0;

//--------------------
void mfstoolshlp(void);
void mfstoolsexe(cv_t *cv);

uint32_t getmasterversion (void) { return current_masterversion; }

void dirname_inplace(char *path) {					//snapshot
	char *endp;

	if (path==NULL) {
		return;
	}
	if (path[0]=='\0') {
		path[0]='.';
		path[1]='\0';
		return;
	}

	// Strip trailing slashes
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	// Find the start of the dir
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0]!='/') {
			path[0]='.';
		}
		path[1]='\0';
		return;
	} else {
		*endp = '\0';
	}
}

int master_register(int rfd) {
#define PAYLOAD 73
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr,regbuff[8+PAYLOAD];

	wptr = regbuff;
	put32bit(&wptr, CLTOMA_FUSE_REGISTER);
	put32bit(&wptr, PAYLOAD);

	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr += 64;

	put8bit(&wptr,REGISTER_TOOLS);
	put32bit(&wptr,current_mastercuid);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);

	if (tcpwrite(rfd,regbuff,8+PAYLOAD) != 8+PAYLOAD) {
		printf("register to master: send error\n");
		goto error;
	}
	if (tcpread(rfd,regbuff,9) != 9) {
		printf("register to master: receive error\n");
		goto error;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		goto error;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		printf("register to master: wrong answer (length)\n");
		goto error;
	}
	if (*rptr) {
		printf("register to master: %s\n",mfsstrerr(*rptr));
		goto error;
	}

//ok:
	return 0;

error:
	return -1;
#undef PAYLOAD
}

int master_socket(void) {
	return current_master;
}

int master_reconnect(void) {
	int sd;
	uint8_t cnt;

	if (current_master>=0) {
		close(current_master);
		current_master = -1;
	}
	cnt = 0;
	while (cnt<10) {
		sd = tcpsocket();
		if (sd<0) {
			printf("can't create connection socket: %s\n",strerr(errno));
			return -1;
		}
		tcpreuseaddr(sd);
		tcpnumbind(sd,0,0);
		if (tcpnumtoconnect(sd, current_masterip, current_masterport, (cnt%2) ? (300*(1<<(cnt>>1))) : (200*(1<<(cnt>>1))))<0) {
			cnt++;
			if (cnt==10) {
				printf("can't connect to master: %s\n", strerr(errno));
				return -1;
			}
			tcpclose(sd);
		} else {
			cnt = 10;
		}
	}
	tcpnodelay(sd);
	if (current_masterversion<VERSION2INT(2,0,0)) {
		if (master_register(sd)<0) {
			printf("can't register to master\n");
			return -1;
		}
	}
	current_master = sd;
	return 0;
}

void reset_master(void) {
	if (current_master<0) {
		return;
	}
	close(current_master);
	current_master = -1;
	current_masterprocessid = 0;
}

int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, uint64_t *leng, uint8_t needsamedev, uint8_t needrwfs) {
	char rpath[PATH_MAX+1];
	struct stat stb;
	struct statvfs stvfsb;
	int fd;
	uint8_t masterinfo[22];
	const uint8_t *miptr;
	uint32_t pinode;
	uint64_t master_processid;
	int rpathlen;

	rpath[0]=0;
	if (realpath(name,rpath)==NULL) {
		printf("%s: realpath error on (%s): %s\n", name, rpath, strerr(errno));
		return -1;
	}
//	p = rpath;
	if (needrwfs) {
		if (statvfs(rpath,&stvfsb)!=0) {
			printf("%s: (%s) statvfs error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		if (stvfsb.f_flag&ST_RDONLY) {
			printf("%s: (%s) Read-only file system\n", name, rpath);
			return -1;
		}
	}
	if (lstat(rpath,&stb)!=0) {
		printf("%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
		return -1;
	}
	pinode = stb.st_ino;
	if (inode!=NULL) {
		*inode = pinode;
	}
	if (mode!=NULL) {
		*mode = stb.st_mode;
	}
	if (leng!=NULL) {
		*leng = stb.st_size;
	}
	for (;;) {
		rpathlen = strlen(rpath);
		if (rpathlen+strlen("/.masterinfo")<PATH_MAX) {
			strcpy(rpath+rpathlen,"/.masterinfo");
			if (lstat(rpath,&stb)==0) {
				if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0 && (stb.st_size==10 || stb.st_size==14 || stb.st_size==22)) {
					if (stb.st_ino==0x7FFFFFFE && inode!=NULL) {	// meta master
						if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_SUSTAINED) {
							printf("%s: only files in 'trash' and 'sustained' are usable in mfsmeta\n",name);
							return -1;
						}
						(*inode)&=INODE_VALUE_MASK;
					}
					fd = open(rpath,O_RDONLY);
					if (stb.st_size==10) {
						if (read(fd,masterinfo,10)!=10) {
							printf("%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					} else if (stb.st_size==14) {
						if (read(fd,masterinfo,14)!=14) {
							printf("%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					} else if (stb.st_size==22) {
						if (read(fd,masterinfo,22)!=22) {
							printf("%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					}
					close(fd);
					miptr = masterinfo;
					current_masterip   = get32bit(&miptr);
					current_masterport = get16bit(&miptr);
					current_mastercuid = get32bit(&miptr);
					if (stb.st_size>=14) {
						current_masterversion = get32bit(&miptr);
					} else {
						current_masterversion = 0;
					}
					if (stb.st_size>=22) {
						master_processid = get64bit(&miptr);
					} else {
						master_processid = stb.st_dev;
					}
					if (current_master>=0) {
						if (current_masterprocessid==master_processid) {
							return 0;
						}
						if (needsamedev) {
							printf("%s: different master process id\n",name);
							return -1;
						}
					}
					if (current_master>=0) {
						close(current_master);
						current_master=-1;
					}
					current_masterprocessid = master_processid;
					if (current_masterip==0 || current_masterport==0 || current_mastercuid==0) {
						printf("%s: incorrect '.masterinfo'\n",name);
						return -1;
					}
					return master_reconnect();
				}
			} else if (pinode==1) { // this is root inode - if there is no .masterinfo here then it is not MFS.
				printf("%s: not MFS object\n", name);
				return -1;
			}
		} else if (pinode==1) { // found root inode, but path is still to long - give up
			printf("%s: path too long\n", name);
			return -1;
		}
		rpath[rpathlen]='\0';
		if (rpath[0]!='/' || rpath[1]=='\0') { // went to '/' without success - this is not MFS
			printf("%s: not MFS object\n", name);
			return -1;
		}
		dirname_inplace(rpath);
		if (lstat(rpath,&stb)!=0) {
			printf("%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		pinode = stb.st_ino;
	}
	return -1;
}

void print_lines(const char** line) {
	while (*line != NULL) {
		fprintf(stderr,"%s\n",*line++);
	}
}

void checkminarglist(cv_t *cv, int n) {
	//if (cv->completion_mode || cv->argc >= n) return;
	if (cv->argc >= n) return;
	printf("missing argument(s)\n\n");
	usagefull(cv);	//usagesimple(cv);
}

void usagesimple(cv_t *cv) {
	fprintf(stderr,"For more info use '-?' option.\n%s -?\n", cv->comm->name);
	exit(1);
}


int command_str_match(const char *str, const char *name) {
	while (*str) {
		if (tolower(*str++) != tolower(*name++)) {
			return -1;
		}
	}
	return 0;
}

mfstcommand_t *findcommbyname(const char *str, mfstcommand_t *commstab) {
	mfstcommand_t *lastfound = NULL;
	const char *comm;

	comm = str+strlen(str);
	while (comm >= str && *comm!= '/') { //skip path
		comm--;
	}
	comm++;
	if (!command_str_match(COMM_PREFIX, comm)) {
		comm+= COMM_PREFIX_LEN;
	}

	while (commstab->name!=NULL) {
		if (command_str_match(comm, commstab->name)==0) {
			if (lastfound) {
				printf("error: '%s' is ambiguous - please, give me more characters.\n", comm);
				return NULL;
			}
			lastfound = commstab;
		}
		commstab++;
	}

	return lastfound;
}

void usagefull(cv_t *cv) {
	if (cv->comm->hlpfun != NULL) {
		cv->comm->hlpfun();
	} else {
		fprintf(stderr,"Help for '%s' not found!\n", cv->comm->name);
	}
	exit(1);
}

void exeviatable(cv_t *cv, char *notfoundstr) {
	if (strcmp(cv->argv[0], "-?") == 0) {
		usagefull(cv);
	}
	cv->comm = findcommbyname(cv->argv[0], cv->commstab);
	if (cv->comm == NULL) {
		fprintf(stderr,"%s '%s'\n", notfoundstr, cv->argv[0]);
		exit(1);
	} else {
/*
		if(cv->argc<2 && cv->comm->flags&REQPARAM_F){
			usagefull(cv);
		} else // */
		if (cv->comm->exefun!=NULL) {
			reqmasterversion = cv->comm->minmasterver;
			// todo: ad: use minmasterserver, and it should be checked before completion
			(cv->comm->exefun)(cv);
		} else {
			fprintf(stderr,"command '%s' not implemented yet!\n", cv->comm->name);
			exit(1);
		}
	}
}

mfstcommand_t toolCommands[]={
//    name,       mastReqVer,exefun,               hlpfun,               flags
	{"trashtool",         3, mfstoolsexe,          mfstoolshlp,          HIDE_F},		//|REQPARAM_F
// TRASH CONTENT MANAGEMENT TOOLS (new v4)
	{"trashlist",         4, trashlistexe,         trashlisthlp,         NEWGRP_F},
	{"trashrecover",      4, trashrecoverexe,      trashrecoverhlp,      0},
	{"trashpurge",        4, trashpurgeexe,        trashpurgehlp,        0},
	{"sustainedlist",     4, sustainedexe,         sustainedhlp,         0},
// end of table!
	{NULL,               ~0, NULL,                 NULL,                 0}
};

void listcommands(mfstcommand_t *comm) {
	while (comm->name) {
		if (!(comm->flags&HIDE_F)) {
			printf("%s%s%s",
				(comm->flags&NEWGRP_F) ? "\n\n\t" : (comm->flags&TWINCOMM_F) ? " / " : "\n\t",
			 comm->name, (comm->flags&OBSOLETE_F) ? " - obsolete, will be removed soon" : "");
		}
		comm++;
	}
	putchar('\n');
}

static const char
	*mfstoolstxt1[] = {
		"mfs trash tool - unofficial, will be changed, do not use unless you have to",
		"",
		"usage:",
		"\tmfstrashtool create - create symlinks (mfs<toolname> -> mfstrashtool [mfs]<toolname>)",
		"\tmfstrashtool [mfs]<toolname> ... - works as a given tool",
		"",
		"tools:",
		NULL
	};
void mfstoolshlp(void) {
	print_lines(mfstoolstxt1);
	listcommands(toolCommands);
}

void mfstoolsexe(cv_t *cv) {
	mfstcommand_t *tab = toolCommands;
	char symlinkErrRap[50]="error creating symlink '"COMM_PREFIX;
#define symlinkstr symlinkErrRap+24
#define nameptr	symlinkErrRap+24+COMM_PREFIX_LEN

	checkminarglist(cv, 2);
	if (*cv->argv[1] == '-') {
//		parse_options_help(cv);
		usagefull(cv);
	}
	if (cv->argc == 2 && strcmp(cv->argv[1], "create") == 0) {
		fprintf(stderr,"create symlinks\n");
		while (tab->name) {
			if (!(tab->flags&(HIDE_F))) {// (HIDE_F|OBSOLETE_F)
				strcpy(nameptr, tab->name);
				if (symlink(cv->argv[0], symlinkstr) < 0) {
					strcat(nameptr, "'");
					perror(symlinkErrRap);
				} else {
					fprintf(stderr,"\tlink '%s' done\n", symlinkstr);
				}
			}
			tab++;
		}
		return;
	}
	cv->argv++;
	cv->argc--;
	cv->commstab = toolCommands;
	exeviatable(cv, "unknown binary name");		// subcommand execution
}

int main(int argc,char **argv) {
	cv_t cv;

	memset(&cv, 0, sizeof(cv));

	cv.argc = argc;
	cv.argv = argv;
	strerr_init();
	cv.commstab = toolCommands;

	exeviatable(&cv, "unknown binary name:");
	return cv.status;
}
