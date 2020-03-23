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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <errno.h>

#include "labelparser.h"
#include "datapack.h"
#include "massert.h"
// #include "strerr.h"
#include "mfsstrerr.h"
#include "sockets.h"
#include "hashfn.h"
#include "clocks.h"
#include "mfsalloc.h"
#include "md5.h"
#include "MFSCommunication.h"

#define tcpread(s,b,l) tcptoread(s,b,l,20000)
#define tcpwrite(s,b,l) tcptowrite(s,b,l,20000)

#define INODE_VALUE_MASK 0x1FFFFFFF
#define INODE_TYPE_MASK 0x60000000
#define INODE_TYPE_TRASH 0x20000000
#define INODE_TYPE_SUSTAINED 0x40000000
#define INODE_TYPE_SPECIAL 0x00000000


static void dirname_inplace(char *path) {
	char *endp;

	if (path==NULL) {
		return;
	}
	if (path[0]=='\0') {
		path[0]='.';
		path[1]='\0';
		return;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0]=='/') {
			path[1]='\0';
		} else {
			path[0]='.';
			path[1]='\0';
		}
		return;
	} else {
		*endp = '\0';
	}
}

static int master_register(int rfd,uint32_t cuid) {
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr,regbuff[8+73];

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,73);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_TOOLS);
	put32bit(&wptr,cuid);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	if (tcpwrite(rfd,regbuff,8+73)!=8+73) {
		printf("register to master: send error\n");
		return -1;
	}
	if (tcpread(rfd,regbuff,9)!=9) {
		printf("register to master: receive error\n");
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		return -1;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		printf("register to master: wrong answer (length)\n");
		return -1;
	}
	if (*rptr) {
		printf("register to master: %s\n",mfsstrerr(*rptr));
		return -1;
	}
	return 0;
}

typedef struct _master_conn {
	dev_t device;
	uint32_t masterversion;
	uint32_t masterip;
	uint16_t masterport;
	uint32_t mastercuid;

	int fd;
	uint32_t sbuffsize,rbuffsize;
	uint8_t *sbuff,*rbuff;
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t pleng;
	uint8_t err;
} master_conn;

static master_conn *mc;

static int master_connect(void) {
	uint8_t cnt;
	cnt=0;
	while (cnt<10) {
		mc->fd = tcpsocket();
		if (mc->fd<0) {
			printf("can't create connection socket: %s\n",strerr(errno));
			return -1;
		}
		tcpreuseaddr(mc->fd);
		tcpnumbind(mc->fd,0,0);
		if (tcpnumtoconnect(mc->fd,mc->masterip,mc->masterport,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
			tcpclose(mc->fd);
			mc->fd = -1;
			cnt++;
			if (cnt==10) {
				printf("can't connect to master (.masterinfo): %s\n",strerr(errno));
				return -1;
			}
		} else {
			cnt=10;
		}
	}
	tcpnodelay(mc->fd);
	if (master_register(mc->fd,mc->mastercuid)<0) {
		printf("can't register to master (.masterinfo)\n");
		tcpclose(mc->fd);
		mc->fd = -1;
		return -1;
	}
	return 0;
}

int master_prepare_conn(const char *name,uint32_t *inode,mode_t *mode,uint64_t *leng,uint8_t needsamedev,uint8_t needrwfs) {
	char rpath[PATH_MAX+1];
	struct stat stb;
	struct statvfs stvfsb;
	int sd;
	uint8_t masterinfo[14];
	const uint8_t *miptr;
	uint32_t pinode;
	int rpathlen;

	rpath[0]=0;
	if (realpath(name,rpath)==NULL) {
		printf("%s: realpath error on (%s): %s\n",name,rpath,strerr(errno));
		return -1;
	}
//	p = rpath;
	if (needrwfs) {
		if (statvfs(rpath,&stvfsb)!=0) {
			printf("%s: (%s) statvfs error: %s\n",name,rpath,strerr(errno));
			return -1;
		}
		if (stvfsb.f_flag&ST_RDONLY) {
			printf("%s: (%s) Read-only file system\n",name,rpath);
			return -1;
		}
	}
	if (lstat(rpath,&stb)!=0) {
		printf("%s: (%s) lstat error: %s\n",name,rpath,strerr(errno));
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
	if (mc->fd>=0) {
		if (mc->device==stb.st_dev) {
			return 0;
		}
		if (needsamedev) {
			printf("%s: different device\n",name);
			return -1;
		}
	}
	if (mc->fd>=0) {
		close(mc->fd);
		mc->fd=-1;
	}
	mc->device = stb.st_dev;
	for(;;) {
		rpathlen = strlen(rpath);
		if (rpathlen+strlen("/.masterinfo")<PATH_MAX) {
			strcpy(rpath+rpathlen,"/.masterinfo");
			if (lstat(rpath,&stb)==0) {
				if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0 && (stb.st_size==10 || stb.st_size==14)) {
					if (stb.st_ino==0x7FFFFFFE && inode!=NULL) {	// meta master
						if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_SUSTAINED) {
							printf("%s: only files in 'trash' and 'sustained' are usable in mfsmeta\n",name);
							return -1;
						}
						(*inode)&=INODE_VALUE_MASK;
					}
					sd = open(rpath,O_RDONLY);
					if (stb.st_size==10) {
						if (read(sd,masterinfo,10)!=10) {
							printf("%s: can't read '.masterinfo'\n",name);
							close(sd);
							return -1;
						}
					} else if (stb.st_size==14) {
						if (read(sd,masterinfo,14)!=14) {
							printf("%s: can't read '.masterinfo'\n",name);
							close(sd);
							return -1;
						}
					}
					close(sd);
					miptr = masterinfo;
					mc->masterip = get32bit(&miptr);
					mc->masterport = get16bit(&miptr);
					mc->mastercuid = get32bit(&miptr);
					if (stb.st_size==14) {
						mc->masterversion = get32bit(&miptr);
					} else {
						mc->masterversion = 0;
					}
					if (mc->masterip==0 || mc->masterport==0 || mc->mastercuid==0) {
						printf("%s: incorrect '.masterinfo'\n",name);
						return -1;
					}
					return 0;
				}
			} else if (pinode==1) { // this is root inode - if there is no .masterinfo here then it is not MFS.
				printf("%s: not MFS object\n",name);
				return -1;
			}
		} else if (pinode==1) { // found root inode, but path is still to long - give up
			printf("%s: path too long\n",name);
			return -1;
		}
		rpath[rpathlen]='\0';
		if (rpath[0]!='/' || rpath[1]=='\0') { // went to '/' without success - this is not MFS
			printf("%s: not MFS object\n",name);
			return -1;
		}
		dirname_inplace(rpath);
		if (lstat(rpath,&stb)!=0) {
			printf("%s: (%s) lstat error: %s\n",name,rpath,strerr(errno));
			return -1;
		}
		pinode = stb.st_ino;
	}
	return -1;
}

static void master_close_conn(int err) {
	if (mc->fd<0) {
		return;
	}
	if (err) {
		close(mc->fd);
		mc->fd = -1;
		mc->device = 0;
	}
}

uint32_t master_get_version(void) {
	return mc->masterversion;
}

void master_init(void) {
	mc = malloc(sizeof(master_conn));
	passert(mc);

	mc->device = 0;
	mc->masterversion = 0;
	mc->masterip = 0;
	mc->masterport = 0;
	mc->mastercuid = 0;
	mc->fd = -1;
	mc->sbuffsize = 0;
	mc->rbuffsize = 0;
	mc->sbuff = NULL;
	mc->rbuff = NULL;
	mc->wptr = NULL;
	mc->rptr = NULL;
	mc->err = 0;
}

void master_error(void) {
	close(mc->fd);
	mc->fd = -1;
	mc->device = 0;
	mc->masterversion = 0;
	mc->masterip = 0;
	mc->masterport = 0;
	mc->mastercuid = 0;
}

void master_new_packet(void) {
	mc->err = 0;
	mc->wptr = mc->sbuff + 12;
}

static inline void master_sendcheck(uint8_t bytes) {
	if (mc->sbuffsize==0 || mc->sbuff==NULL || mc->wptr==NULL) {
		mc->sbuffsize = 100;
		mc->sbuff = malloc(mc->sbuffsize);
		mc->wptr = mc->sbuff + 12; // leave space for command and length
	} else if ((mc->wptr - mc->sbuff) + bytes > (long int)mc->sbuffsize) {
		uint32_t pleng;
		pleng = (mc->wptr - mc->sbuff);
		if (bytes>mc->sbuffsize) {
			mc->sbuffsize += (bytes * 3) / 2;
		} else {
			mc->sbuffsize *= 3;
			mc->sbuffsize /= 2;
		}
		mc->sbuff = mfsrealloc(mc->sbuff,mc->sbuffsize);
		mc->wptr = mc->sbuff + pleng;
	}
	passert(mc->sbuff);
}

void master_put8bit(uint8_t d8) {
	master_sendcheck(1);
	put8bit(&(mc->wptr),d8);
}

void master_put16bit(uint16_t d16) {
	master_sendcheck(2);
	put16bit(&(mc->wptr),d16);
}

void master_put32bit(uint32_t d32) {
	master_sendcheck(4);
	put32bit(&(mc->wptr),d32);
}

void master_put64bit(uint64_t d64) {
	master_sendcheck(8);
	put64bit(&(mc->wptr),d64);
}

void master_putname(uint8_t nleng,const char name[256]) {
	master_sendcheck(nleng+1);
	put8bit(&(mc->wptr),nleng);
	memcpy(mc->wptr,name,nleng);
	mc->wptr += nleng;
}

int master_send_and_receive(uint32_t scmd,uint32_t ecmd) {
	uint8_t hdr[12];
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t pleng,rcmd;
	uint8_t cnt;

	pleng = (mc->wptr - mc->sbuff);
	wptr = mc->sbuff;
	put32bit(&wptr,scmd);
	put32bit(&wptr,pleng-8);
	put32bit(&wptr,0); // query id
	cnt = 0;
	while(1) {
		if (mc->fd<0) {
			if (master_connect()<0) {
				cnt++;
				if (cnt>=10) {
					printf("can't connect to master\n");
					master_close_conn(1);
					return -1;
				}
				sleep(1);
				continue;
			}
		}
		if (tcpwrite(mc->fd,mc->sbuff,pleng)!=(ssize_t)pleng) {
			master_close_conn(1);
			cnt++;
			if (cnt>=10) {
				printf("master query: send error\n");
				return -1;
			}
			continue;
		}
		if (tcpread(mc->fd,hdr,12)!=12) {
			master_close_conn(1);
			cnt++;
			if (cnt>=10) {
				printf("master query: receive error\n");
				return -1;
			}
			continue;
		}
		rptr = hdr;
		rcmd = get32bit(&rptr);
		mc->pleng = get32bit(&rptr);
		if (rcmd!=ecmd) {
			printf("master query: unexpected answer\n");
			master_close_conn(1);
			return -1;
		}
		if (mc->pleng<4) {
			printf("master query: packet too short\n");
			master_close_conn(1);
			return -1;
		}
		if (get32bit(&rptr)!=0) {
			printf("master query: unexpected query id\n");
			master_close_conn(1);
			return -1;
		}
		mc->pleng -= 4;
		if (mc->rbuffsize<mc->pleng) {
			if (mc->rbuff!=NULL) {
				free(mc->rbuff);
			}
			mc->rbuff = malloc(mc->pleng);
			passert(mc->rbuff);
			mc->rbuffsize = mc->pleng;
		}
		mc->rptr = mc->rbuff;
		if (mc->pleng>0) {
			if (tcpread(mc->fd,mc->rbuff,mc->pleng)!=(ssize_t)(mc->pleng)) {
				master_close_conn(1);
				cnt++;
				if (cnt>=10) {
					printf("master query: receive error\n");
					return -1;
				}
				continue;
			}
		}
		return 0;
	}
	return 0;
}

static inline int master_recvcheck(uint8_t bytes) {
	if (mc->err || mc->rbuffsize==0 || mc->rbuff==NULL || mc->rptr==NULL) {
		mc->err = 1;
		return -1;
	} else if ((mc->rptr - mc->rbuff) + bytes > (long int)mc->pleng) {
		mc->err = 1;
		return -1;
	}
	return 0;
}

uint32_t master_get_leng(void) {
	return mc->pleng;
}

uint8_t master_get8bit(void) {
	if (master_recvcheck(1)<0) {
		return 0;
	} else {
		return get8bit(&(mc->rptr));
	}
}

uint16_t master_get16bit(void) {
	if (master_recvcheck(2)<0) {
		return 0;
	} else {
		return get16bit(&(mc->rptr));
	}
}

uint32_t master_get32bit(void) {
	if (master_recvcheck(4)<0) {
		return 0;
	} else {
		return get32bit(&(mc->rptr));
	}
}

uint64_t master_get64bit(void) {
	if (master_recvcheck(8)<0) {
		return 0;
	} else {
		return get64bit(&(mc->rptr));
	}
}

void master_getname(char name[256]) {
	uint8_t nleng = master_get8bit();
	if (master_recvcheck(nleng)<0) {
		name[0] = 0;
	} else {
		memcpy(name,mc->rptr,nleng);
		name[nleng]=0;
		mc->rptr += nleng;
	}
	return;
}

uint32_t master_bytes_left(void) {
	if (mc->err) {
		return 0;
	}
	return mc->pleng - (mc->rptr - mc->rbuff);
}

uint8_t master_end_packet(void) {
	if (mc->err==1) {
		master_error();
		return 0;
	}
	if ((mc->rptr - mc->rbuff) != (long int)mc->pleng) {
		master_error();
		return 0;
	}
	return 1;
}
