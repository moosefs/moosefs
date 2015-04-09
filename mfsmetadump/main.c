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
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "datapack.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " VERSSTR ", written by Jakub Kruszona-Zawadzki";

#define MAX_INDEX 0x7FFFFFFF
#define MAX_CHUNKS_PER_FILE (MAX_INDEX+1)

static inline char dispchar(uint8_t c) {
	return (c>=32 && c<=126)?c:'.';
}

static inline void makestrip(char strip[16],uint32_t ip) {
	snprintf(strip,16,"%03"PRIu8".%03"PRIu8".%03"PRIu8".%03"PRIu8,(uint8_t)(ip>>24),(uint8_t)(ip>>16),(uint8_t)(ip>>8),(uint8_t)ip);
	strip[15]=0;
}

void print_name(FILE *in,uint32_t nleng) {
	uint8_t buff[1024];
	uint32_t x,y,i;
	size_t happy;
	while (nleng>0) {
		y = (nleng>1024)?1024:nleng;
		x = fread(buff,1,y,in);
		for (i=0 ; i<x ; i++) {
			if (buff[i]<32 || buff[i]>127) {
				buff[i]='.';
			}
		}
		happy = fwrite(buff,1,x,stdout);
		(void)happy;
		if (x!=y) {
			return;
		}
		nleng -= x;
	}
}

void print_hex(FILE *in,uint32_t vleng) {
	uint8_t buff[1024];
	uint32_t x,y,i;
	while (vleng>0) {
		y = (vleng>1024)?1024:vleng;
		x = fread(buff,1,y,in);
		for (i=0 ; i<x ; i++) {
			printf("%02"PRIX8,buff[i]);
		}
		if (x!=y) {
			return;
		}
		vleng -= x;
	}
}

int chunk_load(FILE *fd,uint8_t mver) {
	uint8_t hdr[8];
	uint8_t loadbuff[16];
	const uint8_t *ptr;
	int32_t r;
	uint64_t chunkid,nextchunkid;
	uint32_t version,lockedto;

	(void)mver;

	if (fread(hdr,1,8,fd)!=8) {
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	printf("# nextchunkid: %016"PRIX64"\n",nextchunkid);
	for (;;) {
		r = fread(loadbuff,1,16,fd);
		(void)r;
		ptr = loadbuff;
		chunkid = get64bit(&ptr);
		version = get32bit(&ptr);
		lockedto = get32bit(&ptr);
		if (chunkid==0 && version==0 && lockedto==0) {
			return 0;
		}
		printf("*|i:%016"PRIX64"|v:%08"PRIX32"|t:%10"PRIu32"\n",chunkid,version,lockedto);
	}
}

int fs_loadedge(FILE *fd,uint8_t mver) {
	uint8_t uedgebuff[4+4+8+2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
	uint64_t edge_id;
	uint16_t nleng;

	ptr = uedgebuff;
	if (mver<=0x10) {
		if (fread(uedgebuff,1,4+4+2,fd)!=4+4+2) {
			fprintf(stderr,"loading edge: read error\n");
			return -1;
		}
		parent_id = get32bit(&ptr);
		child_id = get32bit(&ptr);
		edge_id = 0;
		nleng = get16bit(&ptr);
	} else {
		if (fread(uedgebuff,1,4+4+8+2,fd)!=4+4+8+2) {
			fprintf(stderr,"loading edge: read error\n");
			return -1;
		}
		parent_id = get32bit(&ptr);
		child_id = get32bit(&ptr);
		edge_id = get64bit(&ptr);
		nleng = get16bit(&ptr);
	}
	if (parent_id==0 && child_id==0) {	// last edge
		return 1;
	}

	if (parent_id==0) {
		printf("E|p:      NULL|c:%10"PRIu32"|i:%016"PRIX64"|n:",child_id,edge_id);
	} else {
		printf("E|p:%10"PRIu32"|c:%10"PRIu32"|i:%016"PRIX64"|n:",parent_id,child_id,edge_id);
	}
	print_name(fd,nleng);
	printf("\n");
	return 0;
}

static inline uint8_t fsnodes_type_convert(uint8_t type) {
	switch (type) {
		case DISP_TYPE_FILE:
			return TYPE_FILE;
		case DISP_TYPE_DIRECTORY:
			return TYPE_DIRECTORY;
		case DISP_TYPE_SYMLINK:
			return TYPE_SYMLINK;
		case DISP_TYPE_FIFO:
			return TYPE_FIFO;
		case DISP_TYPE_BLOCKDEV:
			return TYPE_BLOCKDEV;
		case DISP_TYPE_CHARDEV:
			return TYPE_CHARDEV;
		case DISP_TYPE_SOCKET:
			return TYPE_SOCKET;
		case DISP_TYPE_TRASH:
			return TYPE_TRASH;
		case DISP_TYPE_SUSTAINED:
			return TYPE_SUSTAINED;
	}
	return 0;
}

int fs_loadnode(FILE *fd,uint8_t mver) {
	uint8_t unodebuff[4+1+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	const uint8_t *ptr,*chptr;
	uint8_t type,goal,flags;
	uint32_t nodeid,uid,gid,atimestamp,mtimestamp,ctimestamp,trashtime;
	uint16_t mode;
	uint32_t hdrsize;
	char c;

	type = fgetc(fd);
	if (type==0) {	// last node
		return 1;
	}
	if (mver<=0x11) {
		hdrsize = 4+1+2+6*4;
	} else {
		hdrsize = 4+1+1+2+6*4;
		if (mver<=0x12) {
			type = fsnodes_type_convert(type);
		}
	}
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (fread(unodebuff,1,hdrsize,fd)!=hdrsize) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (fread(unodebuff,1,hdrsize+4,fd)!=hdrsize+4) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		if (fread(unodebuff,1,hdrsize+8+4+2,fd)!=hdrsize+8+4+2) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	default:
		fprintf(stderr,"loading node: unrecognized node type: %c\n",type);
		return -1;
	}
	c='?';
	switch (type) {
	case TYPE_DIRECTORY:
		c='D';
		break;
	case TYPE_SOCKET:
		c='S';
		break;
	case TYPE_FIFO:
		c='F';
		break;
	case TYPE_BLOCKDEV:
		c='B';
		break;
	case TYPE_CHARDEV:
		c='C';
		break;
	case TYPE_SYMLINK:
		c='L';
		break;
	case TYPE_FILE:
		c='-';
		break;
	case TYPE_TRASH:
		c='T';
		break;
	case TYPE_SUSTAINED:
		c='R';
		break;
	}
	ptr = unodebuff;
	nodeid = get32bit(&ptr);
	goal = get8bit(&ptr);
	if (mver<=0x11) {
		uint16_t flagsmode = get16bit(&ptr);
		flags = flagsmode >> 12;
		mode = flagsmode & 0xFFF;
	} else {
		flags = get8bit(&ptr);
		mode = get16bit(&ptr);
	}
	uid = get32bit(&ptr);
	gid = get32bit(&ptr);
	atimestamp = get32bit(&ptr);
	mtimestamp = get32bit(&ptr);
	ctimestamp = get32bit(&ptr);
	trashtime = get32bit(&ptr);

	printf("%c|i:%10"PRIu32"|#:%"PRIu8"|e:%1"PRIX8"|m:%04"PRIo16"|u:%10"PRIu32"|g:%10"PRIu32"|a:%10"PRIu32",m:%10"PRIu32",c:%10"PRIu32"|t:%10"PRIu32,c,nodeid,goal,flags,mode,uid,gid,atimestamp,mtimestamp,ctimestamp,trashtime);

	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		uint32_t rdev;
		rdev = get32bit(&ptr);
		printf("|d:%5"PRIu32",%5"PRIu32"\n",rdev>>16,rdev&0xFFFF);
	} else if (type==TYPE_SYMLINK) {
		uint32_t pleng;
		pleng = get32bit(&ptr);
		printf("|p:");
		print_name(fd,pleng);
		printf("\n");
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		uint64_t length,chunkid;
		uint32_t ci,ch,sessionid;
		uint16_t sessionids;

		length = get64bit(&ptr);
		ch = get32bit(&ptr);
		sessionids = get16bit(&ptr);

		printf("|l:%20"PRIu64"|c:(",length);
		while (ch>65536) {
			chptr = ptr;
			if (fread((uint8_t*)ptr,1,8*65536,fd)!=8*65536) {
				fprintf(stderr,"loading node: read error\n");
				return -1;
			}
			for (ci=0 ; ci<65536 ; ci++) {
				chunkid = get64bit(&chptr);
				if (chunkid>0) {
					printf("%016"PRIX64,chunkid);
				} else {
					printf("N");
				}
				printf(",");
			}
			ch-=65536;
		}

		if (fread((uint8_t*)ptr,1,8*ch+4*sessionids,fd)!=8*ch+4*sessionids) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}

		while (ch>0) {
			chunkid = get64bit(&ptr);
			if (chunkid>0) {
				printf("%016"PRIX64,chunkid);
			} else {
				printf("N");
			}
			if (ch>1) {
				printf(",");
			}
			ch--;
		}
		printf(")|r:(");
		while (sessionids>0) {
			sessionid = get32bit(&ptr);
			printf("%"PRIu32,sessionid);
			if (sessionids>1) {
				printf(",");
			}
			sessionids--;
		}
		printf(")\n");
	} else {
		printf("\n");
	}

	return 0;
}

int fs_loadnodes(FILE *fd,uint8_t fver,uint8_t mver) {
	int s;
	uint8_t hdr[8];
	const uint8_t *ptr;
	uint32_t maxnodeid,hashelements;
	(void)fver;
	if (fver>=0x20) {
		if (mver<=0x10) {
			if (fread(hdr,1,4,fd)!=4) {
				fprintf(stderr,"loading node: read error\n");
				return -1;
			}
			ptr = hdr;
			maxnodeid = get32bit(&ptr);
			printf("# maxinode: %"PRIu32"\n",maxnodeid);
		} else {
			if (fread(hdr,1,8,fd)!=8) {
				fprintf(stderr,"loading node: read error\n");
				return -1;
			}
			ptr = hdr;
			maxnodeid = get32bit(&ptr);
			hashelements = get32bit(&ptr);
			printf("# maxinode: %"PRIu32" ; hashelements: %"PRIu32"\n",maxnodeid,hashelements);
		}
	}
	do {
		s = fs_loadnode(fd,mver);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadedges(FILE *fd,uint8_t mver) {
	uint8_t hdr[8];
	const uint8_t *ptr;
	uint64_t nextedgeid;
	int s;
	if (mver>0x10) {
		if (fread(hdr,1,8,fd)!=8) {
			fprintf(stderr,"loading edge: read error\n");
			return -1;
		}
		ptr = hdr;
		nextedgeid = get64bit(&ptr);
		printf("# nextedgeid: %016"PRIX64"\n",nextedgeid);
	}
	do {
		s = fs_loadedge(fd,mver);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadfree(FILE *fd,uint8_t mver) {
	uint8_t rbuff[8];
	const uint8_t *ptr;
	uint32_t t,nodeid,ftime;
	(void)mver;
	if (fread(rbuff,1,4,fd)!=4) {
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);
	printf("# free nodes: %"PRIu32"\n",t);
	while (t>0) {
		if (fread(rbuff,1,8,fd)!=8) {
			return -1;
		}
		ptr = rbuff;
		nodeid = get32bit(&ptr);
		ftime = get32bit(&ptr);
		printf("I|i:%10"PRIu32"|f:%10"PRIu32"\n",nodeid,ftime);
		t--;
	}
	return 0;
}

int fs_loadquota(FILE *fd,uint8_t mver) {
	uint8_t rbuff[66];
	const uint8_t *ptr;
	uint8_t exceeded,flags;
	uint32_t t,nodeid,stimestamp,sinodes,hinodes;
	uint64_t slength,hlength,ssize,hsize,srealsize,hrealsize;
	(void)mver;
	if (fread(rbuff,1,4,fd)!=4) {
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);
	printf("# quota nodes: %"PRIu32"\n",t);
	while (t>0) {
		if (fread(rbuff,1,66,fd)!=66) {
			return -1;
		}
		ptr = rbuff;
		nodeid = get32bit(&ptr);
		exceeded = get8bit(&ptr);
		flags = get8bit(&ptr);
		stimestamp = get32bit(&ptr);
		sinodes = get32bit(&ptr);
		hinodes = get32bit(&ptr);
		slength = get64bit(&ptr);
		hlength = get64bit(&ptr);
		ssize = get64bit(&ptr);
		hsize = get64bit(&ptr);
		srealsize = get64bit(&ptr);
		hrealsize = get64bit(&ptr);
		printf("Q|i:%10"PRIu32"|e:%c|f:%02"PRIX8"|s:%10"PRIu32,nodeid,(exceeded)?'1':'0',flags,stimestamp);
		if (flags&QUOTA_FLAG_SINODES) {
			printf("|si:%10"PRIu32,sinodes);
		} else {
			printf("|si:         -");
		}
		if (flags&QUOTA_FLAG_HINODES) {
			printf("|hi:%10"PRIu32,hinodes);
		} else {
			printf("|hi:         -");
		}
		if (flags&QUOTA_FLAG_SLENGTH) {
			printf("|sl:%20"PRIu64,slength);
		} else {
			printf("|sl:                   -");
		}
		if (flags&QUOTA_FLAG_HLENGTH) {
			printf("|hl:%20"PRIu64,hlength);
		} else {
			printf("|hl:                   -");
		}
		if (flags&QUOTA_FLAG_SSIZE) {
			printf("|ss:%20"PRIu64,ssize);
		} else {
			printf("|ss:                   -");
		}
		if (flags&QUOTA_FLAG_HSIZE) {
			printf("|hs:%20"PRIu64,hsize);
		} else {
			printf("|hs:                   -");
		}
		if (flags&QUOTA_FLAG_SREALSIZE) {
			printf("|sr:%20"PRIu64,srealsize);
		} else {
			printf("|sr:                   -");
		}
		if (flags&QUOTA_FLAG_HREALSIZE) {
			printf("|hr:%20"PRIu64,hrealsize);
		} else {
			printf("|hr:                   -");
		}
		printf("\n");
		t--;
	}
	return 0;
}

int xattr_load(FILE *fd,uint8_t mver) {
	uint8_t hdrbuff[4+1+4];
	const uint8_t *ptr;
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;

	(void)mver;

        while (1) {
                if (fread(hdrbuff,1,4+1+4,fd)!=4+1+4) {
                        fprintf(stderr,"loading xattr: read error");
                        return -1;
                }
                ptr = hdrbuff;
                inode = get32bit(&ptr);
                anleng = get8bit(&ptr);
                avleng = get32bit(&ptr);
		if (inode==0) {
			return 0;
		}
		if (anleng==0) {
			fprintf(stderr,"loading xattr: empty name");
			fseek(fd,anleng+avleng,SEEK_CUR);
			continue;
		}
		printf("X|i:%10"PRIu32"|n:",inode);
		print_name(fd,anleng);
		printf("|v:");
		print_hex(fd,avleng);
		printf("\n");
	}
}

int posix_acl_load(FILE *fd,uint8_t mver) {
	uint8_t hdrbuff[4+1+2*6];
	uint8_t aclbuff[6*100];
	const uint8_t *ptr;
	uint32_t inode;
	uint8_t acltype;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint32_t aclid;
	uint16_t aclperm;
	uint32_t acls,acbcnt;

	(void)mver;

	while (1) {
                if (fread(hdrbuff,1,4+1+2*6,fd)!=4+1+2*6) {
                        fprintf(stderr,"loading posix_acl: read error");
                        return -1;
                }
                ptr = hdrbuff;
                inode = get32bit(&ptr);
		if (inode==0) {
			return 0;
		}
                acltype = get8bit(&ptr);
                userperm = get16bit(&ptr);
                groupperm = get16bit(&ptr);
                otherperm = get16bit(&ptr);
                mask = get16bit(&ptr);
                namedusers = get16bit(&ptr);
                namedgroups = get16bit(&ptr);
		printf("A|i:%10"PRIu32"|t:%"PRIu8"|u:%"PRIo16"|g:%"PRIo16"|o:%"PRIo16"|m:%"PRIo16"|n:(",inode,acltype,userperm,groupperm,otherperm,mask);
		acls = namedusers+namedgroups;
		acbcnt = 0;
		while (acls>0) {
			if (acbcnt==0) {
				if (acls>100) {
					acbcnt=100;
				} else {
					acbcnt=acls;
				}
				if (fread(aclbuff,6,acbcnt,fd)!=acbcnt) {
					fprintf(stderr,"loading posix_acl: read error");
					return -1;
				}
				ptr = aclbuff;
			}
			aclid = get32bit(&ptr);
			aclperm = get16bit(&ptr);
			acls--;
			acbcnt--;
			if (acls>=namedgroups) {
				printf("u(%"PRIu32"):%"PRIo16,aclid,aclperm);
			} else {
				printf("g(%"PRIu32"):%"PRIo16,aclid,aclperm);
			}
			if (acls>0) {
				printf(",");
			}
		}
		printf(")\n");
	}
}

int sessions_load(FILE *fd,uint8_t mver) {
	uint8_t hdr[8];
	uint8_t *fsesrecord;
	const uint8_t *ptr;
	uint16_t dver,statsinfile;
	uint32_t recsize;
	uint32_t i,nextsessionid,sessionid;
	uint32_t ileng,peerip,rootinode,mintrashtime,maxtrashtime,rootuid,rootgid,mapalluid,mapallgid,disconnected;
	uint8_t sesflags,mingoal,maxgoal;
	char strip[16];

	if (mver<=0x11) {
		if (fread(hdr,1,8,fd)!=8) {
			fprintf(stderr,"loading sessions: read error");
			return -1;
		}

		ptr = hdr;
		dver = get16bit(&ptr);
		if (dver==1) {
			mver = 0x10;
		} else if (dver==2) {
			mver = 0x11;
		}
	} else {
		dver = 0; // makes old gcc happy
		if (fread(hdr,1,6,fd)!=6) {
			fprintf(stderr,"loading sessions: read error");
			return -1;
		}
		ptr = hdr;
	}
	nextsessionid = get32bit(&ptr);
	statsinfile = get16bit(&ptr);

	if (mver<=0x11) {
		printf("# dver: %"PRIu16" ; nextsessionid: %"PRIu32" ; statscount: %"PRIu16"\n",dver,nextsessionid,statsinfile);
	} else {
		printf("# nextsessionid: %"PRIu32" ; statscount: %"PRIu16"\n",nextsessionid,statsinfile);
	}
	if (mver<0x11) {
		recsize = 43+statsinfile*8;
	} else {
		recsize = 47+statsinfile*8;
	}
	fsesrecord = malloc(recsize);
	if (fsesrecord==NULL) {
		fprintf(stderr,"loading sessions: out of memory\n");
		return -1;
	}

	while (1) {
		if (fread(fsesrecord,1,recsize,fd)!=(size_t)(recsize)) {
			free(fsesrecord);
			fprintf(stderr,"loading sessions: read error\n");
			return -1;
		}
		ptr = fsesrecord;
		sessionid = get32bit(&ptr);
		if (sessionid==0) {
			free(fsesrecord);
			return 0;
		}
		ileng = get32bit(&ptr);
		peerip = get32bit(&ptr);
		rootinode = get32bit(&ptr);
		sesflags = get8bit(&ptr);
		mingoal = get8bit(&ptr);
		maxgoal = get8bit(&ptr);
		mintrashtime = get32bit(&ptr);
		maxtrashtime = get32bit(&ptr);
		rootuid = get32bit(&ptr);
		rootgid = get32bit(&ptr);
		mapalluid = get32bit(&ptr);
		mapallgid = get32bit(&ptr);
		makestrip(strip,peerip);
		if (mver>=0x11) {
			disconnected = get32bit(&ptr);
			printf("M|s:%10"PRIu32"|p:%s|r:%10"PRIu32"|f:%02"PRIX8"|g:%"PRIu8"-%"PRIu8"|t:%10"PRIu32"-%10"PRIu32"|m:%10"PRIu32",%10"PRIu32",%10"PRIu32",%10"PRIu32"|d:%10"PRIu32"|c:",sessionid,strip,rootinode,sesflags,mingoal,maxgoal,mintrashtime,maxtrashtime,rootuid,rootgid,mapalluid,mapallgid,disconnected);
		} else {
			printf("M|s:%10"PRIu32"|p:%s|r:%10"PRIu32"|f:%02"PRIX8"|g:%"PRIu8"-%"PRIu8"|t:%10"PRIu32"-%10"PRIu32"|m:%10"PRIu32",%10"PRIu32",%10"PRIu32",%10"PRIu32"|c:",sessionid,strip,rootinode,sesflags,mingoal,maxgoal,mintrashtime,maxtrashtime,rootuid,rootgid,mapalluid,mapallgid);
		}
		for (i=0 ; i<statsinfile ; i++) {
			printf("%c%"PRIu32,(i==0)?'[':',',get32bit(&ptr));
		}
		printf("]|l:");
		for (i=0 ; i<statsinfile ; i++) {
			printf("%c%"PRIu32,(i==0)?'[':',',get32bit(&ptr));
		}
		printf("]|i:");
		print_name(fd,ileng);
		printf("\n");
	}
}

int csdb_load(FILE *fd,uint8_t mver) {
	uint8_t csdbbuff[9];
	const uint8_t *ptr;
	uint32_t t;
	uint32_t ip;
	uint16_t port;
	uint16_t csid;
	uint8_t maintenance;
	size_t bsize;
	char strip[16];

	if (mver<=0x10) {
		bsize = 6;
	} else if (mver<=0x11) {
		bsize = 8;
	} else {
		bsize = 9;
	}

	if (fread(csdbbuff,1,4,fd)!=4) {
		return -1;
	}
	ptr = csdbbuff;
	t = get32bit(&ptr);
	printf("# chunk servers: %"PRIu32"\n",t);
	while (t>0) {
		if (fread(csdbbuff,1,bsize,fd)!=bsize) {
			fprintf(stderr,"loading chunk servers: read error\n");
			return -1;
		}
		ptr = csdbbuff;
		if (mver<=0x10) {
			ip = get32bit(&ptr);
			port = get16bit(&ptr);
			makestrip(strip,ip);
			printf("Z|i:%s|p:%5"PRIu16"\n",strip,port);
		} else if (mver<=0x11) {
			ip = get32bit(&ptr);
			port = get16bit(&ptr);
			csid = get16bit(&ptr);
			makestrip(strip,ip);
			printf("Z|i:%s|p:%5"PRIu16"|#:%5"PRIu16"\n",strip,port,csid);
		} else {
			ip = get32bit(&ptr);
			port = get16bit(&ptr);
			csid = get16bit(&ptr);
			maintenance = get8bit(&ptr);
			makestrip(strip,ip);
			printf("Z|i:%s|p:%5"PRIu16"|#:%5"PRIu16"|m:%u\n",strip,port,csid,(maintenance)?1:0);
		}
		t--;
	}
	return 0;
}

int of_load(FILE *fd,uint8_t mver) {
	uint8_t loadbuff[8];
	const uint8_t *ptr;
	uint32_t sessionid,inode;

	(void)mver;

	for (;;) {
		if (fread(loadbuff,1,8,fd)!=8) {
			fprintf(stderr,"loading open files: read error\n");
			return -1;
		}
		ptr = loadbuff;
		sessionid = get32bit(&ptr);
		inode = get32bit(&ptr);
		if (sessionid>0 && inode>0) {
			printf("O|s:%10"PRIu32"|i:%10"PRIu32"\n",sessionid,inode);
		} else {
			return 0;
		}
	}
	return 0;       // unreachable
}

int hexdump(FILE *fd,uint64_t sleng) {
	uint8_t lbuff[32];
	uint32_t i;
	while (sleng>32) {
		if (fread(lbuff,1,32,fd)!=32) {
			return -1;
		}
		for (i=0 ; i<32 ; i++) {
			printf("%02"PRIX8" ",lbuff[i]);
		}
		printf(" |");
		for (i=0 ; i<32 ; i++) {
			printf("%c",dispchar(lbuff[i]));
		}
		printf("|\n");
		sleng-=32;
	}
	if (sleng>0) {
		if (fread(lbuff,1,sleng,fd)!=(size_t)sleng) {
			return -1;
		}
		for (i=0 ; i<32 ; i++) {
			if (i<sleng) {
				printf("%02"PRIX8" ",lbuff[i]);
			} else {
				printf("   ");
			}
		}
		printf(" |");
		for (i=0 ; i<32 ; i++) {
			if (i<sleng) {
				printf("%c",dispchar(lbuff[i]));
			} else {
				printf(" ");
			}
		}
		printf("|\n");
	}
	return 0;
}

int fs_load_pre17(FILE *fd) {
	uint32_t maxnodeid,nextsessionid;
	uint64_t version;
	uint8_t hdr[16];
	const uint8_t *ptr;
	if (fread(hdr,1,16,fd)!=16) {
		return -1;
	}
	ptr = hdr;
	maxnodeid = get32bit(&ptr);
	version = get64bit(&ptr);
	nextsessionid = get32bit(&ptr);

	printf("# maxnodeid: %"PRIu32" ; version: %"PRIu64" ; nextsessionid: %"PRIu32"\n",maxnodeid,version,nextsessionid);

	printf("# -------------------------------------------------------------------\n");
	if (fs_loadnodes(fd,0x15,0x10)<0) {
		printf("error reading metadata (node)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	if (fs_loadedges(fd,0x10)<0) {
		printf("error reading metadata (edge)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	if (fs_loadfree(fd,0x10)<0) {
		printf("error reading metadata (free)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	return 0;
}

int fs_load(FILE *fd,uint8_t fver) {
	uint32_t maxnodeid,nextsessionid;
	uint64_t sleng;
	off_t offbegin;
	uint64_t version,fileid;
	uint8_t hdr[16];
	const uint8_t *ptr;
	uint8_t mver;
	if (fread(hdr,1,16,fd)!=16) {
		return -1;
	}
	ptr = hdr;

	if (fver<0x20) {
		maxnodeid = get32bit(&ptr);
		version = get64bit(&ptr);
		nextsessionid = get32bit(&ptr);

		printf("# maxnodeid: %"PRIu32" ; version: %"PRIu64" ; nextsessionid: %"PRIu32"\n",maxnodeid,version,nextsessionid);
	} else {
		version = get64bit(&ptr);
		fileid = get64bit(&ptr);

		printf("# version: %"PRIu64" ; fileid: 0x%"PRIX64"\n",version,fileid);
	}

	while (1) {
		if (fread(hdr,1,16,fd)!=16) {
			printf("can't read section header\n");
			return -1;
		}
		if (memcmp(hdr,"[MFS EOF MARKER]",16)==0) {
			printf("# -------------------------------------------------------------------\n");
			printf("# MFS END OF FILE MARKER\n");
			printf("# -------------------------------------------------------------------\n");
			return 0;
		}
		ptr = hdr+8;
		sleng = get64bit(&ptr);
		offbegin = ftello(fd);
		printf("# -------------------------------------------------------------------\n");
		printf("# section header: %c%c%c%c%c%c%c%c (%02X%02X%02X%02X%02X%02X%02X%02X) ; length: %"PRIu64"\n",dispchar(hdr[0]),dispchar(hdr[1]),dispchar(hdr[2]),dispchar(hdr[3]),dispchar(hdr[4]),dispchar(hdr[5]),dispchar(hdr[6]),dispchar(hdr[7]),hdr[0],hdr[1],hdr[2],hdr[3],hdr[4],hdr[5],hdr[6],hdr[7],sleng);
		mver = (((hdr[5]-'0')&0xF)<<4)+(hdr[7]&0xF);
		printf("# section type: %c%c%c%c ; version: 0x%02"PRIX8"\n",dispchar(hdr[0]),dispchar(hdr[1]),dispchar(hdr[2]),dispchar(hdr[3]),mver);
		if (memcmp(hdr,"NODE",4)==0) {
			if (fs_loadnodes(fd,fver,mver)<0) {
				printf("error reading metadata (NODE)\n");
				return -1;
			}
		} else if (memcmp(hdr,"EDGE",4)==0) {
			if (fs_loadedges(fd,mver)<0) {
				printf("error reading metadata (EDGE)\n");
				return -1;
			}
		} else if (memcmp(hdr,"FREE",4)==0) {
			if (fs_loadfree(fd,mver)<0) {
				printf("error reading metadata (FREE)\n");
				return -1;
			}
		} else if (memcmp(hdr,"QUOT",4)==0) {
			if (fs_loadquota(fd,mver)<0) {
				printf("error reading metadata (QUOT)\n");
				return -1;
			}
		} else if (memcmp(hdr,"CHNK",4)==0) {
			if (chunk_load(fd,mver)<0) {
				printf("error reading metadata (CHNK)\n");
				return -1;
			}
		} else if (memcmp(hdr,"XATR",4)==0) {
			if (xattr_load(fd,mver)<0) {
				printf("error reading metadata (XATR)\n");
				return -1;
			}
		} else if (memcmp(hdr,"PACL",4)==0) {
			if (posix_acl_load(fd,mver)<0) {
				printf("error reading metadata (PACL)\n");
				return -1;
			}
		} else if (memcmp(hdr,"SESS",4)==0) {
			if (sessions_load(fd,mver)<0) {
				printf("error reading metadata (SESS)\n");
				return -1;
			}
		} else if (memcmp(hdr,"OPEN",4)==0) {
			if (of_load(fd,mver)<0) {
				printf("error reading metadata (OPEN)\n");
				return -1;
			}
		} else if (memcmp(hdr,"CSDB",4)==0) {
			if (csdb_load(fd,mver)<0) {
				printf("error reading metadata (CSDB)\n");
				return -1;
			}
		} else {
			printf("unknown file part\n");
			if (hexdump(fd,sleng)<0) {
				return -1;
			}
		}
		if ((off_t)(offbegin+sleng)!=ftello(fd)) {
			fprintf(stderr,"some data in this section have not been read - file corrupted\n");
			return -1;
		}
	}
	return 0;
}

int fs_loadall(const char *fname) {
	FILE *fd;
	uint8_t hdr[8];
	uint8_t fver;

	fd = fopen(fname,"r");

	if (fd==NULL) {
		printf("can't open metadata file\n");
		return -1;
	}
	if (fread(hdr,1,8,fd)!=8) {
		printf("can't read metadata header\n");
		fclose(fd);
		return -1;
	}
	printf("# header: %c%c%c%c%c%c%c%c (%02X%02X%02X%02X%02X%02X%02X%02X)\n",dispchar(hdr[0]),dispchar(hdr[1]),dispchar(hdr[2]),dispchar(hdr[3]),dispchar(hdr[4]),dispchar(hdr[5]),dispchar(hdr[6]),dispchar(hdr[7]),hdr[0],hdr[1],hdr[2],hdr[3],hdr[4],hdr[5],hdr[6],hdr[7]);
	if (memcmp(hdr,"MFSM NEW",8)==0) {
		printf("empty file\n");
	} else if (memcmp(hdr,MFSSIGNATURE "M ",5)==0 && hdr[5]>='1' && hdr[5]<='9' && hdr[6]=='.' && hdr[7]>='0' && hdr[7]<='9') {
		fver = ((hdr[5]-'0')<<4)+(hdr[7]-'0');
		if (fver<0x17) {
			if (fs_load_pre17(fd)<0) {
				printf("error reading metadata (structure)\n");
				fclose(fd);
				return -1;
			}
			if (chunk_load(fd,0x10)<0) {
				printf("error reading metadata (chunks)\n");
				fclose(fd);
				return -1;
			}
		} else {
			if (fs_load(fd,fver)<0) {
				fclose(fd);
				return -1;
			}
		}
	} else {
		printf("wrong metadata header (old version ?)\n");
		fclose(fd);
		return -1;
	}
	if (ferror(fd)!=0) {
		printf("error reading metadata\n");
		fclose(fd);
		return -1;
	}
	fclose(fd);
	return 0;
}

int main(int argc,char **argv) {
	if (argc!=2) {
		printf("usage: %s metadata_file\n",argv[0]);
		return 1;
	}
	return (fs_loadall(argv[1])<0)?1:0;
}
