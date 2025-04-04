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
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "md5.h"
#include "exports.h"
#include "datapack.h"
#include "main.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "crc.h"
#include "hashfn.h"
#include "timeparser.h"

typedef struct _exports {
	uint32_t pleng;
	const uint8_t *path;	// without '/' at the begin and at the end
	uint32_t fromip,toip;
	uint32_t minversion;
	uint8_t passworddigest[16];
	unsigned alldirs:1;
	unsigned needpassword:1;
	unsigned meta:1;
	unsigned rootredefined:1;
//	unsigned old:1;
	uint8_t sesflags;
	uint16_t sclassgroups;
	uint16_t umask;
	uint32_t mintrashretention;
	uint32_t maxtrashretention;
	uint32_t rootuid;
	uint32_t rootgid;
	uint32_t mapalluid;
	uint32_t mapallgid;
	uint32_t disables;
	struct _exports *next;
} exports;

static exports *exports_records;
static uint64_t exports_csum;
static char *ExportsFileName;

uint64_t exports_entry_checksum(exports *e) {
	uint64_t csum;
	uint8_t edata[62];
	uint8_t *ptr;
	uint32_t crc,murmur;

	ptr = edata;
	put32bit(&ptr,e->fromip);
	put32bit(&ptr,e->toip);
	put32bit(&ptr,e->minversion);
	if (e->needpassword) {
		memcpy(ptr,e->passworddigest,16);
	} else {
		memset(ptr,0,16);
	}
	ptr+=16;
	put8bit(&ptr,(e->alldirs<<3) + (e->needpassword<<2) + (e->meta<<1) + e->rootredefined);
	put8bit(&ptr,e->sesflags);
	put16bit(&ptr,e->sclassgroups);
	put16bit(&ptr,e->umask);
	put32bit(&ptr,e->mintrashretention);
	put32bit(&ptr,e->maxtrashretention);
	put32bit(&ptr,e->rootuid);
	put32bit(&ptr,e->rootgid);
	put32bit(&ptr,e->mapalluid);
	put32bit(&ptr,e->mapallgid);
	put32bit(&ptr,e->disables);
	crc = mycrc32(0xFFFFFFFF,edata,62);
	murmur = murmur3_32(edata,62,0);
	if (e->pleng>0) {
		crc = mycrc32(crc,e->path,e->pleng);
		murmur = murmur3_32(e->path,e->pleng,murmur);
	}
	csum = crc;
	csum <<= 32;
	csum |= murmur;
	return csum;
}

uint64_t exports_checksum(void) {
	return exports_csum;
}

char* exports_strsep(char **stringp, const char *delim) {
	char *s;
	const char *spanp;
	int c, sc;
	char *tok;

	s = *stringp;
	if (s==NULL) {
		return NULL;
	}
	while (*s==' ' || *s=='\t') {
		s++;
	}
	tok = s;
	while (1) {
		c = *s++;
		spanp = delim;
		do {
			if ((sc=*spanp++)==c) {
				if (c==0) {
					*stringp = NULL;
				} else {
					*stringp = s;
				}
				s--;
				while ((s>tok) && (s[-1]==' ' || s[-1]=='\t')) {
					s--;
				}
				*s = 0;
				return tok;
			}
		} while (sc!=0);
	}
	return NULL;	// unreachable
}


uint32_t exports_info_size(uint8_t versmode) {
	exports *e;
	uint32_t size=0;
	uint32_t add=0;
	if (versmode>0) {
		add+=10;
	}
	if (versmode>1) {
		add+=2;
	}
	if (versmode>2) {
		add+=4;
	}
	for (e=exports_records ; e ; e=e->next) {
		if (e->meta) {
			size+=35+add;
		} else {
			size+=35+add+e->pleng;
		}
	}
	return size;
}

void exports_info_data(uint8_t versmode,uint8_t *buff) {
	exports *e;
	for (e=exports_records ; e ; e=e->next) {
		put32bit(&buff,e->fromip);
		put32bit(&buff,e->toip);
		if (e->meta) {
			put32bit(&buff,1);
			put8bit(&buff,'.');
		} else {
			put32bit(&buff,e->pleng+1);
			put8bit(&buff,'/');
			if (e->pleng>0) {
				memcpy(buff,e->path,e->pleng);
				buff+=e->pleng;
			}
		}
		put32bit(&buff,e->minversion);
		put8bit(&buff,(e->alldirs?1:0)+(e->needpassword?2:0));
		put8bit(&buff,e->sesflags);
		if (versmode>1) {
			put16bit(&buff,e->umask);
		}
		put32bit(&buff,e->rootuid);
		put32bit(&buff,e->rootgid);
		put32bit(&buff,e->mapalluid);
		put32bit(&buff,e->mapallgid);
		if (versmode>0) {
			if (versmode>3) {
				put16bit(&buff,e->sclassgroups);
			} else {
				put8bit(&buff,0); // mingoal
				put8bit(&buff,0); // maxgoal
			}
			put32bit(&buff,e->mintrashretention);
			put32bit(&buff,e->maxtrashretention);
		}
		if (versmode>2) {
			put32bit(&buff,e->disables);
		}
	}
}

uint8_t exports_check(uint32_t ip,uint32_t version,const uint8_t *path,const uint8_t rndcode[32],const uint8_t passcode[16],uint8_t *sesflags,uint16_t *umaskval,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid,uint16_t *sclassgroups,uint32_t *mintrashretention,uint32_t *maxtrashretention,uint32_t *disables) {
	const uint8_t *p;
	uint32_t pleng,i;
	uint8_t rndstate;
	uint8_t meta;
	int ok,nopass;
	md5ctx md5c;
	uint8_t entrydigest[16];
	exports *e,*f;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"check exports for: %u.%u.%u.%u:%s",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF,path);
	meta = (path==NULL)?1:0;

	if (meta==0) {
		p = path;
		while (*p=='/') {
			p++;
		}
		pleng = 0;
		while (p[pleng]) {
			pleng++;
		}
		while (pleng>0 && p[pleng-1]=='/') {
			pleng--;
		}
	} else {
		p = NULL;
		pleng = 0;
	}
	rndstate=0;
	if (rndcode!=NULL) {
		for (i=0 ; i<32 ; i++) {
			rndstate|=rndcode[i];
		}
	}
	nopass=0;
	f=NULL;
	for (e=exports_records ; e ; e=e->next) {
		ok = 0;
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"entry: network:%u.%u.%u.%u-%u.%u.%u.%u",(e->fromip>>24)&0xFF,(e->fromip>>16)&0xFF,(e->fromip>>8)&0xFF,e->fromip&0xFF,(e->toip>>24)&0xFF,(e->toip>>16)&0xFF,(e->toip>>8)&0xFF,e->toip&0xFF);
		if (ip>=e->fromip && ip<=e->toip && version>=e->minversion && meta==e->meta) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"ip and version ok");
			// path check
			if (meta) {	// no path in META
				ok=1;
			} else {
				if (e->pleng==0) {	// root dir
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"rootdir entry (pleng:%u)",pleng);
					if (pleng==0) {
						ok=1;
					} else if (e->alldirs) {
						ok=1;
					}
				} else {
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"entry path: %s (pleng:%u)",e->path,e->pleng);
					if (pleng==e->pleng && memcmp(p,e->path,pleng)==0) {
						ok=1;
					} else if (e->alldirs && pleng>e->pleng && p[e->pleng]=='/' && memcmp(p,e->path,e->pleng)==0) {
						ok=1;
					}
				}
			}
//			if (ok) {
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"path ok");
//			}
			if (ok && e->needpassword) {
				if (rndstate==0 || rndcode==NULL || passcode==NULL) {
					ok=0;
					nopass=1;
				} else {
					md5_init(&md5c);
					md5_update(&md5c,rndcode,16);
					md5_update(&md5c,e->passworddigest,16);
					md5_update(&md5c,rndcode+16,16);
					md5_final(entrydigest,&md5c);
					if (memcmp(entrydigest,passcode,16)!=0) {
						ok=0;
						nopass=1;
					}
				}
			}
		}
		if (ok) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"entry accepted");
			if (f==NULL) {
				f=e;
			} else {
				if ((e->sesflags&SESFLAG_READONLY)==0 && (f->sesflags&SESFLAG_READONLY)!=0) {	// prefer rw to ro
					f=e;
				} else if (e->rootuid==0 && f->rootuid!=0) {	// prefer root not restricted to restricted
					f=e;
				} else if ((e->sesflags&SESFLAG_ADMIN)!=0 && (f->sesflags&SESFLAG_ADMIN)==0) {	// prefer lines with more privileges
					f=e;
				} else if (e->needpassword==1 && f->needpassword==0) {	// prefer lines with passwords
					f=e;
				} else if (e->pleng > f->pleng) {	// prefer more accurate path
					f=e;
				}
			}
		}
	}
	if (f==NULL) {
		if (nopass) {
			if (rndstate==0 || rndcode==NULL || passcode==NULL) {
				return MFS_ERROR_NOPASSWORD;
			} else {
				return MFS_ERROR_BADPASSWORD;
			}
		}
		return MFS_ERROR_EACCES;
	}
	*sesflags = f->sesflags;
	*umaskval = f->umask;
	*rootuid = f->rootuid;
	*rootgid = f->rootgid;
	*mapalluid = f->mapalluid;
	*mapallgid = f->mapallgid;
	*sclassgroups = f->sclassgroups;
	*mintrashretention = f->mintrashretention;
	*maxtrashretention = f->maxtrashretention;
	*disables = f->disables;
	return MFS_STATUS_OK;
}

void exports_freelist(exports *arec) {
	exports *drec;
	while (arec) {
		drec = arec;
		arec = arec->next;
		if (drec->path) {
			free((uint8_t *)(drec->path));
		}
		free(drec);
	}
}

// format:
// ip[/bits]	path	options
//
// options:
//  readonly
//  ro
//  readwrite
//  rw
//  maproot=uid[:gid]
//  mapall=uid[:gid]
//  alldirs
//  md5pass=md5(password)
//  password=password
//  dynamicip
//  ignoregid
//  admin
//  umask=0###
//  sclassgroups=-|#[:#[...]]
//  mintrashretention=[#w][#d][#h][#m][#[s]]
//  maxtrashretention=[#w][#d][#h][#m][#[s]]
//  disable=cmd[:cmd[...]]
//  minversion=#.#.#
// deprecated:
//  mintrashtime=[#w][#d][#h][#m][#[s]]
//  maxtrashtime=[#w][#d][#h][#m][#[s]]
//  mingoal=# (sets corresponding bits in sclassgroups)
//  maxgoal=# (sets corresponding bits in sclassgroups)
//  canchangequota
//
// ip[/bits] can be '*' (same as 0.0.0.0/0)
//
// default:
// *	/	alldirs,maproot=0

int exports_parsenet(const char *net,uint32_t *fromip,uint32_t *toip) {
	uint32_t ip,i,octet;
	if (net[0]=='*' && net[1]==0) {
		*fromip = 0;
		*toip = 0xFFFFFFFFU;
		return 0;
	}
	ip=0;
	for (i=0 ; i<4; i++) {
		if (*net>='0' && *net<='9') {
			octet=0;
			while (*net>='0' && *net<='9') {
				octet*=10;
				octet+=*net-'0';
				net++;
				if (octet>255) {
					return -1;
				}
			}
		} else {
			return -1;
		}
		if (i<3) {
			if (*net!='.') {
				return -1;
			}
			net++;
		}
		ip*=256;
		ip+=octet;
	}
	if (*net==0) {
		*fromip = ip;
		*toip = ip;
		return 0;
	}
	if (*net=='/') {	// ip/bits and ip/mask
		*fromip = ip;
		ip=0;
		net++;
		for (i=0 ; i<4; i++) {
			if (*net>='0' && *net<='9') {
				octet=0;
				while (*net>='0' && *net<='9') {
					octet*=10;
					octet+=*net-'0';
					net++;
					if (octet>255) {
						return -1;
					}
				}
			} else {
				return -1;
			}
			if (i==0 && *net==0 && octet<=32) {	// bits -> convert to mask and skip rest of loop
				ip = 0xFFFFFFFF;
				if (octet<32) {
					ip<<=32-octet;
				}
				break;
			}
			if (i<3) {
				if (*net!='.') {
					return -1;
				}
				net++;
			}
			ip*=256;
			ip+=octet;
		}
		if (*net!=0) {
			return -1;
		}
		*fromip &= ip;
		*toip = *fromip | (ip ^ 0xFFFFFFFFU);
		return 0;
	}
	if (*net=='-') {	// ip1-ip2
		*fromip = ip;
		ip=0;
		net++;
		for (i=0 ; i<4; i++) {
			if (*net>='0' && *net<='9') {
				octet=0;
				while (*net>='0' && *net<='9') {
					octet*=10;
					octet+=*net-'0';
					net++;
					if (octet>255) {
						return -1;
					}
				}
			} else {
				return -1;
			}
			if (i<3) {
				if (*net!='.') {
					return -1;
				}
				net++;
			}
			ip*=256;
			ip+=octet;
		}
		if (*net!=0) {
			return -1;
		}
		*toip = ip;
		return 0;
	}
	return -1;
}

int exports_parsesclassgroups(const char *sgstr,uint16_t *sclassgroups) {
	uint8_t group;
	uint16_t result;

	result = 0;
	if (*sgstr=='-' && sgstr[1]==0) {
		*sclassgroups = 0;
		return 0;
	}
	while (*sgstr>='0' && *sgstr<='9') {
		group = 0;
		while (*sgstr>='0' && *sgstr<='9' && group<EXPORT_GROUPS) {
			group = (group * 10) + (*sgstr-'0');
			sgstr++;
		}
		if (group>=EXPORT_GROUPS) {
			return -1;
		}
		result |= (1<<group);
		if (*sgstr==0) {
			*sclassgroups = result;
			return 0;
		}
		if (*sgstr!=':') {
			return -1;
		}
		sgstr++;
	}
	return -1;
}

int exports_parsegoal(const char *goalstr,uint8_t *goal) {
	if (*goalstr<'1' || *goalstr>'9' || *(goalstr+1)) {
		return -1;
	}
	*goal = *goalstr-'0';
	return 0;
}

// we only accept octal format: 0###
int exports_parseumask(const char *umaskstr,uint16_t *umaskval) {
	if (*umaskstr!='0' || umaskstr[1]<'0' || umaskstr[1]>'7' || umaskstr[2]<'0' || umaskstr[2]>'7' || umaskstr[3]<'0' || umaskstr[3]>'7') {
		return -1;
	}
	*umaskval = (umaskstr[1]-'0') * 64 + (umaskstr[2]-'0') * 8 + (umaskstr[3]-'0');
	return 0;
}

#if 0
// use "timeparser.h" 
// # | [#w][#d][#h][#m][#s]
int exports_parsetime(char *timestr,uint32_t *rettime) {
	uint64_t t;
	uint64_t tp;
	uint8_t bits;
	char *p;
	for (p=timestr ; *p ; p++) {
		if (*p>='a' && *p<='z') {
			*p -= ('a'-'A');
		}
	}
	t = 0;
	bits = 0;
	while (1) {
		if (*timestr<'0' || *timestr>'9') {
			return -1;
		}
		tp = 0;
		while (*timestr>='0' && *timestr<='9') {
			tp *= 10;
			tp += *timestr-'0';
			timestr++;
			if (tp>UINT64_C(0x100000000)) {
				return -1;
			}
		}
		switch (*timestr) {
		case 'W':
			if (bits&0x1F) {
				return -1;
			}
			bits |= 0x10;
			tp *= 604800;
			timestr++;
			break;
		case 'D':
			if (bits&0x0F) {
				return -1;
			}
			bits |= 0x08;
			tp *= 86400;
			timestr++;
			break;
		case 'H':
			if (bits&0x07) {
				return -1;
			}
			bits |= 0x04;
			tp *= 3600;
			timestr++;
			break;
		case 'M':
			if (bits&0x03) {
				return -1;
			}
			bits |= 0x02;
			tp *= 60;
			timestr++;
			break;
		case 'S':
			if (bits&0x01) {
				return -1;
			}
			bits |= 0x01;
			timestr++;
			break;
		case '\0':
			if (bits) {
				return -1;
			}
			break;
		default:
			return -1;
		}
		t += tp;
		if (t>UINT64_C(0x100000000)) {
			return -1;
		}
		if (*timestr=='\0') {
			*rettime = t;
			return 0;
		}
	}
	return -1;	// unreachable
}
#endif

// x | x.y | x.y.z -> ( x<<16 + y<<8 + z )
int exports_parseversion(const char *verstr,uint32_t *version) {
	uint32_t vp;
	if (*verstr<'0' || *verstr>'9') {
		return -1;
	}
	vp=0;
	while (*verstr>='0' && *verstr<='9') {
		vp*=10;
		vp+=*verstr-'0';
		verstr++;
	}
	if (vp>255 || (*verstr!='.' && *verstr)) {
		return -1;
	}
	*version = vp<<16;
	if (*verstr==0) {
		return 0;
	}
	verstr++;
	if (*verstr<'0' || *verstr>'9') {
		return -1;
	}
	vp=0;
	while (*verstr>='0' && *verstr<='9') {
		vp*=10;
		vp+=*verstr-'0';
		verstr++;
	}
	if (vp>255 || (*verstr!='.' && *verstr)) {
		return -1;
	}
	*version += vp<<8;
	if (*verstr==0) {
		return 0;
	}
	verstr++;
	if (*verstr<'0' || *verstr>'9') {
		return -1;
	}
	vp=0;
	while (*verstr>='0' && *verstr<='9') {
		vp*=10;
		vp+=*verstr-'0';
		verstr++;
	}
	if (vp>255 || *verstr) {
		return -1;
	}
	*version += vp;
	return 0;
}

int exports_parseuidgid(char *maproot,uint32_t lineno,uint32_t *ruid,uint32_t *rgid) {
	char *uptr,*gptr,*eptr;
	struct group *grrec,grp;
	struct passwd *pwrec,pwd;
	char pwgrbuff[16384];
	uint32_t uid,gid;
	int gidok;

	uptr = maproot;
	gptr = maproot;
	while (*gptr && *gptr!=':') {
		gptr++;
	}

	if (*gptr==':') {
		*gptr = 0;
		gid = 0;
		eptr = gptr+1;
		while (*eptr>='0' && *eptr<='9') {
			gid*=10;
			gid+=*eptr-'0';
			eptr++;
		}
		if (*eptr!=0) {	// not only digits - treat it as a groupname
			if (getgrnam_r(gptr+1,&grp,pwgrbuff,16384,&grrec)!=0) {
				grrec = NULL;
			}
			if (grrec==NULL) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports/maproot: can't find group named '%s' defined in line: %"PRIu32,gptr+1,lineno);
				return -1;
			}
			gid = grrec->gr_gid;
		}
		gidok = 1;
	} else {
		gidok = 0;
		gid = 0;
		gptr = NULL;
	}

	uid = 0;
	eptr = uptr;
	while (*eptr>='0' && *eptr<='9') {
		uid*=10;
		uid+=*eptr-'0';
		eptr++;
	}
	if (*eptr!=0) {	// not only digits - treat it as a username
		if (getpwnam_r(uptr,&pwd,pwgrbuff,16384,&pwrec)!=0) {
			pwrec = NULL;
		}
		if (pwrec==NULL) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports/maproot: can't find user named '%s' defined in line: %"PRIu32,uptr,lineno);
			return -1;
		}
		*ruid = pwrec->pw_uid;
		if (gidok==0) {
			*rgid = pwrec->pw_gid;
		} else {
			*rgid = gid;
		}
		return 0;
	} else if (gidok==1) {
		*ruid = uid;
		*rgid = gid;
		return 0;
	} else {
		if (getpwuid_r(uid,&pwd,pwgrbuff,16384,&pwrec)!=0) {
			pwrec = NULL;
		}
		if (pwrec==NULL) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports/maproot: can't determine gid, because can't find user with uid %"PRIu32" defined in line: %"PRIu32,uid,lineno);
			return -1;
		}
		*ruid = pwrec->pw_uid;
		*rgid = pwrec->pw_gid;
		return 0;
	}
	return -1;	// unreachable
}

int exports_parsedisable(char *disablestr,uint32_t lineno,uint32_t *disables) {
	char *p;
	int o;

	while ((p=exports_strsep(&disablestr,":"))) {
		o=0;
		switch (*p) {
			case 'a':
				if (strcmp(p,"appendchunks")==0) {
					*disables |= DISABLE_APPENDCHUNKS;
					o=1;
				}
				break;
			case 'c':
				if (strcmp(p,"chown")==0) {
					*disables |= DISABLE_CHOWN;
					o=1;
				} else if (strcmp(p,"chmod")==0) {
					*disables |= DISABLE_CHMOD;
					o=1;
				} else if (strcmp(p,"create")==0) {
					*disables |= DISABLE_CREATE;
					o=1;
				}
				break;
			case 'l':
				if (strcmp(p,"link")==0) {
					*disables |= DISABLE_LINK;
					o=1;
				}
				break;
			case 'm':
				if (strcmp(p,"mkfifo")==0) {
					*disables |= DISABLE_MKFIFO;
					o=1;
				} else if (strcmp(p,"mkdev")==0) {
					*disables |= DISABLE_MKDEV;
					o=1;
				} else if (strcmp(p,"mksock")==0) {
					*disables |= DISABLE_MKSOCK;
					o=1;
				} else if (strcmp(p,"mkdir")==0) {
					*disables |= DISABLE_MKDIR;
					o=1;
				} else if (strcmp(p,"move")==0) {
					*disables |= DISABLE_MOVE;
					o=1;
				}
				break;
			case 'r':
				if (strcmp(p,"rmdir")==0) {
					*disables |= DISABLE_RMDIR;
					o=1;
				} else if (strcmp(p,"rename")==0) {
					*disables |= DISABLE_RENAME;
					o=1;
				} else if (strcmp(p,"readdir")==0) {
					*disables |= DISABLE_READDIR;
					o=1;
				} else if (strcmp(p,"read")==0) {
					*disables |= DISABLE_READ;
					o=1;
				}
				break;
			case 's':
				if (strcmp(p,"symlink")==0) {
					*disables |= DISABLE_SYMLINK;
					o=1;
				} else if (strcmp(p,"setlength")==0) {
					*disables |= DISABLE_SETLENGTH;
					o=1;
				} else if (strcmp(p,"snapshot")==0) {
					*disables |= DISABLE_SNAPSHOT;
					o=1;
				} else if (strcmp(p,"settrash")==0) {
					*disables |= DISABLE_SETTRASH;
					o=1;
				} else if (strcmp(p,"setsclass")==0) {
					*disables |= DISABLE_SETSCLASS;
					o=1;
				} else if (strcmp(p,"seteattr")==0) {
					*disables |= DISABLE_SETEATTR;
					o=1;
				} else if (strcmp(p,"setxattr")==0) {
					*disables |= DISABLE_SETXATTR;
					o=1;
				} else if (strcmp(p,"setfacl")==0) {
					*disables |= DISABLE_SETFACL;
					o=1;
				}
				break;
			case 't':
				if (strcmp(p,"truncate")==0) {
					*disables |= DISABLE_TRUNCATE;
					o=1;
				}
				break;
			case 'u':
				if (strcmp(p,"unlink")==0) {
					*disables |= DISABLE_UNLINK;
					o=1;
				}
				break;
			case 'w':
				if (strcmp(p,"write")==0) {
					*disables |= DISABLE_WRITE;
					o=1;
				}
				break;
		}
		if (o==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: unknown disable command '%s' in line: %"PRIu32" (ignored)",p,lineno);
		}
	}
	return 0;
}

int exports_parseoptions(char *opts,uint32_t lineno,exports *arec) {
	char *p;
	int o;
	md5ctx ctx;
	uint8_t mingoal=1,maxgoal=9;
	uint8_t goal_defined = 0;
	uint8_t sclassgroups_defined = 0;
	uint16_t sclassgroups;

	while ((p=exports_strsep(&opts,","))) {
		o=0;
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"option: %s",p);
		switch (*p) {
		case 'r':
			if (strcmp(p,"ro")==0) {
				arec->sesflags |= SESFLAG_READONLY;
				o=1;
			} else if (strcmp(p,"readonly")==0) {
				arec->sesflags |= SESFLAG_READONLY;
				o=1;
			} else if (strcmp(p,"rw")==0) {
				arec->sesflags &= ~SESFLAG_READONLY;
				o=1;
			} else if (strcmp(p,"readwrite")==0) {
				arec->sesflags &= ~SESFLAG_READONLY;
				o=1;
			}
			break;
		case 'i':
			if (strcmp(p,"ignoregid")==0) {
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					arec->sesflags |= SESFLAG_IGNOREGID;
				}
				o=1;
			}
			break;
		case 'a':
			if (strcmp(p,"alldirs")==0) {
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					arec->alldirs = 1;
				}
				o=1;
			} else if (strcmp(p,"admin")==0) {
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					arec->sesflags |= SESFLAG_ADMIN;
				}
				o=1;
			}
			break;
		case 'd':
			if (strcmp(p,"dynamicip")==0) {
				arec->sesflags |= SESFLAG_DYNAMICIP;
				o=1;
			} else if (strncmp(p,"disable=",8)==0) {
				o=1;
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					if (exports_parsedisable(p+8,lineno,&arec->disables)<0) {
						return -1;
					}
				}
			}
			break;
		case 'c':
			if (strcmp(p,"canchangequota")==0) { // deprecated - use 'admin'
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					arec->sesflags |= SESFLAG_ADMIN;
				}
				o=1;
			}
			break;
		case 'm':
			if (strncmp(p,"maproot=",8)==0) {
				o=1;
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					if (exports_parseuidgid(p+8,lineno,&arec->rootuid,&arec->rootgid)<0) {
						return -1;
					}
					arec->rootredefined = 1;
				}
			} else if (strncmp(p,"mapall=",7)==0) {
				o=1;
				if (arec->meta) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"meta option ignored: %s",p);
				} else {
					if (exports_parseuidgid(p+7,lineno,&arec->mapalluid,&arec->mapallgid)<0) {
						return -1;
					}
					arec->sesflags |= SESFLAG_MAPALL;
				}
			} else if (strncmp(p,"md5pass=",8)==0) {
				char *ptr = p+8;
				uint32_t i=0;
				o=1;
				while ((*ptr>='0' && *ptr<='9') || (*ptr>='a' && *ptr<='f') || (*ptr>='A' && *ptr<='F')) {
					ptr++;
					i++;
				}
				if (*ptr==0 && i==32) {
					ptr = p+8;
					for (i=0 ; i<16 ; i++) {
						if (*ptr>='0' && *ptr<='9') {
							arec->passworddigest[i]=(*ptr-'0')<<4;
						} else if (*ptr>='a' && *ptr<='f') {
							arec->passworddigest[i]=(*ptr-'a'+10)<<4;
						} else {
							arec->passworddigest[i]=(*ptr-'A'+10)<<4;
						}
						ptr++;
						if (*ptr>='0' && *ptr<='9') {
							arec->passworddigest[i]+=(*ptr-'0');
						} else if (*ptr>='a' && *ptr<='f') {
							arec->passworddigest[i]+=(*ptr-'a'+10);
						} else {
							arec->passworddigest[i]+=(*ptr-'A'+10);
						}
						ptr++;
					}
					arec->needpassword=1;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect md5pass definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			} else if (strncmp(p,"minversion=",11)==0) {
				o=1;
				if (exports_parseversion(p+11,&arec->minversion)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect minversion definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			} else if (strncmp(p,"mingoal=",8)==0) {
				o=1;
				if (sclassgroups_defined) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: mingoal defined together with sclassgroupsin line: %"PRIu32" - use only sclassgroups",lineno);
					return -1;
				}
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsexports: mingoal option is deprecated, use sclassgroups instead");
				if (exports_parsegoal(p+8,&mingoal)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect mingoal definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				if (mingoal>maxgoal) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: mingoal>maxgoal in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				goal_defined=1;
			} else if (strncmp(p,"maxgoal=",8)==0) {
				o=1;
				if (sclassgroups_defined) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: maxgoal defined together with sclassgroupsin line: %"PRIu32" - use only sclassgroups",lineno);
					return -1;
				}
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsexports: maxgoal option is deprecated, use sclassgroups instead");
				if (exports_parsegoal(p+8,&maxgoal)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect maxgoal definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				if (mingoal>maxgoal) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: maxgoal<mingoal in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				goal_defined=1;
			} else if (strncmp(p,"mintrashretention=",18)==0) {
				o=1;
				if (parse_hperiod(p+18,&arec->mintrashretention)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect mintrashretention definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				arec->mintrashretention*=3600;
				if (arec->mintrashretention>arec->maxtrashretention) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: mintrashretention>maxtrashretention in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			} else if (strncmp(p,"maxtrashretention=",18)==0) {
				o=1;
				if (parse_hperiod(p+18,&arec->maxtrashretention)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect maxtrashretention definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				arec->maxtrashretention*=3600;
				if (arec->mintrashretention>arec->maxtrashretention) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: maxtrashretention<mintrashretention in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			} else if (strncmp(p,"mintrashtime=",13)==0) {
				o=1;
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsexports: mintrashtime option is deprecated, use mintrashretention instead");
				if (parse_speriod(p+13,&arec->mintrashretention)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect mintrashtime definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				if (arec->mintrashretention>arec->maxtrashretention) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: mintrashtime>maxtrashtime in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			} else if (strncmp(p,"maxtrashtime=",13)==0) {
				o=1;
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsexports: maxtrashtime option is deprecated, use maxtrashretention instead");
				if (parse_speriod(p+13,&arec->maxtrashretention)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect maxtrashtime definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				if (arec->mintrashretention>arec->maxtrashretention) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: maxtrashtime<mintrashtime in definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
			}
			break;
		case 'u':
			if (strncmp(p,"umask=",6)==0) {
				if (exports_parseumask(p+6,&arec->umask)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect umask definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				o=1;
			}
			break;
		case 'p':
			if (strncmp(p,"password=",9)==0) {
				md5_init(&ctx);
				md5_update(&ctx,(uint8_t*)(p+9),strlen(p+9));
				md5_final(arec->passworddigest,&ctx);
				arec->needpassword=1;
				o=1;
			}
			break;
		case 's':
			if (strncmp(p,"sclassgroups=",13)==0) {
				if (goal_defined) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: sclassgroups defined together with mingoal/maxgoal in line: %"PRIu32" - use only sclassgroups",lineno);
					return -1;
				}
				if (exports_parsesclassgroups(p+13,&sclassgroups)<0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect sclassgroups definition (%s) in line: %"PRIu32,p,lineno);
					return -1;
				}
				if (sclassgroups_defined) {
					arec->sclassgroups |= sclassgroups;
				} else {
					arec->sclassgroups = sclassgroups;
					sclassgroups_defined = 1;
				}
				o=1;
			}
			break;
		}
		if (o==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: unknown option '%s' in line: %"PRIu32" (ignored)",p,lineno);
		}
	}
	if (goal_defined) {
		arec->sclassgroups = 1; // group '0'
		for (o=mingoal ; o<=maxgoal ; o++) {
			arec->sclassgroups |= (1<<o);
		}
	}
	return 0;
}

int exports_parseline(char *line,uint32_t lineno,exports *arec,exports **defaults) {
	char *net,*path;
	char *p;
	uint32_t pleng;

	if ((*defaults)==NULL) {
		arec->pleng = 0;
		arec->path = NULL;
		arec->fromip = 0;
		arec->toip = 0;
		arec->minversion = 0;
		arec->alldirs = 0;
		arec->needpassword = 0;
		arec->meta = 0;
		arec->rootredefined = 0;
		arec->sesflags = SESFLAG_READONLY;
		arec->umask = 0;
		arec->sclassgroups = 0xFFFF;
		arec->mintrashretention = 0;
		arec->maxtrashretention = UINT32_C(0xFFFFFFFF);
		arec->rootuid = 999;
		arec->rootgid = 999;
		arec->mapalluid = 999;
		arec->mapallgid = 999;
		arec->disables = 0;
		arec->next = NULL;
	} else {
		memcpy(arec,*defaults,sizeof(exports));
	}

	p = line;
	while (*p==' ' || *p=='\t') {
		p++;
	}
	if (*p==0 || *p=='#') { // empty line or line with comment only
		return -1;
	}
	net = p;
	while (*p && *p!=' ' && *p!='\t') {
		p++;
	}
	if (*p==0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incomplete definition in line: %"PRIu32,lineno);
		return -1;
	}
	*p=0;
	p++;
	if (strcasecmp(net,"DEFAULTS")==0) {
		while (*p==' ' || *p=='\t') {
			p++;
		}

		if (exports_parseoptions(p,lineno,arec)<0) {
			return -1;
		}

		if ((arec->sesflags&SESFLAG_MAPALL) && (arec->rootredefined==0)) {
			arec->rootuid = arec->mapalluid;
			arec->rootgid = arec->mapallgid;
		}

		if ((*defaults)==NULL) {
			*defaults = malloc(sizeof(exports));
			passert(*defaults);
		}
		memcpy(*defaults,arec,sizeof(exports));
		return 0;
	} else if (exports_parsenet(net,&arec->fromip,&arec->toip)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports: incorrect ip/network definition in line: %"PRIu32,lineno);
		return -1;
	}

	while (*p==' ' || *p=='\t') {
		p++;
	}
	if (p[0]=='.' && (p[1]==0 || p[1]==' ' || p[1]=='\t')) {
		path = NULL;
		pleng = 0;
		if (arec->rootredefined==0) {
			arec->rootuid = 0;
			arec->rootgid = 0;
		}
		arec->meta = 1;
		p++;
	} else {
		while (*p=='/') {
			p++;
		}
		path = p;
		pleng = 0;
		while (*p && *p!=' ' && *p!='\t') {
			p++;
			pleng++;
		}
		while (pleng>0 && path[pleng-1]=='/') {
			pleng--;
		}
	}
	if (*p==0) {
		// no options - use defaults
		arec->pleng = pleng;
		if (pleng>0) {
			arec->path = malloc(pleng+1);
			passert(arec->path);
			memcpy((uint8_t*)(arec->path),path,pleng);
			((uint8_t*)(arec->path))[pleng]=0;
		} else {
			arec->path = NULL;
		}

		return 0;
	}
	while (*p==' ' || *p=='\t') {
		p++;
	}

	if (exports_parseoptions(p,lineno,arec)<0) {
		return -1;
	}

	if ((arec->sesflags&SESFLAG_MAPALL) && (arec->rootredefined==0)) {
		arec->rootuid = arec->mapalluid;
		arec->rootgid = arec->mapallgid;
	}

	arec->pleng = pleng;
	if (pleng>0) {
		arec->path = malloc(pleng+1);
		passert(arec->path);
		memcpy((uint8_t*)(arec->path),path,pleng);
		((uint8_t*)(arec->path))[pleng]=0;
	} else {
		arec->path = NULL;
	}

	return 0;
}

void exports_loadexports(void) {
	FILE *fd;
	char *linebuff;
	size_t lbsize;
	uint32_t s,lineno;
	exports *newexports,**netail,*arec,*defaults;

	fd = fopen(ExportsFileName,"r");
	if (fd==NULL) {
		if (errno==ENOENT) {
			if (exports_records) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"mfsexports configuration file (%s) not found - exports not changed",ExportsFileName);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsexports configuration file (%s) not found - no exports !!!",ExportsFileName);
				fprintf(stderr,"mfsexports configuration file (%s) not found - please create one (you can copy %s.sample to get a base configuration)\n",ExportsFileName,ExportsFileName);
			}
		} else {
			if (exports_records) {
				mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"can't open mfsexports configuration file (%s) - exports not changed, error",ExportsFileName);
			} else {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"can't open mfsexports configuration file (%s) - no exports !!!, error",ExportsFileName);
			}
		}
		return;
	}
	newexports = NULL;
	netail = &newexports;
	lineno = 1;
	arec = malloc(sizeof(exports));
	passert(arec);
	defaults = NULL;
	lbsize = 10000;
	linebuff = malloc(lbsize);
	while (getline(&linebuff,&lbsize,fd)!=-1) {
		s=strlen(linebuff);
		while (s>0 && (linebuff[s-1]=='\r' || linebuff[s-1]=='\n' || linebuff[s-1]=='\t' || linebuff[s-1]==' ')) {
			s--;
		}
		if (s>0) {
			linebuff[s]=0;
			if (exports_parseline(linebuff,lineno,arec,&defaults)>=0) {
				*netail = arec;
				netail = &(arec->next);
				arec = malloc(sizeof(exports));
				passert(arec);
			}
		}
		lineno++;
	}
	free(linebuff);
	free(arec);
	if (defaults!=NULL) {
		free(defaults);
	}
	if (ferror(fd)) {
		fclose(fd);
		if (exports_records) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error reading mfsexports file - exports not changed");
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error reading mfsexports file - no exports !!!");
		}
		exports_freelist(newexports);
		return;
	}
	fclose(fd);
	exports_freelist(exports_records);
	exports_records = newexports;
	exports_csum = 0;
	for (arec=exports_records ; arec!=NULL ; arec=arec->next) {
		exports_csum += exports_entry_checksum(arec);
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"exports file has been loaded");
}

void exports_reload(void) {
	int fd;
	if (ExportsFileName) {
		free(ExportsFileName);
	}
	if (!cfg_isdefined("EXPORTS_FILENAME")) {
		ExportsFileName = strdup(ETC_PATH "/mfs/mfsexports.cfg");
		passert(ExportsFileName);
		if ((fd = open(ExportsFileName,O_RDONLY))<0 && errno==ENOENT) {
			char *tmpname;
			tmpname = strdup(ETC_PATH "/mfsexports.cfg");
			if ((fd = open(tmpname,O_RDONLY))>=0) {
				free(ExportsFileName);
				ExportsFileName = tmpname;
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"default sysconf path has changed - please move mfsexports.cfg from "ETC_PATH"/ to "ETC_PATH"/mfs/");
			} else {
				free(tmpname);
			}
		}
		if (fd>=0) {
			close(fd);
		}
		cfg_use_option("EXPORTS_FILENAME",ExportsFileName);
	} else {
		ExportsFileName = cfg_getstr("EXPORTS_FILENAME",ETC_PATH "/mfs/mfsexports.cfg");
	}
	exports_loadexports();
}

void exports_term(void) {
	exports_freelist(exports_records);
	if (ExportsFileName) {
		free(ExportsFileName);
	}
}

int exports_init(void) {
	exports_records = NULL;
	ExportsFileName = NULL;
	exports_reload();
	if (exports_records==NULL) {
		fprintf(stderr,"no exports defined !!!\n");
		return -1;
	}
//	main_reload_register(exports_reload); // reload called by matoclserv_reload_sessions
	main_destruct_register(exports_term);
	return 0;
}
