/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <inttypes.h>
#include <errno.h>

#include "MFSCommunication.h"

#include "bio.h"
#include "sessions.h"
#include "dictionary.h"
#include "xattr.h"
#include "posixacl.h"
#include "flocklocks.h"
#include "posixlocks.h"
#include "openfiles.h"
#include "csdb.h"
#include "storageclass.h"
#include "patterns.h"
#include "chunks.h"
#include "filesystem.h"
#include "metadata.h"
#include "datapack.h"
#include "sockets.h"
#include "random.h"
#include "mfslog.h"
#include "massert.h"
#include "merger.h"
#include "changelog.h"
#include "clocks.h"
#include "matoclserv.h"
#include "matocsserv.h"
#include "matomlserv.h"
#include "crc.h"
#include "processname.h"

#include "cfg.h"
#include "main.h"

#define META_SOCKET_MSECTO 10000
#define META_SOCKET_BUFFER_SIZE 0x10000
#define META_FILE_BUFFER_SIZE 0x1000000

#define MAXIDHOLE 10000

#ifdef EWOULDBLOCK
#  define LOCK_ERRNO_ERROR (errno!=EACCES && errno!=EAGAIN && errno!=EWOULDBLOCK)
#else
#  define LOCK_ERRNO_ERROR (errno!=EACCES && errno!=EAGAIN)
#endif

// max seconds to keep
#define CHLOG_KEEP_SEND 7200

// seconds to keep after sending
#define CHLOG_KEEP_ATEND 300

#define STORE_UNIT 60

static uint64_t metaversion;
static uint64_t metaid;

static uint8_t ignoreflag = 0;
static uint8_t allowautorestore = 0;
static uint8_t emptystart = 0;
static uint8_t verboselevel = 0;

static uint32_t lastsuccessfulstore = 0;
static double laststoretime = 0.0;
static uint8_t laststorestatus = 0;

static uint64_t laststoremetaversion = 0;
static uint32_t laststorechecksum = 0;

static uint32_t BackMetaCopies;
static uint32_t MetaSaveFreq;
static uint32_t MetaCheckFreq;
static uint32_t MetaDownloadFreq;
static uint32_t MetaSaveOffset;
static uint8_t MetaSaveOffsetLocal;

static pid_t metasaverpid = -1;
static uint8_t metasavermode = 0;
static int metasaverkilled = 0;

typedef struct _chlog_keep {
	uint64_t version;
	uint32_t validtime;
	pid_t saverpid;
	struct _chlog_keep *next;
} chlog_keep;

static chlog_keep *chlog_keep_head = NULL;

static void meta_send_start(pid_t saverpid) {
	chlog_keep *ck;

	ck = malloc(sizeof(chlog_keep));
	ck->version = metaversion;
	ck->validtime = main_time()+CHLOG_KEEP_SEND;
	ck->saverpid = saverpid;
	ck->next = chlog_keep_head;
	chlog_keep_head = ck;
}

static void meta_send_stop(pid_t saverpid,uint8_t ok) {
	chlog_keep *ck;

	for (ck=chlog_keep_head ; ck!=NULL ; ck=ck->next) {
		if (ck->saverpid==saverpid) {
			ck->validtime = main_time() + (ok?CHLOG_KEEP_ATEND:0);
		}
	}
}

uint64_t meta_chlog_keep_version(void) {
	chlog_keep *ck,**ckp;
	uint64_t minversion;
	uint32_t now;

	minversion = metaversion;
	now = main_time();

	ckp = &chlog_keep_head;
	while ((ck=*ckp)!=NULL) {
		if (now>ck->validtime) {
			*ckp = ck->next;
			free(ck);
		} else {
			ckp = &(ck->next);
			if (ck->version < minversion) {
				minversion = ck->version;
			}
		}
	}
	return minversion;
}

static void meta_chlog_keep_free(void) {
	chlog_keep *ck,**ckp;

	ckp = &chlog_keep_head;
	while ((ck=*ckp)!=NULL) {
		*ckp = ck->next;
		free(ck);
	}
}

int meta_store_chunk(bio *fd,uint8_t (*storefn)(bio *),const char chunkname[4]) {
	uint8_t hdr[16];
	uint8_t *ptr;
	uint8_t mver;
	uint64_t offbegin=0,offend;

	if (storefn==NULL) {
		memcpy(hdr,"[MFS EOF MARKER]",16);
	} else {
		memcpy(hdr,chunkname,4);
		mver = storefn(NULL);
		hdr[4] = ' ';
		hdr[5] = '0'+((mver>>4)&0xF);
		hdr[6] = '.';
		hdr[7] = '0'+(mver&0xF);
		offbegin = bio_file_position(fd);
		memset(hdr+8,0xFF,8);
	}
	if (bio_write(fd,hdr,16)!=(size_t)16) {
		return -1;
	}

	if (storefn) {
		storefn(fd);
		if (bio_error(fd)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error writing section '%c%c%c%c'",chunkname[0],chunkname[1],chunkname[2],chunkname[3]);
		}

		if (offbegin!=0) {
			offend = bio_file_position(fd);
			ptr = hdr+8;
			put64bit(&ptr,offend-offbegin-16);
			bio_seek(fd,offbegin+8,SEEK_SET);
			if (bio_write(fd,hdr+8,8)!=(size_t)8) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error updating size of section '%c%c%c%c'",chunkname[0],chunkname[1],chunkname[2],chunkname[3]);
				return -1;
			}
			bio_seek(fd,offend,SEEK_SET);
		}
	}
	return 0;
}

void meta_store(bio *fd,const char *crcfname) {
	bio *crcfd;
	uint8_t hdr[16];
	uint8_t *ptr;

#define STORE_CRC(section) if (crcfd!=NULL) { \
		ptr = hdr; \
		memcpy(ptr,section,4); \
		ptr += 4; \
		put32bit(&ptr,bio_crc(fd)); \
		bio_write(crcfd,hdr,8); \
	}

	if (crcfname!=NULL) {
		crcfd = bio_file_open(crcfname,BIO_WRITE,1024);
	} else {
		crcfd = NULL;
	}

	ptr = hdr;
	put64bit(&ptr,metaversion);
	put64bit(&ptr,metaid);
	if (bio_write(fd,hdr,16)!=(size_t)16) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error writing metadata header");
		return;
	}
	if (crcfd!=NULL) {
		bio_write(crcfd,hdr,16);
	}
	STORE_CRC("HEAD")
	if (meta_store_chunk(fd,sessions_store,"SESS")<0) { // (metadump!!!)
		return;
	}
	STORE_CRC("SESS")
	if (meta_store_chunk(fd,sclass_store,"SCLA")<0) { // needs to be before NODE (refcnt)
		return;
	}
	STORE_CRC("SCLA")
	if (meta_store_chunk(fd,patterns_store,"PATT")<0) { // needs to be before NODE (refcnt)
		return;
	}
	STORE_CRC("PATT")
	if (meta_store_chunk(fd,fs_storenodes,"NODE")<0) {
		return;
	}
	STORE_CRC("NODE")
	if (meta_store_chunk(fd,fs_storeedges,"EDGE")<0) {
		return;
	}
	STORE_CRC("EDGE")
	if (meta_store_chunk(fd,fs_storefree,"FREE")<0) {
		return;
	}
	STORE_CRC("FREE")
	if (meta_store_chunk(fd,fs_storequota,"QUOT")<0) {
		return;
	}
	STORE_CRC("QUOT")
	if (meta_store_chunk(fd,xattr_store,"XATR")<0) { // dependency: NODE (fsnodes<->xattr)
		return;
	}
	STORE_CRC("XATR")
	if (meta_store_chunk(fd,posix_acl_store,"PACL")<0) { // dependency: NODE (fsnodes<->posix_acl))
		return;
	}
	STORE_CRC("PACL")
	if (meta_store_chunk(fd,of_store,"OPEN")<0) {
		return;
	}
	STORE_CRC("OPEN")
	if (meta_store_chunk(fd,flock_store,"FLCK")<0) { // dependency: OPEN
		return;
	}
	STORE_CRC("FLCK")
	if (meta_store_chunk(fd,posix_lock_store,"PLCK")<0) { // dependency: OPEN
		return;
	}
	STORE_CRC("PLCK")
	if (meta_store_chunk(fd,csdb_store,"CSDB")<0) {
		return;
	}
	STORE_CRC("CSDB")
	if (meta_store_chunk(fd,chunk_store,"CHNK")<0) {
		return;
	}
	STORE_CRC("CHNK")
	if (meta_store_chunk(fd,NULL,NULL)<0) {
		return;
	}
	STORE_CRC("TAIL")
	if (crcfd!=NULL) {
		bio_sync(crcfd);
		if (bio_error(crcfd)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error writing metadata crc file");
		}
		bio_close(crcfd);
	}
}

#define META_CHECK_OK 0
#define META_CHECK_NOFILE 1
#define META_CHECK_IOERROR 2
#define META_CHECK_BADHEADER 3
#define META_CHECK_BADENDING 4

uint8_t meta_check_metadatafile(const char *name,uint64_t *ver,uint64_t *id) {
	int fd;
	int err;
	uint8_t chkbuff[16];
	uint8_t eofmark[16];
	uint8_t fver;
	const uint8_t *ptr;

	*ver = 0;
	*id = 0;
	fd = open(name,O_RDONLY);
	if (fd<0) {
		if (errno==ENOENT) {
			return META_CHECK_NOFILE;
		} else {
			return META_CHECK_IOERROR;
		}
	}
	if (read(fd,chkbuff,8)!=8) {
		err = errno;
		close(fd);
		errno = err;
		return META_CHECK_IOERROR;
	}
	if (memcmp(chkbuff,"MFSM NEW",8)==0) {
		close(fd);
		*ver = 1;
		return META_CHECK_OK;
	}
	if (memcmp(chkbuff,MFSSIGNATURE "M ",5)==0 && chkbuff[5]>='1' && chkbuff[5]<='9' && chkbuff[6]=='.' && chkbuff[7]>='0' && chkbuff[7]<='9') {
		fver = ((chkbuff[5]-'0')<<4)+(chkbuff[7]-'0');
	} else {
		close(fd);
		return META_CHECK_BADHEADER;
	}
	if (fver<0x16) {
		memset(eofmark,0,16);
	} else {
		memcpy(eofmark,"[MFS EOF MARKER]",16);
	}
	if (read(fd,chkbuff,16)!=16) {
		err = errno;
		close(fd);
		errno = err;
		return META_CHECK_IOERROR;
	}
	if (fver<0x20) {
		ptr = chkbuff+4;
		*ver = get64bit(&ptr);
//		*id = 0;
	} else {
		ptr = chkbuff;
		*ver = get64bit(&ptr);
		*id = get64bit(&ptr);
	}
	lseek(fd,-16,SEEK_END);
	if (read(fd,chkbuff,16)!=16) {
		err = errno;
		close(fd);
		errno = err;
		return META_CHECK_IOERROR;
	}
	close(fd);
	if (memcmp(chkbuff,eofmark,16)!=0) {
		return META_CHECK_BADENDING;
	}
	return META_CHECK_OK;
}

int meta_load(bio *fd,uint8_t fver,uint8_t *afterload) {
	uint8_t hdr[16];
	const uint8_t *ptr;
	off_t offbegin=0;
	uint64_t sleng;
	uint32_t maxnodeid=0,nextsessionid;
	double profdata;
	uint8_t mver;

	if (bio_read(fd,hdr,16)!=16) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error loading metadata header");
		return -1;
	}

	*afterload = 1; // by default set afterload to 'true'
	ptr = hdr;
	if (fver<0x20) {
		sessions_import();
		maxnodeid = get32bit(&ptr);
		metaversion = get64bit(&ptr);
		nextsessionid = get32bit(&ptr);
		sessions_set_nextsessionid(nextsessionid);
		metaid = 0;
//		metaid = main_time();
//		metaid <<= 32;
//		metaid |= rndu32();
	} else {
		metaversion = get64bit(&ptr);
		metaid = get64bit(&ptr);
	}


	if (fver<0x16) {
		fprintf(stderr,"loading objects (files,directories,etc.) ... ");
		fflush(stderr);
		if (fs_importnodes(fd,maxnodeid,ignoreflag)<0) {
			fprintf(stderr,"error\n");
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (node)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading names ... ");
		fflush(stderr);
		if (fs_loadedges(fd,0x10,ignoreflag)<0) {
			fprintf(stderr,"error\n");
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (edge)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading deletion timestamps ... ");
		fflush(stderr);
		if (fs_loadfree(fd,0x10,ignoreflag)<0) {
			fprintf(stderr,"error\n");
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (free)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading chunks data ... ");
		fflush(stderr);
		if (chunk_load(fd,0x10,ignoreflag)<0) {
			fprintf(stderr,"error\n");
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (chunks)");
			return -1;
		}
		fprintf(stderr,"ok\n");
	} else { // fver>=0x16
		while (1) {
			if (bio_read(fd,hdr,16)!=16) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error loading metadata section header");
				return -1;
			}
			if (memcmp(hdr,"[MFS EOF MARKER]",16)==0) {
				break;
			}
			ptr = hdr+8;
			sleng = get64bit(&ptr);
			if (sleng<UINT64_C(0xFFFFFFFFFFFFFFFF)) {
				offbegin = bio_file_position(fd);
			}
			profdata = monotonic_seconds();
			mver = (((hdr[5]-'0')&0xF)<<4)+((hdr[7]-'0')&0xF);
			if (memcmp(hdr,"NODE",4)==0) {
				if (fver<0x20) {
					fprintf(stderr,"loading objects (files,directories,etc.) ... ");
					fflush(stderr);
					if (fs_importnodes(fd,maxnodeid,ignoreflag)<0) {
						fprintf(stderr,"error\n");
						mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (node)");
						return -1;
					}
				} else {
					if (mver>fs_storenodes(NULL)) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (node) - metadata in file have been stored by newer version of MFS !!!");
						return -1;
					}
					fprintf(stderr,"loading objects (files,directories,etc.) ... ");
					fflush(stderr);
					if (fs_loadnodes(fd,mver,ignoreflag)<0) {
						fprintf(stderr,"error\n");
						mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (node)");
						return -1;
					}
				}
			} else if (memcmp(hdr,"EDGE",4)==0) {
				if (mver>fs_storeedges(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (edge) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading names ... ");
				fflush(stderr);
				if (fs_loadedges(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (edge)");
					return -1;
				}
			} else if (memcmp(hdr,"FREE",4)==0) {
				if (mver>fs_storefree(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (free) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading deletion timestamps ... ");
				fflush(stderr);
				if (fs_loadfree(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (free)");
					return -1;
				}
			} else if (memcmp(hdr,"QUOT",4)==0) {
				if (mver>fs_storequota(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (quota) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading quota definitions ... ");
				fflush(stderr);
				if (fs_loadquota(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (quota)");
					return -1;
				}
			} else if (memcmp(hdr,"XATR",4)==0) {
				if (mver>xattr_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (xattr) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading xattr data ... ");
				fflush(stderr);
				if (xattr_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (xattr)");
					return -1;
				}
			} else if (memcmp(hdr,"PACL",4)==0) {
				if (mver>posix_acl_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (posix_acl) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading posix_acl data ... ");
				fflush(stderr);
				if (posix_acl_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (posix_acl)");
					return -1;
				}
			} else if (memcmp(hdr,"FLCK",4)==0) {
				if (mver>flock_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (flock_locks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading flock_locks data ... ");
				fflush(stderr);
				if (flock_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (flock_locks)");
					return -1;
				}
			} else if (memcmp(hdr,"PLCK",4)==0) {
				if (mver>posix_lock_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (posix_locks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading posix_locks data ... ");
				fflush(stderr);
				if (posix_lock_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (posix_locks)");
					return -1;
				}
			} else if (memcmp(hdr,"CSDB",4)==0) {
				if (mver>csdb_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (csdb) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading chunkservers data ... ");
				fflush(stderr);
				if (csdb_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (csdb)");
					return -1;
				}
			} else if (memcmp(hdr,"SESS",4)==0) {
				if (mver>sessions_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (sessions) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading sessions data ... ");
				fflush(stderr);
				if (sessions_load(fd,mver)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (sessions)");
					return -1;
				}
			} else if (memcmp(hdr,"LABS",4)==0 || memcmp(hdr,"SCLA",4)==0) {
				if (mver>sclass_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (storage classes) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading storage classes data ... ");
				fflush(stderr);
				if (sclass_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (storage classes)");
					return -1;
				}
			} else if (memcmp(hdr,"PATT",4)==0) {
				if (mver>patterns_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (patterns) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading patterns data ... ");
				fflush(stderr);
				if (patterns_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (patterns)");
					return -1;
				}
			} else if (memcmp(hdr,"OPEN",4)==0) {
				if (mver>of_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (open files) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading open files data ... ");
				fflush(stderr);
				if (of_load(fd,mver)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (open files)");
					return -1;
				}
			} else if (memcmp(hdr,"CHNK",4)==0) {
				if (mver>chunk_store(NULL)) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata (chunks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading chunks data ... ");
				fflush(stderr);
				if (chunk_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error reading metadata (chunks)");
					return -1;
				}
				*afterload = chunk_is_afterload_needed(mver);
			} else {
				hdr[8]=0;
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"unknown section found (leng:%"PRIu64",name:%s) - all data from this section will be lost !!!",sleng,hdr);
					bio_skip(fd,sleng);
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error: unknown section found (leng:%"PRIu64",name:%s)",sleng,hdr);
					return -1;
				}
			}
			profdata = monotonic_seconds()-profdata;
			if (sleng<UINT64_C(0xFFFFFFFFFFFFFFFF)) {
				if ((offbegin>=0) && ((offbegin+sleng)!=bio_file_position(fd))) {
					if (ignoreflag) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"not all section has been read - file corrupted - ignoring");
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"not all section has been read - file corrupted");
						return -1;
					}
				}
			}
			fprintf(stderr,"ok (%.4lf)\n",profdata);
		}
	}
	return fs_check_consistency(ignoreflag);
}

int meta_file_storeall(const char *fname) {
	bio *fd;
	fd = bio_file_open(fname,BIO_WRITE,META_FILE_BUFFER_SIZE);
	if (fd==NULL) {
		return -1;
	}
	if (bio_write(fd,MFSSIGNATURE "M 2.0",8)!=(size_t)8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error writing metadata signature in emergency mode, file name: %s",fname);
	} else {
		meta_store(fd,NULL);
	}
	bio_sync(fd);
	if (bio_error(fd)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"can't write metadata in emergency mode, file name: %s",fname);
		bio_close(fd);
		return -1;
	}
	mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"metadata file stored in emergency mode, file name: %s",fname);
	bio_close(fd);
	return 0;
}

const char* meta_emergency_locations[] = {
	"/metadata.mfs.emergency",
	"/tmp/metadata.mfs.emergency",
	"/var/metadata.mfs.emergency",
	"/usr/metadata.mfs.emergency",
	"/usr/share/metadata.mfs.emergency",
	"/usr/local/metadata.mfs.emergency",
	"/usr/local/var/metadata.mfs.emergency",
	"/usr/local/share/metadata.mfs.emergency",
	NULL
};

char* meta_create_homedir_emergency_filename(void) {
#if defined(HAVE_PWD_H) && defined(HAVE_GETPWUID)
	struct passwd *p;
	p = getpwuid(getuid());
	if (p) {
		char *fname;
		int l;
		l = strlen(p->pw_dir);
		fname = malloc(l+24);
		if (fname) {
			memcpy(fname,p->pw_dir,l);
			fname[l]='/';
			memcpy(fname+l+1,"metadata.mfs.emergency",22);
			fname[l+23]=0;
		}
		return fname;
	} else {
		return NULL;
	}
#else
	return NULL;
#endif
}

int meta_emergency_saves(void) {
	char *hfname;
	const char *fname;
	int i;
	if (meta_file_storeall("metadata.mfs.emergency")==0) {
		return 0;
	}
	hfname = meta_create_homedir_emergency_filename();
	if (hfname) {
		if (meta_file_storeall(hfname)==0) {
			free(hfname);
			return 0;
		}
		free(hfname);
	}
	i = 0;
	while ((fname = meta_emergency_locations[i++])!=NULL) {
		if (meta_file_storeall(fname)==0) {
			return 0;
		}
	}
	return -1;
}

static double storestarttime;

void meta_process_crcdata(void) {
	const uint8_t *rptr;
	uint64_t crcmetaversion,crcmetaid;
	uint8_t *crcdata;
	int64_t crcdatasize;
	bio* crcfd;

	crcfd = bio_file_open("metadata.crc",BIO_READ,1024);
	if (crcfd==NULL) {
		crcdata = NULL;
		crcdatasize = 0;
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't process crc file");
	} else {
		crcdatasize = bio_file_size(crcfd);
		crcdata = malloc(crcdatasize);
		passert(crcdata);
		if (bio_read(crcfd,crcdata,crcdatasize)!=crcdatasize) {
			free(crcdata);
			crcdata = NULL;
			crcdatasize = 0;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't process crc file");
		}
		bio_close(crcfd);
	}
	if (crcdatasize>=16) {
		rptr = crcdata;
		crcmetaversion = get64bit(&rptr);
		crcmetaid = get64bit(&rptr);
		if (crcmetaid != metaid) {
			laststoremetaversion = 0;
			laststorechecksum = 0;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"wrong metaid - ignoring crc data");
		} else {
			laststoremetaversion = crcmetaversion;
			laststorechecksum = mycrc32(0,rptr,crcdatasize-16);
		}
	}
	free(crcdata);
}

void meta_sendended(pid_t pid,int status) {
	uint8_t ok;

	ok = 0;

	if (WIFEXITED(status)) {
		if (WEXITSTATUS(status)==0) {
			ok = 1;
		}
	}

	meta_send_stop(pid,ok);
}

void meta_storeended(pid_t pid,int status) {
	int chstatus;
	(void)pid;

	if (metasaverpid!=pid) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"pid mismatch - saver pid: %d, sigchld pid: %d",(int)metasaverpid,(int)pid);
	}
	if (metasaverkilled) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"store process has been killed due to termination");
		unlink("metadata.mfs.back.tmp");
		unlink("metadata.crc");
		chstatus = 0;
		storestarttime = 0.0;
	} else {
		if (storestarttime>0) {
			laststoretime = monotonic_seconds()-storestarttime;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"store process has finished - store time: %.3lf",laststoretime);
		} else {
			laststoretime = 0.0;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"store process has finished - unknown store time");
		}
		if (WIFEXITED(status)) {
			chstatus = WEXITSTATUS(status);
		} else {
			chstatus = 3;
		}
		if (chstatus==1) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"metadata stored in emergency mode (in non-standard location) - exiting");
			main_exit();
		} else if (chstatus==2) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"metadata not stored !!! (child exited) - exiting");
			main_exit();
		} else if (chstatus==3) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"metadata not stored !!! (child was signaled) - exiting");
			main_exit();
		} else if (chstatus!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"metadata not stored !!! (unknown status) - exiting");
			main_exit();
		} else {
			storestarttime = 0.0;
			laststorestatus = (metasavermode)?3:0;
			lastsuccessfulstore = main_time();
			meta_process_crcdata();
		}
	}
	metasaverpid = -1;
	metasavermode = 0;
	metasaverkilled = 0;
}

int meta_storeall(int bg,uint8_t dontstore) {
	bio *fd;
	int i,estat;
	int mfd;
	int pfd[2];

//	struct stat sb;
	if (metaversion==0) {
		return 2;
	}
	if (metasaverpid >= 0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"previous metadata save process hasn't finished yet - do not start another one");
		return -1;
	}
//	if (stat("metadata.mfs.back.tmp",&sb)==0) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"previous metadata save process hasn't finished yet - do not start another one");
//		return -1;
//	}
	if (dontstore==0) {
		mfd = open("metadata.mfs.back.tmp",O_RDWR);
		if (mfd>=0) {
			if (lockf(mfd,F_TEST,0)<0) {
				if (LOCK_ERRNO_ERROR) {
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"metadata store lockf error");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"previous metadata save process hasn't finished yet - do not start another one");
				}
				close(mfd);
				return -1;
			}
			close(mfd);
		}
	}
	if (bg) {
		if (pipe(pfd)<0) {
			pfd[0]=-1;
			pfd[1]=-1;
		}
		i = fork();
		if (i<0) {
			if (dontstore==0) { // this is crc only store - do not perform it in foreground
				return -1;
			}
#if defined(__linux__)
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"fork error (store data in foreground - it will block master for a while - check /proc/sys/vm/overcommit_memory and if necessary set to 1)");
#else
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"fork error (store data in foreground - it will block master for a while)");
#endif
		} else if (i==0) { // child
			matocsserv_close_lsock();
			matoclserv_close_lsock();
			matomlserv_close_lsock();
			processname_set("mfsmaster (metadata saver)");
		}
	} else {
		i = -1;
	}
	// if fork returned -1 (fork error) store metadata in foreground !!!
	if (i<=0) {
		if (i==0) { // background
			char c;
			if (read(pfd[0],&c,1)!=1) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"metadata store, pipe read error");
			}
			close(pfd[0]);
			close(pfd[1]);
		} else {
			storestarttime = monotonic_seconds();
		}
		if (dontstore) {
			fd = bio_null_open(BIO_WRITE);
		} else {
			fd = bio_file_open("metadata.mfs.back.tmp",BIO_WRITE,META_FILE_BUFFER_SIZE);
		}
//		fd = fopen("metadata.mfs.back.tmp","w");
		if (fd==NULL) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"metadata store child - open error");
			// try to save in alternative location - just in case
			if (dontstore==0) {
				estat = meta_emergency_saves();
				if (i==0) { // background
					if (estat<0) {
						exit(2); // not stored
					} else {
						exit(1); // stored in emrgency mode
					}
				}
			}
			return 0;
		}
		if (i==0 && dontstore==0) { // store in background - lock file
			mfd = bio_descriptor(fd);
			if (lockf(mfd,F_TLOCK,0)<0) {
				if (LOCK_ERRNO_ERROR) {
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"metadata store child - lockf error");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"metadata store child process - file is already locked !!!");
				}
				bio_close(fd);
				// try to save in alternative location - just in case
				estat = meta_emergency_saves();
				if (estat<0) {
					exit(2); // not stored
				} else {
					exit(1); // stored in emrgency mode
				}
			}
		}
		if (bio_write(fd,MFSSIGNATURE "M 2.0",8)!=(size_t)8) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"error writing metadata signature");
		} else {
			meta_store(fd,"metadata.crc");
		}
		bio_sync(fd);
		if (bio_error(fd)!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"can't write metadata");
			bio_close(fd);
			if (dontstore==0) {
				unlink("metadata.mfs.back.tmp");
			}
			unlink("metadata.crc");
			// try to save in alternative location - just in case
			if (dontstore==0) {
				estat = meta_emergency_saves();
				if (i==0) { // background
					if (estat<0) {
						exit(2); // not stored
					} else {
						exit(1); // stored in emrgency mode
					}
				}
			}
			return 0;
		} else {
			bio_close(fd);
			if (dontstore==0) {
				if (BackMetaCopies>0) {
					char metaname1[100],metaname2[100];
					int n;
					for (n=BackMetaCopies-1 ; n>0 ; n--) {
						snprintf(metaname1,100,"metadata.mfs.back.%"PRIu32,n+1);
						snprintf(metaname2,100,"metadata.mfs.back.%"PRIu32,n);
						rename(metaname2,metaname1);
					}
					rename("metadata.mfs.back","metadata.mfs.back.1");
				}
				rename("metadata.mfs.back.tmp","metadata.mfs.back");
				unlink("metadata.mfs");
			}
		}
		if (i==0) { // background
			exit(0);
		} else {
			lastsuccessfulstore = time(NULL);
			laststoretime = monotonic_seconds()-storestarttime;
			storestarttime = 0.0;
			laststorestatus = 2; // Stored in foreground
			meta_process_crcdata();
		}
	} else {
		storestarttime = monotonic_seconds();
		main_chld_register(i,meta_storeended);
		metasaverpid = i;
		metasavermode = dontstore;
		metasaverkilled = 0;
		if (write(pfd[1],"x",1)!=1) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"metadata store, pipe read error");
		}
		close(pfd[0]);
		close(pfd[1]);
	}
	return 1;
}

void meta_download_status(uint8_t status) {
	if (storestarttime>0.0) {
		laststoretime = monotonic_seconds()-storestarttime;
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"end of downloading metadata - download time: %.3lf",laststoretime);
	} else {
		laststoretime = 0.0;
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"end of downloading metadata - unknown download time");
	}
	storestarttime = 0.0;
	if (status==0) { // download successful
		laststorestatus = 1; // Downloaded
		lastsuccessfulstore = main_time();
		laststoremetaversion = 0;
		laststorechecksum = 0;
	} else {
		if (meta_storeall(1,0)<=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"can't download metadata and can't store them - exiting");
			main_exit();
		}
	}
}


static uint32_t last_store_htime = 0;

void meta_store_task(void) {
	uint32_t curtime,htime,offset;
	uint32_t rhtime;
	time_t t;
	struct tm lt;

	curtime = main_time();
	rhtime = ( curtime / STORE_UNIT );

	if (MetaSaveOffsetLocal) {
		t = curtime;
		memset(&lt,0,sizeof(struct tm));
		localtime_r(&t, &lt);
		if (lt.tm_gmtoff >= 0) {
			offset = (MetaSaveOffset + (24 * 60) - (lt.tm_gmtoff / 60)) % (24 * 60);
		} else {
			offset = (MetaSaveOffset + (lt.tm_gmtoff / 60)) % (24 * 60);
		}
	} else {
		offset = MetaSaveOffset;
	}
	htime = ( curtime / STORE_UNIT ) - offset;
	if ((htime % MetaSaveFreq) == 0) {
		if (metasaverpid>=0 && last_store_htime + 120 * STORE_UNIT > rhtime) { // previus save still in progress - silently ignore this request
			return;
		}
		last_store_htime = rhtime;
		if (meta_storeall(1,0)<=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"can't store metadata - exiting");
			main_exit();
		}
	}
}

void meta_do_store_metadata() {
	if (meta_storeall(1,0)<=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't store metadata");
	} else {
		last_store_htime = main_time() / STORE_UNIT;
	}
}

void meta_cleanup(void) {
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"cleaning metadata ...");
	fprintf(stderr,"cleaning fs data ...");
	fflush(stderr);
	fs_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning chunks data ...");
	fflush(stderr);
	chunk_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning xattr data ...");
	fflush(stderr);
	xattr_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning posix_acl data ...");
	fflush(stderr);
	posix_acl_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning flock locks data ...");
	fflush(stderr);
	flock_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning posix locks data ...");
	fflush(stderr);
	posix_lock_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning chunkservers data ...");
	fflush(stderr);
	csdb_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning open files data ...");
	fflush(stderr);
	of_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning sessions data ...");
	fflush(stderr);
	sessions_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning storage classes data ...");
	fflush(stderr);
	sclass_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning patterns data ...");
	fflush(stderr);
	patterns_cleanup();
	fprintf(stderr,"done\n");
	fprintf(stderr,"cleaning dictionary data ...");
	fflush(stderr);
	dict_cleanup();
	fprintf(stderr,"done\n");
	metaversion = 0;
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"metadata have been cleaned");
}

int meta_mayexit(void) {
	if (metasaverpid>0) {
		if (metasaverkilled==0) {
			if (kill(metasaverpid,SIGKILL)<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't kill meta saver process");
			} else {
				metasaverkilled = 1;
			}
		}
		return 0;
	}
	return 1;
}

void meta_term(void) {
	changelog_rotate(0);
	for (;;) {
		uint8_t status;
		status = meta_storeall(0,0);
		if (status==1) {
			if (rename("metadata.mfs.back","metadata.mfs")<0) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"can't rename metadata.mfs.back -> metadata.mfs");
			}
			meta_cleanup();
			return;
		} else if (status==2) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"no metadata to store");
			return;
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"can't store metadata - try to make more space on your hdd or change privileges - retrying after 10 seconds");
		sleep(10);
	}
	meta_chlog_keep_free();
}

void meta_sendall(int socket) {
	int i;
	bio *fd;
	i = fork();
	if (i==0) {
		fd = bio_socket_open(socket,BIO_WRITE,META_SOCKET_BUFFER_SIZE,META_SOCKET_MSECTO);
		if (bio_write(fd,MFSSIGNATURE "M 2.0",8)!=(size_t)8) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"error sending metadata signature");
		} else {
			meta_store(fd,NULL);
		}
		bio_shutdown(fd); // send 'EOF' to the peer
		if (bio_error(fd)!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error sending metadata");
		} else {
			bio_wait(fd); // wait for close on the other side
		}
		bio_close(fd);
		exit(0);
	} else if (i<0) {
#if defined(__linux__)
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"fork error - can't send metadata - check /proc/sys/vm/overcommit_memory and if necessary set to 1");
#else
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"fork error - can't send metadata");
#endif
	} else {
		main_chld_register(i,meta_sendended);
		meta_send_start(i);
	}
}

int meta_downloadall(int socket) {
	bio *fd;
	uint8_t fver;
	uint8_t al;
	uint8_t hdr[8];

	al = 0;

	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"metadata download start");
	if (socket<0) {
		return -1;
	}
	if (metaversion!=0) {
		meta_cleanup();
	}
	fd = bio_socket_open(socket,BIO_READ,META_SOCKET_BUFFER_SIZE,META_SOCKET_MSECTO);
	if (bio_read(fd,hdr,8)!=8) {
		if (bio_error(fd)) {
			errno=bio_lasterrno(fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error downloading metadata");
		}
		bio_close(fd);
		return -1;
	}
	if (memcmp(hdr,MFSSIGNATURE "M ",5)==0 && hdr[5]>='1' && hdr[5]<='9' && hdr[6]=='.' && hdr[7]>='0' && hdr[7]<='9') {
		fver = ((hdr[5]-'0')<<4)+(hdr[7]-'0');
		if (meta_load(fd,fver,&al)<0) {
			if (bio_error(fd)) {
				errno=bio_lasterrno(fd);
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error downloading metadata");
			}
			meta_cleanup();
			bio_close(fd);
			return -1;
		}
	} else {
		if (bio_error(fd)) {
			errno=bio_lasterrno(fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error downloading metadata");
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"wrong metadata header");
		}
		bio_close(fd);
		return -1;
	}
	if (bio_error(fd)!=0) {
		errno=bio_lasterrno(fd);
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error downloading metadata");
		meta_cleanup();
		bio_close(fd);
		return -1;
	}
	bio_close(fd);
	if (al>0) {
		fs_afterload();
	} else {
		fs_printinfo();
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"metadata download ok");
	return 1;
}

int meta_loadfile(const char *filename) {
	bio *fd;
	uint8_t fver;
	uint8_t al;
	uint8_t hdr[8];

	al = 0;

	fd = bio_file_open(filename,BIO_READ,META_FILE_BUFFER_SIZE);
	if (fd==NULL) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error opening metadata");
		return -1;
	}

	if (bio_read(fd,hdr,8)!=8) {
		if (bio_error(fd)) {
			errno=bio_lasterrno(fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error reading metadata");
		}
		bio_close(fd);
		return -1;
	}

	if (memcmp(hdr,"MFSM NEW",8)==0) {
		bio_close(fd);
		fs_new();
		chunk_newfs();
		sessions_new();
		metaversion = 1;
		metaid = 0;
//		metaid = main_time();
//		metaid <<= 32;
//		metaid |= rndu32();
		return 0;
	}
	if (memcmp(hdr,MFSSIGNATURE "M ",5)==0 && hdr[5]>='1' && hdr[5]<='9' && hdr[6]=='.' && hdr[7]>='0' && hdr[7]<='9') {
		fver = ((hdr[5]-'0')<<4)+(hdr[7]-'0');
		if (meta_load(fd,fver,&al)<0) {
			if (bio_error(fd)) {
				errno=bio_lasterrno(fd);
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error reading metadata");
			}
			meta_cleanup();
			bio_close(fd);
			return -2;
		}
	} else {
		if (bio_error(fd)) {
			errno=bio_lasterrno(fd);
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error reading metadata");
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"wrong metadata header");
		}
		bio_close(fd);
		return -2;
	}
	if (bio_error(fd)!=0) {
		errno=bio_lasterrno(fd);
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"error reading metadata");
		meta_cleanup();
		bio_close(fd);
		return -2;
	}
	bio_close(fd);
	if (al) {
		fs_afterload();
	} else {
		fs_printinfo();
	}
	return 1;
}

void meta_file_infos(void) {
	DIR *dd;
	struct dirent *dp;
	uint64_t ver,id;
	uint8_t status;

	dd = opendir(".");
	if (!dd) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't access data directory");
	} else {
		while ((dp = readdir(dd)) != NULL) {
			if (strlen(dp->d_name)>8 && memcmp(dp->d_name,"metadata",8)==0) {
				status = meta_check_metadatafile(dp->d_name,&ver,&id);
				if (status==META_CHECK_OK) {
					if (id!=0) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO," - found valid metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO," - found valid metadata file: %s (version: %"PRIu64")",dp->d_name,ver);
					}
				} else {
					if (status==META_CHECK_IOERROR) {
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE," - error reading metadata file: %s",dp->d_name);
					} else if (status==META_CHECK_BADHEADER) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE," - found invalid metadata file (wrong header): %s",dp->d_name);
					} else if (status==META_CHECK_BADENDING) {
						if (id!=0) {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE," - found invalid metadata file (wrong ending): %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
						} else {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE," - found invalid metadata file (wrong ending): %s (version: %"PRIu64")",dp->d_name,ver);
						}
					}
				}
			}
		}
		closedir(dd);
	}
}

int meta_loadall(void) {
	DIR *dd;
	struct dirent *dp;
	uint64_t bestver,ver,bestid,id,maxlastlv;
	const char *bestfname;
	uint32_t files,pos;
	char **filenames;
	uint8_t status;
	char *hfname;
	const char *fname;
	struct stat st;
	int i;

	if (emptystart) {
		return 0;
	}
	if (allowautorestore) {
		// find best metadata file
		bestver = 0;
		bestid = 0;
		bestfname = NULL;
		dd = opendir(".");
		if (!dd) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't access data directory");
		} else {
			while ((dp = readdir(dd)) != NULL) {
				if (strlen(dp->d_name)>8 && memcmp(dp->d_name,"metadata",8)==0) {
					status = meta_check_metadatafile(dp->d_name,&ver,&id);
					if (verboselevel>1) {
						if (ver>0 && status==META_CHECK_OK) {
							if (id!=0) {
								mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"found valid metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
							} else {
								mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"found valid metadata file: %s (version: %"PRIu64")",dp->d_name,ver);
							}
						} else {
							if (status==META_CHECK_IOERROR) {
								mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,"error reading metadata file: %s",dp->d_name);
							} else if (status==META_CHECK_BADHEADER) {
								mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"found invalid metadata file (wrong header): %s",dp->d_name);
							} else if (status==META_CHECK_BADENDING) {
								if (id!=0) {
									mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"found invalid metadata file (wrong ending): %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
								} else {
									mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"found invalid metadata file (wrong ending): %s (version: %"PRIu64")",dp->d_name,ver);
								}
							}
						}
					}
					if (status==META_CHECK_OK) {
						if (bestid!=0 && id!=0 && bestid!=id) {
							if (ignoreflag) {
								mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"found metadata file with different id number - ignoring");
							} else {
								mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"found metadata file with different id number - cleanup your working directory or use '-i' flag (might be dangerous without cleaning)");
								closedir(dd);
								meta_file_infos();
								if (bestfname) {
									free((char*)bestfname);
								}
								return -1;
							}
						}
						if (ver>bestver) {
							bestver = ver;
							if (bestfname) {
								free((char*)bestfname);
							}
							bestfname = strdup(dp->d_name);
							if (id) {
								bestid = id;
							}
						}
					}
				}
			}
			closedir(dd);
		}
		if (bestid!=0) { // use emergency locations only if valid id has been found
			hfname = meta_create_homedir_emergency_filename();
			if (hfname) {
				status = meta_check_metadatafile(hfname,&ver,&id);
				if (verboselevel>1) {
					if (ver>0 && status==META_CHECK_OK) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"found valid metadata file: %s (version: %"PRIu64")",hfname,ver);
					}
				}
				if (status==META_CHECK_OK && ver>bestver && id==bestid) {
					bestver = ver;
					if (bestfname) {
						free((char*)bestfname);
					}
					bestfname = strdup(hfname);
				}
				free(hfname);
			}
			i = 0;
			while ((fname = meta_emergency_locations[i++])!=NULL) {
				status = meta_check_metadatafile(fname,&ver,&id);
				if (verboselevel>1) {
					if (ver>0 && status==META_CHECK_OK) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"found valid metadata file: %s (version: %"PRIu64")",fname,ver);
					}
				}
				if (status==META_CHECK_OK && ver>bestver && id==bestid) {
					bestver = ver;
					if (bestfname) {
						free((char*)bestfname);
					}
					bestfname = strdup(fname);
				}
			}
		}

		if (bestver==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"can't find valid metadata file");
			if (bestfname) {
				free((char*)bestfname);
			}
			return -1;
		}
		if (verboselevel>0) {
			if (bestid!=0) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"chosen most recent metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",bestfname,bestver,bestid);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"chosen most recent metadata file: %s (version: %"PRIu64")",bestfname,bestver);
			}
		}
		// load it
		if (meta_loadfile(bestfname)<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error loading metadata file (%s)",bestfname);
			free((char*)bestfname);
			return -1;
		}
//		meta_cleanup();
//		if (meta_loadfile(bestfname)<0) {
//			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"error loading metadata file (%s)",bestfname);
//			free((char*)bestfname);
//			return -1;
//		}
		free((char*)bestfname);
		// apply changelogs
		dd = opendir(".");
		files = 0;
		maxlastlv = 0;
		if (dd) {
			while ((dp = readdir(dd)) != NULL) {
				files += changelog_checkname(dp->d_name);
			}
		}
		if (files>0) {
			filenames = (char**)malloc(sizeof(char*)*files);
			rewinddir(dd);
			pos = 0;
			while ((dp = readdir(dd)) != NULL) {
				if (changelog_checkname(dp->d_name)) {
					uint64_t firstlv,lastlv;
					uint8_t skip;
					filenames[pos] = strdup(dp->d_name);
					firstlv = changelog_findfirstversion(filenames[pos]);
					lastlv = changelog_findlastversion(filenames[pos]);
					skip = (lastlv<metaversion || firstlv==0)?1:0;
					if (verboselevel>0) {
						char firstlvstr[21],*fvp;
						char lastlvstr[21],*lvp;
						if (firstlv>0) {
							fvp = firstlvstr+20;
							*fvp = '\0';
							while (firstlv>0) {
								fvp--;
								*fvp = '0' + (firstlv % 10);
								firstlv /= 10;
							}
						} else {
							fvp = firstlvstr;
							fvp[0] = fvp[1] = fvp[2] = '?';
							fvp[3] = '\0';
						}
						if (lastlv>0) {
							lvp = lastlvstr+20;
							*lvp = '\0';
							while (lastlv>0) {
								lvp--;
								*lvp = '0' + (lastlv % 10);
								lastlv /= 10;
							}
						} else {
							lvp = lastlvstr;
							lvp[0] = lvp[1] = lvp[2] = '?';
							lvp[3] = '\0';
						}
						if (skip) {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"skipping changelog file: %s (changes: %s - %s)",filenames[pos],fvp,lvp);
						} else {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"using changelog file: %s (changes: %s - %s)",filenames[pos],fvp,lvp);
						}
					}
					if (skip) {
						free(filenames[pos]);
						files--;
					} else {
						pos++;
						if (lastlv > maxlastlv) {
							maxlastlv = lastlv;
						}
					}
				}
			}
			closedir(dd);
			merger_start(files,filenames,MAXIDHOLE,bestver,maxlastlv);
			for (pos = 0 ; pos<files ; pos++) {
				free(filenames[pos]);
			}
			free(filenames);
			if (merger_loop(verboselevel)!=0) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"error applying changelogs - ignoring (using best possible metadata version)");
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error applying changelogs - fix changelogs manually or use '-i' flag");
					return -1;
				}
			}
		} else if (dd) {
			closedir(dd);
		}
		if (stat("metadata.mfs",&st)==0) {
			if (st.st_size==0) {
				if (unlink("metadata.mfs")<0) {
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't unlink metadata.mfs");
					return -1;
				}
			} else {
				if (stat("metadata.mfs.back",&st)<0 && errno==ENOENT) {
					if (rename("metadata.mfs","metadata.mfs.back")<0) {
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back");
						return -1;
					}
				} else {
					char *name = strdup("metadata.mfs.XXXXXX");
#ifdef HAVE_MKSTEMP
					int fd;
					fd = mkstemp(name);
					if (fd<0) {
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't create temporary file %s",name);
						free(name);
						return -1;
					}
#elif HAVE_MKTEMP
					if (mktemp(name)==NULL) {
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't create temporary file %s",name);
						free(name);
						return -1;
					}
#endif
					if (rename("metadata.mfs",name)<0) {
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't rename metadata.mfs -> %s",name);
#ifdef HAVE_MKSTEMP
						close(fd);
#endif
						free(name);
						return -1;
					}
#ifdef HAVE_MKSTEMP
					close(fd);
#endif
					free(name);
				}
			}
		}
	} else {
		switch (meta_check_metadatafile("metadata.mfs",&ver,&id)) {
			case META_CHECK_NOFILE:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"can't find metadata.mfs - try using option '-a'");
				return -1;
			case META_CHECK_IOERROR:
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"error reading metadata.mfs - try using option '-a'");
				return -1;
			case META_CHECK_BADHEADER:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"metadata.mfs has wrong header - try using option '-a'");
				return -1;
			case META_CHECK_BADENDING:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"metadata.mfs has wrong ending - try using option '-a'");
				return -1;
		}
		if (meta_check_metadatafile("metadata.mfs.back",&bestver,&bestid)==META_CHECK_OK && ((ver==1 && id==0) || (bestver>ver && bestid==id) || (bestid!=0 && id!=0 && bestid!=id))) {
			if (ver==1 && id==0) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"backup file exists but current metadata file is empty - please check it manually - try using option '-a'");
			} else if (bestver>ver) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"backup file is newer than current file - please check it manually - try using option '-a'");
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"backup file has different file id - please check it manually - try using option '-a' and '-i'");
			}
			return -1;
		}
		if (meta_check_metadatafile("metadata_ml.mfs.back",&bestver,&bestid)==META_CHECK_OK && ((ver==1 && id==0) || (bestver>ver && bestid==id) || (bestid!=0 && id!=0 && bestid!=id))) {
			if (ver==1 && id==0) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"metalogger file exists but current metadata file is empty - please check it manually - try using option '-a'");
			} else if (bestver>ver) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"metalogger file is newer than current file - please check it manually - try using option '-a'");
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"metalogger file has different file id - please check it manually - try using option '-a' and '-i'");
			}
			return -1;
		}
		if (meta_loadfile("metadata.mfs")<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"error loading metadata.mfs - try using option '-a'");
			return -1;
		}
		if (rename("metadata.mfs","metadata.mfs.back")<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back");
			return -1;
		}
	}

	unlink("metadata.mfs.back.tmp");
	return 0;
}

uint64_t meta_version_inc(void) {
	return metaversion++;
}

uint64_t meta_version(void) {
	return metaversion;
}

void meta_setignoreflag(void) {
	ignoreflag = 1;
}

void meta_allowautorestore(void) {
	allowautorestore = 1;
}

void meta_emptystart(void) {
	emptystart = 1;
}

void meta_incverboselevel(void) {
	verboselevel++;
}

void meta_info(uint32_t *lsstore,uint32_t *lstime,uint8_t *lsstat,uint64_t *lsmetavers,uint32_t *lschecksum) {
	*lsstore = lastsuccessfulstore;
	*lstime = (laststoretime*1000);
	*lsstat = laststorestatus;
	*lsmetavers = laststoremetaversion;
	*lschecksum = laststorechecksum;
}

uint64_t meta_get_id(void) {
	return metaid;
}

void meta_set_id(uint64_t newmetaid) {
	metaid = newmetaid;
	if (fs_set_root_times(metaid>>32)<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"internal error: can't set atime/mtime/ctime on root node");
		main_exit();
	}
}

// parse number in format X:Y -> X*60+Y
void meta_parse_offset(const char* offsetstr) {
	uint32_t hours,mins;
	uint8_t error;
	const char *p;

	hours = 0;
	mins = 0;
	MetaSaveOffsetLocal = 0;
	for (p=offsetstr ; *p ; p++) {
		error = 0;
		if (*p>='0' && *p<='9') {
			if (MetaSaveOffsetLocal) {
				error = 1;
			} else {
				mins *= 10;
				mins += *p - '0';
			}
		} else if (*p==':') {
			if (MetaSaveOffsetLocal || hours) {
				error = 1;
			} else {
				hours = mins;
				mins = 0;
			}
		} else if (*p=='L' || *p=='l') {
			MetaSaveOffsetLocal = 1;
		} else if (*p!=' ' && *p!='\t') {
			error = 1;
		}
		if (error) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"METADATA_SAVE_OFFSET - parse error - using zero in UTC");
			MetaSaveOffset = 0;
			MetaSaveOffsetLocal = 0;
			return;
		}
	}
	MetaSaveOffset = hours * 60 + mins;
}

void meta_reload(void) {
	uint32_t back_logs;
	char *MetaSaveOffsetStr;

	MetaSaveFreq = cfg_getuint32("METADATA_SAVE_FREQ",1);
	MetaSaveOffsetStr = cfg_getstr("METADATA_SAVE_OFFSET","0");
	meta_parse_offset(MetaSaveOffsetStr);
	free(MetaSaveOffsetStr);

	MetaDownloadFreq = 0;
	MetaCheckFreq = 0;

	back_logs = cfg_getuint32("BACK_LOGS",50);

	if (MetaSaveFreq>(back_logs/2)) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"METADATA_SAVE_FREQ is higher than half of BACK_LOGS - decreasing");
		MetaSaveFreq = back_logs/2;
	}
	if (MetaSaveFreq==0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"METADATA_SAVE_FREQ is zero - set to one");
		MetaSaveFreq = 1;
	}
	MetaSaveFreq *= 60;
	MetaDownloadFreq *= 60;
	MetaCheckFreq *= 60;
	if (MetaSaveOffset > 60*24) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"METADATA_SAVE_OFFSET is higher than 24 hours - using value modulo 24 hours");
		MetaSaveOffset = MetaSaveOffset % 60*24;
	}

	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",1);
	if (BackMetaCopies>99) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"BACK_META_KEEP_PREVIOUS is too high (>99) - decreasing");
		BackMetaCopies=99;
	}
}

void meta_check_id(void) {
	if (metaid==0) {
		uint32_t now = main_time();
		if (fs_set_root_times(now)<0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"internal error: can't set atime/mtime/ctime on root node");
			main_exit();
		} else {
			metaid = now;
			metaid <<= 32;
			metaid |= rndu32() + monotonic_useconds();
			changelog("%"PRIu32"|SETMETAID(%"PRIu64")",now,metaid);
		}
	}
}


uint8_t meta_mr_setmetaid(uint64_t newmetaid) {
	if (metaid==0 || metaid==newmetaid) {
		if (metaid==0) {
			if (fs_set_root_times(metaid>>32)<0) {
				return MFS_ERROR_ENOENT;
			}
		}
		metaversion++;
		metaid = newmetaid;
		return MFS_STATUS_OK;
	} else {
		return MFS_ERROR_EINVAL;
	}
}


int meta_prepare_data_structures(void) {
	metaversion = 0;
	metaid = 0;
	if (dict_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"dictionary init error");
		return -1;
	}
	if (sclass_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"storage class init error");
		return -1;
	}
	if (patterns_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"patterns init error");
		return -1;
	}
	if (fs_strinit()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"filesystem-tree init error");
		return -1;
	}
	if (chunk_strinit()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"chunk init error");
		return -1;
	}
	if (xattr_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"xattr init error");
		return -1;
	}
	if (posix_acl_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"posix_acl init error");
		return -1;
	}
	if (flock_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"flock_locks init error");
		return -1;
	}
	if (posix_lock_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"posix_locks init error");
		return -1;
	}
	if (csdb_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"csdb init error");
		return -1;
	}
	if (sessions_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"sessions init error");
		return -1;
	}
	if (of_init()<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"open-files init error");
		return -1;
	}
	return 0;
}

int meta_restore(void) {
	uint8_t status;

	if (meta_prepare_data_structures()<0) {
		return -1;
	}
	allowautorestore = 1;
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"loading metadata ...");
	if (meta_loadall()<0) {
		return -1;
	}
	status = meta_storeall(0,0);
	if (status==1) {
		if (rename("metadata.mfs.back","metadata.mfs")<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"can't rename metadata.mfs.back -> metadata.mfs");
		}
		meta_cleanup();
		return 0;
	} else if (status==2) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"no metadata to store");
		return 0;
	}
	return -1;
}

int meta_init(void) {
	if (meta_prepare_data_structures()<0) {
		return -1;
	}
	if (emptystart==0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"loading metadata ...");
		if (meta_loadall()<0) {
			return -1;
		}
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"metadata file has been loaded");
	} else {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"can't run master without metadata");
		return -1;
	}
	meta_reload();
	main_reload_register(meta_reload);
	main_time_register(STORE_UNIT,0,meta_store_task);
	main_mayexit_register(meta_mayexit);
	main_destruct_register(meta_term);
	fs_renumerate_edge_test();
	meta_check_id();
	return 0;
}
