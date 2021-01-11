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
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
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
#include "chunks.h"
#include "filesystem.h"
#include "metadata.h"
#include "datapack.h"
#include "sockets.h"
#include "random.h"
#include "slogger.h"
#include "massert.h"
#include "merger.h"
#include "changelog.h"
#include "clocks.h"
#include "matoclserv.h"
#include "matocsserv.h"
#include "matomlserv.h"
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

static uint64_t metaversion;
static uint64_t metaid;

static uint8_t ignoreflag = 0;
static uint8_t allowautorestore = 0;
static uint8_t verboselevel = 0;

static uint32_t lastsuccessfulstore = 0;
static double laststoretime = 0.0;
static uint8_t laststorestatus = 0;

static uint32_t BackMetaCopies;
static uint32_t MetaSaveFreq;

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
		syslog(LOG_NOTICE,"write error");
		return -1;
	}

	if (storefn) {
		storefn(fd);

		if (offbegin!=0) {
			offend = bio_file_position(fd);
			ptr = hdr+8;
			put64bit(&ptr,offend-offbegin-16);
			bio_seek(fd,offbegin+8,SEEK_SET);
			if (bio_write(fd,hdr+8,8)!=(size_t)8) {
				syslog(LOG_NOTICE,"write error");
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
		syslog(LOG_NOTICE,"write error");
		return;
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

int meta_load(bio *fd,uint8_t fver) {
	uint8_t hdr[16];
	const uint8_t *ptr;
	off_t offbegin=0;
	uint64_t sleng;
	uint32_t maxnodeid=0,nextsessionid;
	double profdata;
	uint8_t mver;

	if (bio_read(fd,hdr,16)!=16) {
		fprintf(stderr,"error loading header\n");
		return -1;
	}

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
		if (fs_importnodes(fd,maxnodeid)<0) {
			syslog(LOG_ERR,"error reading metadata (node)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading names ... ");
		fflush(stderr);
		if (fs_loadedges(fd,0x10,ignoreflag)<0) {
			syslog(LOG_ERR,"error reading metadata (edge)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading deletion timestamps ... ");
		fflush(stderr);
		if (fs_loadfree(fd,0x10)<0) {
			syslog(LOG_ERR,"error reading metadata (free)");
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading chunks data ... ");
		fflush(stderr);
		if (chunk_load(fd,0x10)<0) {
			fprintf(stderr,"error\n");
			syslog(LOG_ERR,"error reading metadata (chunks)");
			return -1;
		}
		fprintf(stderr,"ok\n");
	} else { // fver>=0x16
		while (1) {
			if (bio_read(fd,hdr,16)!=16) {
				fprintf(stderr,"error section header\n");
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
					if (fs_importnodes(fd,maxnodeid)<0) {
						syslog(LOG_ERR,"error reading metadata (node)");
						return -1;
					}
				} else {
					if (mver>fs_storenodes(NULL)) {
						mfs_syslog(LOG_ERR,"error reading metadata (node) - metadata in file have been stored by newer version of MFS !!!");
						return -1;
					}
					fprintf(stderr,"loading objects (files,directories,etc.) ... ");
					fflush(stderr);
					if (fs_loadnodes(fd,mver)<0) {
						syslog(LOG_ERR,"error reading metadata (node)");
						return -1;
					}
				}
			} else if (memcmp(hdr,"EDGE",4)==0) {
				if (mver>fs_storeedges(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (edge) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading names ... ");
				fflush(stderr);
				if (fs_loadedges(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (edge)");
					return -1;
				}
			} else if (memcmp(hdr,"FREE",4)==0) {
				if (mver>fs_storefree(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (free) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading deletion timestamps ... ");
				fflush(stderr);
				if (fs_loadfree(fd,mver)<0) {
					syslog(LOG_ERR,"error reading metadata (free)");
					return -1;
				}
			} else if (memcmp(hdr,"QUOT",4)==0) {
				if (mver>fs_storequota(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (quota) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading quota definitions ... ");
				fflush(stderr);
				if (fs_loadquota(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (quota)");
					return -1;
				}
			} else if (memcmp(hdr,"XATR",4)==0) {
				if (mver>xattr_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (xattr) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading xattr data ... ");
				fflush(stderr);
				if (xattr_load(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (xattr)");
					return -1;
				}
			} else if (memcmp(hdr,"PACL",4)==0) {
				if (mver>posix_acl_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (posix_acl) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading posix_acl data ... ");
				fflush(stderr);
				if (posix_acl_load(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (posix_acl)");
					return -1;
				}
			} else if (memcmp(hdr,"FLCK",4)==0) {
				if (mver>flock_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (flock_locks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading flock_locks data ... ");
				fflush(stderr);
				if (flock_load(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (flock_locks)");
					return -1;
				}
			} else if (memcmp(hdr,"PLCK",4)==0) {
				if (mver>posix_lock_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (posix_locks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading posix_locks data ... ");
				fflush(stderr);
				if (posix_lock_load(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (posix_locks)");
					return -1;
				}
			} else if (memcmp(hdr,"CSDB",4)==0) {
				if (mver>csdb_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (csdb) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading chunkservers data ... ");
				fflush(stderr);
				if (csdb_load(fd,mver,ignoreflag)<0) {
					fprintf(stderr,"error\n");
					syslog(LOG_ERR,"error reading metadata (csdb)");
					return -1;
				}
			} else if (memcmp(hdr,"SESS",4)==0) {
				if (mver>sessions_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (sessions) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading sessions data ... ");
				fflush(stderr);
				if (sessions_load(fd,mver)<0) {
					fprintf(stderr,"error\n");
					syslog(LOG_ERR,"error reading metadata (sessions)");
					return -1;
				}
			} else if (memcmp(hdr,"LABS",4)==0 || memcmp(hdr,"SCLA",4)==0) {
				if (mver>sclass_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (storage classes) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading storage classes data ... ");
				fflush(stderr);
				if (sclass_load(fd,mver,ignoreflag)<0) {
					syslog(LOG_ERR,"error reading metadata (storage classes)");
					return -1;
				}
			} else if (memcmp(hdr,"OPEN",4)==0) {
				if (mver>of_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (open files) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading open files data ... ");
				fflush(stderr);
				if (of_load(fd,mver)<0) {
					fprintf(stderr,"error\n");
					syslog(LOG_ERR,"error reading metadata (open files)");
					return -1;
				}
			} else if (memcmp(hdr,"CHNK",4)==0) {
				if (mver>chunk_store(NULL)) {
					mfs_syslog(LOG_ERR,"error reading metadata (chunks) - metadata in file have been stored by newer version of MFS !!!");
					return -1;
				}
				fprintf(stderr,"loading chunks data ... ");
				fflush(stderr);
				if (chunk_load(fd,mver)<0) {
					fprintf(stderr,"error\n");
					syslog(LOG_ERR,"error reading metadata (chunks)");
					return -1;
				}
			} else {
				hdr[8]=0;
				if (ignoreflag) {
					fprintf(stderr,"unknown section found (leng:%"PRIu64",name:%s) - all data from this section will be lost !!!\n",sleng,hdr);
					bio_skip(fd,sleng);
				} else {
					fprintf(stderr,"error: unknown section found (leng:%"PRIu64",name:%s)\n",sleng,hdr);
					return -1;
				}
			}
			profdata = monotonic_seconds()-profdata;
			if (sleng<UINT64_C(0xFFFFFFFFFFFFFFFF)) {
				if ((offbegin>=0) && ((offbegin+sleng)!=bio_file_position(fd))) {
					fprintf(stderr,"not all section has been read - file corrupted\n");
					if (ignoreflag==0) {
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
		syslog(LOG_NOTICE,"write error");
	} else {
		meta_store(fd,NULL);
	}
	if (bio_error(fd)!=0) {
		bio_close(fd);
		return -1;
	}
	syslog(LOG_NOTICE,"metadata file stored in emergency mode, file name: %s",fname);
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

int meta_emergency_saves() {
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

void meta_storeended(int status) {
	int chstatus;
	if (storestarttime>0) {
		laststoretime = monotonic_seconds()-storestarttime;
		syslog(LOG_NOTICE,"store process has finished - store time: %.3lf",laststoretime);
	} else {
		laststoretime = 0.0;
		syslog(LOG_NOTICE,"store process has finished - unknown store time");
	}
	if (WIFEXITED(status)) {
		chstatus = WEXITSTATUS(status);
	} else {
		chstatus = 3;
	}
	if (chstatus==1) {
		syslog(LOG_ERR,"metadata stored in emergency mode (in non-standard location) - exiting");
		main_exit();
	} else if (chstatus==2) {
		syslog(LOG_ERR,"metadata not stored !!! (child exited) - exiting");
		main_exit();
	} else if (chstatus==3) {
		syslog(LOG_ERR,"metadata not stored !!! (child was signaled) - exiting");
		main_exit();
	} else if (chstatus!=0) {
		syslog(LOG_ERR,"metadata not stored !!! (unknown status) - exiting");
		main_exit();
	} else {
		storestarttime = 0.0;
		laststorestatus = 0;
		lastsuccessfulstore = main_time();
	}
}

int meta_storeall(int bg) {
	bio *fd;
	int i,estat;
	int mfd;
	int pfd[2];

//	struct stat sb;
	if (metaversion==0) {
		return 2;
	}
//	if (stat("metadata.mfs.back.tmp",&sb)==0) {
//		syslog(LOG_ERR,"previous metadata save process hasn't finished yet - do not start another one");
//		return -1;
//	}
	mfd = open("metadata.mfs.back.tmp",O_RDWR);
	if (mfd>=0) {
		if (lockf(mfd,F_TEST,0)<0) {
			if (LOCK_ERRNO_ERROR) {
				mfs_errlog(LOG_ERR,"metadata store lockf error");
			} else {
				syslog(LOG_ERR,"previous metadata save process hasn't finished yet - do not start another one");
			}
			close(mfd);
			return -1;
		}
		close(mfd);
	}
	if (bg) {
		if (pipe(pfd)<0) {
			pfd[0]=-1;
			pfd[1]=-1;
		}
		i = fork();
		if (i<0) {
#if defined(__linux__)
			mfs_errlog(LOG_WARNING,"fork error (store data in foreground - it will block master for a while - check /proc/sys/vm/overcommit_memory and if necessary set to 1)");
#else
			mfs_errlog(LOG_WARNING,"fork error (store data in foreground - it will block master for a while)");
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
				syslog(LOG_WARNING,"metadata store, pipe read error");
			}
			close(pfd[0]);
			close(pfd[1]);
		} else {
			storestarttime = monotonic_seconds();
		}
		fd = bio_file_open("metadata.mfs.back.tmp",BIO_WRITE,META_FILE_BUFFER_SIZE);
//		fd = fopen("metadata.mfs.back.tmp","w");
		if (fd==NULL) {
			mfs_errlog(LOG_ERR,"metadata store child - open error");
			// try to save in alternative location - just in case
			estat = meta_emergency_saves();
			if (i==0) { // background
				if (estat<0) {
					exit(2); // not stored
				} else {
					exit(1); // stored in emrgency mode
				}
			}
			return 0;
		}
		if (i==0) { // store in background - lock file
			mfd = bio_descriptor(fd);
			if (lockf(mfd,F_TLOCK,0)<0) {
				if (LOCK_ERRNO_ERROR) {
					mfs_errlog(LOG_ERR,"metadata store child - lockf error");
				} else {
					syslog(LOG_ERR,"metadata store child process - file is already locked !!!");
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
			syslog(LOG_NOTICE,"write error");
		} else {
			meta_store(fd,"metadata.crc");
		}
		if (bio_error(fd)!=0) {
			syslog(LOG_ERR,"can't write metadata");
			bio_close(fd);
			unlink("metadata.mfs.back.tmp");
			// try to save in alternative location - just in case
			estat = meta_emergency_saves();
			if (i==0) { // background
				if (estat<0) {
					exit(2); // not stored
				} else {
					exit(1); // stored in emrgency mode
				}
			}
			return 0;
		} else {
			bio_close(fd);
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
		if (i==0) { // background
			exit(0);
		} else {
			lastsuccessfulstore = time(NULL);
			laststoretime = monotonic_seconds()-storestarttime;
			laststorestatus = 2; // Stored in foreground
			storestarttime = 0.0;
		}
	} else {
		storestarttime = monotonic_seconds();
		main_chld_register(i,meta_storeended);
		if (write(pfd[1],"x",1)!=1) {
			syslog(LOG_WARNING,"metadata store, pipe read error");
		}
		close(pfd[0]);
		close(pfd[1]);
	}
	return 1;
}


void meta_dostoreall(void) {
	changelog_rotate();
	if (((main_time() / 3600) % MetaSaveFreq) == 0) {
			if (meta_storeall(1)<=0) {
				syslog(LOG_ERR,"can't store metadata - exiting");
				main_exit();
			}
		}
}

void meta_cleanup(void) {
	mfs_syslog(LOG_NOTICE,"cleaning metadata ...");
	fs_cleanup();
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
	fprintf(stderr,"cleaning dictionary data ...");
	fflush(stderr);
	dict_cleanup();
	fprintf(stderr,"done\n");
	metaversion = 0;
	mfs_syslog(LOG_NOTICE,"metadata have been cleaned");
}

void meta_term(void) {
	changelog_rotate();
	for (;;) {
		uint8_t status;
		status = meta_storeall(0);
		if (status==1) {
			if (rename("metadata.mfs.back","metadata.mfs")<0) {
				mfs_errlog(LOG_WARNING,"can't rename metadata.mfs.back -> metadata.mfs");
			}
			meta_cleanup();
			return;
		} else if (status==2) {
			mfs_syslog(LOG_NOTICE,"no metadata to store");
			return;
		}
		syslog(LOG_ERR,"can't store metadata - try to make more space on your hdd or change privieleges - retrying after 10 seconds");
		sleep(10);
	}
}

void meta_sendall(int socket) {
	int i;
	bio *fd;
	i = fork();
	if (i==0) {
		fd = bio_socket_open(socket,BIO_WRITE,META_SOCKET_BUFFER_SIZE,META_SOCKET_MSECTO);
		if (bio_write(fd,MFSSIGNATURE "M 2.0",8)!=(size_t)8) {
			syslog(LOG_NOTICE,"write error");
		} else {
			meta_store(fd,NULL);
		}
		bio_close(fd);
		exit(0);
	} else if (i<0) {
#if defined(__linux__)
		mfs_errlog(LOG_WARNING,"fork error - can't send metadata - check /proc/sys/vm/overcommit_memory and if necessary set to 1");
#else
		mfs_errlog(LOG_WARNING,"fork error - can't send metadata");
#endif
//	} else {
//		main_chld_register(i,meta_sendended);
	}
}

int meta_downloadall(int socket) {
	bio *fd;
	uint8_t fver;
	uint8_t hdr[8];

	fprintf(stderr,"download start\n");
	if (socket<0) {
		return -1;
	}
	if (metaversion!=0) {
		meta_cleanup();
	}
	fd = bio_socket_open(socket,BIO_READ,META_SOCKET_BUFFER_SIZE,META_SOCKET_MSECTO);
	if (bio_read(fd,hdr,8)!=8) {
		bio_close(fd);
		fprintf(stderr,"download error\n");
		return -1;
	}
	if (memcmp(hdr,MFSSIGNATURE "M ",5)==0 && hdr[5]>='1' && hdr[5]<='9' && hdr[6]=='.' && hdr[7]>='0' && hdr[7]<='9') {
		fver = ((hdr[5]-'0')<<4)+(hdr[7]-'0');
		if (meta_load(fd,fver)<0) {
			meta_cleanup();
			bio_close(fd);
			fprintf(stderr,"download error\n");
			return -1;
		}
	} else {
		bio_close(fd);
		fprintf(stderr,"download error\n");
		return -1;
	}
	if (bio_error(fd)!=0) {
		meta_cleanup();
		bio_close(fd);
		fprintf(stderr,"download error\n");
		return -1;
	}
	bio_close(fd);
	fs_afterload();
	fprintf(stderr,"download ok\n");
	return 1;
}

int meta_loadfile(const char *filename) {
	bio *fd;
	uint8_t fver;
	uint8_t hdr[8];

	fd = bio_file_open(filename,BIO_READ,META_FILE_BUFFER_SIZE);
	if (fd==NULL) {
		return -1;
	}

	if (bio_read(fd,hdr,8)!=8) {
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
		if (meta_load(fd,fver)<0) {
			meta_cleanup();
			bio_close(fd);
			return -2;
		}
	} else {
		bio_close(fd);
		return -2;
	}
	if (bio_error(fd)!=0) {
		meta_cleanup();
		bio_close(fd);
		return -2;
	}
	bio_close(fd);
	fs_afterload();
	return 1;
}

void meta_file_infos(void) {
	DIR *dd;
	struct dirent *dp;
	uint64_t ver,id;
	uint8_t status;

	dd = opendir(".");
	if (!dd) {
		mfs_errlog(LOG_ERR,"can't access data directory");
	} else {
		while ((dp = readdir(dd)) != NULL) {
			if (strlen(dp->d_name)>8 && memcmp(dp->d_name,"metadata",8)==0) {
				status = meta_check_metadatafile(dp->d_name,&ver,&id);
				if (status==META_CHECK_OK) {
					if (id!=0) {
						mfs_arg_syslog(LOG_NOTICE," - found valid metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
					} else {
						mfs_arg_syslog(LOG_NOTICE," - found valid metadata file: %s (version: %"PRIu64")",dp->d_name,ver);
					}
				} else {
					if (status==META_CHECK_IOERROR) {
						mfs_arg_errlog(LOG_NOTICE," - error reading metadata file: %s",dp->d_name);
					} else if (status==META_CHECK_BADHEADER) {
						mfs_arg_syslog(LOG_NOTICE," - found invalid metadata file (wrong header): %s",dp->d_name);
					} else if (status==META_CHECK_BADENDING) {
						if (id!=0) {
							mfs_arg_syslog(LOG_NOTICE," - found invalid metadata file (wrong ending): %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
						} else {
							mfs_arg_syslog(LOG_NOTICE," - found invalid metadata file (wrong ending): %s (version: %"PRIu64")",dp->d_name,ver);
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

	if (allowautorestore) {
		// find best metadata file
		bestver = 0;
		bestid = 0;
		bestfname = NULL;
		dd = opendir(".");
		if (!dd) {
			mfs_errlog(LOG_ERR,"can't access data directory");
		} else {
			while ((dp = readdir(dd)) != NULL) {
				if (strlen(dp->d_name)>8 && memcmp(dp->d_name,"metadata",8)==0) {
					status = meta_check_metadatafile(dp->d_name,&ver,&id);
					if (verboselevel>1) {
						if (ver>0 && status==META_CHECK_OK) {
							if (id!=0) {
								mfs_arg_syslog(LOG_NOTICE,"found valid metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
							} else {
								mfs_arg_syslog(LOG_NOTICE,"found valid metadata file: %s (version: %"PRIu64")",dp->d_name,ver);
							}
						} else {
							if (status==META_CHECK_IOERROR) {
								mfs_arg_errlog(LOG_NOTICE,"error reading metadata file: %s",dp->d_name);
							} else if (status==META_CHECK_BADHEADER) {
								mfs_arg_syslog(LOG_NOTICE,"found invalid metadata file (wrong header): %s",dp->d_name);
							} else if (status==META_CHECK_BADENDING) {
								if (id!=0) {
									mfs_arg_syslog(LOG_NOTICE,"found invalid metadata file (wrong ending): %s (version: %"PRIu64" ; id: %"PRIX64")",dp->d_name,ver,id);
								} else {
									mfs_arg_syslog(LOG_NOTICE,"found invalid metadata file (wrong ending): %s (version: %"PRIu64")",dp->d_name,ver);
								}
							}
						}
					}
					if (status==META_CHECK_OK) {
						if (bestid!=0 && id!=0 && bestid!=id) {
							if (ignoreflag) {
								mfs_syslog(LOG_NOTICE,"found metadata file with different id number - ignoring");
							} else {
								mfs_syslog(LOG_NOTICE,"found metadata file with different id number - cleanup your working directory or use '-i' flag (might be dangerous without cleaning)");
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
						mfs_arg_syslog(LOG_NOTICE,"found valid metadata file: %s (version: %"PRIu64")",hfname,ver);
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
						mfs_arg_syslog(LOG_NOTICE,"found valid metadata file: %s (version: %"PRIu64")",fname,ver);
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
			mfs_syslog(LOG_ERR,"can't find valid metadata file");
			if (bestfname) {
				free((char*)bestfname);
			}
			return -1;
		}
		if (verboselevel>0) {
			if (bestid!=0) {
				mfs_arg_syslog(LOG_NOTICE,"chosen most recent metadata file: %s (version: %"PRIu64" ; id: %"PRIX64")",bestfname,bestver,bestid);
			} else {
				mfs_arg_syslog(LOG_NOTICE,"chosen most recent metadata file: %s (version: %"PRIu64")",bestfname,bestver);
			}
		}
		// load it
		if (meta_loadfile(bestfname)<0) {
			mfs_arg_errlog(LOG_ERR,"error loading metadata file (%s)",bestfname);
			free((char*)bestfname);
			return -1;
		}
//		meta_cleanup();
//		if (meta_loadfile(bestfname)<0) {
//			mfs_arg_errlog(LOG_ERR,"error loading metadata file (%s)",bestfname);
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
							mfs_arg_syslog(LOG_NOTICE,"skipping changelog file: %s (changes: %s - %s)",filenames[pos],fvp,lvp);
						} else {
							mfs_arg_syslog(LOG_NOTICE,"using changelog file: %s (changes: %s - %s)",filenames[pos],fvp,lvp);
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
					mfs_syslog(LOG_NOTICE,"error applying changelogs - ignoring (using best possible metadata version)");
				} else {
					mfs_syslog(LOG_NOTICE,"error applying changelogs - fix changelogs manually or use '-i' flag");
					return -1;
				}
			}
		} else if (dd) {
			closedir(dd);
		}
		if (stat("metadata.mfs",&st)==0) {
			if (st.st_size==0) {
				if (unlink("metadata.mfs")<0) {
					mfs_errlog(LOG_ERR,"can't unlink metadata.mfs");
					return -1;
				}
			} else {
				if (stat("metadata.mfs.back",&st)<0 && errno==ENOENT) {
					if (rename("metadata.mfs","metadata.mfs.back")<0) {
						mfs_errlog(LOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back");
						return -1;
					}
				} else {
					char *name = strdup("metadata.mfs.XXXXXX");
#ifdef HAVE_MKSTEMP
					int fd;
					fd = mkstemp(name);
					if (fd<0) {
						mfs_arg_errlog(LOG_ERR,"can't create temporary file %s",name);
						free(name);
						return -1;
					}
#elif HAVE_MKTEMP
					if (mktemp(name)==NULL) {
						mfs_arg_errlog(LOG_ERR,"can't create temporary file %s",name);
						free(name);
						return -1;
					}
#endif
					if (rename("metadata.mfs",name)<0) {
						mfs_arg_errlog(LOG_ERR,"can't rename metadata.mfs -> %s",name);
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
				mfs_syslog(LOG_ERR,"can't find metadata.mfs - try using option '-a'");
				return -1;
			case META_CHECK_IOERROR:
				mfs_errlog(LOG_ERR,"error reading metadata.mfs - try using option '-a'");
				return -1;
			case META_CHECK_BADHEADER:
				mfs_syslog(LOG_ERR,"metadata.mfs has wrong header - try using option '-a'");
				return -1;
			case META_CHECK_BADENDING:
				mfs_syslog(LOG_ERR,"metadata.mfs has wrong ending - try using option '-a'");
				return -1;
		}
		if (meta_check_metadatafile("metadata.mfs.back",&bestver,&bestid)==META_CHECK_OK && bestver>ver && bestid!=0 && id!=0 && bestid!=id) {
			if (bestver>ver) {
				mfs_syslog(LOG_ERR,"backup file is newer than current file - please check it manually - try using option '-a'");
			} else {
				mfs_syslog(LOG_ERR,"backup file has different file id - please check it manually - try using option '-a' and '-i'");
			}
			return -1;
		}
		if (meta_loadfile("metadata.mfs")<0) {
			mfs_syslog(LOG_ERR,"error loading metadata.mfs - try using option '-a'");
			return -1;
		}
		if (rename("metadata.mfs","metadata.mfs.back")<0) {
			mfs_errlog(LOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back");
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

void meta_incverboselevel(void) {
	verboselevel++;
}

void meta_info(uint32_t *lsstore,uint32_t *lstime,uint8_t *lsstat) {
	*lsstore = lastsuccessfulstore;
	*lstime = (laststoretime*1000);
	*lsstat = laststorestatus;
}

uint64_t meta_get_id(void) {
	return metaid;
}

void meta_set_id(uint64_t newmetaid) {
	metaid = newmetaid;
}

void meta_reload(void) {
	uint32_t back_logs;

	MetaSaveFreq = cfg_getuint32("METADATA_SAVE_FREQ",1);
	back_logs = cfg_getuint32("BACK_LOGS",50);
	if (MetaSaveFreq>(back_logs/2)) {
		mfs_syslog(LOG_WARNING,"METADATA_SAVE_FREQ is higher than half of BACK_LOGS - decreasing");
		MetaSaveFreq = back_logs/2;
	}
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",1);
	if (BackMetaCopies>99) {
		mfs_syslog(LOG_WARNING,"BACK_META_KEEP_PREVIOUS is too high (>99) - decreasing");
		BackMetaCopies=99;
	}
}

void meta_check_id(void) {
	if (metaid==0) {
		uint32_t now = main_time();
		metaid = now;
		metaid <<= 32;
		metaid |= rndu32() + monotonic_useconds();
		changelog("%"PRIu32"|SETMETAID(%"PRIu64")",now,metaid);
	}
}


uint8_t meta_mr_setmetaid(uint64_t newmetaid) {
	if (metaid==0 || metaid==newmetaid) {
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
		mfs_syslog(LOG_ERR,"dictionary init error");
		return -1;
	}
	if (sclass_init()<0) {
		mfs_syslog(LOG_ERR,"storage class init error");
		return -1;
	}
	if (fs_strinit()<0) {
		mfs_syslog(LOG_ERR,"filesystem-tree init error");
		return -1;
	}
	if (chunk_strinit()<0) {
		mfs_syslog(LOG_ERR,"chunk init error");
		return -1;
	}
	if (xattr_init()<0) {
		mfs_syslog(LOG_ERR,"xattr init error");
		return -1;
	}
	if (posix_acl_init()<0) {
		mfs_syslog(LOG_ERR,"posix_acl init error");
		return -1;
	}
	if (flock_init()<0) {
		mfs_syslog(LOG_ERR,"flock_locks init error");
		return -1;
	}
	if (posix_lock_init()<0) {
		mfs_syslog(LOG_ERR,"posix_locks init error");
		return -1;
	}
	if (csdb_init()<0) {
		mfs_syslog(LOG_ERR,"csdb init error");
		return -1;
	}
	if (sessions_init()<0) {
		mfs_syslog(LOG_ERR,"sessions init error");
		return -1;
	}
	if (of_init()<0) {
		mfs_syslog(LOG_ERR,"open-files init error");
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
	fprintf(stderr,"loading metadata ...\n");
	if (meta_loadall()<0) {
		return -1;
	}
	status = meta_storeall(0);
	if (status==1) {
		if (rename("metadata.mfs.back","metadata.mfs")<0) {
			mfs_errlog(LOG_WARNING,"can't rename metadata.mfs.back -> metadata.mfs");
		}
		meta_cleanup();
		return 0;
	} else if (status==2) {
		mfs_syslog(LOG_NOTICE,"no metadata to store");
		return 0;
	}
	return -1;
}

int meta_init(void) {
	if (meta_prepare_data_structures()<0) {
		return -1;
	}
	fprintf(stderr,"loading metadata ...\n");
		if (meta_loadall()<0) {
			return -1;
		}
		fprintf(stderr,"metadata file has been loaded\n");
	meta_reload();
	main_reload_register(meta_reload);
	main_time_register(3600,0,meta_dostoreall);
	main_destruct_register(meta_term);
	fs_renumerate_edge_test();
	meta_check_id();
	return 0;
}

/*
void meta_text_dump(FILE *fd) {
	fs_text_dump(fd);
	chunk_text_dump(fd);
	// csdb_text_dump(fd);
	// sessions_text_dump(fd);
}
*/
