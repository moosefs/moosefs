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
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "sharedpointer.h"
#include "filesystem.h"
#include "sessions.h"
#include "openfiles.h"
#include "flocklocks.h"
#include "posixlocks.h"
#include "csdb.h"
#include "chunks.h"
#include "storageclass.h"
#include "metadata.h"
#include "slogger.h"
#include "massert.h"
#include "mfsstrerr.h"

#define EAT(clptr,fn,vno,c) { \
	if (*(clptr)!=(c)) { \
		mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '%c' expected",(fn),(vno),(c)); \
		return -1; \
	} \
	(clptr)++; \
}

#define GETNAME(name,clptr,fn,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	memset((void*)(name),0,256); \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c && _tmp_i<255) { \
		if (_tmp_c=='\0' || _tmp_c=='\r' || _tmp_c=='\n') { \
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '%c' expected",(fn),(vno),(c)); \
			return -1; \
		} \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		name[_tmp_i++] = _tmp_c; \
	} \
	(clptr)--; \
	name[_tmp_i]=0; \
}

#define GETPATH(path,size,clptr,fn,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c) { \
		if (_tmp_c=='\0' || _tmp_c=='\r' || _tmp_c=='\n') { \
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '%c' expected",(fn),(vno),(c)); \
			return -1; \
		} \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		if ((_tmp_i)>=(size)) { \
			(size) = _tmp_i+1000; \
			if ((path)==NULL) { \
				(path) = malloc(size); \
			} else { \
				uint8_t *_tmp_path = (path); \
				(path) = realloc((path),(size)); \
				if ((path)==NULL) { \
					free(_tmp_path); \
				} \
			} \
			passert(path); \
		} \
		(path)[_tmp_i++]=_tmp_c; \
	} \
	if ((_tmp_i)>=(size)) { \
		(size) = _tmp_i+1000; \
		if ((path)==NULL) { \
			(path) = malloc(size); \
		} else { \
			uint8_t *_tmp_path = (path); \
			(path) = realloc((path),(size)); \
			if ((path)==NULL) { \
				free(_tmp_path); \
			} \
		} \
		passert(path); \
	} \
	(clptr)--; \
	(path)[_tmp_i]=0; \
}

#define GETDATA(buff,leng,size,clptr,fn,vno,c) { \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	(leng) = 0; \
	while ((_tmp_c=*((clptr)++))!=c) { \
		if (_tmp_c=='\0' || _tmp_c=='\r' || _tmp_c=='\n') { \
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '%c' expected",(fn),(vno),(c)); \
			return -1; \
		} \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected",(fn),(vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		if ((leng)>=(size)) { \
			(size) = (leng)+1000; \
			if ((buff)==NULL) { \
				(buff) = malloc(size); \
			} else { \
				uint8_t *_tmp_buff = (buff); \
				(buff) = realloc((buff),(size)); \
				if ((buff)==NULL) { \
					free(_tmp_buff); \
				} \
			} \
			passert(buff); \
		} \
		(buff)[(leng)++]=_tmp_c; \
	} \
	(clptr)--; \
}

#define GETARRAYU32(buff,leng,size,clptr,fn,vno) { \
	char _tmp_c; \
	char *eptr; \
	(leng) = 0; \
	_tmp_c = *((clptr)++); \
	if (_tmp_c!='[') { \
		mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '[' expected",(fn),(vno)); \
		return -1; \
	} \
	while ((_tmp_c=*((clptr)++))!=']') { \
		if (_tmp_c=='\0' || _tmp_c=='\r' || _tmp_c=='\n') { \
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": ']' expected",(fn),(vno)); \
			return -1; \
		} \
		if (_tmp_c>='0' && _tmp_c<='9') { \
			if ((leng)>=(size)) { \
				(size) = (leng)+32; \
				if ((buff)==NULL) { \
					(buff) = malloc((size)*sizeof(uint32_t)); \
				} else { \
					uint32_t *_tmp_buff = (buff); \
					(buff) = realloc((buff),(size)*sizeof(uint32_t)); \
					if ((buff)==NULL) { \
						free(_tmp_buff); \
					} \
				} \
				passert(buff); \
			} \
			(buff)[(leng)++] = strtoul(clptr,&eptr,10); \
			clptr = (const char*)eptr; \
		} else if (_tmp_c!=',') { \
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": number or ',' expected",(fn),(vno)); \
			return -1; \
		} \
	} \
}

#define GETU8(data,clptr) { \
	uint32_t tmp; \
	char *eptr; \
	tmp=strtoul(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
	if (tmp>255) { \
		mfs_arg_syslog(LOG_WARNING,"value too big (%"PRIu32" - 0-255 expected)",tmp); \
		return -1; \
	} \
	(data)=tmp; \
}

#define GETU16(data,clptr) { \
	uint32_t tmp; \
	char *eptr; \
	tmp=strtoul(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
	if (tmp>65535) { \
		mfs_arg_syslog(LOG_WARNING,"value too big (%"PRIu32" - 0-65535 expected)",tmp); \
		return -1; \
	} \
	(data)=tmp; \
}

#define GETU32(data,clptr) { \
	char *eptr; \
	(data)=strtoul(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
}

#define GETX32(data,clptr) { \
	char *eptr; \
	(data)=strtoul(clptr,&eptr,16); \
	clptr = (const char*)eptr; \
}

#define GETU64(data,clptr) { \
	char *eptr; \
	(data)=strtoull(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
}

int do_idle(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	meta_version_inc(); // no-operation - just increase meta version and return OK.
	return MFS_STATUS_OK;
}

int do_access(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_access(ts,inode);
}

int do_append(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,inode_src;
	uint32_t slice_from,slice_to;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(inode_src,ptr);
	if (*ptr==')') {
		slice_from = 0xFFFFFFFF;
		slice_to = 0;
	} else {
		EAT(ptr,filename,lv,',');
		GETU32(slice_from,ptr);
		EAT(ptr,filename,lv,',');
		GETU32(slice_to,ptr);
	}
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_append_slice(ts,inode,inode_src,slice_from,slice_to);
}

int do_acquire(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return of_mr_acquire(inode,cuid);
}

int do_archchg(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,nsinodes;
	uint8_t flags;
	uint64_t chgchunks,notchgchunks;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU8(flags,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chgchunks,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(notchgchunks,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nsinodes,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_archchg(ts,inode,uid,flags,chgchunks,notchgchunks,nsinodes);
}

int do_amtime(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,atime,mtime,ctime;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(atime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mtime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(ctime,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_amtime(inode,atime,mtime,ctime);
}

int do_attr(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,mode,uid,gid,atime,mtime,winattr,aclmode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(atime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mtime,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(aclmode,ptr);
		if (*ptr==',') {
			EAT(ptr,filename,lv,',');
			winattr = aclmode;
			GETU32(aclmode,ptr);
		} else {
			winattr = 0;
		}
	} else {
		aclmode = mode;
		winattr = 0;
	}
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_attr(ts,inode,mode,uid,gid,atime,mtime,winattr,aclmode);
}

/*
int do_copy(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_mr_copy(ts,inode,parent,strlen(name),name);
}
*/

int do_create(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t parent,uid,gid,rdev,inode;
	uint16_t mode,cumask;
	uint8_t type,name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	if (*ptr>='0' && *ptr<='9') {
		GETU8(type,ptr);
	} else {
		type = *ptr;
		ptr++;
	}
	EAT(ptr,filename,lv,',');
	GETU16(mode,ptr);
	EAT(ptr,filename,lv,',');
	if (type<16) {
		GETU16(cumask,ptr);
		EAT(ptr,filename,lv,',');
	} else {
		cumask = 0;
	}
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(rdev,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_create(ts,parent,strlen((char*)name),name,type,mode,cumask,uid,gid,rdev,inode);
}

int do_csdbop(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t op,ip,port,arg;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU8(op,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(ip,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(port,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(arg,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return csdb_mr_op(op,ip,port,arg);
}

/* deprecated since version 1.7.25 */
int do_csadd(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t ip,port;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(ip,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(port,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return csdb_mr_csadd(ip,port);
}

/* deprecated since version 1.7.25 */
int do_csdel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t ip,port;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(ip,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(port,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return csdb_mr_csdel(ip,port);
}

int do_chunkadd(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	uint32_t version,lockedto;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(version,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(lockedto,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return chunk_mr_chunkadd(ts,chunkid,version,lockedto);
}

int do_chunkdel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	uint32_t version;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(version,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return chunk_mr_chunkdel(ts,chunkid,version);
}

int do_emptytrash(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sustainedinodes,freeinodes,inode_chksum,bid;
	EAT(ptr,filename,lv,'(');
	if (*ptr!=')') {
		GETU32(bid,ptr);
	} else {
		bid = 0xFFFFFFFF;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sustainedinodes,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(inode_chksum,ptr);
	} else {
		inode_chksum = 0;
	}
	(void)ptr; // silence cppcheck warnings
	return fs_mr_emptytrash(ts,bid,freeinodes,sustainedinodes,inode_chksum);
}

int do_emptysustained(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t freeinodes,inode_chksum,bid;
	EAT(ptr,filename,lv,'(');
	if (*ptr!=')') {
		GETU32(bid,ptr);
	} else {
		bid = 0xFFFFFFFF;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(inode_chksum,ptr);
	} else {
		inode_chksum = 0;
	}
	(void)ptr; // silence cppcheck warnings
	return fs_mr_emptysustained(ts,bid,freeinodes,inode_chksum);
}

int do_flock(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,sessionid;
	uint64_t owner;
	char cmd;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(owner,ptr);
	EAT(ptr,filename,lv,',');
	cmd = *ptr;
	ptr++;
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return flock_mr_change(inode,sessionid,owner,cmd);
}

int do_freeinodes(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sustainedinodes,freeinodes,inode_chksum;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(sustainedinodes,ptr);
	} else {
		sustainedinodes = 0;
	}
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(inode_chksum,ptr);
	} else {
		inode_chksum = 0;
	}
	(void)ptr; // silence cppcheck warnings
	return fs_mr_freeinodes(ts,freeinodes,sustainedinodes,inode_chksum);
}

int do_incversion(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) { // depreciated - replaced by 'setversion'
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return chunk_mr_increase_version(chunkid);
}

int do_setversion(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	uint32_t version;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(version,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return chunk_mr_set_version(chunkid,version);
}


int do_link(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_link(ts,inode,parent,strlen((char*)name),name);
}

int do_length(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	uint64_t length;
	uint8_t canmodmtime;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(length,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU8(canmodmtime,ptr);
	} else {
		canmodmtime = 1;
	}
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_length(ts,inode,length,canmodmtime);
}

int do_move(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent_src,parent_dst;
	uint8_t name_src[256],name_dst[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent_src,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_src,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(parent_dst,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_dst,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_move(ts,parent_src,strlen((char*)name_src),name_src,parent_dst,strlen((char*)name_dst),name_dst,inode);
}

int do_nextchunkid(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return chunk_mr_nextchunkid(chunkid);
}

int do_posixlock(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,sessionid,pid;
	uint64_t owner,start,end;
	char cmd;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(owner,ptr);
	EAT(ptr,filename,lv,',');
	cmd = *ptr;
	ptr++;
	EAT(ptr,filename,lv,',');
	GETU64(start,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(end,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(pid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return posix_lock_mr_change(inode,sessionid,owner,cmd,start,end,pid);
}

int do_purge(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_purge(ts,inode);
}

int do_quota(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,stimestamp,sinodes,hinodes;
	uint64_t slength,ssize,srealsize;
	uint64_t hlength,hsize,hrealsize;
	uint32_t flags,exceeded,timelimit;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(exceeded,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(flags,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(stimestamp,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(hinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(slength,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hlength,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(ssize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hsize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(srealsize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hrealsize,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(timelimit,ptr);
	} else {
		timelimit = 0;
	}
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_quota(ts,inode,exceeded,flags,stimestamp,sinodes,hinodes,slength,hlength,ssize,hsize,srealsize,hrealsize,timelimit);
}

/*
int do_reinit(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_mr_reinit(ts,inode,indx,chunkid);
}
*/
int do_release(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return of_mr_release(inode,cuid);
}

int do_repair(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx;
	uint32_t version;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(version,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_repair(ts,inode,indx,version);
}

int do_renumedges(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t enextedgeid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(enextedgeid,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_renumerate_edges(enextedgeid);
}
/*
int do_remove(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_remove(ts,inode);
}
*/
int do_session(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sessionid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(sessionid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sessions_mr_session(sessionid);
}

int do_sesadd(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t rootinode,sesflags,peerip,sessionid;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint32_t mingoal,maxgoal,mintrashtime,maxtrashtime;
	uint32_t disables;
	uint16_t umaskval;
	uint64_t exportscsum;
	uint32_t ileng;
	static uint8_t *info = NULL;
	static uint32_t infosize = 0;

	(void)ts;
	EAT(ptr,filename,lv,'(');
	if (*ptr=='#') {
		EAT(ptr,filename,lv,'#');
		GETU64(exportscsum,ptr);
		EAT(ptr,filename,lv,',');
	} else {
		exportscsum = 0;
	}
	GETU32(rootinode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sesflags,ptr);
	EAT(ptr,filename,lv,',');
	if (*ptr=='0') {
		if (ptr[1]<'0' || ptr[1]>'7' || ptr[2]<'0' || ptr[2]>'7' || ptr[3]<'0' || ptr[3]>'7') {
			mfs_arg_syslog(LOG_WARNING,"wrong session umask ('%c%c%c' - octal number expected)",ptr[1],ptr[2],ptr[3]);
			return -1;
		}
		umaskval = (ptr[1]-'0') * 64 + (ptr[2]-'0') * 8 + (ptr[3]-'0');
		ptr+=4;
		EAT(ptr,filename,lv,',');
	} else {
		umaskval=0;
	}
	GETU32(rootuid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(rootgid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mapalluid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mapallgid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mingoal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(maxgoal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mintrashtime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(maxtrashtime,ptr);
	EAT(ptr,filename,lv,',');
	if (ptr[0]=='0' && ptr[1]=='x') {
		ptr+=2;
		GETX32(disables,ptr);
		EAT(ptr,filename,lv,',');
	} else {
		disables = 0;
	}
	GETU32(peerip,ptr);
	EAT(ptr,filename,lv,',');
	GETDATA(info,ileng,infosize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(sessionid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sessions_mr_sesadd(exportscsum,rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashtime,maxtrashtime,disables,peerip,info,ileng,sessionid);
}

int do_seschanged(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t rootinode,sesflags,peerip,sessionid;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint32_t mingoal,maxgoal,mintrashtime,maxtrashtime;
	uint32_t disables;
	uint16_t umaskval;
	uint64_t exportscsum;
	uint32_t ileng;
	static uint8_t *info = NULL;
	static uint32_t infosize = 0;

	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,',');
	if (*ptr=='#') {
		EAT(ptr,filename,lv,'#');
		GETU64(exportscsum,ptr);
		EAT(ptr,filename,lv,',');
	} else {
		exportscsum = 0;
	}
	GETU32(rootinode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sesflags,ptr);
	EAT(ptr,filename,lv,',');
	if (*ptr=='0') {
		if (ptr[1]<'0' || ptr[1]>'7' || ptr[2]<'0' || ptr[2]>'7' || ptr[3]<'0' || ptr[3]>'7') {
			mfs_arg_syslog(LOG_WARNING,"wrong session umask ('%c%c%c' - octal number expected)",ptr[1],ptr[2],ptr[3]);
			return -1;
		}
		umaskval = (ptr[1]-'0') * 64 + (ptr[2]-'0') * 8 + (ptr[3]-'0');
		ptr+=4;
		EAT(ptr,filename,lv,',');
	} else {
		umaskval=0;
	}
	GETU32(rootuid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(rootgid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mapalluid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mapallgid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mingoal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(maxgoal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mintrashtime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(maxtrashtime,ptr);
	EAT(ptr,filename,lv,',');
	if (ptr[0]=='0' && ptr[1]=='x') {
		ptr+=2;
		GETX32(disables,ptr);
		EAT(ptr,filename,lv,',');
	} else {
		disables = 0;
	}
	GETU32(peerip,ptr);
	EAT(ptr,filename,lv,',');
	GETDATA(info,ileng,infosize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return sessions_mr_seschanged(sessionid,exportscsum,rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashtime,maxtrashtime,disables,peerip,info,ileng);
}

int do_sesdel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sessionid;

	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return sessions_mr_sesdel(sessionid);
}

int do_sesdisconnected(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sessionid;

	EAT(ptr,filename,lv,'(');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return sessions_mr_disconnected(sessionid,ts);
}

int do_rollback(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx;
	uint64_t prevchunkid,chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(prevchunkid,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_rollback(inode,indx,prevchunkid,chunkid);
}

int do_seteattr(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t eattr,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(eattr,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_seteattr(ts,inode,uid,eattr,smode,ci,nci,npi);
}

int do_setfilechunk(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_set_file_chunk(inode,indx,chunkid);
}

// deprecated
int do_setgoal(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t sclassid,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sclassid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_setsclass(ts,inode,uid,sclassid,sclassid,smode,ci,nci,npi);
}

int do_setsclass(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t src_sclassid,dst_sclassid,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(src_sclassid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(dst_sclassid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_setsclass(ts,inode,uid,src_sclassid,dst_sclassid,smode,ci,nci,npi);
}

int do_setmetaid(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t metaid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(metaid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return meta_mr_setmetaid(metaid);
}

int do_setpath(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	static uint8_t *path = NULL;
	static uint32_t pathsize = 0;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_setpath(inode,path);
}

int do_settrashtime(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint32_t trashtime;
	uint8_t smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(trashtime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_settrashtime(ts,inode,uid,trashtime,smode,ci,nci,npi);
}

int do_setxattr(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,valueleng,mode;
	uint8_t name[256];
	static uint8_t *value = NULL;
	static uint32_t valuesize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETDATA(value,valueleng,valuesize,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_setxattr(ts,inode,strlen((char*)name),name,valueleng,value,mode);
}

int do_setacl(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,aclblobleng;
	uint8_t acltype,changectime;
	uint16_t mode,userperm,groupperm,otherperm,mask,namedusers,namedgroups;
	static uint8_t *aclblob = NULL;
	static uint32_t aclblobsize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU8(changectime,ptr);
	EAT(ptr,filename,lv,',');
	GETU8(acltype,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(userperm,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(groupperm,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(otherperm,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(mask,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(namedusers,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(namedgroups,ptr);
	EAT(ptr,filename,lv,',');
	GETDATA(aclblob,aclblobleng,aclblobsize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	if (aclblobleng!=6U*(namedusers+namedgroups)) {
		return MFS_ERROR_MISMATCH;
	}
	return fs_mr_setacl(ts,inode,mode,changectime,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
}

int do_snapshot(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent,smode,sesflags,uid,gids,umask;
	uint32_t inodecheck,removed,same,exisiting,hardlinks,new;
	static uint8_t *gidstr = NULL;
	static uint32_t gidstrsize = 0;
	static uint32_t *gid = NULL;
	static uint32_t gidsize = 0;
	uint32_t gidleng;
	uint32_t *gidtab;
	uint8_t name[256];
	uint8_t mode;
	uint32_t i;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sesflags,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	if (*ptr=='[') {
		mode=2;
	} else {
		GETU32(gids,ptr);
		EAT(ptr,filename,lv,',');
		mode = 0;
		for (i=0 ; ptr[i] && ptr[i]!=')' ; i++) {
			if (ptr[i]<'0' || ptr[i]>'9') {
				mode=1;
				break;
			}
		}
	}
	if (mode>=1) {
		if (mode==2) {
			GETARRAYU32(gid,gids,gidsize,ptr,filename,lv);
			gidtab = gid;
		} else {
			GETDATA(gidstr,gidleng,gidstrsize,ptr,filename,lv,',');
			if (gids*4!=gidleng) {
				return MFS_ERROR_MISMATCH;
			}
			gidtab = (uint32_t*)gidstr;
		}
		EAT(ptr,filename,lv,',');
		GETU32(umask,ptr);
		EAT(ptr,filename,lv,')');
		if (*ptr==':') {
			EAT(ptr,filename,lv,':');
			GETU32(inodecheck,ptr);
			EAT(ptr,filename,lv,',');
			GETU32(removed,ptr);
			EAT(ptr,filename,lv,',');
			GETU32(same,ptr);
			EAT(ptr,filename,lv,',');
			GETU32(exisiting,ptr);
			EAT(ptr,filename,lv,',');
			GETU32(hardlinks,ptr);
			EAT(ptr,filename,lv,',');
			GETU32(new,ptr);
		} else {
			inodecheck=0;
			removed=0;
			same=0;
			exisiting=0;
			hardlinks=0;
			new=0;
		}
		(void)ptr; // silence cppcheck warnings
		return fs_mr_snapshot(ts,inode,parent,strlen((char*)name),name,smode,sesflags,uid,gids,gidtab,umask,inodecheck,removed,same,exisiting,hardlinks,new);
	} else {
		GETU32(umask,ptr);
		EAT(ptr,filename,lv,')');
		(void)ptr; // silence cppcheck warnings
		return fs_mr_snapshot(ts,inode,parent,strlen((char*)name),name,smode,sesflags,uid,1,&gids,umask,0,0,0,0,0,0);
	}
}

int do_symlink(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t parent,uid,gid,inode;
	uint8_t name[256];
	static uint8_t *path = NULL;
	static uint32_t pathsize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_symlink(ts,parent,strlen((char*)name),name,path,uid,gid,inode);
}

int do_scdel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint8_t name[256];
	uint16_t spid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU16(spid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sclass_mr_delete_entry(strlen((char*)name),name,spid);
}

int do_scdup(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint8_t sname[256];
	uint8_t dname[256];
	uint16_t sspid,dspid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETNAME(sname,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETNAME(dname,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU16(sspid,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(dspid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sclass_mr_duplicate_entry(strlen((char*)sname),sname,strlen((char*)dname),dname,sspid,dspid);
}

int do_scren(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint8_t sname[256];
	uint8_t dname[256];
	uint16_t spid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETNAME(sname,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETNAME(dname,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU16(spid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sclass_mr_rename_entry(strlen((char*)sname),sname,strlen((char*)dname),dname,spid);
}

int do_scset(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint8_t name[256];
	uint16_t spid;
	uint16_t arch_delay;
	uint8_t new_flag,adminonly;
	uint8_t create_labelscnt,keep_labelscnt,arch_labelscnt,create_mode,i;
	uint32_t create_labelmasks[MAXLABELSCNT*MASKORGROUP];
	uint32_t keep_labelmasks[MAXLABELSCNT*MASKORGROUP];
	uint32_t arch_labelmasks[MAXLABELSCNT*MASKORGROUP];
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU8(new_flag,ptr);
	EAT(ptr,filename,lv,',');
	EAT(ptr,filename,lv,'W');
	GETU8(create_labelscnt,ptr);
	EAT(ptr,filename,lv,',');
	EAT(ptr,filename,lv,'K');
	GETU8(keep_labelscnt,ptr);
	EAT(ptr,filename,lv,',');
	EAT(ptr,filename,lv,'A');
	GETU8(arch_labelscnt,ptr);
	EAT(ptr,filename,lv,',');
	GETU8(create_mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(arch_delay,ptr);
	EAT(ptr,filename,lv,',');
	GETU8(adminonly,ptr);
	if (create_labelscnt>MAXLABELSCNT || keep_labelscnt>MAXLABELSCNT || arch_labelscnt>MAXLABELSCNT) {
		return MFS_ERROR_EINVAL;
	}
	if (create_labelscnt+keep_labelscnt+arch_labelscnt==0) {
		EAT(ptr,filename,lv,',');
		EAT(ptr,filename,lv,'-');
	} else {
		for (i=0 ; i<create_labelscnt*MASKORGROUP ; i++) {
			EAT(ptr,filename,lv,',');
			GETU32(create_labelmasks[i],ptr);
		}
		for (i=0 ; i<keep_labelscnt*MASKORGROUP ; i++) {
			EAT(ptr,filename,lv,',');
			GETU32(keep_labelmasks[i],ptr);
		}
		for (i=0 ; i<arch_labelscnt*MASKORGROUP ; i++) {
			EAT(ptr,filename,lv,',');
			GETU32(arch_labelmasks[i],ptr);
		}
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU16(spid,ptr);
	(void)ptr; // silence cppcheck warnings
	return sclass_mr_set_entry(strlen((char*)name),name,spid,new_flag,adminonly,create_mode,create_labelscnt,create_labelmasks,keep_labelscnt,keep_labelmasks,arch_labelscnt,arch_labelmasks,arch_delay);
}

int do_undel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_undel(ts,inode);
}

int do_unlink(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_unlink(ts,parent,strlen((char*)name),name,inode);
}

int do_unlock(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	(void)ptr; // silence cppcheck warnings
	return fs_mr_unlock(chunkid);
}

int do_trunc(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_trunc(ts,inode,indx,chunkid);
}

int do_write(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx,opflag;
	uint64_t chunkid;
	uint8_t canmodmtime;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(opflag,ptr);
	} else {
		opflag=1;
	}
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU8(canmodmtime,ptr);
	} else {
		canmodmtime=1;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	(void)ptr; // silence cppcheck warnings
	return fs_mr_write(ts,inode,indx,opflag,canmodmtime,chunkid);
}

#define HASHCODESTR(str) (((((uint8_t*)(str))[0]*256U+((uint8_t*)(str))[1])*256U+((uint8_t*)(str))[2])*256U+((uint8_t*)(str))[3])
#define HASHCODE(a,b,c,d) (((((uint8_t)a)*256U+(uint8_t)b)*256U+(uint8_t)c)*256U+(uint8_t)d)

int restore_line(const char *filename,uint64_t lv,const char *line,uint32_t *rts) {
	const char *ptr;
	uint32_t ts;
	uint32_t hc;
	int status;
//	char* errormsgs[]={ MFS_ERROR_STRINGS };

	status = MFS_ERROR_MISMATCH;
	ptr = line;

//	EAT(ptr,filename,lv,':');
//	EAT(ptr,filename,lv,' ');
	GETU32(ts,ptr);
	if (rts!=NULL) {
		*rts = ts;
	}
	EAT(ptr,filename,lv,'|');
	hc = HASHCODESTR(ptr);
	switch (hc) {
		case HASHCODE('I','D','L','E'):
			return do_idle(filename,lv,ts,ptr+4);
		case HASHCODE('A','C','C','E'):
			if (strncmp(ptr,"ACCESS",6)==0) {
				return do_access(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('A','T','T','R'):
			return do_attr(filename,lv,ts,ptr+4);
		case HASHCODE('A','P','P','E'):
			if (strncmp(ptr,"APPEND",6)==0) {
				return do_append(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('A','C','Q','U'):
			if (strncmp(ptr,"ACQUIRE",7)==0) {
				return do_acquire(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('A','Q','U','I'):
			if (strncmp(ptr,"AQUIRE",6)==0) {
				return do_acquire(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('A','R','C','H'):
			if (strncmp(ptr,"ARCHCHG",7)==0) {
				return do_archchg(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('A','M','T','I'):
			if (strncmp(ptr,"AMTIME",6)==0) {
				return do_amtime(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('C','R','E','A'):
			if (strncmp(ptr,"CREATE",6)==0) {
				return do_create(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('C','H','U','N'):
			if (strncmp(ptr,"CHUNKADD",8)==0) {
				return do_chunkadd(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"CHUNKDEL",8)==0) {
				return do_chunkdel(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('C','S','A','D'):
			if (strncmp(ptr,"CSADD",5)==0) {		// deprecated
				return do_csadd(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('C','S','D','B'):
			if (strncmp(ptr,"CSDBOP",6)==0) {
				return do_csdbop(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('C','S','D','E'):
			if (strncmp(ptr,"CSDEL",5)==0) {		// deprecated
				return do_csdel(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('C','U','S','T'):
			if (strncmp(ptr,"CUSTOMER",8)==0) {	// deprecated
				return do_session(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('E','M','P','T'):
			if (strncmp(ptr,"EMPTYTRASH",10)==0) {
				return do_emptytrash(filename,lv,ts,ptr+10);
			} else if (strncmp(ptr,"EMPTYSUSTAINED",14)==0) {
				return do_emptysustained(filename,lv,ts,ptr+14);
			} else if (strncmp(ptr,"EMPTYRESERVED",13)==0) {
				return do_emptysustained(filename,lv,ts,ptr+13);
			}
			break;
		case HASHCODE('F','L','O','C'):
			if (strncmp(ptr,"FLOCK",5)==0) {
				return do_flock(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('F','R','E','E'):
			if (strncmp(ptr,"FREEINODES",10)==0) {
				return do_freeinodes(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('I','N','C','V'):
			if (strncmp(ptr,"INCVERSION",10)==0) {
				return do_incversion(filename,lv,ts,ptr+10); // deprecated -> SETVERSION
			}
			break;
		case HASHCODE('L','E','N','G'):
			if (strncmp(ptr,"LENGTH",6)==0) {
				return do_length(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('L','I','N','K'):
			return do_link(filename,lv,ts,ptr+4);
		case HASHCODE('M','O','V','E'):
			return do_move(filename,lv,ts,ptr+4);
		case HASHCODE('N','E','X','T'):
			if (strncmp(ptr,"NEXTCHUNKID",11)==0) {
				return do_nextchunkid(filename,lv,ts,ptr+11); // deprecated
			}
			break;
		case HASHCODE('P','O','S','I'):
			if (strncmp(ptr,"POSIXLOCK",9)==0) {
				return do_posixlock(filename,lv,ts,ptr+9);
			}
			break;
		case HASHCODE('P','U','R','G'):
			if (strncmp(ptr,"PURGE",5)==0) {
				return do_purge(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('Q','U','O','T'):
			if (strncmp(ptr,"QUOTA",5)==0) {
				return do_quota(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('R','E','L','E'):
			if (strncmp(ptr,"RELEASE",7)==0) {
				return do_release(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('R','E','N','U'):
			if (strncmp(ptr,"RENUMERATEEDGES",15)==0) {
				return do_renumedges(filename,lv,ts,ptr+15);
			}
			break;
		case HASHCODE('R','E','P','A'):
			if (strncmp(ptr,"REPAIR",6)==0) {
				return do_repair(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('R','O','L','L'):
			if (strncmp(ptr,"ROLLBACK",8)==0) {
				return do_rollback(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','C','D','E'):
			if (strncmp(ptr,"SCDEL",5)==0) {
				return do_scdel(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('S','C','D','U'):
			if (strncmp(ptr,"SCDUP",5)==0) {
				return do_scdup(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('S','C','R','E'):
			if (strncmp(ptr,"SCREN",5)==0) {
				return do_scren(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('S','C','S','E'):
			if (strncmp(ptr,"SCSET",5)==0) {
				return do_scset(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('S','E','S','A'):
			if (strncmp(ptr,"SESADD",6)==0) {
				return do_sesadd(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('S','E','S','C'):
			if (strncmp(ptr,"SESCHANGED",10)==0) {
				return do_seschanged(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('S','E','S','D'):
			if (strncmp(ptr,"SESDEL",6)==0) {
				return do_sesdel(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SESDISCONNECTED",15)==0) {
				return do_sesdisconnected(filename,lv,ts,ptr+15);
			}
			break;
		case HASHCODE('S','E','S','S'):
			if (strncmp(ptr,"SESSION",7)==0) { // deprecated
				return do_session(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','T','A'):
			if (strncmp(ptr,"SETACL",6)==0) {
				return do_setacl(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('S','E','T','E'):
			if (strncmp(ptr,"SETEATTR",8)==0) {
				return do_seteattr(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','E','T','F'):
			if (strncmp(ptr,"SETFILECHUNK",12)==0) {
				return do_setfilechunk(filename,lv,ts,ptr+12);
			}
			break;
		case HASHCODE('S','E','T','G'):
			if (strncmp(ptr,"SETGOAL",7)==0) {
				return do_setgoal(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','T','M'):
			if (strncmp(ptr,"SETMETAID",9)==0) {
				return do_setmetaid(filename,lv,ts,ptr+9);
			}
			break;
		case HASHCODE('S','E','T','P'):
			if (strncmp(ptr,"SETPATH",7)==0) {
				return do_setpath(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','T','S'):
			if (strncmp(ptr,"SETSCLASS",9)==0) {
				return do_setsclass(filename,lv,ts,ptr+9);
			}
			break;
		case HASHCODE('S','E','T','T'):
			if (strncmp(ptr,"SETTRASHTIME",12)==0) {
				return do_settrashtime(filename,lv,ts,ptr+12);
			}
			break;
		case HASHCODE('S','E','T','V'):
			if (strncmp(ptr,"SETVERSION",10)==0) {
				return do_setversion(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('S','E','T','X'):
			if (strncmp(ptr,"SETXATTR",8)==0) {
				return do_setxattr(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','N','A','P'):
			if (strncmp(ptr,"SNAPSHOT",8)==0) {
				return do_snapshot(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','Y','M','L'):
			if (strncmp(ptr,"SYMLINK",7)==0) {
				return do_symlink(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('T','R','U','N'):
			if (strncmp(ptr,"TRUNC",5)==0) {
				return do_trunc(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('U','N','D','E'):
			if (strncmp(ptr,"UNDEL",5)==0) {
				return do_undel(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('U','N','L','I'):
			if (strncmp(ptr,"UNLINK",6)==0) {
				return do_unlink(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('U','N','L','O'):
			if (strncmp(ptr,"UNLOCK",6)==0) {
				return do_unlock(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('W','R','I','T'):
			if (strncmp(ptr,"WRITE",5)==0) {
				return do_write(filename,lv,ts,ptr+5);
			}
			break;
	}
	mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
#if 0
	switch (*ptr) {
		case 'A':
			if (strncmp(ptr,"ACCESS",6)==0) {
				status = do_access(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ATTR",4)==0) {
				status = do_attr(filename,lv,ts,ptr+4);
			} else if (strncmp(ptr,"APPEND",6)==0) {
				status = do_append(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ACQUIRE",7)==0) {
				status = do_acquire(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"AQUIRE",6)==0) {
				status = do_acquire(filename,lv,ts,ptr+6);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'C':
			if (strncmp(ptr,"CREATE",6)==0) {
				status = do_create(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"CHUNKADD",8)==0) {
				status = do_chunkadd(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"CHUNKDEL",8)==0) {
				status = do_chunkdel(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"CSDBOP",6)==0) {
				status = do_csdbop(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"CSADD",5)==0) {		// deprecated
				status = do_csadd(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"CSDEL",5)==0) {		// deprecated
				status = do_csdel(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"CUSTOMER",8)==0) {	// deprecated
				status = do_session(filename,lv,ts,ptr+8);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'E':
			if (strncmp(ptr,"EMPTYTRASH",10)==0) {
				status = do_emptytrash(filename,lv,ts,ptr+10);
			} else if (strncmp(ptr,"EMPTYSUSTAINED",14)==0) {
				status = do_emptysustained(filename,lv,ts,ptr+14);
			} else if (strncmp(ptr,"EMPTYRESERVED",13)==0) {
				status = do_emptysustained(filename,lv,ts,ptr+13);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'F':
			if (strncmp(ptr,"FREEINODES",10)==0) {
				status = do_freeinodes(filename,lv,ts,ptr+10);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'I':
			if (strncmp(ptr,"INCVERSION",10)==0) {
				status = do_incversion(filename,lv,ts,ptr+10);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'L':
			if (strncmp(ptr,"LENGTH",6)==0) {
				status = do_length(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"LINK",4)==0) {
				status = do_link(filename,lv,ts,ptr+4);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'N':
			if (strncmp(ptr,"NEXTCHUNKID",11)==0) {
				status = do_nextchunkid(filename,lv,ts,ptr+11); // deprecated
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
		case 'M':
			if (strncmp(ptr,"MOVE",4)==0) {
				status = do_move(filename,lv,ts,ptr+4);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'P':
			if (strncmp(ptr,"PURGE",5)==0) {
				status = do_purge(filename,lv,ts,ptr+5);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'Q':
			if (strncmp(ptr,"QUOTA",5)==0) {
				status = do_quota(filename,lv,ts,ptr+5);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'R':
			if (strncmp(ptr,"RELEASE",7)==0) {
				status = do_release(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"REPAIR",6)==0) {
				status = do_repair(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"RENUMEDGES",10)==0) {
				status = do_renumedges(filename,lv,ts,ptr+10);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'S':
			if (strncmp(ptr,"SETEATTR",8)==0) {
				status = do_seteattr(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SETGOAL",7)==0) {
				status = do_setgoal(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETPATH",7)==0) {
				status = do_setpath(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETTRASHTIME",12)==0) {
				status = do_settrashtime(filename,lv,ts,ptr+12);
			} else if (strncmp(ptr,"SETXATTR",8)==0) {
				status = do_setxattr(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SETACL",6)==0) {
				status = do_setacl(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SNAPSHOT",8)==0) {
				status = do_snapshot(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SYMLINK",7)==0) {
				status = do_symlink(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SESSION",7)==0) { // deprecated
				status = do_session(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SESADD",6)==0) {
				status = do_sesadd(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SESDEL",6)==0) {
				status = do_sesdel(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SESDISCONNECTED",15)==0) {
				status = do_sesdisconnected(filename,lv,ts,ptr+15);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'T':
			if (strncmp(ptr,"TRUNC",5)==0) {
				status = do_trunc(filename,lv,ts,ptr+5);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'U':
			if (strncmp(ptr,"UNLINK",6)==0) {
				status = do_unlink(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"UNDEL",5)==0) {
				status = do_undel(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"UNLOCK",6)==0) {
				status = do_unlock(filename,lv,ts,ptr+6);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'W':
			if (strncmp(ptr,"WRITE",5)==0) {
				status = do_write(filename,lv,ts,ptr+5);
			} else {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		default:
			mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
	}
#endif
//	if (status>MFS_STATUS_OK) {
//		mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": error: %d (%s)\n",filename,lv,status,errormsgs[status]);
//	}
	return status;
}

int restore_net(uint64_t lv,const char *ptr,uint32_t *rts) {
	int status;
	if (lv!=meta_version()) {
		syslog(LOG_WARNING,"desync - invalid meta version (version in packet: %"PRIu64" / expected: %"PRIu64" / packet data: %s)",lv,meta_version(),ptr);
		return -1;
	}
	status = restore_line("NET",lv,ptr,rts);
	if (status<0) {
		syslog(LOG_WARNING,"desync - operation (%s) parse error",ptr);
		return -1;
	}
	if (status!=MFS_STATUS_OK) {
		syslog(LOG_WARNING,"desync - operation (%s) error: %d (%s)",ptr,status,mfsstrerr(status));
		return -1;
	}
	if (lv+1!=meta_version()) {
		syslog(LOG_WARNING,"desync - meta version has not been increased after the operation (%s)",ptr);
		return -1;
	}
	return 0;
}

static uint64_t v=0,lastv=0;
static void *lastshfn = NULL;

int restore_file(void *shfilename,uint64_t lv,const char *ptr,uint8_t vlevel) {
	int status;
	char *lastfn;
	char *filename = (char*)shp_get(shfilename);
	if (lastv==0 || v==0 || lastshfn==NULL) {
		v = meta_version();
		lastv = lv-1;
		lastfn = "(no file)";
	} else {
		lastfn = (char*)shp_get(lastshfn);
	}
	if (vlevel>1) {
		mfs_arg_syslog(LOG_NOTICE,"filename: %s ; current meta version: %"PRIu64" ; previous changeid: %"PRIu64" ; current changeid: %"PRIu64" ; change data%s",filename,v,lastv,lv,ptr);
	}
	if (lv<lastv) {
		mfs_arg_syslog(LOG_WARNING,"merge error - possibly corrupted input file - ignore entry (filename: %s)\n",filename);
		return 0;
	} else if (lv>=v) {
		if (lv==lastv) {
			if (vlevel>1) {
				mfs_arg_syslog(LOG_WARNING,"duplicated entry: %"PRIu64" (previous file: %s, current file: %s)\n",lv,lastfn,filename);
			}
		} else if (lv>lastv+1) {
			mfs_arg_syslog(LOG_WARNING,"hole in change files (entries from %s:%"PRIu64" to %s:%"PRIu64" are missing) - add more files\n",lastfn,lastv+1,filename,lv-1);
			return -2;
		} else {
			if (vlevel>0) {
				mfs_arg_syslog(LOG_WARNING,"%s: change%s",filename,ptr);
			}
			status = restore_line(filename,lv,ptr,NULL);
			if (status<0) { // parse error - just ignore this line
				return 0;
			}
			if (status>0) { // other errors - stop processing data
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": operation (%s) error: %d (%s)",filename,lv,ptr,status,mfsstrerr(status));
				return -1;
			}
			v = meta_version();
			if (lv+1!=v) {
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": version mismatch\n",filename,lv);
				return -1;
			}
		}
	}
	lastv = lv;
	if (shfilename!=lastshfn) {
		shp_inc(shfilename);
		if (lastshfn!=NULL) {
			shp_dec(lastshfn);
		}
		lastshfn = shfilename;
	}
	return 0;
}
