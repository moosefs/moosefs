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
#include <inttypes.h>

#include "MFSCommunication.h"
#include "sharedpointer.h"
#include "filesystem.h"
#include "sessions.h"
#include "openfiles.h"
#include "csdb.h"
#include "chunks.h"
#include "metadata.h"
#include "slogger.h"
#include "massert.h"
#include "mfsstrerr.h"

#define EAT(clptr,fn,vno,c) { \
	if (*(clptr)!=(c)) { \
		mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": '%c' expected\n",(fn),(vno),(c)); \
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
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
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
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
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
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": hex expected\n",(fn),(vno)); \
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

#define GETU8(data,clptr) { \
	uint32_t tmp; \
	char *eptr; \
	tmp=strtoul(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
	if (tmp>255) { \
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
		return -1; \
	} \
	(data)=tmp; \
}

#define GETU32(data,clptr) { \
	char *eptr; \
	(data)=strtoul(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
}

#define GETU64(data,clptr) { \
	char *eptr; \
	(data)=strtoull(clptr,&eptr,10); \
	clptr = (const char*)eptr; \
}

int do_access(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_access(ts,inode);
}

int do_append(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,inode_src;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(inode_src,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_append(ts,inode,inode_src);
}

int do_acquire(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	return of_mr_acquire(inode,cuid);
}

int do_attr(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,mode,uid,gid,atime,mtime;
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
	EAT(ptr,filename,lv,')');
	return fs_mr_attr(ts,inode,mode,uid,gid,atime,mtime);
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
	return fs_mr_create(ts,parent,strlen((char*)name),name,type,mode,cumask,uid,gid,rdev,inode);
}

int do_csdbop(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t op,ip,port,csid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU8(op,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(ip,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(port,ptr);
	EAT(ptr,filename,lv,',');
	GETU16(csid,ptr);
	EAT(ptr,filename,lv,')');
	return csdb_mr_op(op,ip,port,csid);
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
	return csdb_mr_csdel(ip,port);
}

int do_chunkadd(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	uint32_t version,lockedto;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(version,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(lockedto,ptr);
	EAT(ptr,filename,lv,')');
	return chunk_mr_chunkadd(chunkid,version,lockedto);
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
	return chunk_mr_chunkdel(chunkid,version);
}

int do_emptytrash(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sustainedinodes,freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sustainedinodes,ptr);
	return fs_mr_emptytrash(ts,freeinodes,sustainedinodes);
}

int do_emptysustained(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_mr_emptysustained(ts,freeinodes);
}

int do_freeinodes(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_mr_freeinodes(ts,freeinodes);
}

int do_incversion(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_incversion(chunkid);
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
	return fs_mr_link(ts,inode,parent,strlen((char*)name),name);
}

int do_length(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	uint64_t length;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(length,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_length(ts,inode,length);
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
	return fs_mr_move(ts,parent_src,strlen((char*)name_src),name_src,parent_dst,strlen((char*)name_dst),name_dst,inode);
}

int do_nextchunkid(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return chunk_mr_nextchunkid(chunkid);
}

int do_purge(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_mr_purge(ts,inode);
}

int do_quota(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,stimestamp,sinodes,hinodes;
	uint64_t slength,ssize,srealsize;
	uint64_t hlength,hsize,hrealsize;
	uint32_t flags,exceeded;
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
	EAT(ptr,filename,lv,')');
	return fs_mr_quota(ts,inode,exceeded,flags,stimestamp,sinodes,hinodes,slength,hlength,ssize,hsize,srealsize,hrealsize);
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
	return fs_mr_repair(ts,inode,indx,version);
}

int do_renumedges(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t enextedgeid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(enextedgeid,ptr);
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
	return sessions_mr_session(sessionid);
}

int do_sesadd(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t rootinode,sesflags,peerip,sessionid;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint32_t mingoal,maxgoal,mintrashtime,maxtrashtime;
	static uint8_t *info = NULL;
	static uint32_t infosize = 0;

	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(rootinode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sesflags,ptr);
	EAT(ptr,filename,lv,',');
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
	GETU32(peerip,ptr);
	EAT(ptr,filename,lv,',');
	GETPATH(info,infosize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(sessionid,ptr);
	return sessions_mr_sesadd(rootinode,sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashtime,maxtrashtime,peerip,info,infosize,sessionid);
}

int do_sesdel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sessionid;

	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,')');
	return sessions_mr_sesdel(sessionid);
}

int do_sesdisconnected(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t sessionid;

	EAT(ptr,filename,lv,'(');
	GETU32(sessionid,ptr);
	EAT(ptr,filename,lv,')');
	return sessions_mr_disconnected(sessionid,ts);
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
	return fs_mr_seteattr(ts,inode,uid,eattr,smode,ci,nci,npi);
}

int do_setgoal(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t goal,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(goal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	return fs_mr_setgoal(ts,inode,uid,goal,smode,ci,nci,npi);
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
	if (aclblobleng!=6U*(namedusers+namedgroups)) {
		return ERROR_MISMATCH;
	}
	return fs_mr_setacl(ts,inode,mode,changectime,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
}

int do_snapshot(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,parent,smode,sesflags,uid,gids,umask;
	static uint8_t *gid = NULL;
	static uint32_t gidsize = 0;
	uint32_t gidleng;
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
	GETU32(gids,ptr);
	EAT(ptr,filename,lv,',');
	mode = 0;
	for (i=0 ; ptr[i] && ptr[i]!=')' ; i++) {
		if (ptr[i]<'0' || ptr[i]>'9') {
			mode=1;
			break;
		}
	}
	if (mode==1) {
		GETDATA(gid,gidleng,gidsize,ptr,filename,lv,',');
		EAT(ptr,filename,lv,',');
		GETU32(umask,ptr);
		EAT(ptr,filename,lv,')');
		if (gids*4!=gidleng) {
			return ERROR_MISMATCH;
		}
		return fs_mr_snapshot(ts,inode,parent,strlen((char*)name),name,smode,sesflags,uid,gids,(uint32_t*)gid,umask);
	} else {
		GETU32(umask,ptr);
		EAT(ptr,filename,lv,')');
		return fs_mr_snapshot(ts,inode,parent,strlen((char*)name),name,smode,sesflags,uid,1,&gids,umask);
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
	return fs_mr_symlink(ts,parent,strlen((char*)name),name,path,uid,gid,inode);
}

int do_undel(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
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
	return fs_mr_unlink(ts,parent,strlen((char*)name),name,inode);
}

int do_unlock(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
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
	return fs_mr_trunc(ts,inode,indx,chunkid);
}

int do_write(const char *filename,uint64_t lv,uint32_t ts,const char *ptr) {
	uint32_t inode,indx,opflag;
	uint64_t chunkid;
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
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_mr_write(ts,inode,indx,opflag,chunkid);
}

#define HASHCODESTR(str) (((((uint8_t*)(str))[0]*256U+((uint8_t*)(str))[1])*256U+((uint8_t*)(str))[2])*256U+((uint8_t*)(str))[3])
#define HASHCODE(a,b,c,d) (((((uint8_t)a)*256U+(uint8_t)b)*256U+(uint8_t)c)*256U+(uint8_t)d)

int restore_line(const char *filename,uint64_t lv,const char *line) {
	const char *ptr;
	uint32_t ts;
	uint32_t hc;
	int status;
//	char* errormsgs[]={ ERROR_STRINGS };

	status = ERROR_MISMATCH;
	ptr = line;

//	EAT(ptr,filename,lv,':');
//	EAT(ptr,filename,lv,' ');
	GETU32(ts,ptr);
	EAT(ptr,filename,lv,'|');
	hc = HASHCODESTR(ptr);
	switch (hc) {
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
		case HASHCODE('C','S','D','B'):
			if (strncmp(ptr,"CSDBOP",6)==0) {
				return do_csdbop(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('C','S','A','D'):
			if (strncmp(ptr,"CSADD",5)==0) {		// deprecated
				return do_csadd(filename,lv,ts,ptr+5);
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
		case HASHCODE('F','R','E','E'):
			if (strncmp(ptr,"FREEINODES",10)==0) {
				return do_freeinodes(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('I','N','C','V'):
			if (strncmp(ptr,"INCVERSION",10)==0) {
				return do_incversion(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('L','E','N','G'):
			if (strncmp(ptr,"LENGTH",6)==0) {
				return do_length(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('L','I','N','K'):
			return do_link(filename,lv,ts,ptr+4);
		case HASHCODE('N','E','X','T'):
			if (strncmp(ptr,"NEXTCHUNKID",11)==0) {
				return do_nextchunkid(filename,lv,ts,ptr+11); // deprecated
			}
			break;
		case HASHCODE('M','O','V','E'):
			return do_move(filename,lv,ts,ptr+4);
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
		case HASHCODE('R','E','P','A'):
			if (strncmp(ptr,"REPAIR",6)==0) {
				return do_repair(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('R','E','N','U'):
			if (strncmp(ptr,"RENUMEDGES",10)==0) {
				return do_renumedges(filename,lv,ts,ptr+10);
			}
			break;
		case HASHCODE('S','E','T','E'):
			if (strncmp(ptr,"SETEATTR",8)==0) {
				return do_seteattr(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','E','T','G'):
			if (strncmp(ptr,"SETGOAL",7)==0) {
				return do_setgoal(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','T','P'):
			if (strncmp(ptr,"SETPATH",7)==0) {
				return do_setpath(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','T','T'):
			if (strncmp(ptr,"SETTRASHTIME",12)==0) {
				return do_settrashtime(filename,lv,ts,ptr+12);
			}
			break;
		case HASHCODE('S','E','T','X'):
			if (strncmp(ptr,"SETXATTR",8)==0) {
				return do_setxattr(filename,lv,ts,ptr+8);
			}
			break;
		case HASHCODE('S','E','T','A'):
			if (strncmp(ptr,"SETACL",6)==0) {
				return do_setacl(filename,lv,ts,ptr+6);
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
		case HASHCODE('S','E','S','S'):
			if (strncmp(ptr,"SESSION",7)==0) { // deprecated
				return do_session(filename,lv,ts,ptr+7);
			}
			break;
		case HASHCODE('S','E','S','A'):
			if (strncmp(ptr,"SESADD",6)==0) {
				return do_sesadd(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('S','E','S','D'):
			if (strncmp(ptr,"SESDEL",6)==0) {
				return do_sesdel(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SESDISCONNECTED",15)==0) {
				return do_sesdisconnected(filename,lv,ts,ptr+15);
			}
			break;
		case HASHCODE('T','R','U','N'):
			if (strncmp(ptr,"TRUNC",5)==0) {
				return do_trunc(filename,lv,ts,ptr+5);
			}
			break;
		case HASHCODE('U','N','L','I'):
			if (strncmp(ptr,"UNLINK",6)==0) {
				return do_unlink(filename,lv,ts,ptr+6);
			}
			break;
		case HASHCODE('U','N','D','E'):
			if (strncmp(ptr,"UNDEL",5)==0) {
				return do_undel(filename,lv,ts,ptr+5);
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
//	if (status>STATUS_OK) {
//		mfs_arg_syslog(LOG_WARNING,"%s:%"PRIu64": error: %d (%s)\n",filename,lv,status,errormsgs[status]);
//	}
	return status;
}

int restore_net(uint64_t lv,const char *ptr) {
	uint8_t status;
	if (lv!=meta_version()) {
		syslog(LOG_WARNING,"desync - invalid meta version (version in packet: %"PRIu64" / expected: %"PRIu64")",lv,meta_version());
		return -1;
	}
	status = restore_line("NET",lv,ptr);
	if (status!=STATUS_OK) {
		syslog(LOG_WARNING,"desync - operation (%s) error: %d (%s)",ptr,status,mfsstrerr(status));
		return -1;
	}
	if (lv+1!=meta_version()) {
		syslog(LOG_WARNING,"desync - meta version has not been increased after operation (%s)",ptr);
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
			status = restore_line(filename,lv,ptr);
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
