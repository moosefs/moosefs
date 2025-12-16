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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#else
#define nobreak (void)0
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "MFSCommunication.h"
#include "massert.h"
#include "mfslog.h"
#include "datapack.h"
#include "filesystem.h"
#include "xattr.h"
#include "dictionary.h"
#include "bio.h"
#include "glue.h"

typedef struct _xattrpair {
	struct _xattrpair *next;
	void *dictname;
	void *dictvalue;
} xattrpair;

typedef struct _xattrentry {
	struct _xattrentry *next;
	uint32_t inode;
	//	uint32_t anleng;
	//	uint32_t avleng;
	xattrpair *pairhead;
} xattrentry;

#define LOHASH_BITS 20
#define ENTRY_TYPE xattrentry
#define GLUE_FN_NAME_PREFIX(Y) GLUE(xattr,Y)
#define HASH_ARGS_TYPE_LIST uint32_t inode
#define HASH_ARGS_LIST inode
#define GLUE_HASH_TAB_PREFIX(Y) GLUE(xattr,Y)

static inline int GLUE_FN_NAME_PREFIX(_cmp)(ENTRY_TYPE *e,HASH_ARGS_TYPE_LIST) {
	return (e->inode==inode);
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_hash)(HASH_ARGS_TYPE_LIST) {
	return inode;
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_ehash)(ENTRY_TYPE *e) {
	return e->inode;
}

static inline void GLUE_FN_NAME_PREFIX(_print)(ENTRY_TYPE *e) {
	printf("%" PRIu32,e->inode);
}

#include "hash_begin.h"

static inline void xattr_cleanup_node(xattrentry *xe) {
	xattrpair *xp;
	while ((xp=xe->pairhead)!=NULL) {
		dict_dec_ref(xp->dictname);
		dict_dec_ref(xp->dictvalue);
		xe->pairhead = xp->next;
		free(xp);
	}
}

/* externals */

int xattr_namecheck(uint8_t anleng,const uint8_t *attrname) {
	uint32_t i;
	for (i=0 ; i<anleng ; i++) {
		if (attrname[i]=='\0') {
			return -1;
		}
	}
	return 0;
}

void xattr_removeinode(uint32_t inode) {
	xattrentry *xe;
	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (xe) {
		xattr_cleanup_node(xe);
		GLUE_FN_NAME_PREFIX(_delete)(xe);
		free(xe);
	}
}

uint8_t xattr_setattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode) {
	xattrentry *xe;
	xattrpair *xp,**xpp;
	void *dictname;
	void *dictvalue;
	uint32_t inode_anleng;

	if (anleng==0 || mode>MFS_XATTR_REMOVE) {
		return MFS_ERROR_EINVAL;
	}
#if MFS_XATTR_NAME_MAX<255
	if (anleng>MFS_XATTR_NAME_MAX) {
		return MFS_ERROR_ERANGE;
	}
#endif
	if (avleng>MFS_XATTR_SIZE_MAX && mode!=MFS_XATTR_REMOVE) {
		return MFS_ERROR_ERANGE;
	}
	if (anleng + 1 > MFS_XATTR_LIST_MAX) {
		return MFS_ERROR_ERANGE;
	}
	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (mode==MFS_XATTR_REPLACE_ONLY || mode==MFS_XATTR_REMOVE) {
		if (xe==NULL) {
			return MFS_ERROR_ENOATTR;
		}
		dictname = dict_search(attrname,anleng);
		if (dictname==NULL) {
			return MFS_ERROR_ENOATTR;
		}
	} else {
		if (xe==NULL) {
			xe = malloc(sizeof(xattrentry));
			passert(xe);
			xe->inode = inode;
//			xe->anleng = 0;
//			xe->avleng = 0;
			xe->pairhead = NULL;
			GLUE_FN_NAME_PREFIX(_add)(xe);
			fs_set_xattrflag(inode);
		}
		dictname = dict_insert(attrname,anleng); // inc_ref
	}

	inode_anleng = 0;
	xpp = &(xe->pairhead);
	while ((xp=*xpp)!=NULL) {
		if (xp->dictname==dictname) {
			switch (mode) {
				case MFS_XATTR_CREATE_ONLY:
					dict_dec_ref(dictname);
					return MFS_ERROR_EEXIST;
				case MFS_XATTR_REMOVE:
					// remove
//					xe->anleng -= (anleng + 1);
//					xe->avleng -= dict_get_leng(xp->dictvalue);
					dict_dec_ref(xp->dictname);
					dict_dec_ref(xp->dictvalue);
					*xpp = xp->next;
					free(xp);
					if (xe->pairhead==NULL) {
						GLUE_FN_NAME_PREFIX(_delete)(xe);
						free(xe);
						fs_del_xattrflag(inode);
					}
					return MFS_STATUS_OK;
				case MFS_XATTR_CREATE_OR_REPLACE:
					dict_dec_ref(dictname);
					// no break on purpose
					nobreak;
				case MFS_XATTR_REPLACE_ONLY:
					if (avleng>MFS_XATTR_SIZE_MAX) {
						return MFS_ERROR_ERANGE;
					}
//					xe->avleng -= dict_get_leng(xp->dictvalue);
					dictvalue = dict_insert(attrvalue,avleng);
					if (xp->dictvalue==dictvalue) {
						dict_dec_ref(dictvalue);
					} else {
						dict_dec_ref(xp->dictvalue);
						xp->dictvalue = dictvalue;
					}
//					xe->avleng += avleng;
					return MFS_STATUS_OK;
				default:
					dict_dec_ref(dictname);
					return MFS_ERROR_EINVAL;
			}
		}
		inode_anleng += dict_get_leng(xp->dictname)+1;
		xpp = &(xp->next);
	}
	if (mode==MFS_XATTR_REPLACE_ONLY || mode==MFS_XATTR_REMOVE) {
		return MFS_ERROR_ENOATTR;
	}
	if (inode_anleng + anleng + 1 > MFS_XATTR_LIST_MAX) {
		dict_dec_ref(dictname);
		return MFS_ERROR_ERANGE;
	}
	xp = malloc(sizeof(xattrpair));
	xp->dictname = dictname;
	xp->dictvalue = dict_insert(attrvalue,avleng);
	xp->next = xe->pairhead;
	xe->pairhead = xp;
//	xe->anleng += anleng + 1;
//	xe->avleng += avleng;
	return MFS_STATUS_OK;
}

uint8_t xattr_getattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,const uint8_t **attrvalue) {
	xattrentry *xe;
	xattrpair *xp;
	void *dictname;

	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (xe) {
		dictname = dict_search(attrname,anleng);
		if (dictname!=NULL) {
			for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
				if (xp->dictname==dictname) {
					*avleng = dict_get_leng(xp->dictvalue);
					*attrvalue = dict_get_ptr(xp->dictvalue);
					return MFS_STATUS_OK;
				}
			}
		}
	}
	return MFS_ERROR_ENOATTR;
}

uint8_t xattr_listattr_leng(uint32_t inode,void **xanode,uint32_t *xasize) {
	xattrentry *xe;
	xattrpair *xp;
	uint32_t inode_anleng;

	inode_anleng = 0;
	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (xe!=NULL) {
		for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
			inode_anleng += dict_get_leng(xp->dictname) + 1;
		}
	}
	if (inode_anleng>MFS_XATTR_LIST_MAX) {
		return MFS_ERROR_ERANGE;
	}
	*xanode = (void*)xe;
	*xasize = inode_anleng;
	return MFS_STATUS_OK;
}

void xattr_listattr_data(void *xanode,uint8_t *xabuff) {
	xattrentry *xe = (xattrentry*)xanode;
	xattrpair *xp;
	uint32_t i;

	i = 0;
	if (xe!=NULL) {
		for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
			memcpy(xabuff+i,dict_get_ptr(xp->dictname),dict_get_leng(xp->dictname));
			i+=dict_get_leng(xp->dictname);
			xabuff[i++]=0;
		}
	}
}

uint32_t xattr_getall(uint32_t inode,uint8_t *dbuff) {
	xattrentry *xe;
	xattrpair *xp;
	uint32_t leng;
	uint16_t cnt;
	uint32_t anleng;
	uint32_t avleng;
	uint8_t *cntptr;

	leng = 2;
	cnt = 0;
	cntptr = dbuff;
	if (dbuff!=NULL) {
		dbuff+=2;
	}

	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (xe!=NULL) {
		for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
			cnt++;
			anleng = dict_get_leng(xp->dictname);
			avleng = dict_get_leng(xp->dictvalue);
			leng += 1U + anleng + 4U + avleng;
			if (dbuff!=NULL) {
				put8bit(&dbuff,anleng);
				memcpy(dbuff,dict_get_ptr(xp->dictname),anleng);
				dbuff+=anleng;
				put32bit(&dbuff,avleng);
				memcpy(dbuff,dict_get_ptr(xp->dictvalue),avleng);
				dbuff+=avleng;
			}
		}
	}
	if (cntptr!=NULL) {
		put16bit(&cntptr,cnt);
	}
	return leng;
}

// return values
//  0 - different
//  1 - same
//  2 - error
uint8_t xattr_check(uint32_t inode,const uint8_t *dbuff,uint32_t leng,uint32_t *pleng) {
	xattrentry *xe;
	xattrpair *xp;
	uint16_t i,cnt,xattrcnt,checkcnt;
	uint8_t anleng,xattranleng;
	const uint8_t *attrname;
	uint32_t avleng,xattravleng;
	const uint8_t *attrvalue;
	uint32_t ileng;

	ileng = leng;
	*pleng = 0;
	if (leng<2) {
		return 2;
	}
	cnt = get16bit(&dbuff);
	leng -= 2;

	xe = GLUE_FN_NAME_PREFIX(_find)(inode);

	xattrcnt = 0;
	if (xe!=NULL) {
		for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
			xattrcnt++;
		}
	}

	checkcnt = 0;
	for (i=0 ; i<cnt ; i++) {
		if (leng<1) {
			return 2;
		}
		anleng = get8bit(&dbuff);
		leng--;
		if (leng<anleng) {
			return 2;
		}
		attrname = dbuff;
		dbuff += anleng;
		leng -= anleng;
		if (leng<4) {
			return 2;
		}
		avleng = get32bit(&dbuff);
		leng-=4;
		if (leng<avleng) {
			return 2;
		}
		attrvalue = dbuff;
		dbuff += avleng;
		leng -= avleng;
		if (xe!=NULL && xattrcnt==cnt && i==checkcnt) { // check xattr existence only if the inode has xattrs, the number of xattrs is the same and all previous xattrs have been found
			for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
				xattranleng = dict_get_leng(xp->dictname);
				xattravleng = dict_get_leng(xp->dictvalue);
				if (xattranleng==anleng && xattravleng==avleng) {
					if (memcmp(attrname,dict_get_ptr(xp->dictname),anleng)==0 && memcmp(attrvalue,dict_get_ptr(xp->dictvalue),avleng)==0) {
						checkcnt++;
					}
				}
			}
		}
	}
	*pleng = ileng-leng;
	return (checkcnt==cnt && xattrcnt==cnt)?1:0;
}

uint8_t xattr_setall(uint32_t inode,const uint8_t *dbuff) {
	xattrentry *xe;
	xattrpair *xp,*xpnew,**xptail,**xpprev;
	uint16_t i,cnt,moved;
	uint8_t anleng;
	const uint8_t *attrname;
	uint32_t avleng;
	const uint8_t *attrvalue;
	void *dictname;
	void *dictvalue;

	cnt = get16bit(&dbuff);

	xe = GLUE_FN_NAME_PREFIX(_find)(inode);
	if (xe==NULL) {
		if (cnt==0) { // no xattrs in inode and no new replace them - it is ok.
			return 0; // clear xattr flag
		} else {
			xe = malloc(sizeof(xattrentry));
			passert(xe);
			xe->inode = inode;
//			xe->anleng = 0;
//			xe->avleng = 0;
			xe->pairhead = NULL;
			GLUE_FN_NAME_PREFIX(_add)(xe);
//			fs_set_xattrflag(inode);
		}
	}

	xpnew = NULL;
	xptail = &xpnew;

	for (i=0 ; i<cnt ; i++) {
		anleng = get8bit(&dbuff);
		attrname = dbuff;
		dbuff += anleng;
		avleng = get32bit(&dbuff);
		attrvalue = dbuff;
		dbuff += avleng;
		dictname = dict_insert(attrname,anleng);
		moved = 0;
		xpprev = &(xe->pairhead);
		while (moved==0 && (xp = *xpprev)!=NULL) {
			if (xp->dictname==dictname) {
				dict_dec_ref(xp->dictname);
				dictvalue = dict_insert(attrvalue,avleng);
				if (xp->dictvalue==dictvalue) {
					dict_dec_ref(dictvalue);
				} else {
					dict_dec_ref(xp->dictvalue);
					xp->dictvalue = dictvalue;
				}
				*xpprev = xp->next;
				*xptail = xp;
				xp->next = NULL;
				xptail = &(xp->next);
				moved = 1;
			} else {
				xpprev = &(xp->next);
			}
		}
		if (moved==0) {
			xp = malloc(sizeof(xattrpair));
			xp->dictname = dictname;
			xp->dictvalue = dict_insert(attrvalue,avleng);
			*xptail = xp;
			xp->next = NULL;
			xptail = &(xp->next);
		}
	}
	while ((xp=xe->pairhead)!=NULL) {
		dict_dec_ref(xp->dictname);
		dict_dec_ref(xp->dictvalue);
		xe->pairhead = xp->next;
		free(xp);
	}
	if (xpnew==NULL) { // remove node
		GLUE_FN_NAME_PREFIX(_delete)(xe);
		free(xe);
		return 0;
	}
	xe->pairhead = xpnew;
	return 1;
}

uint8_t xattr_copy(uint32_t srcinode,uint32_t dstinode) {
	xattrentry *sxe,*dxe;
	xattrpair *sxp,*dxp,**xpt;

	sxe = GLUE_FN_NAME_PREFIX(_find)(srcinode);
	if (sxe==NULL) {
		return 0;
	}
	dxe = malloc(sizeof(xattrentry));
	passert(dxe);
	dxe->inode = dstinode;
	dxe->pairhead = NULL;
	xpt = &(dxe->pairhead);
	GLUE_FN_NAME_PREFIX(_add)(dxe);
	for (sxp=sxe->pairhead ; sxp!=NULL ; sxp=sxp->next) {
		dxp = malloc(sizeof(xattrpair));
		passert(dxp);
		dxp->dictname = sxp->dictname;
		dxp->dictvalue = sxp->dictvalue;
		dict_inc_ref(dxp->dictname);
		dict_inc_ref(dxp->dictvalue);
		dxp->next = NULL;
		*xpt = dxp;
		xpt = &(dxp->next);
	}
	return 1;
}

void xattr_cleanup(void) {
	uint16_t i;
	uint32_t j;
	xattrentry *xe,*xen;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (GLUE_HASH_TAB_PREFIX(hashtab)[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				for (xe=GLUE_HASH_TAB_PREFIX(hashtab)[i][j] ; xe!=NULL ; xe=xen) {
					xattr_cleanup_node(xe);
					xen = xe->next;
					free(xe);
				}
				GLUE_HASH_TAB_PREFIX(hashtab)[i][j] = NULL;
			}
		}
	}
	GLUE_FN_NAME_PREFIX(_hash_cleanup)();
}

uint8_t xattr_store(bio *fd) {
	uint8_t hdrbuff[4+1+4];
	uint8_t *ptr;
	uint16_t i;
	uint32_t j;
	uint32_t anleng,avleng;
	xattrentry *xe;
	xattrpair *xp;

	if (fd==NULL) {
		return 0x10;
	}
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (GLUE_HASH_TAB_PREFIX(hashtab)[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				for (xe=GLUE_HASH_TAB_PREFIX(hashtab)[i][j] ; xe!=NULL ; xe=xe->next) {
					for (xp=xe->pairhead ; xp!=NULL ; xp=xp->next) {
						anleng = dict_get_leng(xp->dictname);
						avleng = dict_get_leng(xp->dictvalue);
						if (anleng>255) {
							anleng = 255;
						}
						ptr = hdrbuff;
						put32bit(&ptr,xe->inode);
						put8bit(&ptr,anleng);
						put32bit(&ptr,avleng);
						if (bio_write(fd,hdrbuff,4+1+4)!=(4+1+4)) {
							return 0xFF;
						}
						if (bio_write(fd,dict_get_ptr(xp->dictname),anleng)!=anleng) {
							return 0xFF;
						}
						if (avleng>0) {
							if (bio_write(fd,dict_get_ptr(xp->dictvalue),avleng)!=avleng) {
								return 0xFF;
							}
						}
					}
				}
			}
		}
	}
	memset(hdrbuff,0,4+1+4);
	if (bio_write(fd,hdrbuff,4+1+4)!=(4+1+4)) {
		return 0xFF;
	}
	return 0;
}

int xattr_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t hdrbuff[4+1+4];
	uint8_t *databuff;
	const uint8_t *ptr;
	uint32_t inode;
	uint32_t lastinode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t nl=1;
	xattrentry *xe;
	xattrpair *xp,**xpt;

	(void)mver;
	xpt = NULL;

	databuff = malloc(MFS_XATTR_NAME_MAX+MFS_XATTR_SIZE_MAX);
	passert(databuff);
	lastinode = 0;
	while (1) {
		if (bio_read(fd,hdrbuff,4+1+4)!=(4+1+4)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			errno = err;
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading xattr: read error");
			free(databuff);
			return -1;
		}
		ptr = hdrbuff;
		inode = get32bit(&ptr);
		anleng = get8bit(&ptr);
		avleng = get32bit(&ptr);
		if (inode==0) {
			free(databuff);
			return 1;
		}
		if (fs_check_inode(inode)==0) { // inode does not exist - skip it
			bio_skip(fd,anleng+avleng);
			continue;
		}
		if (anleng==0) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			if (ignoreflag) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading xattr: empty name - ignoring");
				bio_skip(fd,anleng+avleng);
				continue;
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading xattr: empty name");
				free(databuff);
				return -1;
			}
		}
#if MFS_XATTR_NAME_MAX<255
		if (avleng>MFS_XATTR_SIZE_MAX || anleng>MFS_XATTR_NAME_MAX) {
#else
		if (avleng>MFS_XATTR_SIZE_MAX) {
#endif
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			if (ignoreflag) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading xattr: value oversized - ignoring");
				bio_skip(fd,anleng+avleng);
				continue;
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading xattr: value oversized");
				free(databuff);
				return -1;
			}
		}
		if (inode!=lastinode) {
			xe = GLUE_FN_NAME_PREFIX(_find)(inode);
			if (xe==NULL) {
				xe = malloc(sizeof(xattrentry));
				passert(xe);
				xe->inode = inode;
				xe->pairhead = NULL;
				GLUE_FN_NAME_PREFIX(_add)(xe);
				fs_set_xattrflag(inode);
				xpt = &(xe->pairhead);
			} else { // import from old file format
				xpt = &(xe->pairhead);
				while ((*xpt)!=NULL) {
					xpt = &((*xpt)->next);
				}
			}
			lastinode = inode;
		}
		if (bio_read(fd,databuff,anleng+avleng)!=(anleng+avleng)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			errno = err;
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading xattr: read error");
			free(databuff);
			return -1;
		}
		xp = malloc(sizeof(xattrpair));
		xp->dictname = dict_insert(databuff,anleng);
		xp->dictvalue = dict_insert(databuff+anleng,avleng);
		xp->next = NULL;
		*xpt = xp;
		xpt = &(xp->next);
	}
}

int xattr_init(void) {
	GLUE_FN_NAME_PREFIX(_hash_init)();
	return 0;
}

#include "hash_end.h"

#undef LOHASH_BITS
#undef ENTRY_TYPE
#undef GLUE_FN_NAME_PREFIX
#undef HASH_ARGS_TYPE_LIST
#undef HASH_ARGS_LIST
#undef GLUE_HASH_TAB_PREFIX
