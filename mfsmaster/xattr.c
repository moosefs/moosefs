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

#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "massert.h"
#include "slogger.h"
#include "datapack.h"
#include "filesystem.h"
#include "xattr.h"
#include "bio.h"

#define XATTR_INODE_HASH_SIZE 65536
#define XATTR_DATA_HASH_SIZE 524288

typedef struct _xattr_data_entry {
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t *attrname;
	uint8_t *attrvalue;
	struct _xattr_data_entry **previnode,*nextinode;
	struct _xattr_data_entry **prev,*next;
} xattr_data_entry;

typedef struct _xattr_inode_entry {
	uint32_t inode;
	uint32_t anleng;
	uint32_t avleng;
	struct _xattr_data_entry *data_head;
	struct _xattr_inode_entry *next;
} xattr_inode_entry;

static xattr_inode_entry **xattr_inode_hash;
static xattr_data_entry **xattr_data_hash;


static inline uint32_t xattr_inode_hash_fn(uint32_t inode) {
	return ((inode*0x72B5F387U)&(XATTR_INODE_HASH_SIZE-1));
}

static inline uint32_t xattr_data_hash_fn(uint32_t inode,uint8_t anleng,const uint8_t *attrname) {
	uint32_t hash = inode*5381U;
	while (anleng) {
		hash = (hash * 33U) + (*attrname);
		attrname++;
		anleng--;
	}
	return (hash&(XATTR_DATA_HASH_SIZE-1));
}

int xattr_namecheck(uint8_t anleng,const uint8_t *attrname) {
	uint32_t i;
	for (i=0 ; i<anleng ; i++) {
		if (attrname[i]=='\0') {
			return -1;
		}
	}
	return 0;
}

static inline void xattr_removeentry(xattr_data_entry *xa) {
	*(xa->previnode) = xa->nextinode;
	if (xa->nextinode) {
		xa->nextinode->previnode = xa->previnode;
	}
	*(xa->prev) = xa->next;
	if (xa->next) {
		xa->next->prev = xa->prev;
	}
	free(xa->attrname);
	if (xa->attrvalue) {
		free(xa->attrvalue);
	}
	free(xa);
}

void xattr_removeinode(uint32_t inode) {
	xattr_inode_entry *ih,**ihp;

	ihp = &(xattr_inode_hash[xattr_inode_hash_fn(inode)]);
	while ((ih = *ihp)) {
		if (ih->inode==inode) {
			while (ih->data_head) {
				xattr_removeentry(ih->data_head);
			}
			*ihp = ih->next;
			free(ih);
		} else {
			ihp = &(ih->next);
		}
	}
	fs_del_xattrflag(inode);
}

uint8_t xattr_setattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode) {
	xattr_inode_entry *ih;
	xattr_data_entry *xa;
	uint32_t hash,ihash;

	if (avleng>MFS_XATTR_SIZE_MAX) {
		return ERROR_ERANGE;
	}
#if MFS_XATTR_NAME_MAX<255
	if (anleng==0U || anleng>MFS_XATTR_NAME_MAX) {
#else
	if (anleng==0U) {
#endif
		return ERROR_EINVAL;
	}

	ihash = xattr_inode_hash_fn(inode);
	for (ih = xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

	hash = xattr_data_hash_fn(inode,anleng,attrname);
	for (xa = xattr_data_hash[hash]; xa ; xa=xa->next) {
		if (xa->inode==inode && xa->anleng==anleng && memcmp(xa->attrname,attrname,anleng)==0) {
			passert(ih);
			if (mode==MFS_XATTR_CREATE_ONLY) { // create only
				return ERROR_EEXIST;
			}
			if (mode==MFS_XATTR_REMOVE) { // remove
				ih->anleng -= anleng+1U;
				ih->avleng -= xa->avleng;
				xattr_removeentry(xa);
				if (ih->data_head==NULL) {
					if (ih->anleng!=0 || ih->avleng!=0) {
						syslog(LOG_WARNING,"xattr non zero lengths on remove (inode:%"PRIu32",anleng:%"PRIu32",avleng:%"PRIu32")",ih->inode,ih->anleng,ih->avleng);
					}
					xattr_removeinode(inode);
				}
				return STATUS_OK;
			}
			ih->avleng -= xa->avleng;
			if (xa->attrvalue) {
				free(xa->attrvalue);
			}
			if (avleng>0) {
				xa->attrvalue = malloc(avleng);
				passert(xa->attrvalue);
				memcpy(xa->attrvalue,attrvalue,avleng);
			} else {
				xa->attrvalue = NULL;
			}
			xa->avleng = avleng;
			ih->avleng += avleng;
			return STATUS_OK;
		}
	}

	if (mode==MFS_XATTR_REPLACE_ONLY || mode==MFS_XATTR_REMOVE) {
		return ERROR_ENOATTR;
	}

	if (ih && ih->anleng+anleng+1>MFS_XATTR_LIST_MAX) {
		return ERROR_ERANGE;
	}

	xa = malloc(sizeof(xattr_data_entry));
	passert(xa);
	xa->inode = inode;
	xa->attrname = malloc(anleng);
	passert(xa->attrname);
	memcpy(xa->attrname,attrname,anleng);
	xa->anleng = anleng;
	if (avleng>0) {
		xa->attrvalue = malloc(avleng);
		passert(xa->attrvalue);
		memcpy(xa->attrvalue,attrvalue,avleng);
	} else {
		xa->attrvalue = NULL;
	}
	xa->avleng = avleng;
	xa->next = xattr_data_hash[hash];
	if (xa->next) {
		xa->next->prev = &(xa->next);
	}
	xa->prev = xattr_data_hash + hash;
	xattr_data_hash[hash] = xa;

	if (ih) {
		xa->nextinode = ih->data_head;
		if (xa->nextinode) {
			xa->nextinode->previnode = &(xa->nextinode);
		}
		xa->previnode = &(ih->data_head);
		ih->data_head = xa;
		ih->anleng += anleng+1U;
		ih->avleng += avleng;
	} else {
		ih = malloc(sizeof(xattr_inode_entry));
		passert(ih);
		ih->inode = inode;
		xa->nextinode = NULL;
		xa->previnode = &(ih->data_head);
		ih->data_head = xa;
		ih->anleng = anleng+1U;
		ih->avleng = avleng;
		ih->next = xattr_inode_hash[ihash];
		xattr_inode_hash[ihash] = ih;
		fs_set_xattrflag(inode);
	}
	return STATUS_OK;
}

uint8_t xattr_getattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue) {
	xattr_data_entry *xa;

	for (xa = xattr_data_hash[xattr_data_hash_fn(inode,anleng,attrname)] ; xa ; xa=xa->next) {
		if (xa->inode==inode && xa->anleng==anleng && memcmp(xa->attrname,attrname,anleng)==0) {
			if (xa->avleng>MFS_XATTR_SIZE_MAX) {
				return ERROR_ERANGE;
			}
			*attrvalue = xa->attrvalue;
			*avleng = xa->avleng;
			return STATUS_OK;
		}
	}
	return ERROR_ENOATTR;
}

uint8_t xattr_listattr_leng(uint32_t inode,void **xanode,uint32_t *xasize) {
	xattr_inode_entry *ih;
	xattr_data_entry *xa;

	*xasize = 0;
	for (ih = xattr_inode_hash[xattr_inode_hash_fn(inode)] ; ih ; ih=ih->next) {
		if (ih->inode==inode) {
			*xanode = ih;
			for (xa=ih->data_head ; xa ; xa=xa->nextinode) {
				*xasize += xa->anleng+1U;
			}
			if (*xasize>MFS_XATTR_LIST_MAX) {
				return ERROR_ERANGE;
			}
			return STATUS_OK;
		}
	}
	*xanode = NULL;
	return STATUS_OK;
}

void xattr_listattr_data(void *xanode,uint8_t *xabuff) {
	xattr_inode_entry *ih = (xattr_inode_entry*)xanode;
	xattr_data_entry *xa;
	uint32_t l;

	l = 0;
	if (ih) {
		for (xa=ih->data_head ; xa ; xa=xa->nextinode) {
			memcpy(xabuff+l,xa->attrname,xa->anleng);
			l+=xa->anleng;
			xabuff[l++]=0;
		}
	}
}

uint8_t xattr_copy(uint32_t srcinode,uint32_t dstinode) {
	xattr_inode_entry *sih,*dih;
	xattr_data_entry *sxa,*dxa;
	uint32_t hash,sihash,dihash;

	sihash = xattr_inode_hash_fn(srcinode);
	for (sih = xattr_inode_hash[sihash]; sih && sih->inode!=srcinode; sih=sih->next) {}
	if (sih==NULL || sih->data_head==NULL) {
		return 0;
	}
	dihash = xattr_inode_hash_fn(dstinode);
	for (dih = xattr_inode_hash[dihash]; dih && dih->inode!=dstinode; dih=dih->next) {}
	if (dih==NULL) {
		dih = malloc(sizeof(xattr_inode_entry));
		passert(dih);
		dih->inode = dstinode;
		dih->data_head = NULL;
		dih->anleng = 0;
		dih->avleng = 0;
		dih->next = xattr_inode_hash[dihash];
		xattr_inode_hash[dihash] = dih;
		// fs_set_xattrflag(inode); - caller will do it
	}

	for (sxa=sih->data_head ; sxa ; sxa=sxa->nextinode) {
		dxa = malloc(sizeof(xattr_data_entry));
		passert(dxa);
		dxa->inode = dstinode;
		dxa->attrname = malloc(sxa->anleng);
		passert(dxa->attrname);
		memcpy(dxa->attrname,sxa->attrname,sxa->anleng);
		dxa->anleng = sxa->anleng;
		if (sxa->avleng>0) {
			dxa->attrvalue = malloc(sxa->avleng);
			passert(dxa->attrvalue);
			memcpy(dxa->attrvalue,sxa->attrvalue,sxa->avleng);
		} else {
			dxa->attrvalue = NULL;
		}
		dxa->avleng = sxa->avleng;
		hash = xattr_data_hash_fn(dstinode,sxa->anleng,sxa->attrname);
		dxa->next = xattr_data_hash[hash];
		if (dxa->next) {
			dxa->next->prev = &(dxa->next);
		}
		dxa->prev = xattr_data_hash + hash;
		xattr_data_hash[hash] = dxa;
		dxa->nextinode = dih->data_head;
		if (dxa->nextinode) {
			dxa->nextinode->previnode = &(dxa->nextinode);
		}
		dxa->previnode = &(dih->data_head);
		dih->data_head = dxa;
		dih->anleng += sxa->anleng+1U;
		dih->avleng += sxa->avleng;
	}
	return 1;
}

void xattr_cleanup(void) {
	uint32_t i;
	xattr_data_entry *xa,*nxa;
	xattr_inode_entry *ih,*nih;

	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=xattr_data_hash[i] ; xa ; xa=nxa) {
			nxa = xa->next;
			if (xa->attrname) {
				free(xa->attrname);
			}
			if (xa->attrvalue) {
				free(xa->attrvalue);
			}
			free(xa);
		}
		xattr_data_hash[i]=NULL;
	}
	for (i=0 ; i<XATTR_INODE_HASH_SIZE ; i++) {
		for (ih=xattr_inode_hash[i] ; ih ; ih=nih) {
			nih = ih->next;
			free(ih);
		}
		xattr_inode_hash[i]=NULL;
	}
}

uint8_t xattr_store(bio *fd) {
	uint8_t hdrbuff[4+1+4];
	uint8_t *ptr;
	uint32_t i;
	xattr_data_entry *xa;

	if (fd==NULL) {
		return 0x10;
	}
	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=xattr_data_hash[i] ; xa ; xa=xa->next) {
			ptr = hdrbuff;
			put32bit(&ptr,xa->inode);
			put8bit(&ptr,xa->anleng);
			put32bit(&ptr,xa->avleng);
			if (bio_write(fd,hdrbuff,4+1+4)!=(4+1+4)) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
			if (bio_write(fd,xa->attrname,xa->anleng)!=(xa->anleng)) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
			if (xa->avleng>0) {
				if (bio_write(fd,xa->attrvalue,xa->avleng)!=(xa->avleng)) {
					syslog(LOG_NOTICE,"write error");
					return 0xFF;
				}
			}
		}
	}
	memset(hdrbuff,0,4+1+4);
	if (bio_write(fd,hdrbuff,4+1+4)!=(4+1+4)) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	return 0;
}

int xattr_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t hdrbuff[4+1+4];
	const uint8_t *ptr;
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t nl=1;
	xattr_data_entry *xa;
	xattr_inode_entry *ih;
	uint32_t hash,ihash;

	(void)mver;

	while (1) {
		if (bio_read(fd,hdrbuff,4+1+4)!=(4+1+4)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading xattr: read error");
			return -1;
		}
		ptr = hdrbuff;
		inode = get32bit(&ptr);
		anleng = get8bit(&ptr);
		avleng = get32bit(&ptr);
		if (inode==0) {
			return 1;
		}
		if (anleng==0) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: empty name");
			if (ignoreflag) {
				bio_skip(fd,anleng+avleng);
				continue;
			} else {
				return -1;
			}
		}
		if (avleng>MFS_XATTR_SIZE_MAX) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: value oversized");
			if (ignoreflag) {
				bio_skip(fd,anleng+avleng);
				continue;
			} else {
				return -1;
			}
		}

		ihash = xattr_inode_hash_fn(inode);
		for (ih = xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

		if (ih && ih->anleng+anleng+1>MFS_XATTR_LIST_MAX) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: name list too long");
			if (ignoreflag) {
				bio_skip(fd,anleng+avleng);
				continue;
			} else {
				return -1;
			}
		}

		xa = malloc(sizeof(xattr_data_entry));
		passert(xa);
		xa->inode = inode;
		xa->attrname = malloc(anleng);
		passert(xa->attrname);
		if (bio_read(fd,xa->attrname,anleng)!=anleng) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			free(xa->attrname);
			free(xa);
			errno = err;
			mfs_errlog(LOG_ERR,"loading xattr: read error");
			return -1;
		}
		xa->anleng = anleng;
		if (avleng>0) {
			xa->attrvalue = malloc(avleng);
			passert(xa->attrvalue);
			if (bio_read(fd,xa->attrvalue,avleng)!=avleng) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					// nl=0;
				}
				free(xa->attrname);
				free(xa->attrvalue);
				free(xa);
				errno = err;
				mfs_errlog(LOG_ERR,"loading xattr: read error");
				return -1;
			}
		} else {
			xa->attrvalue = NULL;
		}
		xa->avleng = avleng;
		hash = xattr_data_hash_fn(inode,xa->anleng,xa->attrname);
		xa->next = xattr_data_hash[hash];
		if (xa->next) {
			xa->next->prev = &(xa->next);
		}
		xa->prev = xattr_data_hash + hash;
		xattr_data_hash[hash] = xa;

		if (ih) {
			xa->nextinode = ih->data_head;
			if (xa->nextinode) {
				xa->nextinode->previnode = &(xa->nextinode);
			}
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng += anleng+1U;
			ih->avleng += avleng;
		} else {
			ih = malloc(sizeof(xattr_inode_entry));
			passert(ih);
			ih->inode = inode;
			xa->nextinode = NULL;
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng = anleng+1U;
			ih->avleng = avleng;
			ih->next = xattr_inode_hash[ihash];
			xattr_inode_hash[ihash] = ih;
			fs_set_xattrflag(inode);
		}
	}
}

int xattr_init(void) {
	uint32_t i;
	xattr_data_hash = malloc(sizeof(xattr_data_entry*)*XATTR_DATA_HASH_SIZE);
	passert(xattr_data_hash);
	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		xattr_data_hash[i]=NULL;
	}
	xattr_inode_hash = malloc(sizeof(xattr_inode_entry*)*XATTR_INODE_HASH_SIZE);
	passert(xattr_inode_hash);
	for (i=0 ; i<XATTR_INODE_HASH_SIZE ; i++) {
		xattr_inode_hash[i]=NULL;
	}
	return 0;
}

