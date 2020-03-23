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

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "datapack.h"
#include "slogger.h"
#include "massert.h"
#include "filesystem.h"

#include "glue.h"

typedef struct acl_entry {
	uint32_t id;
	uint16_t perm;
} acl_entry;

typedef struct acl_node {
	uint32_t inode;
	uint8_t acltype;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	acl_entry *acltab;
	struct acl_node *next;
} acl_node;

#define LOHASH_BITS 20
#define ENTRY_TYPE acl_node
#define GLUE_FN_NAME_PREFIX(Y) GLUE(posix_acl_xxx,Y)
#define HASH_ARGS_TYPE_LIST uint32_t inode,uint8_t acltype
#define HASH_ARGS_LIST inode,acltype
#define GLUE_HASH_TAB_PREFIX(Y) GLUE(pacl,Y)

static inline int GLUE_FN_NAME_PREFIX(_cmp)(ENTRY_TYPE *e,HASH_ARGS_TYPE_LIST) {
	return (e->inode==inode && e->acltype==acltype);
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_hash)(HASH_ARGS_TYPE_LIST) {
	return inode*0x56BF7623+acltype;
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_ehash)(ENTRY_TYPE *e) {
	return GLUE_FN_NAME_PREFIX(_hash)(e->inode,e->acltype);
}

#include "hash_begin.h"

static acl_node* posix_acl_create(uint32_t inode,uint8_t acltype) {
	acl_node *acn;

	acn = malloc(sizeof(acl_node));
	passert(acn);
	acn->inode = inode;
	acn->acltype = acltype;
	acn->userperm = 0;
	acn->groupperm = 0;
	acn->otherperm = 0;
	acn->mask = 0;
	acn->namedusers = 0;
	acn->namedgroups = 0;
	acn->acltab = NULL;
	GLUE_FN_NAME_PREFIX(_add)(acn);
	return acn;
}

uint16_t posix_acl_getmode(uint32_t inode) {
	acl_node *acn;

	acn = GLUE_FN_NAME_PREFIX(_find)(inode,POSIX_ACL_ACCESS);
//	(acn->mask==0xFFFF) ???
	return ((acn->userperm & 7) << 6) | ((acn->mask & 7) << 3) | (acn->otherperm & 7);
}

void posix_acl_setmode(uint32_t inode,uint16_t mode) {
	acl_node *acn;

	acn = GLUE_FN_NAME_PREFIX(_find)(inode,POSIX_ACL_ACCESS);
	if (acn!=NULL) {
		acn->userperm &= 0xFFF8;
		acn->userperm |= (mode>>6)&7;
		acn->mask &= 0xFFF8;
		acn->mask |= (mode>>3)&7;
		acn->otherperm &= 0xFFF8;
		acn->otherperm |= mode&7;
	}
}

uint8_t posix_acl_accmode(uint32_t inode,uint32_t auid,uint32_t agids,uint32_t *agid,uint32_t fuid,uint32_t fgid) {
	static uint8_t modetoaccmode[8] = MODE_TO_ACCMODE;
	acl_node *acn;
	int f;
	uint16_t i;
	uint32_t j;
	uint8_t modemask;

	if (auid==0) {
		return modetoaccmode[7];
	}
	acn = GLUE_FN_NAME_PREFIX(_find)(inode,POSIX_ACL_ACCESS);
	if (acn==NULL) {
		return modetoaccmode[0];
	}
	if (auid==fuid) {
		return modetoaccmode[acn->userperm & 0x7];
	} else {
		for (i=0 ; i<acn->namedusers ; i++) {
			if (auid==acn->acltab[i].id) {
				return modetoaccmode[acn->acltab[i].perm & acn->mask & 0x7];
			}
		}
		f = 0;
		modemask = 0;
		for (j=0 ; j<agids ; j++) {
			if (agid[j]==fgid) {
				modemask |= modetoaccmode[acn->groupperm & acn->mask & 0x7];
				f = 1;
			}
		}
		for (i=acn->namedusers ; i<acn->namedusers+acn->namedgroups ; i++) {
			for (j=0 ; j<agids ; j++) {
				if (agid[j]==acn->acltab[i].id) {
					modemask |= modetoaccmode[acn->acltab[i].perm & acn->mask & 0x7];
					f = 1;
				}
			}
		}
		if (f==1) {
			return modemask;
		}
		return modetoaccmode[acn->otherperm & 0x7];
	}
}

uint8_t posix_acl_copydefaults(uint32_t parent,uint32_t inode,uint8_t directory,uint16_t *mode) {
	uint16_t i,acls;
	uint8_t ret;
	acl_node *pacn;
	acl_node *acn;

	ret = 0;
	pacn = GLUE_FN_NAME_PREFIX(_find)(parent,POSIX_ACL_DEFAULT);
	if (pacn==NULL) {
		return ret;
	}
	acls = pacn->namedusers + pacn->namedgroups;
	if (acls==0 && pacn->userperm<=7 && pacn->groupperm<=7 && pacn->otherperm<=7 && pacn->mask==0xFFFF) { // simple ACL as DEFAULT - just modify mode
		*mode &= 0xFE00 | (pacn->userperm << 6) | (pacn->groupperm << 3) | pacn->otherperm;
	} else {
		acn = GLUE_FN_NAME_PREFIX(_find)(inode,POSIX_ACL_ACCESS);
		if (acn==NULL) {
			acn = posix_acl_create(inode,POSIX_ACL_ACCESS);
			ret |= 1;
		}
		acn->userperm &= 0xFFF8;
		acn->userperm |= ((*mode)>>6)&7;
		acn->userperm &= pacn->userperm;
		acn->groupperm = pacn->groupperm;
		acn->otherperm &= 0xFFF8;
		acn->otherperm |= (*mode)&7;
		acn->otherperm &= pacn->otherperm;
		acn->mask &= 0xFFF8;
		acn->mask |= ((*mode)>>3)&7;
		acn->mask &= pacn->mask;
		*mode = ((*mode) & 0xFE00) | ((acn->userperm&7) << 6) | ((acn->groupperm&7) << 3) | (acn->otherperm&7); // groupperm not mask here !!!
		if (acn->namedusers+acn->namedgroups!=acls) {
			if (acn->acltab!=NULL) {
				free(acn->acltab);
			}
			if (acls>0) {
				acn->acltab = malloc(sizeof(acl_entry)*acls);
				passert(acn->acltab);
			} else {
				acn->acltab = NULL;
			}
		}
		acn->namedusers = pacn->namedusers;
		acn->namedgroups = pacn->namedgroups;
		for (i=0 ; i<acls ; i++) {
			acn->acltab[i].id = pacn->acltab[i].id;
			acn->acltab[i].perm = pacn->acltab[i].perm;
		}
	}
	if (directory) {
		acn = GLUE_FN_NAME_PREFIX(_find)(inode,POSIX_ACL_DEFAULT);
		if (acn==NULL) {
			acn = posix_acl_create(inode,POSIX_ACL_DEFAULT);
			ret |= 2;
		}
		acn->userperm = pacn->userperm;
		acn->groupperm = pacn->groupperm;
		acn->otherperm = pacn->otherperm;
		acn->mask = pacn->mask;
		if (acn->namedusers+acn->namedgroups!=acls) {
			if (acn->acltab!=NULL) {
				free(acn->acltab);
			}
			if (acls>0) {
				acn->acltab = malloc(sizeof(acl_entry)*acls);
				passert(acn->acltab);
			} else {
				acn->acltab = NULL;
			}
		}
		acn->namedusers = pacn->namedusers;
		acn->namedgroups = pacn->namedgroups;
		for (i=0 ; i<acls ; i++) {
			acn->acltab[i].id = pacn->acltab[i].id;
			acn->acltab[i].perm = pacn->acltab[i].perm;
		}
	}
	return ret;
}

void posix_acl_remove(uint32_t inode,uint8_t acltype) {
	acl_node *acn;
	acn = GLUE_FN_NAME_PREFIX(_find)(inode,acltype);
	if (acn) {
		GLUE_FN_NAME_PREFIX(_delete)(acn);
		if (acn->acltab) {
			free(acn->acltab);
		}
		free(acn);
	}
}

void posix_acl_set(uint32_t inode,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob) {
	uint16_t i,acls;
	acl_node *acn;

	if (acltype==POSIX_ACL_ACCESS && ((namedusers | namedgroups) == 0) && userperm<=7 && groupperm<=7 && otherperm<=7 && mask==0xFFFF) {
		posix_acl_remove(inode,acltype);
		fs_del_aclflag(inode,acltype);
		return;
	}

	acn = GLUE_FN_NAME_PREFIX(_find)(inode,acltype);
	if (acn==NULL) {
		acn = posix_acl_create(inode,acltype);
		fs_set_aclflag(inode,acltype);
	}

	acls = namedusers + namedgroups;
	acn->userperm = userperm;
	acn->groupperm = groupperm;
	acn->otherperm = otherperm;
	acn->mask = mask;
	if (acn->namedusers+acn->namedgroups!=acls) {
		if (acn->acltab!=NULL) {
			free(acn->acltab);
		}
		if (acls>0) {
			acn->acltab = malloc(sizeof(acl_entry)*acls);
			passert(acn->acltab);
		} else {
			acn->acltab = NULL;
		}
	}
	acn->namedusers = namedusers;
	acn->namedgroups = namedgroups;
//	syslog(LOG_NOTICE,"acls: %u ; acltab: %p ; aclblob: %p",acls,acn->acltab,aclblob);
	for (i=0 ; i<acls ; i++) {
		acn->acltab[i].id = get32bit(&aclblob);
		acn->acltab[i].perm = get16bit(&aclblob);
	}
}

int32_t posix_acl_get_blobsize(uint32_t inode,uint8_t acltype,void **aclnode) {
	acl_node *acn;
	acn = GLUE_FN_NAME_PREFIX(_find)(inode,acltype);

	*aclnode = (void*)acn;
	if (acn==NULL) {
		return -1;
	} else {
		return (acn->namedusers+acn->namedgroups)*6;
	}
}

void posix_acl_get_data(void *aclnode,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *mask,uint16_t *namedusers,uint16_t *namedgroups,uint8_t *aclblob) {
	acl_node *acn;
	uint16_t i,acls;

	acn = (acl_node*)aclnode;

	*userperm = acn->userperm;
	*groupperm = acn->groupperm;
	*otherperm = acn->otherperm;
	*mask = acn->mask;
	*namedusers = acn->namedusers;
	*namedgroups = acn->namedgroups;
	acls = acn->namedusers+acn->namedgroups;
	for (i=0 ; i<acls ; i++) {
		put32bit(&aclblob,acn->acltab[i].id);
		put16bit(&aclblob,acn->acltab[i].perm);
	}
}

uint8_t posix_acl_copy(uint32_t srcinode,uint32_t dstinode,uint8_t acltype) {
	uint32_t acls;
	acl_node *sacn,*dacn;

	sacn = GLUE_FN_NAME_PREFIX(_find)(srcinode,acltype);
	dacn = GLUE_FN_NAME_PREFIX(_find)(dstinode,acltype);
	if (dacn==NULL) {
		dacn = malloc(sizeof(acl_node));
		passert(dacn);
		dacn->inode = dstinode;
		dacn->acltype = acltype;
		GLUE_FN_NAME_PREFIX(_add)(dacn);
	} else {
		if (dacn->acltab!=NULL) {
			free(dacn->acltab);
		}
	}
	dacn->userperm = sacn->userperm;
	dacn->groupperm = sacn->groupperm;
	dacn->otherperm = sacn->otherperm;
	dacn->mask = sacn->mask;
	dacn->namedusers = sacn->namedusers;
	dacn->namedgroups = sacn->namedgroups;
	acls = sacn->namedusers + sacn->namedgroups;
	if (acls>0) {
		dacn->acltab = malloc(sizeof(acl_entry)*acls);
		passert(dacn->acltab);
		memcpy(dacn->acltab,sacn->acltab,sizeof(acl_entry)*acls);
	} else {
		dacn->acltab = NULL;
	}
	return 1;
}

void posix_acl_cleanup(void) {
	uint32_t i,j;
	acl_node *acn,*nacn;

	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (GLUE_HASH_TAB_PREFIX(hashtab)[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				for (acn=GLUE_HASH_TAB_PREFIX(hashtab)[i][j] ; acn!=NULL ; acn=nacn) {
					nacn = acn->next;
					if (acn->acltab) {
						free(acn->acltab);
					}
					free(acn);
				}
				GLUE_HASH_TAB_PREFIX(hashtab)[i][j] = NULL;
			}
		}
	}
	GLUE_FN_NAME_PREFIX(_hash_cleanup)();
}

uint8_t posix_acl_store(bio *fd) {
	uint8_t hdrbuff[4+1+2*6];
	uint8_t aclbuff[6*100];
	uint8_t *ptr;
	uint32_t i,j,accnt,acbcnt;
	acl_node *acn;

	if (fd==NULL) {
		return 0x11;
	}

	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (GLUE_HASH_TAB_PREFIX(hashtab)[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				for (acn=GLUE_HASH_TAB_PREFIX(hashtab)[i][j] ; acn!=NULL ; acn=acn->next) {
					ptr = hdrbuff;
					put32bit(&ptr,acn->inode);
					put8bit(&ptr,acn->acltype);
					put16bit(&ptr,acn->userperm);
					put16bit(&ptr,acn->groupperm);
					put16bit(&ptr,acn->otherperm);
					put16bit(&ptr,acn->mask);
					put16bit(&ptr,acn->namedusers);
					put16bit(&ptr,acn->namedgroups);
					if (bio_write(fd,hdrbuff,4+1+2*6)!=(4+1+2*6)) {
						syslog(LOG_NOTICE,"write error");
						return 0xFF;
					}
					accnt = 0;
					acbcnt = 0;
					ptr = aclbuff;
					while (accnt<acn->namedusers+acn->namedgroups) {
						if (acbcnt==100) {
							if (bio_write(fd,aclbuff,6*100)!=(6*100)) {
								syslog(LOG_NOTICE,"write error");
								return 0xFF;
							}
							acbcnt = 0;
							ptr = aclbuff;
						}
						put32bit(&ptr,acn->acltab[accnt].id);
						put16bit(&ptr,acn->acltab[accnt].perm);
						accnt++;
						acbcnt++;
					}
					if (acbcnt>0) {
						if (bio_write(fd,aclbuff,6*acbcnt)!=(6*acbcnt)) {
							syslog(LOG_NOTICE,"write error");
							return 0xFF;
						}
					}
				}
			}
		}
	}
	memset(hdrbuff,0,4+1+2*6);
	if (bio_write(fd,hdrbuff,4+1+2*6)!=(4+1+2*6)) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	return 0;
}

int posix_acl_load(bio *fd,uint8_t mver,int ignoreflag) {
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
	uint32_t i,acls,acbcnt;
	uint8_t nl=1;
	acl_node *acn;

	while (1) {
		if (bio_read(fd,hdrbuff,4+1+2*6)!=(4+1+2*6)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading posix_acl: read error");
			return -1;
		}
		ptr = hdrbuff;
		inode = get32bit(&ptr);
		if (inode==0) {
			return 1;
		}
		acltype = get8bit(&ptr);
		userperm = get16bit(&ptr);
		groupperm = get16bit(&ptr);
		otherperm = get16bit(&ptr);
		mask = get16bit(&ptr);
		namedusers = get16bit(&ptr);
		namedgroups = get16bit(&ptr);
		acls = namedusers + namedgroups;
		if (fs_check_inode(inode)==0) { // silently skip acl's for non-existent inodes
			bio_skip(fd,6*acls);
			continue;
		}
		if (acltype!=POSIX_ACL_ACCESS && acltype!=POSIX_ACL_DEFAULT) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading posix_acl: wrong acl type");
			if (ignoreflag) {
				bio_skip(fd,6*acls);
				continue;
			} else {
				return -1;
			}
		}
		acn = GLUE_FN_NAME_PREFIX(_find)(inode,acltype);
		if (acn!=NULL) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading posix_acl: repeated acl");
			if (ignoreflag) {
				bio_skip(fd,6*acls);
				continue;
			} else {
				return -1;
			}
		}
		acn = posix_acl_create(inode,acltype);
		fs_set_aclflag(inode,acltype);
		if (mver==0x10 && mask==0) {
			uint16_t mode;
			mode = fs_get_mode(inode);
			userperm &= 0xFFF8;
			userperm |= (mode>>6)&7;
			mask = 7;
			otherperm &= 0xFFF8;
			otherperm |= mode&7;
			mfs_arg_syslog(LOG_WARNING,"emergency set ACL mask for inode %"PRIu32" to 'rwx'",inode);
		}
		acn->userperm = userperm;
		acn->groupperm = groupperm;
		acn->otherperm = otherperm;
		acn->mask = mask;
		acn->namedusers = namedusers;
		acn->namedgroups = namedgroups;
		if (acls>0) {
			acn->acltab = malloc(sizeof(acl_entry)*acls);
			passert(acn->acltab);
		} else {
			acn->acltab = NULL;
		}
		acbcnt = 0;
		for (i=0 ; i<acls ; i++) {
			if (acbcnt==0) {
				acbcnt = acls-i;
				if (acbcnt>100) {
					acbcnt=100;
				}
				if (bio_read(fd,aclbuff,6*acbcnt)!=6*acbcnt) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					GLUE_FN_NAME_PREFIX(_delete)(acn);
					if (acn->acltab!=NULL) {
						free(acn->acltab);
					}
					free(acn);
					errno = err;
					mfs_errlog(LOG_ERR,"loading posix_acl: read error");
					return -1;
				}
				ptr = aclbuff;
			}
			acn->acltab[i].id = get32bit(&ptr);
			acn->acltab[i].perm = get16bit(&ptr);
			acbcnt--;
		}
	}
}

int posix_acl_init(void) {
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
