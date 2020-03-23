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
#include <syslog.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "matocsserv.h"
#include "metadata.h"
#include "storageclass.h"
#include "slogger.h"
#include "datapack.h"
#include "changelog.h"
#include "bio.h"
#include "main.h"
#include "massert.h"

#define MAXSCLASSNLENG 256

#define CHLOGSTRSIZE ((3*MAXLABELSCNT*10*MASKORGROUP)+1)

/* label sets */

typedef struct _storageclass {
	uint8_t nleng;
	uint8_t name[MAXSCLASSNLENG];
	uint8_t admin_only;
	uint8_t mode;
	uint8_t create_labelscnt;
	uint8_t keep_labelscnt;
	uint8_t arch_labelscnt;
	uint8_t has_create_labels;
	uint8_t has_keep_labels;
	uint8_t has_arch_labels;
	uint32_t *create_labelmasks[MAXLABELSCNT];
	uint32_t *keep_labelmasks[MAXLABELSCNT];
	uint32_t *arch_labelmasks[MAXLABELSCNT];
	uint16_t arch_delay;
	uint32_t files;
	uint32_t directories;
} storageclass;

#define FIRSTSCLASSID 10

static storageclass sclasstab[MAXSCLASS];
static uint32_t firstneverused=0;

static inline uint8_t sclass_name_check(uint8_t nleng,const uint8_t *name) {
	uint8_t i;
	if (nleng==0) {
		return 0;
	}
	for (i=0 ; i<nleng ; i++) {
		if (name[i]==0) {
			return 0;
		}
	}
	return 1;
}

static inline void sclass_make_changelog(uint16_t sclassid,uint8_t new_flag) {
	uint32_t i,j;
	char chlogstr[CHLOGSTRSIZE];
	int chlogstrleng;

	chlogstrleng=0;
	for (i=0 ; i<sclasstab[sclassid].create_labelscnt ; i++) {
		for (j=0 ; j<MASKORGROUP ; j++) {
			if (chlogstrleng<CHLOGSTRSIZE) {
				chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",sclasstab[sclassid].create_labelmasks[i][j]);
			}
		}
	}
	for (i=0 ; i<sclasstab[sclassid].keep_labelscnt ; i++) {
		for (j=0 ; j<MASKORGROUP ; j++) {
			if (chlogstrleng<CHLOGSTRSIZE) {
				chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",sclasstab[sclassid].keep_labelmasks[i][j]);
			}
		}
	}
	for (i=0 ; i<sclasstab[sclassid].arch_labelscnt ; i++) {
		for (j=0 ; j<MASKORGROUP ; j++) {
			if (chlogstrleng<CHLOGSTRSIZE) {
				chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",sclasstab[sclassid].arch_labelmasks[i][j]);
			}
		}
	}
	if (chlogstrleng>0) {
		chlogstr[chlogstrleng-1]='\0';
	} else {
		chlogstr[0]='-';
		chlogstr[1]='\0';
	}
	changelog("%"PRIu32"|SCSET(%s,%"PRIu8",W%"PRIu8",K%"PRIu8",A%"PRIu8",%"PRIu8",%"PRIu16",%"PRIu8",%s):%"PRIu16,main_time(),changelog_escape_name(sclasstab[sclassid].nleng,sclasstab[sclassid].name),new_flag,sclasstab[sclassid].create_labelscnt,sclasstab[sclassid].keep_labelscnt,sclasstab[sclassid].arch_labelscnt,sclasstab[sclassid].mode,sclasstab[sclassid].arch_delay,sclasstab[sclassid].admin_only,chlogstr,sclassid);

}

static inline void sclass_fix_has_labels_fields(uint8_t sclassid) {
	uint32_t i;
	uint8_t has_labels;

	has_labels = 0;
	for (i=0 ; i<sclasstab[sclassid].create_labelscnt ; i++) {
		if (sclasstab[sclassid].create_labelmasks[i][0]!=0) {
			has_labels = 1;
			break;
		}
	}
	sclasstab[sclassid].has_create_labels = has_labels;
	has_labels = 0;
	for (i=0 ; i<sclasstab[sclassid].keep_labelscnt ; i++) {
		if (sclasstab[sclassid].keep_labelmasks[i][0]!=0) {
			has_labels = 1;
			break;
		}
	}
	sclasstab[sclassid].has_keep_labels = has_labels;
	has_labels = 0;
	for (i=0 ; i<sclasstab[sclassid].arch_labelscnt ; i++) {
		if (sclasstab[sclassid].arch_labelmasks[i][0]!=0) {
			has_labels = 1;
			break;
		}
	}
	sclasstab[sclassid].has_arch_labels = has_labels;
}

uint8_t sclass_create_entry(uint8_t nleng,const uint8_t *name,uint8_t admin_only,uint8_t mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay) {
	uint32_t sclassid,fsclassid;
	uint32_t i;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	fsclassid = 0;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==nleng && memcmp(sclasstab[sclassid].name,name,nleng)==0) {
			return MFS_ERROR_CLASSEXISTS;
		}
		if (sclasstab[sclassid].nleng==0 && fsclassid==0) {
			fsclassid = sclassid;
		}
	}
	if (fsclassid==0) {
		if (firstneverused==MAXSCLASS) {
			return MFS_ERROR_CLASSLIMITREACH;
		}
		fsclassid = firstneverused;
		firstneverused++;
	}
	if (create_labelscnt==0 || create_labelscnt>MAXLABELSCNT || keep_labelscnt==0 || keep_labelscnt>MAXLABELSCNT || arch_labelscnt==0 || arch_labelscnt>MAXLABELSCNT) {
		return MFS_ERROR_EINVAL;
	}
	sclasstab[fsclassid].nleng = nleng;
	memcpy(sclasstab[fsclassid].name,name,nleng);
	sclasstab[fsclassid].admin_only = admin_only;
	sclasstab[fsclassid].mode = mode;
	sclasstab[fsclassid].create_labelscnt = create_labelscnt;
	for (i=0 ; i<create_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].create_labelmasks[i],create_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].keep_labelscnt = keep_labelscnt;
	for (i=0 ; i<keep_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].keep_labelmasks[i],keep_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].arch_labelscnt = arch_labelscnt;
	for (i=0 ; i<arch_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].arch_labelmasks[i],arch_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].arch_delay = arch_delay;
	sclass_fix_has_labels_fields(fsclassid);
	sclass_make_changelog(fsclassid,1);
	return MFS_STATUS_OK;
}

uint8_t sclass_change_entry(uint8_t nleng,const uint8_t *name,uint16_t chgmask,uint8_t *admin_only,uint8_t *mode,uint8_t *create_labelscnt,uint32_t *create_labelmasks,uint8_t *keep_labelscnt,uint32_t *keep_labelmasks,uint8_t *arch_labelscnt,uint32_t *arch_labelmasks,uint16_t *arch_delay) {
	uint32_t sclassid,fsclassid;
	uint32_t i;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	fsclassid = 0;
	for (sclassid=1 ; fsclassid==0 && sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==nleng && memcmp(sclasstab[sclassid].name,name,nleng)==0) {
			fsclassid = sclassid;
		}
	}
	if (fsclassid==0) {
		return MFS_ERROR_NOSUCHCLASS;
	}
	if (sclassid<FIRSTSCLASSID && (chgmask&SCLASS_CHG_FORCE)==0 && chgmask!=0) {
		return MFS_ERROR_EPERM;
	}
	if (chgmask & SCLASS_CHG_CREATE_MASKS && (*create_labelscnt==0 || *create_labelscnt > MAXLABELSCNT)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_KEEP_MASKS && (*keep_labelscnt==0 || *keep_labelscnt > MAXLABELSCNT)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_ARCH_MASKS && (*arch_labelscnt==0 || *arch_labelscnt > MAXLABELSCNT)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_ADMIN_ONLY) {
		sclasstab[fsclassid].admin_only = *admin_only;
	} else {
		*admin_only = sclasstab[fsclassid].admin_only;
	}
	if (chgmask & SCLASS_CHG_MODE) {
		sclasstab[fsclassid].mode = *mode;
	} else {
		*mode = sclasstab[fsclassid].mode;
	}
	if (chgmask & SCLASS_CHG_CREATE_MASKS) {
		sclasstab[fsclassid].create_labelscnt = *create_labelscnt;
		for (i=0 ; i<*create_labelscnt ; i++) {
			memcpy(sclasstab[fsclassid].create_labelmasks[i],create_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
		}
	} else {
		*create_labelscnt = sclasstab[fsclassid].create_labelscnt;
		for (i=0 ; i<*create_labelscnt ; i++) {
			memcpy(create_labelmasks+(i*MASKORGROUP),sclasstab[fsclassid].create_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
		}
	}
	if (chgmask & SCLASS_CHG_KEEP_MASKS) {
		sclasstab[fsclassid].keep_labelscnt = *keep_labelscnt;
		for (i=0 ; i<*keep_labelscnt ; i++) {
			memcpy(sclasstab[fsclassid].keep_labelmasks[i],keep_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
		}
	} else {
		*keep_labelscnt = sclasstab[fsclassid].keep_labelscnt;
		for (i=0 ; i<*keep_labelscnt ; i++) {
			memcpy(keep_labelmasks+(i*MASKORGROUP),sclasstab[fsclassid].keep_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
		}
	}
	if (chgmask & SCLASS_CHG_ARCH_MASKS) {
		sclasstab[fsclassid].arch_labelscnt = *arch_labelscnt;
		for (i=0 ; i<*arch_labelscnt ; i++) {
			memcpy(sclasstab[fsclassid].arch_labelmasks[i],arch_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
		}
	} else {
		*arch_labelscnt = sclasstab[fsclassid].arch_labelscnt;
		for (i=0 ; i<*arch_labelscnt ; i++) {
			memcpy(arch_labelmasks+(i*MASKORGROUP),sclasstab[fsclassid].arch_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
		}
	}
	if (chgmask & SCLASS_CHG_ARCH_DELAY) {
		sclasstab[fsclassid].arch_delay = *arch_delay;
	} else {
		*arch_delay = sclasstab[fsclassid].arch_delay;
	}
	if (chgmask & (SCLASS_CHG_CREATE_MASKS|SCLASS_CHG_KEEP_MASKS|SCLASS_CHG_ARCH_MASKS)) {
		sclass_fix_has_labels_fields(fsclassid);
	}
	if (chgmask!=0) {
		sclass_make_changelog(fsclassid,0);
	}
	return MFS_STATUS_OK;
}

uint8_t sclass_mr_set_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid,uint8_t new_flag,uint8_t admin_only,uint8_t mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay) {
	uint32_t sclassid,fsclassid;
	uint32_t i;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	fsclassid = 0;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==nleng && memcmp(sclasstab[sclassid].name,name,nleng)==0) {
			if (new_flag) {
				return MFS_ERROR_CLASSEXISTS;
			} else {
				fsclassid = sclassid;
				break;
			}
		}
		if (sclasstab[sclassid].nleng==0 && fsclassid==0 && new_flag) {
			fsclassid = sclassid;
		}
	}
	if (fsclassid==0) {
		if (new_flag) {
			if (firstneverused==MAXSCLASS) {
				return MFS_ERROR_CLASSLIMITREACH;
			}
			fsclassid = firstneverused;
			firstneverused++;
		} else {
			return MFS_ERROR_NOSUCHCLASS;
		}
	}
	if (fsclassid!=esclassid) {
		return MFS_ERROR_MISMATCH;
	}
	if (create_labelscnt==0 || create_labelscnt>MAXLABELSCNT || keep_labelscnt==0 || keep_labelscnt>MAXLABELSCNT || arch_labelscnt==0 || arch_labelscnt>MAXLABELSCNT) {
		return MFS_ERROR_EINVAL;
	}
	if (new_flag) {
		sclasstab[fsclassid].nleng = nleng;
		memcpy(sclasstab[fsclassid].name,name,nleng);
	}
	sclasstab[fsclassid].admin_only = admin_only;
	sclasstab[fsclassid].mode = mode;
	sclasstab[fsclassid].create_labelscnt = create_labelscnt;
	for (i=0 ; i<create_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].create_labelmasks[i],create_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].keep_labelscnt = keep_labelscnt;
	for (i=0 ; i<keep_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].keep_labelmasks[i],keep_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].arch_labelscnt = arch_labelscnt;
	for (i=0 ; i<arch_labelscnt ; i++) {
		memcpy(sclasstab[fsclassid].arch_labelmasks[i],arch_labelmasks+(i*MASKORGROUP),MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fsclassid].arch_delay = arch_delay;
	sclass_fix_has_labels_fields(fsclassid);
	meta_version_inc();
	return MFS_STATUS_OK;
}

static inline uint8_t sclass_univ_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t essclassid,uint16_t edsclassid) {
	uint32_t sclassid,fssclassid,fdsclassid;
	uint32_t i;
	if (sclass_name_check(oldnleng,oldname)==0 || sclass_name_check(newnleng,newname)==0 || (essclassid==0 && edsclassid>0) || (edsclassid==0 && essclassid>0)) {
		return MFS_ERROR_EINVAL;
	}
	fssclassid = 0;
	for (sclassid=1 ; fssclassid==0 && sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==oldnleng && memcmp(sclasstab[sclassid].name,oldname,oldnleng)==0) {
			fssclassid = sclassid;
		}
	}
	if (fssclassid==0) {
		return MFS_ERROR_NOSUCHCLASS;
	}
	if (essclassid!=0 && fssclassid!=essclassid) {
		return MFS_ERROR_MISMATCH;
	}
	fdsclassid = 0;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==newnleng && memcmp(sclasstab[sclassid].name,newname,newnleng)==0) {
			return MFS_ERROR_CLASSEXISTS;
		}
		if (sclasstab[sclassid].nleng==0 && fdsclassid==0) {
			fdsclassid = sclassid;
		}
	}
	if (fdsclassid==0) {
		if (firstneverused==MAXSCLASS) {
			return MFS_ERROR_CLASSLIMITREACH;
		}
		fdsclassid = firstneverused;
		firstneverused++;
	}
	if (edsclassid!=0 && fdsclassid!=edsclassid) {
		return MFS_ERROR_MISMATCH;
	}
	sclasstab[fdsclassid].nleng = newnleng;
	memcpy(sclasstab[fdsclassid].name,newname,newnleng);
	sclasstab[fdsclassid].admin_only = sclasstab[fssclassid].admin_only;
	sclasstab[fdsclassid].mode = sclasstab[fssclassid].mode;
	sclasstab[fdsclassid].create_labelscnt = sclasstab[fssclassid].create_labelscnt;
	for (i=0 ; i<sclasstab[fssclassid].create_labelscnt ; i++) {
		memcpy(sclasstab[fdsclassid].create_labelmasks[i],sclasstab[fssclassid].create_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fdsclassid].has_create_labels = sclasstab[fssclassid].has_create_labels;
	sclasstab[fdsclassid].keep_labelscnt = sclasstab[fssclassid].keep_labelscnt;
	for (i=0 ; i<sclasstab[fssclassid].keep_labelscnt ; i++) {
		memcpy(sclasstab[fdsclassid].keep_labelmasks[i],sclasstab[fssclassid].keep_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fdsclassid].has_keep_labels = sclasstab[fssclassid].has_keep_labels;
	sclasstab[fdsclassid].arch_labelscnt = sclasstab[fssclassid].arch_labelscnt;
	for (i=0 ; i<sclasstab[fssclassid].arch_labelscnt ; i++) {
		memcpy(sclasstab[fdsclassid].arch_labelmasks[i],sclasstab[fssclassid].arch_labelmasks[i],MASKORGROUP*sizeof(uint32_t));
	}
	sclasstab[fdsclassid].has_arch_labels = sclasstab[fssclassid].has_arch_labels;
	sclasstab[fdsclassid].arch_delay = sclasstab[fssclassid].arch_delay;
	if (essclassid==0 && edsclassid==0) {
		changelog("%"PRIu32"|SCDUP(%s,%s):%"PRIu32",%"PRIu32,main_time(),changelog_escape_name(oldnleng,oldname),changelog_escape_name(newnleng,newname),fssclassid,fdsclassid);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t sclass_mr_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t essclassid,uint16_t edsclassid) {
	return sclass_univ_duplicate_entry(oldnleng,oldname,newnleng,newname,essclassid,edsclassid);
}

uint8_t sclass_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname) {
	return sclass_univ_duplicate_entry(oldnleng,oldname,newnleng,newname,0,0);
}

static inline uint8_t sclass_univ_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t esclassid) {
	uint32_t sclassid,fsclassid;
	if (sclass_name_check(oldnleng,oldname)==0 || sclass_name_check(newnleng,newname)==0) {
		return MFS_ERROR_EINVAL;
	}
	fsclassid = 0;
	for (sclassid=1 ; fsclassid==0 && sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==oldnleng && memcmp(sclasstab[sclassid].name,oldname,oldnleng)==0) {
			fsclassid = sclassid;
		}
	}
	if (fsclassid==0) {
		return MFS_ERROR_NOSUCHCLASS;
	}
	if (fsclassid<FIRSTSCLASSID) {
		return MFS_ERROR_EPERM;
	}
	if (esclassid!=0 && fsclassid!=esclassid) {
		return MFS_ERROR_MISMATCH;
	}
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==newnleng && memcmp(sclasstab[sclassid].name,newname,newnleng)==0) {
			return MFS_ERROR_CLASSEXISTS;
		}
	}
	sclasstab[fsclassid].nleng = newnleng;
	memcpy(sclasstab[fsclassid].name,newname,newnleng);
	if (esclassid==0) {
		changelog("%"PRIu32"|SCREN(%s,%s):%"PRIu32,main_time(),changelog_escape_name(oldnleng,oldname),changelog_escape_name(newnleng,newname),fsclassid);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t sclass_mr_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t esclassid) {
	return sclass_univ_rename_entry(oldnleng,oldname,newnleng,newname,esclassid);
}

uint8_t sclass_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname) {
	return sclass_univ_rename_entry(oldnleng,oldname,newnleng,newname,0);
}

static inline uint8_t sclass_univ_delete_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid) {
	uint32_t sclassid,fsclassid;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	fsclassid = 0;
	for (sclassid=1 ; fsclassid==0 && sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==nleng && memcmp(sclasstab[sclassid].name,name,nleng)==0) {
			fsclassid = sclassid;
		}
	}
	if (fsclassid==0) {
		return MFS_ERROR_NOSUCHCLASS;
	}
	if (fsclassid<FIRSTSCLASSID) {
		return MFS_ERROR_EPERM;
	}
	if (sclasstab[fsclassid].files>0 || sclasstab[fsclassid].directories>0) {
		return MFS_ERROR_CLASSINUSE;
	}
	if (esclassid!=0 && fsclassid!=esclassid) {
		return MFS_ERROR_MISMATCH;
	}
	sclasstab[fsclassid].nleng = 0;
	if (esclassid==0) {
		changelog("%"PRIu32"|SCDEL(%s):%"PRIu32,main_time(),changelog_escape_name(nleng,name),fsclassid);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t sclass_mr_delete_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid) {
	return sclass_univ_delete_entry(nleng,name,esclassid);
}

uint8_t sclass_delete_entry(uint8_t nleng,const uint8_t *name) {
	return sclass_univ_delete_entry(nleng,name,0);
}


uint32_t sclass_list_entries(uint8_t *buff,uint8_t longmode) {
	uint32_t sclassid;
	uint32_t ret;
	uint32_t i,og;

	ret = 0;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng>0) {
			if (buff==NULL) {
				ret += sclasstab[sclassid].nleng+1;
				if (longmode&1) {
					ret += (sclasstab[sclassid].create_labelscnt + sclasstab[sclassid].keep_labelscnt + sclasstab[sclassid].arch_labelscnt)*4*MASKORGROUP+7;
				}
			} else {
				put8bit(&buff,sclasstab[sclassid].nleng);
				memcpy(buff,sclasstab[sclassid].name,sclasstab[sclassid].nleng);
				buff+=sclasstab[sclassid].nleng;
				if (longmode&1) {
					put8bit(&buff,sclasstab[sclassid].admin_only);
					put8bit(&buff,sclasstab[sclassid].mode);
					put16bit(&buff,sclasstab[sclassid].arch_delay);
					put8bit(&buff,sclasstab[sclassid].create_labelscnt);
					put8bit(&buff,sclasstab[sclassid].keep_labelscnt);
					put8bit(&buff,sclasstab[sclassid].arch_labelscnt);
					for (i=0 ; i<sclasstab[sclassid].create_labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,sclasstab[sclassid].create_labelmasks[i][og]);
						}
					}
					for (i=0 ; i<sclasstab[sclassid].keep_labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,sclasstab[sclassid].keep_labelmasks[i][og]);
						}
					}
					for (i=0 ; i<sclasstab[sclassid].arch_labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,sclasstab[sclassid].arch_labelmasks[i][og]);
						}
					}
				}
			}
		}
	}
	return ret;
}

uint8_t sclass_find_by_name(uint8_t nleng,const uint8_t *name) {
	uint32_t sclassid;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng==nleng && memcmp(sclasstab[sclassid].name,name,nleng)==0) {
			return sclassid;
		}
	}
	return 0;
}

uint8_t sclass_get_nleng(uint8_t sclassid) {
	return sclasstab[sclassid].nleng;
}

const uint8_t* sclass_get_name(uint8_t sclassid) {
	return sclasstab[sclassid].name;
}

void sclass_incref(uint16_t sclassid,uint8_t type) {
	if (type==TYPE_DIRECTORY) {
		sclasstab[sclassid].directories++;
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		sclasstab[sclassid].files++;
	}
}

void sclass_decref(uint16_t sclassid,uint8_t type) {
	if (type==TYPE_DIRECTORY) {
		sclasstab[sclassid].directories--;
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		sclasstab[sclassid].files--;
	}
}

uint8_t sclass_get_mode(uint16_t sclassid) {
	return sclasstab[sclassid].mode;
}

uint8_t sclass_get_create_goal(uint16_t sclassid) {
	return sclasstab[sclassid].create_labelscnt;
}

uint8_t sclass_get_keepmax_goal(uint16_t sclassid) {
	if (sclasstab[sclassid].arch_labelscnt>sclasstab[sclassid].keep_labelscnt) {
		return sclasstab[sclassid].arch_labelscnt;
	} else {
		return sclasstab[sclassid].keep_labelscnt;
	}
}

uint8_t sclass_get_keeparch_goal(uint16_t sclassid,uint8_t archflag) {
	if (archflag) {
		return sclasstab[sclassid].arch_labelscnt;
	} else {
		return sclasstab[sclassid].keep_labelscnt;
	}
}

uint8_t sclass_get_create_labelmasks(uint16_t sclassid,uint32_t ***labelmasks) {
	*labelmasks = sclasstab[sclassid].create_labelmasks;
	return sclasstab[sclassid].create_labelscnt;
}

uint8_t sclass_get_keeparch_labelmasks(uint16_t sclassid,uint8_t archflag,uint32_t ***labelmasks) {
	if (archflag) {
		*labelmasks = sclasstab[sclassid].arch_labelmasks;
		return sclasstab[sclassid].arch_labelscnt;
	} else {
		*labelmasks = sclasstab[sclassid].keep_labelmasks;
		return sclasstab[sclassid].keep_labelscnt;
	}
}

uint8_t sclass_is_simple_goal(uint16_t sclassid) {
	if (sclassid>=FIRSTSCLASSID) {
		return 0;
	} else {
		return 1;
	}
}

uint8_t sclass_is_admin_only(uint16_t sclassid) {
	return (sclasstab[sclassid].admin_only);
}

uint8_t sclass_has_create_labels(uint16_t sclassid) {
	return sclasstab[sclassid].has_create_labels;
}

uint8_t sclass_has_keeparch_labels(uint16_t sclassid,uint8_t archflag) {
	if (archflag) {
		return sclasstab[sclassid].has_arch_labels;
	} else {
		return sclasstab[sclassid].has_keep_labels;
	}
}

uint16_t sclass_get_arch_delay(uint16_t sclassid) {
	return sclasstab[sclassid].arch_delay;
}

uint32_t sclass_info(uint8_t *buff) {
	uint32_t leng,i,j,k;
	uint64_t sunder,sexact,sover;
	uint64_t aunder,aexact,aover;

	if (buff==NULL) {
		leng = 2;
		for (i=1 ; i<firstneverused ; i++) {
			if (sclasstab[i].nleng>0) {
				leng += 20 + 3 * 16;
				leng += sclasstab[i].nleng;
				leng += ((uint32_t)sclasstab[i].create_labelscnt) * ( MASKORGROUP * 4 + 2 );
				leng += ((uint32_t)sclasstab[i].keep_labelscnt) * ( MASKORGROUP * 4 + 2 );
				leng += ((uint32_t)sclasstab[i].arch_labelscnt) * ( MASKORGROUP * 4 + 2 );
			}
		}
		return leng;
	} else {
		chunk_labelset_can_be_fulfilled(0,NULL); // init server list
		put16bit(&buff,matocsserv_servers_count());
		for (i=1 ; i<firstneverused ; i++) {
			if (sclasstab[i].nleng>0) {
				put8bit(&buff,i);
				put8bit(&buff,sclasstab[i].nleng);
				memcpy(buff,sclasstab[i].name,sclasstab[i].nleng);
				buff+=sclasstab[i].nleng;
				put32bit(&buff,sclasstab[i].files);
				put32bit(&buff,sclasstab[i].directories);
				chunk_sclass_counters(i,0,sclasstab[i].keep_labelscnt,&sunder,&sexact,&sover);
				chunk_sclass_counters(i,1,sclasstab[i].arch_labelscnt,&aunder,&aexact,&aover);
				put64bit(&buff,sunder);
				put64bit(&buff,aunder);
				put64bit(&buff,sexact);
				put64bit(&buff,aexact);
				put64bit(&buff,sover);
				put64bit(&buff,aover);
				put8bit(&buff,sclasstab[i].admin_only);
				put8bit(&buff,sclasstab[i].mode);
				put16bit(&buff,sclasstab[i].arch_delay);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(sclasstab[i].create_labelscnt,sclasstab[i].create_labelmasks));
				put8bit(&buff,sclasstab[i].create_labelscnt);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(sclasstab[i].keep_labelscnt,sclasstab[i].keep_labelmasks));
				put8bit(&buff,sclasstab[i].keep_labelscnt);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(sclasstab[i].arch_labelscnt,sclasstab[i].arch_labelmasks));
				put8bit(&buff,sclasstab[i].arch_labelscnt);
				for (j=0 ; j<sclasstab[i].create_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,sclasstab[i].create_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(sclasstab[i].create_labelmasks[j]));
				}
				for (j=0 ; j<sclasstab[i].keep_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,sclasstab[i].keep_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(sclasstab[i].keep_labelmasks[j]));
				}
				for (j=0 ; j<sclasstab[i].arch_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,sclasstab[i].arch_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(sclasstab[i].arch_labelmasks[j]));
				}
			}
		}
		return 0;
	}
}

uint8_t sclass_store(bio *fd) {
	uint8_t databuff[10+MAXSCLASSNLENG+3*(1+9*4*MASKORGROUP)];
	uint8_t *ptr;
	uint16_t i,j,k;
	int32_t wsize;
	if (fd==NULL) {
		return 0x16;
	}
	ptr = databuff;
	put8bit(&ptr,MASKORGROUP);
	if (bio_write(fd,databuff,1)!=1) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}

	for (i=1 ; i<firstneverused ; i++) {
		if ((sclasstab[i].nleng)>0) {
			ptr = databuff;
			put16bit(&ptr,i);
			put8bit(&ptr,sclasstab[i].nleng);
			put8bit(&ptr,sclasstab[i].admin_only);
			put8bit(&ptr,sclasstab[i].mode);
			put16bit(&ptr,sclasstab[i].arch_delay);
			put8bit(&ptr,sclasstab[i].create_labelscnt);
			put8bit(&ptr,sclasstab[i].keep_labelscnt);
			put8bit(&ptr,sclasstab[i].arch_labelscnt);
			memcpy(ptr,sclasstab[i].name,sclasstab[i].nleng);
			ptr+=sclasstab[i].nleng;
			for (j=0 ; j<sclasstab[i].create_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,sclasstab[i].create_labelmasks[j][k]);
				}
			}
			for (j=0 ; j<sclasstab[i].keep_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,sclasstab[i].keep_labelmasks[j][k]);
				}
			}
			for (j=0 ; j<sclasstab[i].arch_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,sclasstab[i].arch_labelmasks[j][k]);
				}
			}
			wsize = 10+sclasstab[i].nleng+(sclasstab[i].create_labelscnt+sclasstab[i].keep_labelscnt+sclasstab[i].arch_labelscnt)*4*MASKORGROUP;
			if (bio_write(fd,databuff,wsize)!=wsize) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
		}
	}
	memset(databuff,0,10);
	if (bio_write(fd,databuff,10)!=10) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	return 0;
}

int sclass_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t *databuff = NULL;
	const uint8_t *ptr;
	uint32_t labelmask;
	uint32_t chunkcount;
	uint16_t sclassid;
	uint16_t arch_delay;
	uint8_t mode;
	uint8_t create_labelscnt;
	uint8_t keep_labelscnt;
	uint8_t arch_labelscnt;
	uint8_t descrleng;
	uint8_t nleng;
	uint8_t admin_only;
	uint8_t name[MAXSCLASSNLENG];
	uint8_t i,j;
	uint8_t orgroup;
	uint8_t hdrleng;

	if (mver<0x16) { // skip label descriptions
		for (i=0 ; i<26 ; i++) {
			if (bio_read(fd,&descrleng,1)!=1) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading storage class data: read error");
				return -1;
			}
			if (descrleng>128) {
				mfs_syslog(LOG_ERR,"loading storage class data: description too long");
				return -1;
			}
			bio_skip(fd,descrleng);
		}
	}
	if (mver==0x10) {
		orgroup = 1;
	} else {
		if (bio_read(fd,&orgroup,1)!=1) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_errlog(LOG_ERR,"loading storage class: read error");
			return -1;
		}
		if (orgroup>MASKORGROUP) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading storage class data: too many or-groups - ignore");
			} else {
				mfs_syslog(LOG_ERR,"loading storage class data: too many or-groups");
				return -1;
			}
		}
	}
	if (orgroup<1) {
		mfs_syslog(LOG_ERR,"loading storage class data: zero or-groups !!!");
		return -1;
	}
	databuff = malloc(3U*9U*4U*(uint32_t)orgroup);
	passert(databuff);
	hdrleng = (mver==0x12)?11:(mver<=0x13)?3:(mver<=0x14)?5:(mver<=0x15)?8:10;
	while (1) {
		if (bio_read(fd,databuff,hdrleng)!=hdrleng) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_errlog(LOG_ERR,"loading storage class data: read error");
			free(databuff);
			databuff=NULL;
			return -1;
		}
		ptr = databuff;
		sclassid = get16bit(&ptr);
		if (mver>0x15) {
			nleng = get8bit(&ptr);
			admin_only = get8bit(&ptr);
			mode = get8bit(&ptr);
			arch_delay = get16bit(&ptr);
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = get8bit(&ptr);
			arch_labelscnt = get8bit(&ptr);
			chunkcount = 0;
		} else if (mver>0x14) {
			nleng = 0;
			admin_only = 0;
			mode = get8bit(&ptr);
			arch_delay = get16bit(&ptr);
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = get8bit(&ptr);
			arch_labelscnt = get8bit(&ptr);
			chunkcount = 0;
		} else if (mver>0x13) {
			nleng = 0;
			admin_only = 0;
			mode = get8bit(&ptr);
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = get8bit(&ptr);
			arch_labelscnt = keep_labelscnt;
			arch_delay = 0;
			chunkcount = 0;
		} else {
			nleng = 0;
			admin_only = 0;
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = create_labelscnt;
			arch_labelscnt = create_labelscnt;
			mode = SCLASS_MODE_STD;
			arch_delay = 0;
			if (mver==0x12) {
				chunkcount = get32bit(&ptr);
				ptr+=4;
			} else {
				chunkcount = 0;
			}
		}
		if (nleng==0) {
			if (sclassid>=FIRSTSCLASSID) {
				nleng = snprintf((char*)name,MAXSCLASSNLENG,"sclass_%"PRIu32,(uint32_t)(sclassid+1-FIRSTSCLASSID));
			} else {
				nleng = 0;
			}
		} else {
			if (bio_read(fd,name,nleng)!=nleng) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		}
		if (sclassid==0 && create_labelscnt==0 && keep_labelscnt==0 && arch_labelscnt==0 && chunkcount==0 && arch_delay==0) {
			break;
		}
		if (create_labelscnt==0 || create_labelscnt>MAXLABELSCNT || keep_labelscnt==0 || keep_labelscnt>MAXLABELSCNT || arch_labelscnt==0 || arch_labelscnt>MAXLABELSCNT) {
			mfs_arg_syslog(LOG_ERR,"loading storage class data: data format error (sclassid: %"PRIu16" ; mode: %"PRIu8" ; create_labelscnt: %"PRIu8" ; keep_labelscnt: %"PRIu8" ; arch_labelscnt: %"PRIu8" ; arch_delay: %"PRIu16")",sclassid,mode,create_labelscnt,keep_labelscnt,arch_labelscnt,arch_delay);
			free(databuff);
			databuff = NULL;
			return -1;
		}
		if (sclassid==0 || sclassid>=MAXSCLASS || nleng==0) {
			if (ignoreflag) {
				mfs_arg_syslog(LOG_ERR,"loading storage class data: bad sclassid (%"PRIu16") - ignore",sclassid);
				if (mver>0x14) {
					bio_skip(fd,(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup);
				} else if (mver>0x13) {
					bio_skip(fd,(create_labelscnt+keep_labelscnt)*4*orgroup);
				} else {
					bio_skip(fd,(create_labelscnt)*4*orgroup);
				}
				continue;
			} else {
				mfs_arg_syslog(LOG_ERR,"loading storage class data: bad sclassid (%"PRIu16")",sclassid);
				free(databuff);
				databuff=NULL;
				return -1;
			}
		}
		if (mver>0x14) {
			if (bio_read(fd,databuff,(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup)!=(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		} else if (mver>0x13) {
			if (bio_read(fd,databuff,(create_labelscnt+keep_labelscnt)*4*orgroup)!=(create_labelscnt+keep_labelscnt)*4*orgroup) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		} else {
			if (bio_read(fd,databuff,create_labelscnt*4*orgroup)!=create_labelscnt*4*orgroup) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		}
		if (sclassid>=FIRSTSCLASSID && sclasstab[sclassid].nleng>0) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading storage class data: repeated sclassid - ignore");
				if (chunkcount>0) {
					bio_skip(fd,chunkcount*8);
				}
				continue;
			} else {
				mfs_syslog(LOG_ERR,"loading storage class data: repeated sclassid");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		}

		ptr = databuff;
		for (i=0 ; i<create_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (j<orgroup) {
					labelmask = get32bit(&ptr);
				} else {
					labelmask = 0;
				}
				sclasstab[sclassid].create_labelmasks[i][j] = labelmask;
			}
		}
		for (i=0 ; i<keep_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (mver>0x13) {
					if (j<orgroup) {
						labelmask = get32bit(&ptr);
					} else {
						labelmask = 0;
					}
				} else {
					labelmask = sclasstab[sclassid].create_labelmasks[i][j];
				}
				sclasstab[sclassid].keep_labelmasks[i][j] = labelmask;
			}
		}
		for (i=0 ; i<arch_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (mver>0x14) {
					if (j<orgroup) {
						labelmask = get32bit(&ptr);
					} else {
						labelmask = 0;
					}
				} else {
					labelmask = sclasstab[sclassid].keep_labelmasks[i][j];
				}
				sclasstab[sclassid].arch_labelmasks[i][j] = labelmask;
			}
		}
		sclasstab[sclassid].mode = mode;
		sclasstab[sclassid].arch_delay = arch_delay;
		sclasstab[sclassid].create_labelscnt = create_labelscnt;
		sclasstab[sclassid].keep_labelscnt = keep_labelscnt;
		sclasstab[sclassid].arch_labelscnt = arch_labelscnt;
		sclasstab[sclassid].admin_only = admin_only;
		sclasstab[sclassid].nleng = nleng;
		memcpy(sclasstab[sclassid].name,name,nleng);
		sclasstab[sclassid].files = 0;
		sclasstab[sclassid].directories = 0;
		sclass_fix_has_labels_fields(sclassid);
		if (sclassid>=firstneverused) {
			firstneverused = sclassid+1;
		}
		if (chunkcount>0) {
			bio_skip(fd,chunkcount*8);
		}
	}
	free(databuff);
	databuff=NULL;
	return 1;
}

void sclass_cleanup(void) {
	uint32_t i,j;

	for (i=0 ; i<MAXSCLASS ; i++) {
		sclasstab[i].nleng = 0;
		sclasstab[i].name[0] = 0;
		sclasstab[i].admin_only = 0;
		sclasstab[i].mode = SCLASS_MODE_STD;
		sclasstab[i].has_create_labels = 0;
		sclasstab[i].create_labelscnt = 0;
		sclasstab[i].has_keep_labels = 0;
		sclasstab[i].keep_labelscnt = 0;
		sclasstab[i].has_arch_labels = 0;
		sclasstab[i].arch_labelscnt = 0;
		sclasstab[i].arch_delay = 0;
		sclasstab[i].files = 0;
		sclasstab[i].directories = 0;
	}

	for (i=1 ; i<FIRSTSCLASSID ; i++) {
		sclasstab[i].nleng = snprintf((char*)(sclasstab[i].name),MAXSCLASSNLENG,"%"PRIu32,i);
		sclasstab[i].create_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].create_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		sclasstab[i].keep_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].keep_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		sclasstab[i].arch_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].arch_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
	}
	firstneverused = FIRSTSCLASSID;
}

int sclass_init(void) {
	uint32_t i,j;

	for (i=0 ; i<MAXSCLASS ; i++) {
		sclasstab[i].nleng = 0;
		sclasstab[i].name[0] = 0;
		sclasstab[i].admin_only = 0;
		sclasstab[i].mode = SCLASS_MODE_STD;
		sclasstab[i].has_create_labels = 0;
		sclasstab[i].create_labelscnt = 0;
		sclasstab[i].has_keep_labels = 0;
		sclasstab[i].keep_labelscnt = 0;
		sclasstab[i].has_arch_labels = 0;
		sclasstab[i].arch_labelscnt = 0;
		sclasstab[i].arch_delay = 0;
		sclasstab[i].files = 0;
		sclasstab[i].directories = 0;
		for (j=0 ; j<MAXLABELSCNT ; j++) {
			sclasstab[i].create_labelmasks[j]=malloc(MASKORGROUP*sizeof(uint32_t));
			passert(sclasstab[i].create_labelmasks[j]);
			sclasstab[i].keep_labelmasks[j]=malloc(MASKORGROUP*sizeof(uint32_t));
			passert(sclasstab[i].keep_labelmasks[j]);
			sclasstab[i].arch_labelmasks[j]=malloc(MASKORGROUP*sizeof(uint32_t));
			passert(sclasstab[i].arch_labelmasks[j]);
		}
	}

	for (i=1 ; i<FIRSTSCLASSID ; i++) {
		sclasstab[i].nleng = snprintf((char*)(sclasstab[i].name),MAXSCLASSNLENG,"%"PRIu32,i);
		sclasstab[i].create_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].create_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		sclasstab[i].keep_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].keep_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		sclasstab[i].arch_labelscnt = i;
		for (j=0 ; j<i ; j++) {
			memset(sclasstab[i].arch_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
	}
	firstneverused = FIRSTSCLASSID;

	return 0;
}
