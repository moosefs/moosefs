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
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "patterns.h"
#include "matocsserv.h"
#include "matoclserv.h"
#include "metadata.h"
#include "storageclass.h"
#include "mfslog.h"
#include "datapack.h"
#include "changelog.h"
#include "bio.h"
#include "main.h"
#include "cfg.h"
#include "massert.h"


#define CHLOGSTRSIZE (4*MAXLABELSCNT*(SCLASS_EXPR_MAX_SIZE*2+1)+1)

#define MAX_EC_LEVEL 9
#define REDUCED_EC_LEVEL 1

#define COMPAT_ECMODE 8

typedef struct _storageclass {
	uint8_t nleng;
	uint8_t dleng;
	uint8_t name[MAXSCLASSNAMELENG];
	uint8_t desc[MAXSCLASSDESCLENG];
	uint32_t priority;
	uint8_t export_group;
	uint8_t admin_only;
	uint8_t arch_mode;
	uint8_t labels_mode;
	storagemode create,keep,arch,trash;
	uint16_t arch_delay;
	uint16_t min_trashretention;
	uint64_t arch_min_size;
	uint32_t files; // internal use
	uint32_t directories; // internal use
} storageclass;

static storagemode tmp_storagemode;

static storageclass sclasstab[MAXSCLASS];
static uint32_t firstneverused=0;

static uint8_t ec_current_version = 0;

static uint8_t MaxECRedundancyLevel = 1;

static uint8_t DefaultECMODE = 8; // = Default number of data parts

// compatibility with 3.x tools
void sclass_maskorgroup_to_labelexpr(uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE],uint32_t labelmasks[MAXLABELSCNT*MASKORGROUP],uint8_t labelscnt) {
	uint32_t mask;
	uint8_t b,ands,ors,i,exprpos;
	if (labelscnt>MAXLABELSCNT) {
		labelscnt = MAXLABELSCNT;
	}
	for (i=0 ; i<labelscnt ; i++) {
		memset(labelexpr[i],0,SCLASS_EXPR_MAX_SIZE);
		exprpos = 0;
		ors = 0;
		while (ors<4 && labelmasks[i*MASKORGROUP+ors]!=0) {
			ands = 0;
			for (b=0,mask=1 ; b<26 ; b++,mask<<=1) {
				if (labelmasks[i*MASKORGROUP+ors]&mask) {
					labelexpr[i][exprpos++]=SCLASS_EXPR_SYMBOL+b;
					ands++;
				}
			}
			if (ands>1) {
				labelexpr[i][exprpos++]=SCLASS_EXPR_OP_AND+(ands-2);
			}
			ors++;
		}
		if (ors>1) {
			labelexpr[i][exprpos++]=SCLASS_EXPR_OP_OR+(ors-2);
		}
		labelexpr[i][exprpos++]=0;
	}
}

uint8_t sclass_labelexpr_to_maskorgroup(uint32_t labelmasks[MAXLABELSCNT*MASKORGROUP],uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE],uint8_t labelscnt) {
	uint32_t mask;
	uint8_t i,ors,ands,exprpos;
	if (labelscnt>MAXLABELSCNT) {
		return 0;
	}
	for (i=0 ; i<labelscnt ; i++) {
		exprpos = 0;
		while (exprpos<SCLASS_EXPR_MAX_SIZE && labelexpr[i][exprpos]!=0) {
			exprpos++;
		}
		if (exprpos>=SCLASS_EXPR_MAX_SIZE) { // bad expression
			return 0;
		}
		memset(labelmasks+(i*MASKORGROUP),0,sizeof(uint32_t)*MASKORGROUP);
		if (exprpos>0) {
			if ((labelexpr[i][exprpos-1]&SCLASS_EXPR_TYPE_MASK)==SCLASS_EXPR_OP_OR) {
				exprpos--;
				ors = (labelexpr[i][exprpos]&SCLASS_EXPR_VALUE_MASK) + 2;
			} else {
				ors = 1;
			}
			while (ors>0) {
				ors--;
				if (exprpos==0) {
					return 0;
				}
				if ((labelexpr[i][exprpos-1]&SCLASS_EXPR_TYPE_MASK)==SCLASS_EXPR_OP_AND) {
					exprpos--;
					ands = (labelexpr[i][exprpos]&SCLASS_EXPR_VALUE_MASK) + 2;
				} else {
					ands = 1;
				}
				mask = 0;
				while (ands>0) {
					ands--;
					if (exprpos==0) {
						return 0;
					}
					exprpos--;
					if ((labelexpr[i][exprpos]&SCLASS_EXPR_TYPE_MASK)==SCLASS_EXPR_SYMBOL) {
						mask |= 1<<(labelexpr[i][exprpos]&SCLASS_EXPR_VALUE_MASK);
					} else {
						return 0;
					}
				}
				labelmasks[i*MASKORGROUP+ors]=mask;
			}
			if (exprpos!=0) {
				return 0;
			}
		}
	}
	return 1;
}




uint8_t sclass_ec_version(void) {
	return ec_current_version;
}

static uint8_t sclass_check_ec(uint8_t ec_new_version) {
	if (ec_current_version>=ec_new_version) {
		return 1;
	}
	if (ec_new_version>=1) {
		if (matocsserv_get_min_cs_version()<VERSION2INT(4,0,0)) {
			return 0;
		}
		if (matoclserv_get_min_cl_version()<VERSION2INT(4,0,0)) {
			return 0;
		}
	}
	if (ec_new_version>=2) {
		if (matocsserv_get_min_cs_version()<VERSION2INT(4,26,0)) {
			return 0;
		}
		if (matoclserv_get_min_cl_version()<VERSION2INT(4,26,0)) {
			return 0;
		}
	}
	changelog("%"PRIu32"|SCECVERSION(%u)",main_time(),ec_new_version);
	ec_current_version = ec_new_version;
	return 1;
}

uint8_t sclass_mr_ec_version(uint8_t ec_new_version) {
	if (ec_current_version>=ec_new_version) {
		return MFS_ERROR_MISMATCH;
	}
	meta_version_inc();
	ec_current_version = ec_new_version;
	return MFS_STATUS_OK;
}

static inline uint8_t sclass_name_check(uint8_t nleng,const uint8_t *name) {
	uint8_t i;
	if (nleng==0) {
		return 0;
	}
	if (name[0]==32) {
		return 0;
	}
	if (name[nleng-1]==32) {
		return 0;
	}
	for (i=0 ; i<nleng ; i++) {
		if (name[i]<32) {
			return 0;
		}
	}
	return 1;
}

static inline uint32_t sclass_make_changelog_mode(storagemode *m,char *buff,uint32_t maxleng) {
	uint32_t leng = 0;
	uint32_t i,j;
	for (i=0 ; i<m->labelscnt ; i++) {
		for (j=0 ; j<SCLASS_EXPR_MAX_SIZE && m->labelexpr[i][j]!=0 ; j++) {
			if (leng<maxleng) {
				leng += snprintf(buff+leng,maxleng-leng,"%02"PRIX8,m->labelexpr[i][j]);
			}
		}
		if (leng<maxleng) {
			buff[leng++]=',';
		}
	}
	return leng;
}

static inline void sclass_make_changelog(uint16_t sclassid,uint8_t new_flag) {
	char chlogstr[CHLOGSTRSIZE];
	int chlogstrleng;

	chlogstrleng = 0;
	chlogstrleng += sclass_make_changelog_mode(&(sclasstab[sclassid].create),chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng);
	chlogstrleng += sclass_make_changelog_mode(&(sclasstab[sclassid].keep),chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng);
	chlogstrleng += sclass_make_changelog_mode(&(sclasstab[sclassid].arch),chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng);
	chlogstrleng += sclass_make_changelog_mode(&(sclasstab[sclassid].trash),chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng);
	if (chlogstrleng>0) {
		chlogstr[chlogstrleng-1] = '\0';
	} else {
		chlogstr[0] = '-';
		chlogstr[1] = '\0';
	}
	changelog("%"PRIu32"|SCSET(%s,%"PRIu8",1,%s,%"PRIu32",%"PRIu8",C(%"PRIu8":%"PRIu32":%"PRIu8"),K(%"PRIu8":%"PRIu32":%"PRIu8"),A(%"PRIu8":%"PRIu32":%"PRIu8":%"PRIu8"),T(%"PRIu8":%"PRIu32":%"PRIu8":%"PRIu8"),%"PRIu8",(%"PRIu8":%"PRIu16":%"PRIu64"),%"PRIu16",%"PRIu8",%s):%"PRIu16,
			main_time(),changelog_escape_name(sclasstab[sclassid].nleng,sclasstab[sclassid].name),new_flag,
			changelog_escape_name(sclasstab[sclassid].dleng,sclasstab[sclassid].desc),
			sclasstab[sclassid].priority,
			sclasstab[sclassid].export_group,
			sclasstab[sclassid].create.labelscnt,sclasstab[sclassid].create.uniqmask,sclasstab[sclassid].create.labels_mode,
			sclasstab[sclassid].keep.labelscnt,sclasstab[sclassid].keep.uniqmask,sclasstab[sclassid].keep.labels_mode,
			sclasstab[sclassid].arch.labelscnt,sclasstab[sclassid].arch.uniqmask,sclasstab[sclassid].arch.ec_data_chksum_parts,sclasstab[sclassid].arch.labels_mode,
			sclasstab[sclassid].trash.labelscnt,sclasstab[sclassid].trash.uniqmask,sclasstab[sclassid].trash.ec_data_chksum_parts,sclasstab[sclassid].trash.labels_mode,
			sclasstab[sclassid].labels_mode,
			sclasstab[sclassid].arch_mode,sclasstab[sclassid].arch_delay,sclasstab[sclassid].arch_min_size,
			sclasstab[sclassid].min_trashretention,sclasstab[sclassid].admin_only,chlogstr,sclassid);
}

static inline void sclass_mode_fix_has_labels(storagemode *m) {
	uint32_t i;
	if (m->uniqmask) {
		m->has_labels = 1;
	} else {
		m->has_labels = 0;
		for (i=0 ; i<m->labelscnt ; i++) {
			if (!(m->labelexpr[i][0] == 0 || ((m->labelexpr[i][0]==SCLASS_EXPR_SYMBOL_ANY) && m->labelexpr[i][1]==0))) {
//			if (m->labelexpr[i][0]!=0) {
				m->has_labels = 1;
				return;
			}
		}
	}
}

static inline void sclass_fix_has_labels_fields(uint8_t sclassid) {
	sclass_mode_fix_has_labels(&(sclasstab[sclassid].create));
	sclass_mode_fix_has_labels(&(sclasstab[sclassid].keep));
	sclass_mode_fix_has_labels(&(sclasstab[sclassid].arch));
	sclass_mode_fix_has_labels(&(sclasstab[sclassid].trash));
}

static void sclass_fix_matching_servers_fields(void) {
	uint32_t i;
	chunk_labelset_fix_matching_servers(NULL);
	for (i=1 ; i<firstneverused ; i++) {
		if (sclasstab[i].nleng>0) {
			chunk_labelset_fix_matching_servers(&(sclasstab[i].create));
			chunk_labelset_fix_matching_servers(&(sclasstab[i].keep));
			chunk_labelset_fix_matching_servers(&(sclasstab[i].arch));
			chunk_labelset_fix_matching_servers(&(sclasstab[i].trash));
		}
	}
}

uint8_t sclass_create_entry(uint8_t nleng,const uint8_t *name,uint8_t dleng,const uint8_t *desc,uint32_t priority,uint8_t export_group,uint8_t admin_only,uint8_t labels_mode,uint8_t arch_mode,uint16_t arch_delay,uint64_t arch_min_size,uint16_t min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash) {
	uint32_t sclassid,fsclassid;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	if (dleng>0 && sclass_name_check(dleng,desc)==0) {
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
	if (create->ec_data_chksum_parts>0 || keep->ec_data_chksum_parts>0 || create->labelscnt>MAXLABELSCNT || keep->labelscnt==0 || keep->labelscnt>MAXLABELSCNT) {
		return MFS_ERROR_EINVAL;
	}
	if (arch->ec_data_chksum_parts>0) {
		if (sclass_check_ec(1)==0) {
			return MFS_ERROR_INCOMPATVERSION;
		}
		if (arch->ec_data_chksum_parts>>4) { // data parts defined
			if ((arch->ec_data_chksum_parts>>4)!=4 && (arch->ec_data_chksum_parts>>4)!=8) { // not supported?
				return MFS_ERROR_EINVAL;
			}
		} else { // not ? - use defaults
			arch->ec_data_chksum_parts |= (DefaultECMODE)<<4;
		}
		if ((arch->ec_data_chksum_parts>>4)==4) {
			if (sclass_check_ec(2)==0) {
				return MFS_ERROR_INCOMPATVERSION;
			}
		}
		if (arch->labelscnt>2 || (arch->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			return MFS_ERROR_EINVAL;
		}
	} else {
		if (arch->labelscnt>MAXLABELSCNT) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (trash->ec_data_chksum_parts>0) {
		if (sclass_check_ec(1)==0) {
			return MFS_ERROR_INCOMPATVERSION;
		}
		if (trash->ec_data_chksum_parts>>4) { // data parts defined
			if ((trash->ec_data_chksum_parts>>4)!=4 && (trash->ec_data_chksum_parts>>4)!=8) { // not supported?
				return MFS_ERROR_EINVAL;
			}
		} else { // not ? - use defaults
			trash->ec_data_chksum_parts |= (DefaultECMODE)<<4;
		}
		if ((trash->ec_data_chksum_parts>>4)==4) {
			if (sclass_check_ec(2)==0) {
				return MFS_ERROR_INCOMPATVERSION;
			}
		}
		if (trash->labelscnt>2 || (trash->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			return MFS_ERROR_EINVAL;
		}
	} else {
		if (trash->labelscnt>MAXLABELSCNT) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (arch_mode==0 || (arch_mode&0xC0)!=0) {
		return MFS_ERROR_EINVAL;
	}
	sclasstab[fsclassid].nleng = nleng;
	memcpy(sclasstab[fsclassid].name,name,nleng);
	sclasstab[fsclassid].dleng = dleng;
	if (dleng>0) {
		memcpy(sclasstab[fsclassid].desc,desc,dleng);
	}
	sclasstab[fsclassid].priority = priority;
	sclasstab[fsclassid].export_group = export_group;
	sclasstab[fsclassid].admin_only = admin_only;
	sclasstab[fsclassid].labels_mode = labels_mode;
	sclasstab[fsclassid].arch_mode = arch_mode;
	sclasstab[fsclassid].create = *create;
	sclasstab[fsclassid].keep = *keep;
	sclasstab[fsclassid].arch = *arch;
	sclasstab[fsclassid].trash = *trash;
	sclasstab[fsclassid].arch_delay = arch_delay;
	sclasstab[fsclassid].min_trashretention = min_trashretention;
	sclasstab[fsclassid].arch_min_size = arch_min_size;
	sclass_fix_has_labels_fields(fsclassid);
	sclass_make_changelog(fsclassid,1);
	return MFS_STATUS_OK;
}

uint8_t sclass_change_entry(uint8_t nleng,const uint8_t *name,uint16_t chgmask,uint8_t *dleng,uint8_t *desc,uint32_t *priority,uint8_t *export_group,uint8_t *admin_only,uint8_t *labels_mode,uint8_t *arch_mode,uint16_t *arch_delay,uint64_t *arch_min_size,uint16_t *min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash) {
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
	if (chgmask & SCLASS_CHG_CREATE_MASKS && (create->ec_data_chksum_parts>0 || create->labelscnt > MAXLABELSCNT)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_KEEP_MASKS && (keep->ec_data_chksum_parts>0 || keep->labelscnt==0 || keep->labelscnt > MAXLABELSCNT)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_ARCH_MASKS) {
		if (arch->ec_data_chksum_parts>0) {
			if (sclass_check_ec(1)==0) {
				return MFS_ERROR_INCOMPATVERSION;
			}
			if (arch->ec_data_chksum_parts>>4) { // data parts defined
				if ((arch->ec_data_chksum_parts>>4)!=4 && (arch->ec_data_chksum_parts>>4)!=8) { // not supported?
					return MFS_ERROR_EINVAL;
				}
			} else { // not ? - use defaults
				arch->ec_data_chksum_parts |= (DefaultECMODE)<<4;
			}
			if ((arch->ec_data_chksum_parts>>4)==4) {
				if (sclass_check_ec(2)==0) {
					return MFS_ERROR_INCOMPATVERSION;
				}
			}
			if (arch->labelscnt>2 || (arch->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
				return MFS_ERROR_EINVAL;
			}
		} else {
			if (arch->labelscnt>MAXLABELSCNT) {
				return MFS_ERROR_EINVAL;
			}
		}
	}
	if (chgmask & SCLASS_CHG_TRASH_MASKS) {
		if (trash->ec_data_chksum_parts>0) {
			if (sclass_check_ec(1)==0) {
				return MFS_ERROR_INCOMPATVERSION;
			}
			if (trash->ec_data_chksum_parts>>4) { // data parts defined
				if ((trash->ec_data_chksum_parts>>4)!=4 && (trash->ec_data_chksum_parts>>4)!=8) { // not supported?
					return MFS_ERROR_EINVAL;
				}
			} else { // not ? - use defaults
				trash->ec_data_chksum_parts |= (DefaultECMODE)<<4;
			}
			if ((trash->ec_data_chksum_parts>>4)==4) {
				if (sclass_check_ec(2)==0) {
					return MFS_ERROR_INCOMPATVERSION;
				}
			}
			if (trash->labelscnt>2 || (trash->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
				return MFS_ERROR_EINVAL;
			}
		} else {
			if (trash->labelscnt>MAXLABELSCNT) {
				return MFS_ERROR_EINVAL;
			}
		}
	}
	if (chgmask & SCLASS_CHG_ARCH_MODE && ((*arch_mode)==0 || ((*arch_mode)&0xC0)!=0)) {
		return MFS_ERROR_EINVAL;
	}
	if (chgmask & SCLASS_CHG_DESCRIPTION) {
		if ((*dleng)>0 && sclass_name_check(*dleng,desc)==0) {
			return MFS_ERROR_EINVAL;
		}
		sclasstab[fsclassid].dleng = *dleng;
		if (*dleng>0) {
			memcpy(sclasstab[fsclassid].desc,desc,*dleng);
		}
	} else {
		*dleng = sclasstab[fsclassid].dleng;
		if (*dleng>0) {
			memcpy(desc,sclasstab[fsclassid].desc,*dleng);
		}
	}
	if (chgmask & SCLASS_CHG_PRIORITY) {
		sclasstab[fsclassid].priority = *priority;
	} else {
		*priority = sclasstab[fsclassid].priority;
	}
	if (chgmask & SCLASS_CHG_EXPORT_GROUP) {
		sclasstab[fsclassid].export_group = *export_group;
	} else {
		*export_group = sclasstab[fsclassid].export_group;
	}
	if (chgmask & SCLASS_CHG_ADMIN_ONLY) {
		sclasstab[fsclassid].admin_only = *admin_only;
	} else {
		*admin_only = sclasstab[fsclassid].admin_only;
	}
	if (chgmask & SCLASS_CHG_LABELS_MODE) {
		sclasstab[fsclassid].labels_mode = *labels_mode;
	} else {
		*labels_mode = sclasstab[fsclassid].labels_mode;
	}
	if (chgmask & SCLASS_CHG_ARCH_MODE) {
		sclasstab[fsclassid].arch_mode = *arch_mode;
	} else {
		*arch_mode = sclasstab[fsclassid].arch_mode;
	}
	if (chgmask & SCLASS_CHG_CREATE_MASKS) {
		sclasstab[fsclassid].create = *create;
	} else {
		*create = sclasstab[fsclassid].create;
	}
	if (chgmask & SCLASS_CHG_KEEP_MASKS) {
		sclasstab[fsclassid].keep = *keep;
	} else {
		*keep = sclasstab[fsclassid].keep;
	}
	if (chgmask & SCLASS_CHG_ARCH_MASKS) {
		sclasstab[fsclassid].arch = *arch;
	} else {
		*arch = sclasstab[fsclassid].arch;
		if ((arch->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			arch->ec_data_chksum_parts = (arch->ec_data_chksum_parts&0xF0) | MaxECRedundancyLevel;
		}
	}
	if (chgmask & SCLASS_CHG_TRASH_MASKS) {
		sclasstab[fsclassid].trash = *trash;
	} else {
		*trash = sclasstab[fsclassid].trash;
		if ((trash->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			trash->ec_data_chksum_parts = (trash->ec_data_chksum_parts&0xF0) | MaxECRedundancyLevel;
		}
	}
	if (chgmask & SCLASS_CHG_ARCH_DELAY) {
		sclasstab[fsclassid].arch_delay = *arch_delay;
	} else {
		*arch_delay = sclasstab[fsclassid].arch_delay;
	}
	if (chgmask & SCLASS_CHG_MIN_TRASHRETENTION) {
		sclasstab[fsclassid].min_trashretention = *min_trashretention;
	} else {
		*min_trashretention = sclasstab[fsclassid].min_trashretention;
	}
	if (chgmask & SCLASS_CHG_ARCH_MIN_SIZE) {
		sclasstab[fsclassid].arch_min_size = *arch_min_size;
	} else {
		*arch_min_size = sclasstab[fsclassid].arch_min_size;
	}
	if (chgmask & (SCLASS_CHG_CREATE_MASKS|SCLASS_CHG_KEEP_MASKS|SCLASS_CHG_ARCH_MASKS|SCLASS_CHG_TRASH_MASKS)) {
		sclass_fix_has_labels_fields(fsclassid);
	}
	if (chgmask!=0) {
		sclass_make_changelog(fsclassid,0);
	}
	return MFS_STATUS_OK;
}

uint8_t sclass_mr_set_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid,uint8_t new_flag,uint8_t dleng,const uint8_t *desc,uint32_t priority,uint8_t export_group,uint8_t admin_only,uint8_t labels_mode,uint8_t arch_mode,uint16_t arch_delay,uint64_t arch_min_size,uint16_t min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash) {
	uint32_t sclassid,fsclassid;
	if (sclass_name_check(nleng,name)==0) {
		return MFS_ERROR_EINVAL;
	}
	if (dleng>0 && sclass_name_check(dleng,desc)==0) {
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
	if (create->ec_data_chksum_parts>0 || keep->ec_data_chksum_parts>0 || create->labelscnt>MAXLABELSCNT || keep->labelscnt==0 || keep->labelscnt>MAXLABELSCNT) {
		return MFS_ERROR_EINVAL;
	}
	if (arch->ec_data_chksum_parts>0) {
		if (ec_current_version<1) {
			return MFS_ERROR_MISMATCH;
		}
		if (arch->ec_data_chksum_parts>>4) { // data parts defined
			if ((arch->ec_data_chksum_parts>>4)!=4 && (arch->ec_data_chksum_parts>>4)!=8) { // not supported?
				return MFS_ERROR_EINVAL;
			}
		} else { // not ? - use compatibility mode
			arch->ec_data_chksum_parts |= (COMPAT_ECMODE)<<4;
		}
		if ((arch->ec_data_chksum_parts>>4)==4 && ec_current_version<2) {
			return MFS_ERROR_MISMATCH;
		}
		if (arch->labelscnt>2 || (arch->ec_data_chksum_parts&0xF)>MAX_EC_LEVEL) {
			return MFS_ERROR_EINVAL;
		}
	} else {
		if (arch->labelscnt>MAXLABELSCNT) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (trash->ec_data_chksum_parts>0) {
		if (ec_current_version<1) {
			return MFS_ERROR_MISMATCH;
		}
		if (trash->ec_data_chksum_parts>>4) { // data parts defined
			if ((trash->ec_data_chksum_parts>>4)!=4 && (trash->ec_data_chksum_parts>>4)!=8) { // not supported?
				return MFS_ERROR_EINVAL;
			}
		} else { // not ? - use compatibility mode
			trash->ec_data_chksum_parts |= (COMPAT_ECMODE)<<4;
		}
		if ((trash->ec_data_chksum_parts>>4)==4 && ec_current_version<2) {
			return MFS_ERROR_MISMATCH;
		}
		if (trash->labelscnt>2 || (trash->ec_data_chksum_parts&0xF)>MAX_EC_LEVEL) {
			return MFS_ERROR_EINVAL;
		}
	} else {
		if (trash->labelscnt>MAXLABELSCNT) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (arch_mode==0 || (arch_mode&0xC0)!=0) {
		return MFS_ERROR_EINVAL;
	}
	if (new_flag) {
		sclasstab[fsclassid].nleng = nleng;
		memcpy(sclasstab[fsclassid].name,name,nleng);
	}
	sclasstab[fsclassid].dleng = dleng;
	if (dleng>0) {
		memcpy(sclasstab[fsclassid].desc,desc,dleng);
	}
	sclasstab[fsclassid].priority = priority;
	sclasstab[fsclassid].export_group = export_group;
	sclasstab[fsclassid].admin_only = admin_only;
	sclasstab[fsclassid].labels_mode = labels_mode;
	sclasstab[fsclassid].arch_mode = arch_mode;
	sclasstab[fsclassid].create = *create;
	sclasstab[fsclassid].keep = *keep;
	sclasstab[fsclassid].arch = *arch;
	sclasstab[fsclassid].trash = *trash;
	sclasstab[fsclassid].arch_delay = arch_delay;
	sclasstab[fsclassid].min_trashretention = min_trashretention;
	sclasstab[fsclassid].arch_min_size = arch_min_size;
	sclass_fix_has_labels_fields(fsclassid);
	meta_version_inc();
	return MFS_STATUS_OK;
}

static inline uint8_t sclass_univ_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t essclassid,uint16_t edsclassid) {
	uint32_t sclassid,fssclassid,fdsclassid;
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
	sclasstab[fdsclassid].dleng = sclasstab[fssclassid].dleng;
	if (sclasstab[fssclassid].dleng>0) {
		memcpy(sclasstab[fdsclassid].desc,sclasstab[fssclassid].desc,sclasstab[fssclassid].dleng);
	}
	sclasstab[fdsclassid].priority = sclasstab[fssclassid].priority;
	sclasstab[fdsclassid].export_group = sclasstab[fssclassid].export_group;
	sclasstab[fdsclassid].admin_only = sclasstab[fssclassid].admin_only;
	sclasstab[fdsclassid].labels_mode = sclasstab[fssclassid].labels_mode;
	sclasstab[fdsclassid].arch_mode = sclasstab[fssclassid].arch_mode;
	sclasstab[fdsclassid].create = sclasstab[fssclassid].create;
	sclasstab[fdsclassid].keep = sclasstab[fssclassid].keep;
	sclasstab[fdsclassid].arch = sclasstab[fssclassid].arch;
	sclasstab[fdsclassid].trash = sclasstab[fssclassid].trash;
	sclasstab[fdsclassid].arch_delay = sclasstab[fssclassid].arch_delay;
	sclasstab[fdsclassid].min_trashretention = sclasstab[fssclassid].min_trashretention;
	sclasstab[fdsclassid].arch_min_size = sclasstab[fssclassid].arch_min_size;
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
	if (sclasstab[fsclassid].files>0 || sclasstab[fsclassid].directories>0) {
		return MFS_ERROR_CLASSINUSE;
	}
	if (esclassid!=0 && fsclassid!=esclassid) {
		return MFS_ERROR_MISMATCH;
	}
	patterns_sclass_delete(fsclassid);
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


uint32_t sclass_list_entries(uint8_t *buff,uint8_t fver) {
	uint32_t sclassid;
	uint32_t ret;
	uint32_t i,og;
	uint8_t err;

	ret = 0;
	for (sclassid=1 ; sclassid<firstneverused ; sclassid++) {
		if (sclasstab[sclassid].nleng>0) {
			if (buff==NULL) {
				if (fver>=4) {
					ret++;
				}
				ret += sclasstab[sclassid].nleng+1;
				if (fver==1) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt)*4*MASKORGROUP+7;
				} else if (fver==2) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt + sclasstab[sclassid].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE+28;
				} else if (fver==3) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt + sclasstab[sclassid].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE+29;
				} else if (fver==4) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt + sclasstab[sclassid].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE+37;
				} else if (fver==5) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt + sclasstab[sclassid].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE+41;
				} else if (fver==6) {
					ret += (sclasstab[sclassid].create.labelscnt + sclasstab[sclassid].keep.labelscnt + sclasstab[sclassid].arch.labelscnt + sclasstab[sclassid].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE+47;
					ret += sclasstab[sclassid].dleng;
				}
			} else {
				if (fver>=4) {
					put8bit(&buff,sclassid);
				}
				put8bit(&buff,sclasstab[sclassid].nleng);
				memcpy(buff,sclasstab[sclassid].name,sclasstab[sclassid].nleng);
				buff+=sclasstab[sclassid].nleng;
				if (fver==1) {
					uint32_t create_labelmasks[MAXLABELSCNT*MASKORGROUP];
					uint32_t keep_labelmasks[MAXLABELSCNT*MASKORGROUP];
					uint32_t arch_labelmasks[MAXLABELSCNT*MASKORGROUP];
					memset(create_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					memset(keep_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					memset(arch_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					err = 0;
					if (sclasstab[sclassid].arch_delay%24) {
						err = 1;
					}
					if (err==0 && sclass_labelexpr_to_maskorgroup(create_labelmasks,sclasstab[sclassid].create.labelexpr,sclasstab[sclassid].create.labelscnt)==0) {
						err = 1;
						memset(create_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					}
					if (err==0 && sclass_labelexpr_to_maskorgroup(keep_labelmasks,sclasstab[sclassid].keep.labelexpr,sclasstab[sclassid].keep.labelscnt)==0) {
						err = 1;
						memset(keep_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					}
					if (err==0 && sclass_labelexpr_to_maskorgroup(arch_labelmasks,sclasstab[sclassid].arch.labelexpr,sclasstab[sclassid].arch.labelscnt)==0) {
						err = 1;
						memset(arch_labelmasks,0,sizeof(uint32_t)*MAXLABELSCNT*MASKORGROUP);
					}
					if (sclasstab[sclassid].trash.labelscnt>0 || sclasstab[sclassid].trash.ec_data_chksum_parts || sclasstab[sclassid].arch.ec_data_chksum_parts) {
						err = 1;
					}
					if ((sclasstab[sclassid].create.uniqmask | sclasstab[sclassid].keep.uniqmask | sclasstab[sclassid].arch.uniqmask | sclasstab[sclassid].trash.uniqmask) > 0) {
						err = 1;
					}
					if (err) {
						memset(buff-sclasstab[sclassid].nleng,'*',sclasstab[sclassid].nleng);
					}
					put8bit(&buff,sclasstab[sclassid].admin_only);
					put8bit(&buff,sclasstab[sclassid].labels_mode);
					put16bit(&buff,sclasstab[sclassid].arch_delay/24);
					put8bit(&buff,sclasstab[sclassid].create.labelscnt);
					put8bit(&buff,sclasstab[sclassid].keep.labelscnt);
					put8bit(&buff,sclasstab[sclassid].arch.labelscnt);
					for (i=0 ; i<sclasstab[sclassid].create.labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,create_labelmasks[i*MASKORGROUP+og]);
						}
					}
					for (i=0 ; i<sclasstab[sclassid].keep.labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,keep_labelmasks[i*MASKORGROUP+og]);
						}
					}
					for (i=0 ; i<sclasstab[sclassid].arch.labelscnt ; i++) {
						for (og=0 ; og<MASKORGROUP ; og++) {
							put32bit(&buff,arch_labelmasks[i*MASKORGROUP+og]);
						}
					}
				} else if (fver==2 || fver==3 || fver==4 || fver==5 || fver==6) {
					if (fver>=6) {
						put8bit(&buff,sclasstab[sclassid].dleng);
						if (sclasstab[sclassid].dleng>0) {
							memcpy(buff,sclasstab[sclassid].desc,sclasstab[sclassid].dleng);
							buff+=sclasstab[sclassid].dleng;
						}
						put32bit(&buff,sclasstab[sclassid].priority);
						put8bit(&buff,sclasstab[sclassid].export_group);
					}
					put8bit(&buff,sclasstab[sclassid].admin_only);
					put8bit(&buff,sclasstab[sclassid].labels_mode);
					if (fver>=3) {
						put8bit(&buff,sclasstab[sclassid].arch_mode);
					}
					put16bit(&buff,sclasstab[sclassid].arch_delay);
					if (fver>=4) {
						put64bit(&buff,sclasstab[sclassid].arch_min_size);
					}
					put16bit(&buff,sclasstab[sclassid].min_trashretention);
					if ((sclasstab[sclassid].arch.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						put8bit(&buff,(sclasstab[sclassid].arch.ec_data_chksum_parts&0xF0) | MaxECRedundancyLevel);
					} else {
						put8bit(&buff,sclasstab[sclassid].arch.ec_data_chksum_parts);
					}
					if ((sclasstab[sclassid].trash.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						put8bit(&buff,(sclasstab[sclassid].trash.ec_data_chksum_parts&0xF0) | MaxECRedundancyLevel);
					} else {
						put8bit(&buff,sclasstab[sclassid].trash.ec_data_chksum_parts);
					}
					if (fver>=5) {
						put8bit(&buff,sclasstab[sclassid].create.labels_mode);
						put8bit(&buff,sclasstab[sclassid].keep.labels_mode);
						put8bit(&buff,sclasstab[sclassid].arch.labels_mode);
						put8bit(&buff,sclasstab[sclassid].trash.labels_mode);
					}
					put32bit(&buff,sclasstab[sclassid].create.uniqmask);
					put32bit(&buff,sclasstab[sclassid].keep.uniqmask);
					put32bit(&buff,sclasstab[sclassid].arch.uniqmask);
					put32bit(&buff,sclasstab[sclassid].trash.uniqmask);
					put8bit(&buff,sclasstab[sclassid].create.labelscnt);
					put8bit(&buff,sclasstab[sclassid].keep.labelscnt);
					put8bit(&buff,sclasstab[sclassid].arch.labelscnt);
					put8bit(&buff,sclasstab[sclassid].trash.labelscnt);
					for (i=0 ; i<sclasstab[sclassid].create.labelscnt ; i++) {
						memcpy(buff,sclasstab[sclassid].create.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
						buff+=SCLASS_EXPR_MAX_SIZE;
					}
					for (i=0 ; i<sclasstab[sclassid].keep.labelscnt ; i++) {
						memcpy(buff,sclasstab[sclassid].keep.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
						buff+=SCLASS_EXPR_MAX_SIZE;
					}
					for (i=0 ; i<sclasstab[sclassid].arch.labelscnt ; i++) {
						memcpy(buff,sclasstab[sclassid].arch.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
						buff+=SCLASS_EXPR_MAX_SIZE;
					}
					for (i=0 ; i<sclasstab[sclassid].trash.labelscnt ; i++) {
						memcpy(buff,sclasstab[sclassid].trash.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
						buff+=SCLASS_EXPR_MAX_SIZE;
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

uint8_t sclass_get_labels_mode(uint16_t sclassid,storagemode *sm) {
	if (sm->labels_mode==LABELS_MODE_LOOSE || sm->labels_mode==LABELS_MODE_STD || sm->labels_mode==LABELS_MODE_STRICT) {
		return sm->labels_mode;
	}
	return sclasstab[sclassid].labels_mode;
}

uint8_t sclass_get_arch_mode(uint16_t sclassid) {
	return sclasstab[sclassid].arch_mode;
}

// do not use trash mode
static inline uint8_t sclass_get_keeparch_max_goal_equivalent(uint16_t sclassid) {
	uint8_t ec_data_chksum_parts;
	ec_data_chksum_parts = sclasstab[sclassid].arch.ec_data_chksum_parts & 0xF;
	if (ec_data_chksum_parts>MaxECRedundancyLevel) {
		ec_data_chksum_parts = MaxECRedundancyLevel;
	}
	if (ec_data_chksum_parts) {
		if (ec_data_chksum_parts>=sclasstab[sclassid].keep.labelscnt) {
			return ec_data_chksum_parts+1;
		} else {
			return sclasstab[sclassid].keep.labelscnt;
		}
	} else {
		if (sclasstab[sclassid].arch.labelscnt>sclasstab[sclassid].keep.labelscnt) {
			return sclasstab[sclassid].arch.labelscnt;
		} else {
			return sclasstab[sclassid].keep.labelscnt;
		}
	}
}

/* do not count trash here - used only to test quota in filesystem */
uint8_t sclass_get_keeparch_maxstorage_eights(uint16_t sclassid) {
	uint8_t res;
	uint8_t ec_data_parts,ec_chksum_parts;
	if (sclasstab[sclassid].arch.ec_data_chksum_parts) {
		ec_data_parts = sclasstab[sclassid].arch.ec_data_chksum_parts >> 4;
		ec_chksum_parts = sclasstab[sclassid].arch.ec_data_chksum_parts & 0xF;

		if (ec_chksum_parts>MaxECRedundancyLevel) {
			res = MaxECRedundancyLevel + ec_data_parts;
		} else {
			res = ec_data_parts + ec_chksum_parts;
		}
		res *= 8;
		res /= ec_data_parts;
	} else {
		res = sclasstab[sclassid].arch.labelscnt * 8;
	}
	if (res < sclasstab[sclassid].keep.labelscnt * 8) {
		res = sclasstab[sclassid].keep.labelscnt * 8;
	}
	return res;
}

/* do not count trash here - used to calculate realsize in file tree */
uint8_t sclass_get_keeparch_storage_eights(uint16_t sclassid,uint8_t keepflag) {
	uint8_t ec_data_parts,ec_chksum_parts;
	if (keepflag==0 && (sclasstab[sclassid].arch.labelscnt>0 || sclasstab[sclassid].arch.ec_data_chksum_parts)) {
		if (sclasstab[sclassid].arch.ec_data_chksum_parts) {
			ec_data_parts = sclasstab[sclassid].arch.ec_data_chksum_parts >> 4;
			ec_chksum_parts = sclasstab[sclassid].arch.ec_data_chksum_parts & 0xF;

			if (ec_chksum_parts>MaxECRedundancyLevel) {
				return ((MaxECRedundancyLevel + ec_data_parts) * 8) / ec_data_parts;
			} else {
				return ((ec_chksum_parts + ec_data_parts) * 8) / ec_data_parts;
			}
		} else {
			return sclasstab[sclassid].arch.labelscnt * 8;
		}
	}
	return sclasstab[sclassid].keep.labelscnt * 8;
}

storagemode* sclass_get_create_storagemode(uint16_t sclassid) {
	if (sclasstab[sclassid].create.labelscnt>0) {
		return &(sclasstab[sclassid].create);
	} else {
		return &(sclasstab[sclassid].keep);
	}
}

storagemode* sclass_get_keeparch_storagemode(uint16_t sclassid,uint8_t flags) {
	if ((flags & 2)==2 && (sclasstab[sclassid].trash.labelscnt>0 || sclasstab[sclassid].trash.ec_data_chksum_parts)) {
		if ((sclasstab[sclassid].trash.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			tmp_storagemode = sclasstab[sclassid].trash;
			tmp_storagemode.ec_data_chksum_parts = (tmp_storagemode.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel;
			return &tmp_storagemode;
		} else {
			return &(sclasstab[sclassid].trash);
		}
	} else if ((flags & 1)==1 && (sclasstab[sclassid].arch.labelscnt>0 || sclasstab[sclassid].arch.ec_data_chksum_parts)) {
		if ((sclasstab[sclassid].arch.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			tmp_storagemode = sclasstab[sclassid].arch;
			tmp_storagemode.ec_data_chksum_parts = (tmp_storagemode.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel;
			return &tmp_storagemode;
		} else {
			return &(sclasstab[sclassid].arch);
		}
	}
	return &(sclasstab[sclassid].keep);
}

uint64_t sclass_get_joining_priority(uint16_t sclassid) {
	uint64_t ret;
	ret = sclasstab[sclassid].priority; // PRIORITY
	ret <<= 4;
	ret += sclass_get_keeparch_max_goal_equivalent(sclassid); // GOAL
	ret <<= 1;
	if ((sclasstab[sclassid].arch.ec_data_chksum_parts & 0xF)) { // EC in arch
		ret += 1;
	}
	ret <<= 1;
	if (sclasstab[sclassid].keep.has_labels | sclasstab[sclassid].arch.has_labels) { // LABELS in keep+arch
		ret += 1;
	}
	ret <<= 1;
	if ((sclasstab[sclassid].trash.ec_data_chksum_parts & 0xF)) { // EC in trash
		ret += 1;
	}
	ret <<= 1;
	if (sclasstab[sclassid].trash.has_labels) { // LABELS in trash
		ret += 1;
	}
	ret <<= 8;
	ret += sclassid;
	return ret;
}

uint8_t sclass_get_export_group(uint16_t sclassid) {
	return (sclasstab[sclassid].export_group);
}

uint8_t sclass_is_admin_only(uint16_t sclassid) {
	return (sclasstab[sclassid].admin_only);
}

uint16_t sclass_get_min_trashretention(uint16_t sclassid) {
	return sclasstab[sclassid].min_trashretention;
}

uint16_t sclass_get_arch_delay(uint16_t sclassid) {
	return sclasstab[sclassid].arch_delay;
}

uint64_t sclass_get_arch_min_size(uint16_t sclassid) {
	return sclasstab[sclassid].arch_min_size;
}

/*
void sclass_state_change(uint16_t oldsclassid,uint8_t oldflags,uint8_t oldrlevel,uint8_t oldrvc,uint16_t newsclassid,uint8_t newflags,uint8_t newrlevel,uint8_t newrvc) {
	uint8_t class;

	if (oldsclassid>0) {
		if (oldrvc > oldrlevel) {
			class = 2;
		} else if (oldrvc == oldrlevel) {
			class = 1;
		} else {
			class = 0;
		}
		if (oldflags & 1) {
			sclasstab[oldsclassid].archchunks[class]--;
		} else if (oldflags & 2) {
			sclasstab[oldsclassid].trashchunks[class]--;
		} else {
			sclasstab[oldsclassid].stdchunks[class]--;
		}
	}
	if (newsclassid>0) {
		if (newrvc > newrlevel) {
			class = 2;
		} else if (newrvc == newrlevel) {
			class = 1;
		} else {
			class = 0;
		}
		if (newflags & 1) {
			sclasstab[newsclassid].archchunks[class]++;
		} else if (newflags & 2) {
			sclasstab[newsclassid].trashchunks[class]++;
		} else {
			sclasstab[newsclassid].stdchunks[class]++;
		}
	}
}
*/

uint8_t sclass_calc_goal_equivalent(storagemode *sm) {
	if (sm->ec_data_chksum_parts) {
		if ((sm->ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
			return MaxECRedundancyLevel+1;
		} else {
			return (sm->ec_data_chksum_parts&0xF)+1;
		}
	} else {
		return sm->labelscnt;
	}
}

uint32_t sclass_info(uint8_t *buff,uint8_t fver) {
	uint32_t leng,i,j;
	// data order: UNDER_COPY,UNDER_EC,EXACT_COPY,EXACT_EC,OVER_COPY,OVER_EC
	uint64_t keepcnt[6];
	uint64_t archcnt[6];
	uint64_t trashcnt[6];
	uint8_t krlevel,arlevel,trlevel;

	if (buff==NULL) {
		if (fver==128) {
			leng = 2 + 9 + 1; // 9 = strlen("(deleted)")
		} else {
			leng = 2;
		}
		for (i=1 ; i<firstneverused ; i++) {
			if (sclasstab[i].nleng>0) {
				if (fver==128) {
					leng += 2 + sclasstab[i].nleng + 1;
				} else {
					if (fver>=1) {
						leng++;
					}
					if (fver>=2) {
						leng += 3 * 24;
					}
					if (fver>=3) {
						leng += 8;
					}
					if (fver>=4) {
						leng += 4;
					}
					if (fver>=5) {
						leng += 6;
					}
					leng += 42 + 3 * 24;
					leng += sclasstab[i].nleng;
					leng += sclasstab[i].dleng;
					leng += ((uint32_t)sclasstab[i].create.labelscnt) * ( SCLASS_EXPR_MAX_SIZE + 2 );
					leng += ((uint32_t)sclasstab[i].keep.labelscnt) * ( SCLASS_EXPR_MAX_SIZE + 2 );
					leng += ((uint32_t)sclasstab[i].arch.labelscnt) * ( SCLASS_EXPR_MAX_SIZE + 2 );
					leng += ((uint32_t)sclasstab[i].trash.labelscnt) * ( SCLASS_EXPR_MAX_SIZE + 2 );
				}
			}
		}
		return leng;
	} else {
		if (fver==128) {
			put8bit(&buff,0);
			put8bit(&buff,9); // strlen("(deleted)")
			memcpy(buff,"(deleted)",9);
			buff+=9;
			put8bit(&buff,chunk_sclass_has_chunks(0));
			for (i=1 ; i<firstneverused ; i++) {
				if (sclasstab[i].nleng>0) {
					put8bit(&buff,i);
					put8bit(&buff,sclasstab[i].nleng);
					memcpy(buff,sclasstab[i].name,sclasstab[i].nleng);
					buff+=sclasstab[i].nleng;
					put8bit(&buff,chunk_sclass_has_chunks(i));
				}
			}
		} else {
			chunk_labelset_can_be_fulfilled(NULL); // init server list
			put16bit(&buff,matocsserv_servers_count());
			for (i=1 ; i<firstneverused ; i++) {
				if (sclasstab[i].nleng>0) {
					put8bit(&buff,i);
					put8bit(&buff,sclasstab[i].nleng);
					memcpy(buff,sclasstab[i].name,sclasstab[i].nleng);
					buff+=sclasstab[i].nleng;
					if (fver>=5) {
						put8bit(&buff,sclasstab[i].dleng);
						if (sclasstab[i].dleng>0) {
							memcpy(buff,sclasstab[i].desc,sclasstab[i].dleng);
							buff+=sclasstab[i].dleng;
						}
					}
					put32bit(&buff,sclasstab[i].files);
					put32bit(&buff,sclasstab[i].directories);
					krlevel = sclass_calc_goal_equivalent(&(sclasstab[i].keep));
					arlevel = sclass_calc_goal_equivalent(&(sclasstab[i].arch));
					trlevel = sclass_calc_goal_equivalent(&(sclasstab[i].trash));
	//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u (%s) ; krlevel: %u ; arlevel: %u ; trlevel: %u",i,sclasstab[i].name,krlevel,arlevel,trlevel);
					for (j=0 ; j<6 ; j++) {
						keepcnt[j]=0;
						archcnt[j]=0;
						trashcnt[j]=0;
					}
					if (arlevel==0 && trlevel==0) {
						chunk_sclass_inc_counters(i,0,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,1,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,2,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,3,krlevel,keepcnt);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; kunder: %llu ; kexact: %llu ; kover: %llu",i,kunder,kexact,kover);
					} else if (arlevel==0) {
						chunk_sclass_inc_counters(i,0,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,1,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,2,trlevel,trashcnt);
						chunk_sclass_inc_counters(i,3,trlevel,trashcnt);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; kunder: %llu ; kexact: %llu ; kover: %llu",i,kunder,kexact,kover);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; tunder: %llu ; texact: %llu ; tover: %llu",i,tunder,texact,tover);
					} else if (trlevel==0) {
						chunk_sclass_inc_counters(i,0,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,1,arlevel,archcnt);
						chunk_sclass_inc_counters(i,2,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,3,arlevel,archcnt);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; kunder: %llu ; kexact: %llu ; kover: %llu",i,kunder,kexact,kover);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; aunder: %llu ; aexact: %llu ; aover: %llu",i,aunder,aexact,aover);
					} else {
						chunk_sclass_inc_counters(i,0,krlevel,keepcnt);
						chunk_sclass_inc_counters(i,1,arlevel,archcnt);
						chunk_sclass_inc_counters(i,2,trlevel,trashcnt);
						chunk_sclass_inc_counters(i,3,trlevel,trashcnt);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; kunder: %llu ; kexact: %llu ; kover: %llu",i,kunder,kexact,kover);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; aunder: %llu ; aexact: %llu ; aover: %llu",i,aunder,aexact,aover);
	//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"class %u ; tunder: %llu ; texact: %llu ; tover: %llu",i,tunder,texact,tover);
					}
					if (fver>=2) {
						for (j=0 ; j<6 ; j++) {
							put64bit(&buff,keepcnt[j]);
							put64bit(&buff,archcnt[j]);
							put64bit(&buff,trashcnt[j]);
						}
					} else {
						for (j=0 ; j<3 ; j++) {
							put64bit(&buff,keepcnt[2*j]+keepcnt[2*j+1]);
							put64bit(&buff,archcnt[2*j]+archcnt[2*j+1]);
							put64bit(&buff,trashcnt[2*j]+trashcnt[2*j+1]);
						}
					}
					if (fver>=5) {
						put32bit(&buff,sclasstab[i].priority);
						put8bit(&buff,sclasstab[i].export_group);
					}
					put8bit(&buff,sclasstab[i].admin_only);
					put8bit(&buff,sclasstab[i].labels_mode);
					if (fver>=1) {
						put8bit(&buff,sclasstab[i].arch_mode);
					}
					put16bit(&buff,sclasstab[i].arch_delay);
					if (fver>=3) {
						put64bit(&buff,sclasstab[i].arch_min_size);
					}
					put16bit(&buff,sclasstab[i].min_trashretention);
					put8bit(&buff,chunk_labelset_can_be_fulfilled(&(sclasstab[i].create)));
					put8bit(&buff,sclasstab[i].create.labelscnt);
					put32bit(&buff,sclasstab[i].create.uniqmask);
					if (fver>=4) {
						put8bit(&buff,sclasstab[i].create.labels_mode);
					}
					put8bit(&buff,chunk_labelset_can_be_fulfilled(&(sclasstab[i].keep)));
					put8bit(&buff,sclasstab[i].keep.labelscnt);
					put32bit(&buff,sclasstab[i].keep.uniqmask);
					if (fver>=4) {
						put8bit(&buff,sclasstab[i].keep.labels_mode);
					}
					if ((sclasstab[i].arch.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						tmp_storagemode = sclasstab[i].arch;
						tmp_storagemode.ec_data_chksum_parts = (tmp_storagemode.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel;
						put8bit(&buff,chunk_labelset_can_be_fulfilled(&tmp_storagemode));
					} else {
						put8bit(&buff,chunk_labelset_can_be_fulfilled(&(sclasstab[i].arch)));
					}
					put8bit(&buff,sclasstab[i].arch.labelscnt);
					if ((sclasstab[i].arch.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						put8bit(&buff,(sclasstab[i].arch.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel);
					} else {
						put8bit(&buff,sclasstab[i].arch.ec_data_chksum_parts);
					}
					put32bit(&buff,sclasstab[i].arch.uniqmask);
					if (fver>=4) {
						put8bit(&buff,sclasstab[i].arch.labels_mode);
					}
					if ((sclasstab[i].trash.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						tmp_storagemode = sclasstab[i].trash;
						tmp_storagemode.ec_data_chksum_parts = (tmp_storagemode.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel;
						put8bit(&buff,chunk_labelset_can_be_fulfilled(&tmp_storagemode));
					} else {
						put8bit(&buff,chunk_labelset_can_be_fulfilled(&(sclasstab[i].trash)));
					}
					put8bit(&buff,sclasstab[i].trash.labelscnt);
					if ((sclasstab[i].trash.ec_data_chksum_parts&0xF)>MaxECRedundancyLevel) {
						put8bit(&buff,(sclasstab[i].trash.ec_data_chksum_parts & 0xF0) | MaxECRedundancyLevel);
					} else {
						put8bit(&buff,sclasstab[i].trash.ec_data_chksum_parts);
					}
					put32bit(&buff,sclasstab[i].trash.uniqmask);
					if (fver>=4) {
						put8bit(&buff,sclasstab[i].trash.labels_mode);
					}
					for (j=0 ; j<sclasstab[i].create.labelscnt ; j++) {
						memcpy(buff,sclasstab[i].create.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
						buff += SCLASS_EXPR_MAX_SIZE;
						put16bit(&buff,matocsserv_servers_matches_labelexpr(sclasstab[i].create.labelexpr[j]));
					}
					for (j=0 ; j<sclasstab[i].keep.labelscnt ; j++) {
						memcpy(buff,sclasstab[i].keep.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
						buff += SCLASS_EXPR_MAX_SIZE;
						put16bit(&buff,matocsserv_servers_matches_labelexpr(sclasstab[i].keep.labelexpr[j]));
					}
					for (j=0 ; j<sclasstab[i].arch.labelscnt ; j++) {
						memcpy(buff,sclasstab[i].arch.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
						buff += SCLASS_EXPR_MAX_SIZE;
						put16bit(&buff,matocsserv_servers_matches_labelexpr(sclasstab[i].arch.labelexpr[j]));
					}
					for (j=0 ; j<sclasstab[i].trash.labelscnt ; j++) {
						memcpy(buff,sclasstab[i].trash.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
						buff += SCLASS_EXPR_MAX_SIZE;
						put16bit(&buff,matocsserv_servers_matches_labelexpr(sclasstab[i].trash.labelexpr[j]));
					}
				}
			}
		}
		return 0;
	}
}

uint8_t sclass_store(bio *fd) {
	uint8_t databuff[50+MAXSCLASSNAMELENG+MAXSCLASSDESCLENG+(4*MAXLABELSCNT*SCLASS_EXPR_MAX_SIZE)];
	uint8_t *ptr;
	uint16_t i,j;
	int32_t wsize;
	if (fd==NULL) {
		return 0x1C;
	}
	ptr = databuff;
	put16bit(&ptr,SCLASS_EXPR_MAX_SIZE);
	put8bit(&ptr,ec_current_version);
	if (bio_write(fd,databuff,3)!=3) {
		return 0xFF;
	}

	for (i=1 ; i<firstneverused ; i++) {
		if ((sclasstab[i].nleng)>0) {
			ptr = databuff;
			put16bit(&ptr,i);
			put8bit(&ptr,sclasstab[i].nleng);
			put8bit(&ptr,sclasstab[i].dleng);
			put32bit(&ptr,sclasstab[i].priority);
			put8bit(&ptr,sclasstab[i].export_group);
			put8bit(&ptr,sclasstab[i].admin_only);
			put8bit(&ptr,sclasstab[i].labels_mode);
			put8bit(&ptr,sclasstab[i].arch_mode);
			put16bit(&ptr,sclasstab[i].arch_delay);
			put64bit(&ptr,sclasstab[i].arch_min_size);
			put16bit(&ptr,sclasstab[i].min_trashretention);
			put8bit(&ptr,sclasstab[i].create.labelscnt);
			put32bit(&ptr,sclasstab[i].create.uniqmask);
			put8bit(&ptr,sclasstab[i].create.labels_mode);
			put8bit(&ptr,sclasstab[i].keep.labelscnt);
			put32bit(&ptr,sclasstab[i].keep.uniqmask);
			put8bit(&ptr,sclasstab[i].keep.labels_mode);
			put8bit(&ptr,sclasstab[i].arch.labelscnt);
			put8bit(&ptr,sclasstab[i].arch.ec_data_chksum_parts);
			put32bit(&ptr,sclasstab[i].arch.uniqmask);
			put8bit(&ptr,sclasstab[i].arch.labels_mode);
			put8bit(&ptr,sclasstab[i].trash.labelscnt);
			put8bit(&ptr,sclasstab[i].trash.ec_data_chksum_parts);
			put32bit(&ptr,sclasstab[i].trash.uniqmask);
			put8bit(&ptr,sclasstab[i].trash.labels_mode);
			memcpy(ptr,sclasstab[i].name,sclasstab[i].nleng);
			ptr+=sclasstab[i].nleng;
			if (sclasstab[i].dleng>0) {
				memcpy(ptr,sclasstab[i].desc,sclasstab[i].dleng);
				ptr+=sclasstab[i].dleng;
			}
			for (j=0 ; j<sclasstab[i].create.labelscnt ; j++) {
				memcpy(ptr,sclasstab[i].create.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (j=0 ; j<sclasstab[i].keep.labelscnt ; j++) {
				memcpy(ptr,sclasstab[i].keep.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (j=0 ; j<sclasstab[i].arch.labelscnt ; j++) {
				memcpy(ptr,sclasstab[i].arch.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (j=0 ; j<sclasstab[i].trash.labelscnt ; j++) {
				memcpy(ptr,sclasstab[i].trash.labelexpr[j],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			wsize = 50+sclasstab[i].nleng+sclasstab[i].dleng+(sclasstab[i].create.labelscnt+sclasstab[i].keep.labelscnt+sclasstab[i].arch.labelscnt+sclasstab[i].trash.labelscnt)*SCLASS_EXPR_MAX_SIZE;
			if (bio_write(fd,databuff,wsize)!=wsize) {
				return 0xFF;
			}
		}
	}
	ptr = databuff;
	put16bit(&ptr,0);
	if (bio_write(fd,databuff,2)!=2) {
		return 0xFF;
	}
	return 0;
}

int sclass_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t *databuff = NULL;
	uint8_t hdrbuff[3];
	const uint8_t *ptr;
	uint32_t chunkcount;
	uint16_t sclassid;
	uint16_t arch_delay;
	uint16_t min_trashretention;
	uint16_t exprsize;
	uint16_t lexprsize;
	uint8_t labels_mode;
	uint8_t arch_mode;
	uint64_t arch_min_size;
	storagemode create,keep,arch,trash;
	storagemode *smptr;
	uint8_t descrleng;
	uint8_t nleng;
	uint8_t dleng;
	uint32_t priority;
	uint8_t export_group;
	uint8_t admin_only;
	uint8_t name[MAXSCLASSNAMELENG];
	uint8_t desc[MAXSCLASSDESCLENG];
	uint16_t i,j;
	uint8_t orgroup;
	uint8_t hdrleng;
	uint32_t labelmasks[MAXLABELSCNT*MASKORGROUP];

	if (mver<0x16) { // skip label descriptions
		for (i=0 ; i<26 ; i++) {
			if (bio_read(fd,&descrleng,1)!=1) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
				return -1;
			}
			if (descrleng>128) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: description too long");
				return -1;
			}
			bio_skip(fd,descrleng);
		}
	}
	if (mver>=0x17) { // expression format
		uint8_t psize;
		psize = (mver>=0x1C)?48:(mver>=0x1B)?42:(mver>=0x1A)?38:(mver>=0x18)?30:29;
		if (bio_read(fd,hdrbuff,3)!=3) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class: read error");
			return -1;
		}
		ptr = hdrbuff;
		exprsize = get16bit(&ptr);
		if (exprsize>SCLASS_EXPR_MAX_SIZE) {
			if (ignoreflag) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: label expressions too long - ignore");
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: label expressions too long");
				return -1;
			}
		}
		if (exprsize==0) {
			if (ignoreflag) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: label expressions size is zero !!! - ignoring");
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: label expressions size is zero !!!");
				return -1;
			}
		}
		ec_current_version = get8bit(&ptr);
		databuff = malloc(psize);
		passert(databuff);
		while (1) {
			if (bio_read(fd,databuff,2)!=2) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
			ptr = databuff;
			sclassid = get16bit(&ptr);
			if (sclassid==0) {
				break;
			}
			if (bio_read(fd,databuff,psize)!=psize) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
			ptr = databuff;
			memset(&create,0,sizeof(storagemode));
			memset(&keep,0,sizeof(storagemode));
			memset(&arch,0,sizeof(storagemode));
			memset(&trash,0,sizeof(storagemode));
			create.labels_mode = LABELS_MODE_GLOBAL;
			keep.labels_mode = LABELS_MODE_GLOBAL;
			arch.labels_mode = LABELS_MODE_GLOBAL;
			trash.labels_mode = LABELS_MODE_GLOBAL;
			nleng = get8bit(&ptr);
			if (mver>=0x1C) {
				dleng = get8bit(&ptr);
				priority = get32bit(&ptr);
				export_group = get8bit(&ptr);
			} else {
				dleng = 0;
				priority = 0;
				if (sclassid<10) {
					export_group = sclassid;
				} else {
					export_group = 0;
				}
			}
			admin_only = get8bit(&ptr);
			labels_mode = get8bit(&ptr);
			if (mver>=0x18) {
				arch_mode = get8bit(&ptr);
				if ((arch_mode&0xC0)!=0) {
					arch_mode&=0x3F;
				}
				if (arch_mode==0) {
					arch_mode = SCLASS_ARCH_MODE_CTIME;
				}
			} else {
				arch_mode = SCLASS_ARCH_MODE_CTIME;
			}
			arch_delay = get16bit(&ptr);
			if (mver>=0x1A) {
				arch_min_size = get64bit(&ptr);
			} else {
				arch_min_size = 0;
			}
			min_trashretention = get16bit(&ptr);
			create.labelscnt = get8bit(&ptr);
			create.uniqmask = get32bit(&ptr);
			if (mver>=0x1B) {
				create.labels_mode = get8bit(&ptr);
			}
			keep.labelscnt = get8bit(&ptr);
			keep.uniqmask = get32bit(&ptr);
			if (mver>=0x1B) {
				keep.labels_mode = get8bit(&ptr);
			}
			arch.labelscnt = get8bit(&ptr);
			arch.ec_data_chksum_parts = get8bit(&ptr);
			arch.uniqmask = get32bit(&ptr);
			if (mver>=0x1B) {
				arch.labels_mode = get8bit(&ptr);
			}
			trash.labelscnt = get8bit(&ptr);
			trash.ec_data_chksum_parts = get8bit(&ptr);
			trash.uniqmask = get32bit(&ptr);
			if (mver>=0x1B) {
				trash.labels_mode = get8bit(&ptr);
			}
			if (mver<0x19) { // changed meaning of 'ec_data_chksum_parts' from goal equivalent to number of checksums
				if (arch.ec_data_chksum_parts>1) {
					arch.ec_data_chksum_parts--;
				}
				if (trash.ec_data_chksum_parts>1) {
					trash.ec_data_chksum_parts--;
				}
			}
			if (bio_read(fd,name,nleng)!=nleng) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
			if (nleng>0 && mver<0x1C) { // check and convert sclass name
				for (i=0 ; i<nleng ; i++) {
					if (name[i]<32) {
						name[i]=32;
					}
				}
				if (name[0]==32) {
					name[0]='_';
				}
				if (name[nleng-1]==32) {
					name[nleng-1]='_';
				}
			}
			if (dleng>0) {
				if (bio_read(fd,desc,dleng)!=dleng) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			if (keep.labelscnt==0 || keep.labelscnt>MAXLABELSCNT || create.labelscnt>MAXLABELSCNT || arch.labelscnt>MAXLABELSCNT || trash.labelscnt>MAXLABELSCNT || create.ec_data_chksum_parts>0 || keep.ec_data_chksum_parts>0 || (arch.ec_data_chksum_parts&0xF)>MAX_EC_LEVEL || (trash.ec_data_chksum_parts&0xF)>MAX_EC_LEVEL) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (labelscnt,ec_data_chksum_parts) for create: (%"PRIu8",0x%02"PRIX8") ; keep: (%"PRIu8",0x%02"PRIX8") ; arch: (%"PRIu8",0x%02"PRIX8") ; trash: (%"PRIu8",0x%02"PRIX8")",sclassid,create.labelscnt,create.ec_data_chksum_parts,keep.labelscnt,keep.ec_data_chksum_parts,arch.labelscnt,arch.ec_data_chksum_parts,trash.labelscnt,trash.ec_data_chksum_parts);
				free(databuff);
				databuff = NULL;
				return -1;
			}
			if (((arch.ec_data_chksum_parts&0xF)==0 && (arch.ec_data_chksum_parts&0xF0)) || ((trash.ec_data_chksum_parts&0xF)==0 && (trash.ec_data_chksum_parts&0xF0))) {
				if (ignoreflag==0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (ec with data parts and no checksums) - use '-i' to ignore",sclassid);
					free(databuff);
					databuff = NULL;
					return -1;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: sclassid: %"PRIu16" - data format error (ec with data parts and no checksums) - ignoring",sclassid);
				}
				if ((arch.ec_data_chksum_parts&0xF)==0 && (arch.ec_data_chksum_parts&0xF0)) {
					arch.ec_data_chksum_parts = 0;
				}
				if ((trash.ec_data_chksum_parts&0xF)==0 && (trash.ec_data_chksum_parts&0xF0)) {
					trash.ec_data_chksum_parts = 0;
				}
			}
			if (arch.ec_data_chksum_parts>0) {
				if (arch.ec_data_chksum_parts>>4) {
					if ((arch.ec_data_chksum_parts>>4)!=4 && (arch.ec_data_chksum_parts>>4)!=8) {
						if (ignoreflag==0) {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (number of data parts in arch mode: %u) - use '-i' to ignore",sclassid,arch.ec_data_chksum_parts>>4);
							free(databuff);
							databuff = NULL;
							return -1;
						} else {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: sclassid: %"PRIu16" - data format error (number of data parts in arch mode: %u) - ignoring",sclassid,arch.ec_data_chksum_parts>>4);
						}
						arch.ec_data_chksum_parts = (arch.ec_data_chksum_parts & 0xF) | (COMPAT_ECMODE << 4);
					}
				} else {
					arch.ec_data_chksum_parts = (arch.ec_data_chksum_parts & 0xF) | (COMPAT_ECMODE << 4);
				}
			}
			if (trash.ec_data_chksum_parts>0) {
				if (trash.ec_data_chksum_parts>>4) {
					if ((trash.ec_data_chksum_parts>>4)!=4 && (trash.ec_data_chksum_parts>>4)!=8) {
						if (ignoreflag==0) {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (number of data parts in trash mode: %u) - use '-i' to ignore",sclassid,trash.ec_data_chksum_parts>>4);
							free(databuff);
							databuff = NULL;
							return -1;
						} else {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: sclassid: %"PRIu16" - data format error (number of data parts in trash mode: %u) - ignoring",sclassid,trash.ec_data_chksum_parts>>4);
						}
						trash.ec_data_chksum_parts = (trash.ec_data_chksum_parts & 0xF) | (COMPAT_ECMODE << 4);
					}
				} else {
					trash.ec_data_chksum_parts = (trash.ec_data_chksum_parts & 0xF) | (COMPAT_ECMODE << 4);
				}
			}
			if ((arch.ec_data_chksum_parts>0 || trash.ec_data_chksum_parts>0) && ec_current_version<1) {
				ec_current_version = 1;
				if (ignoreflag==0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (arch.ec_data_chksum_parts: 0x%02"PRIX8" ; trash.ec_data_chksum_parts: 0x%02"PRIX8" ; ec_current_version: %u)",sclassid,arch.ec_data_chksum_parts,trash.ec_data_chksum_parts,ec_current_version);
					free(databuff);
					databuff = NULL;
					return -1;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: sclassid: %"PRIu16" - data format error (arch.ec_data_chksum_parts: 0x%02"PRIX8" ; trash.ec_data_chksum_parts: 0x%02"PRIX8" ; ec_current_version: %u) - ignoring",sclassid,arch.ec_data_chksum_parts,trash.ec_data_chksum_parts,ec_current_version);
				}
			}
			if (((arch.ec_data_chksum_parts>>4)==4 || (trash.ec_data_chksum_parts>>4)==4) && ec_current_version<2) {
				ec_current_version = 2;
				if (ignoreflag==0) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: sclassid: %"PRIu16" - data format error (arch.ec_data_chksum_parts: 0x%02"PRIX8" ; trash.ec_data_chksum_parts: 0x%02"PRIX8" ; ec_current_version: %u)",sclassid,arch.ec_data_chksum_parts,trash.ec_data_chksum_parts,ec_current_version);
					free(databuff);
					databuff = NULL;
					return -1;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: sclassid: %"PRIu16" - data format error (arch.ec_data_chksum_parts: 0x%02"PRIX8" ; trash.ec_data_chksum_parts: 0x%02"PRIX8" ; ec_current_version: %u) - ignoring",sclassid,arch.ec_data_chksum_parts,trash.ec_data_chksum_parts,ec_current_version);
				}
			}
			if (sclassid>=MAXSCLASS) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: bad sclassid (%"PRIu16") - ignore",sclassid);
					bio_skip(fd,(create.labelscnt+keep.labelscnt+arch.labelscnt+trash.labelscnt)*exprsize);
					continue;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: bad sclassid (%"PRIu16")",sclassid);
					free(databuff);
					databuff = NULL;
					return -1;
				}
			}
			if (sclasstab[sclassid].nleng>0) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: repeated sclassid (%"PRIu16") - ignore",sclassid);
					bio_skip(fd,(create.labelscnt+keep.labelscnt+arch.labelscnt+trash.labelscnt)*exprsize);
					continue;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: repeated sclassid (%"PRIu16")",sclassid);
					free(databuff);
					databuff = NULL;
					return -1;
				}
			}
			if (exprsize>SCLASS_EXPR_MAX_SIZE) {
				lexprsize = SCLASS_EXPR_MAX_SIZE;
			} else {
				lexprsize = exprsize;
			}
			smptr = NULL; // stupid compilers
			for (i=0 ; i<4 ; i++) {
				switch (i) {
					case 0:
						smptr = &create;
						break;
					case 1:
						smptr = &keep;
						break;
					case 2:
						smptr = &arch;
						break;
					case 3:
						smptr = &trash;
						break;
				}
				for (j=0 ; j<smptr->labelscnt ; j++) {
					if (bio_read(fd,smptr->labelexpr[j],lexprsize)!=lexprsize) {
						int err = errno;
						fputc('\n',stderr);
						errno = err;
						mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
						free(databuff);
						databuff=NULL;
						return -1;
					}
					if (exprsize>SCLASS_EXPR_MAX_SIZE) {
						bio_skip(fd,exprsize-SCLASS_EXPR_MAX_SIZE);
					}
				}
			}
			sclasstab[sclassid].nleng = nleng;
			memcpy(sclasstab[sclassid].name,name,nleng);
			sclasstab[sclassid].dleng = dleng;
			if (dleng>0) {
				memcpy(sclasstab[sclassid].desc,desc,dleng);
			}
			sclasstab[sclassid].priority = priority;
			sclasstab[sclassid].export_group = export_group;
			sclasstab[sclassid].admin_only = admin_only;
			sclasstab[sclassid].labels_mode = labels_mode;
			sclasstab[sclassid].arch_mode = arch_mode;
			sclasstab[sclassid].create = create;
			sclasstab[sclassid].keep = keep;
			sclasstab[sclassid].arch = arch;
			sclasstab[sclassid].trash = trash;
			sclasstab[sclassid].arch_delay = arch_delay;
			sclasstab[sclassid].min_trashretention = min_trashretention;
			sclasstab[sclassid].arch_min_size = arch_min_size;
			sclasstab[sclassid].files = 0;
			sclasstab[sclassid].directories = 0;
			sclass_fix_has_labels_fields(sclassid);
			if (mver<0x1B && labels_mode != LABELS_MODE_LOOSE) {
				if (arch.labelscnt>0 && arch.ec_data_chksum_parts && sclasstab[sclassid].arch.has_labels) {
					sclasstab[sclassid].arch.labels_mode = LABELS_MODE_LOOSE;
				}
				if (trash.labelscnt>0 && trash.ec_data_chksum_parts && sclasstab[sclassid].trash.has_labels) {
					sclasstab[sclassid].trash.labels_mode = LABELS_MODE_LOOSE;
				}
			}
			if (sclassid>=firstneverused) {
				firstneverused = sclassid+1;
			}
		}
	} else { // import from 3.x
		if (mver==0x10) {
			orgroup = 1;
		} else {
			if (bio_read(fd,&orgroup,1)!=1) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class: read error");
				return -1;
			}
			if (orgroup>MASKORGROUP) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: too many or-groups - ignore");
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: too many or-groups");
					return -1;
				}
			}
		}
		if (orgroup<1) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: zero or-groups !!!");
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
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
				free(databuff);
				databuff=NULL;
				return -1;
			}
			ptr = databuff;
			memset(&create,0,sizeof(storagemode));
			memset(&keep,0,sizeof(storagemode));
			memset(&arch,0,sizeof(storagemode));
			memset(&trash,0,sizeof(storagemode));
			create.labels_mode = LABELS_MODE_GLOBAL;
			keep.labels_mode = LABELS_MODE_GLOBAL;
			arch.labels_mode = LABELS_MODE_GLOBAL;
			trash.labels_mode = LABELS_MODE_GLOBAL;
			sclassid = get16bit(&ptr);
			if (mver>0x15) {
				nleng = get8bit(&ptr);
				admin_only = get8bit(&ptr);
				labels_mode = get8bit(&ptr);
				arch_delay = get16bit(&ptr);
				create.labelscnt = get8bit(&ptr);
				keep.labelscnt = get8bit(&ptr);
				arch.labelscnt = get8bit(&ptr);
				chunkcount = 0;
			} else if (mver>0x14) {
				nleng = 0;
				admin_only = 0;
				labels_mode = get8bit(&ptr);
				arch_delay = get16bit(&ptr);
				create.labelscnt = get8bit(&ptr);
				keep.labelscnt = get8bit(&ptr);
				arch.labelscnt = get8bit(&ptr);
				chunkcount = 0;
			} else if (mver>0x13) {
				nleng = 0;
				admin_only = 0;
				labels_mode = get8bit(&ptr);
				create.labelscnt = get8bit(&ptr);
				keep.labelscnt = get8bit(&ptr);
				arch.labelscnt = 0;
				arch_delay = 0;
				chunkcount = 0;
			} else {
				nleng = 0;
				admin_only = 0;
				create.labelscnt = 0;
				keep.labelscnt = get8bit(&ptr);
				arch.labelscnt = 0;
				labels_mode = LABELS_MODE_STD;
				arch_delay = 0;
				if (mver==0x12) {
					chunkcount = get32bit(&ptr);
					ptr+=4;
				} else {
					chunkcount = 0;
				}
			}
			if (nleng==0) {
				nleng = snprintf((char*)name,MAXSCLASSNAMELENG,"sclass_%"PRIu32,(uint32_t)(sclassid));
			} else {
				if (bio_read(fd,name,nleng)!=nleng) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			if (sclassid==0 && create.labelscnt==0 && keep.labelscnt==0 && arch.labelscnt==0 && chunkcount==0 && arch_delay==0) {
				break;
			}
			if (create.labelscnt>MAXLABELSCNT || keep.labelscnt==0 || keep.labelscnt>MAXLABELSCNT || arch.labelscnt>MAXLABELSCNT) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: data format error (sclassid: %"PRIu16" ; labels_mode: %"PRIu8" ; create.labelscnt: %"PRIu8" ; keep.labelscnt: %"PRIu8" ; arch.labelscnt: %"PRIu8" ; arch_delay: %"PRIu16")",sclassid,labels_mode,create.labelscnt,keep.labelscnt,arch.labelscnt,arch_delay);
				free(databuff);
				databuff = NULL;
				return -1;
			}
			if (sclassid==0 || sclassid>=MAXSCLASS || nleng==0) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: bad sclassid (%"PRIu16") - ignore",sclassid);
					if (mver>0x14) {
						bio_skip(fd,(create.labelscnt+keep.labelscnt+arch.labelscnt)*4*orgroup);
					} else if (mver>0x13) {
						bio_skip(fd,(create.labelscnt+keep.labelscnt)*4*orgroup);
					} else {
						bio_skip(fd,(keep.labelscnt)*4*orgroup);
					}
					continue;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: bad sclassid (%"PRIu16")",sclassid);
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			if (mver>0x14) {
				if (bio_read(fd,databuff,(create.labelscnt+keep.labelscnt+arch.labelscnt)*4*orgroup)!=(create.labelscnt+keep.labelscnt+arch.labelscnt)*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			} else if (mver>0x13) {
				if (bio_read(fd,databuff,(create.labelscnt+keep.labelscnt)*4*orgroup)!=(create.labelscnt+keep.labelscnt)*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			} else {
				if (bio_read(fd,databuff,keep.labelscnt*4*orgroup)!=keep.labelscnt*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			if (sclasstab[sclassid].nleng>0) {
				if (ignoreflag) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading storage class data: repeated sclassid - ignore");
					if (chunkcount>0) {
						bio_skip(fd,chunkcount*8);
					}
					continue;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading storage class data: repeated sclassid");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}

			ptr = databuff;
			for (i=0 ; i<create.labelscnt ; i++) {
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (j<orgroup) {
						labelmasks[i*MASKORGROUP+j] = get32bit(&ptr);
					} else {
						labelmasks[i*MASKORGROUP+j] = 0;
					}
				}
			}
			sclass_maskorgroup_to_labelexpr(create.labelexpr,labelmasks,create.labelscnt);
			for (i=0 ; i<keep.labelscnt ; i++) {
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (j<orgroup) {
						labelmasks[i*MASKORGROUP+j] = get32bit(&ptr);
					} else {
						labelmasks[i*MASKORGROUP+j] = 0;
					}
				}
			}
			sclass_maskorgroup_to_labelexpr(keep.labelexpr,labelmasks,keep.labelscnt);
			for (i=0 ; i<arch.labelscnt ; i++) {
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (j<orgroup) {
						labelmasks[i*MASKORGROUP+j] = get32bit(&ptr);
					} else {
						labelmasks[i*MASKORGROUP+j] = 0;
					}
				}
			}
			sclass_maskorgroup_to_labelexpr(arch.labelexpr,labelmasks,arch.labelscnt);
			if (create.labelscnt==keep.labelscnt) {
				j = 0;
				for (i=0 ; i<keep.labelscnt && j==0 ; i++) {
					if (memcmp(create.labelexpr[i],keep.labelexpr[i],SCLASS_EXPR_MAX_SIZE)!=0) {
						j = 1;
					}
				}
				if (j==0) { // create == keep
					memset(&create,0,sizeof(storagemode));
				}
			}
			if (arch.labelscnt==keep.labelscnt) {
				j = 0;
				for (i=0 ; i<keep.labelscnt && j==0 ; i++) {
					if (memcmp(arch.labelexpr[i],keep.labelexpr[i],SCLASS_EXPR_MAX_SIZE)!=0) {
						j = 1;
					}
				}
				if (j==0) { // create == keep
					memset(&arch,0,sizeof(storagemode));
				}
			}
			sclasstab[sclassid].nleng = nleng;
			memcpy(sclasstab[sclassid].name,name,nleng);
			sclasstab[sclassid].dleng = 0;
			sclasstab[sclassid].priority = 0;
			if (sclassid<10) {
				sclasstab[sclassid].export_group = sclassid;
			} else {
				sclasstab[sclassid].export_group = 0;
			}
			sclasstab[sclassid].admin_only = admin_only;
			sclasstab[sclassid].labels_mode = labels_mode;
			sclasstab[sclassid].arch_mode = SCLASS_ARCH_MODE_CTIME;
			sclasstab[sclassid].create = create;
			sclasstab[sclassid].keep = keep;
			sclasstab[sclassid].arch = arch;
			sclasstab[sclassid].trash = trash;
			sclasstab[sclassid].arch_delay = arch_delay * 24;
			sclasstab[sclassid].min_trashretention = 0;
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
	}
	free(databuff);
	databuff=NULL;
	return 1;
}

void sclass_new(void) {
	// sclass 1
	sclasstab[1].nleng = snprintf((char*)(sclasstab[1].name),MAXSCLASSNAMELENG,"2CP");
	sclasstab[1].dleng = snprintf((char*)(sclasstab[1].desc),MAXSCLASSDESCLENG,"two copies");
	sclasstab[1].keep.labelscnt = 2;

	// sclass 2
	sclasstab[2].nleng = snprintf((char*)(sclasstab[2].name),MAXSCLASSNAMELENG,"3CP");
	sclasstab[2].dleng = snprintf((char*)(sclasstab[2].desc),MAXSCLASSDESCLENG,"three copies");
	sclasstab[2].keep.labelscnt = 3;

	// sclass 3
	sclasstab[3].nleng = snprintf((char*)(sclasstab[3].name),MAXSCLASSNAMELENG,"EC4+1");
	sclasstab[3].dleng = snprintf((char*)(sclasstab[3].desc),MAXSCLASSDESCLENG,"two copies in keep mode, four data parts plus checksum part in archive mode");
	sclasstab[3].arch_delay = 24;
	sclasstab[3].keep.labelscnt = 2;
	sclasstab[3].arch.ec_data_chksum_parts = 0x41;
	sclasstab[3].arch_min_size = 512*1024;

	// sclass 4
	sclasstab[4].nleng = snprintf((char*)(sclasstab[4].name),MAXSCLASSNAMELENG,"EC8+1");
	sclasstab[4].dleng = snprintf((char*)(sclasstab[4].desc),MAXSCLASSDESCLENG,"two copies in keep mode, eight data parts plus checksum part in archive mode");
	sclasstab[4].arch_delay = 24;
	sclasstab[4].keep.labelscnt = 2;
	sclasstab[4].arch.ec_data_chksum_parts = 0x81;
	sclasstab[4].arch_min_size = 512*1024;

	firstneverused = 5;
	ec_current_version = 2;
}

void sclass_cleanup(void) {
	uint32_t i;

	for (i=0 ; i<MAXSCLASS ; i++) {
		sclasstab[i].nleng = 0;
		sclasstab[i].dleng = 0;
		sclasstab[i].name[0] = 0;
		sclasstab[i].desc[0] = 0;
		sclasstab[i].priority = 0;
		sclasstab[i].export_group = 0;
		sclasstab[i].admin_only = 0;
		sclasstab[i].labels_mode = LABELS_MODE_STD;
		sclasstab[i].arch_mode = SCLASS_ARCH_MODE_CTIME;
		sclasstab[i].arch_delay = 0;
		sclasstab[i].min_trashretention = 0;
		sclasstab[i].arch_min_size = 0;
		sclasstab[i].files = 0;
		sclasstab[i].directories = 0;
		memset(&(sclasstab[i].create),0,sizeof(storagemode));
		memset(&(sclasstab[i].keep),0,sizeof(storagemode));
		memset(&(sclasstab[i].arch),0,sizeof(storagemode));
		memset(&(sclasstab[i].trash),0,sizeof(storagemode));
		sclasstab[i].create.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].keep.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].arch.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].trash.labels_mode = LABELS_MODE_GLOBAL;
	}

	firstneverused = 1;
}

void sclass_reload(void) {
	uint32_t ecmode;

	ecmode = cfg_getuint32("DEFAULT_EC_DATA_PARTS",8);
	if (ecmode==4 || ecmode==8) {
		DefaultECMODE = ecmode;
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"wrong value for DEFAULT_EC_DATA_PARTS option - only 4 and 8 are currently supported (using: %u)",DefaultECMODE);
	}
}

int sclass_init(void) {
	uint32_t i;

	MaxECRedundancyLevel = REDUCED_EC_LEVEL;
	sclass_reload();
	for (i=0 ; i<MAXSCLASS ; i++) {
		sclasstab[i].nleng = 0;
		sclasstab[i].dleng = 0;
		sclasstab[i].name[0] = 0;
		sclasstab[i].desc[0] = 0;
		sclasstab[i].priority = 0;
		sclasstab[i].export_group = 0;
		sclasstab[i].admin_only = 0;
		sclasstab[i].labels_mode = LABELS_MODE_STD;
		sclasstab[i].arch_mode = SCLASS_ARCH_MODE_CTIME;
		sclasstab[i].arch_delay = 0;
		sclasstab[i].min_trashretention = 0;
		sclasstab[i].arch_min_size = 0;
		sclasstab[i].files = 0;
		sclasstab[i].directories = 0;
		memset(&(sclasstab[i].create),0,sizeof(storagemode));
		memset(&(sclasstab[i].keep),0,sizeof(storagemode));
		memset(&(sclasstab[i].arch),0,sizeof(storagemode));
		memset(&(sclasstab[i].trash),0,sizeof(storagemode));
		sclasstab[i].create.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].keep.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].arch.labels_mode = LABELS_MODE_GLOBAL;
		sclasstab[i].trash.labels_mode = LABELS_MODE_GLOBAL;
	}

	firstneverused = 1;

	main_reload_register(sclass_reload);
	main_time_register(1,0,sclass_fix_matching_servers_fields);

	return 0;
}
