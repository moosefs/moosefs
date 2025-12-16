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

#ifndef _STORAGECLASS_H_
#define _STORAGECLASS_H_

#include <inttypes.h>

#include "MFSCommunication.h"
#include "bio.h"


typedef struct _storagemode {
	uint32_t uniqmask;
	uint8_t ec_data_chksum_parts;
	uint8_t has_labels; // internal
	uint8_t matching_servers; // internal
	// ec counters - begin
	uint8_t valid_ec_counters; // 0 - invalid ; 1 - for one label ; 2 - for two labels
	uint16_t replallowed; // internal
	uint16_t overloaded; // internal
	uint16_t allvalid; // internal
	uint16_t data_replallowed; // internal
	uint16_t data_overloaded; // internal
	uint16_t data_allvalid; // internal
	uint16_t chksum_replallowed; // internal
	uint16_t chksum_overloaded; // internal
	uint16_t chksum_allvalid; // internal
	uint16_t both_replallowed; // internal
	uint16_t both_overloaded; // internal
	uint16_t both_allvalid; // internal
	// ec counters - end
	uint8_t labels_mode;
	uint8_t labelscnt;
	uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE];
} storagemode;

void sclass_maskorgroup_to_labelexpr(uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE],uint32_t labelmasks[MAXLABELSCNT*MASKORGROUP],uint8_t labelscnt);
uint8_t sclass_labelexpr_to_maskorgroup(uint32_t labelmasks[MAXLABELSCNT*MASKORGROUP],uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE],uint8_t labelscnt);

uint8_t sclass_ec_version(void);
uint8_t sclass_mr_ec_version(uint8_t ec_new_version);

uint32_t sclass_info(uint8_t *buff,uint8_t fver);

uint8_t sclass_create_entry(uint8_t nleng,const uint8_t *name,uint8_t dleng,const uint8_t *desc,uint32_t priority,uint8_t export_group,uint8_t admin_only,uint8_t labels_mode,uint8_t arch_mode,uint16_t arch_delay,uint64_t arch_min_size,uint16_t min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash);
uint8_t sclass_change_entry(uint8_t nleng,const uint8_t *name,uint16_t chgmask,uint8_t *dleng,uint8_t *desc,uint32_t *priority,uint8_t *export_group,uint8_t *admin_only,uint8_t *labels_mode,uint8_t *arch_mode,uint16_t *arch_delay,uint64_t *arch_min_size,uint16_t *min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash);
uint8_t sclass_delete_entry(uint8_t nleng,const uint8_t *name);
uint8_t sclass_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname);
uint8_t sclass_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname);
uint32_t sclass_list_entries(uint8_t *buff,uint8_t sclsmode);

uint8_t sclass_mr_set_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid,uint8_t new_flag,uint8_t dleng,const uint8_t *desc,uint32_t priority,uint8_t export_group,uint8_t admin_only,uint8_t labels_mode,uint8_t arch_mode,uint16_t arch_delay,uint64_t arch_min_size,uint16_t min_trashretention,storagemode *create,storagemode *keep,storagemode *arch,storagemode *trash);
uint8_t sclass_mr_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t essclassid,uint16_t edsclassid);
uint8_t sclass_mr_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t esclassid);
uint8_t sclass_mr_delete_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid);

uint8_t sclass_find_by_name(uint8_t nleng,const uint8_t *name);
uint8_t sclass_get_nleng(uint8_t sclassid);
const uint8_t* sclass_get_name(uint8_t sclassid);

void sclass_incref(uint16_t sclassid,uint8_t type);
void sclass_decref(uint16_t sclassid,uint8_t type);
uint8_t sclass_get_labels_mode(uint16_t sclassid,storagemode *sm);
uint8_t sclass_get_arch_mode(uint16_t sclassid);
uint8_t sclass_get_keeparch_storage_eights(uint16_t sclassid,uint8_t keepflag);
uint8_t sclass_get_keeparch_maxstorage_eights(uint16_t sclassid);
storagemode* sclass_get_create_storagemode(uint16_t sclassid);
storagemode* sclass_get_keeparch_storagemode(uint16_t sclassid,uint8_t flags);
uint8_t sclass_calc_goal_equivalent(storagemode *sm);
uint8_t sclass_is_predefined(uint16_t sclassid);
// uint32_t sclass_get_priority(uint16_t sclassid);
uint64_t sclass_get_joining_priority(uint16_t sclassid);
uint8_t sclass_get_export_group(uint16_t sclassid);
uint8_t sclass_is_admin_only(uint16_t sclassid);
uint16_t sclass_get_arch_delay(uint16_t sclassid);
uint64_t sclass_get_arch_min_size(uint16_t sclassid);
uint16_t sclass_get_min_trashretention(uint16_t sclassid);

uint8_t sclass_store(bio *fd);
int sclass_load(bio *fd,uint8_t mver,int ignoreflag);
void sclass_new(void);
void sclass_cleanup(void);
int sclass_init(void);

#endif
