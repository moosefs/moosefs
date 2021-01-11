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

#ifndef _STORAGECLASS_H_
#define _STORAGECLASS_H_

#include <inttypes.h>

#include "bio.h"

#define MAXSCLASS 256

#define MAXLABELSCNT 9

uint32_t sclass_info(uint8_t *buff);

uint8_t sclass_create_entry(uint8_t nleng,const uint8_t *name,uint8_t adminonly,uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay);
uint8_t sclass_change_entry(uint8_t nleng,const uint8_t *name,uint16_t chgmask,uint8_t *adminonly,uint8_t *create_mode,uint8_t *create_labelscnt,uint32_t *create_labelmasks,uint8_t *keep_labelscnt,uint32_t *keep_labelmasks,uint8_t *arch_labelscnt,uint32_t *arch_labelmasks,uint16_t *arch_delay);
uint8_t sclass_delete_entry(uint8_t nleng,const uint8_t *name);
uint8_t sclass_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname);
uint8_t sclass_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname);
uint32_t sclass_list_entries(uint8_t *buff,uint8_t sclsmode);

uint8_t sclass_mr_set_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid,uint8_t new_flag,uint8_t adminonly,uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay);
uint8_t sclass_mr_duplicate_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t essclassid,uint16_t edsclassid);
uint8_t sclass_mr_rename_entry(uint8_t oldnleng,const uint8_t *oldname,uint8_t newnleng,const uint8_t *newname,uint16_t esclassid);
uint8_t sclass_mr_delete_entry(uint8_t nleng,const uint8_t *name,uint16_t esclassid);

uint8_t sclass_find_by_name(uint8_t nleng,const uint8_t *name);
uint8_t sclass_get_nleng(uint8_t sclassid);
const uint8_t* sclass_get_name(uint8_t sclassid);

void sclass_incref(uint16_t sclassid,uint8_t type);
void sclass_decref(uint16_t sclassid,uint8_t type);
uint8_t sclass_get_mode(uint16_t sclassid);
uint8_t sclass_get_create_goal(uint16_t sclassid);
uint8_t sclass_get_keepmax_goal(uint16_t sclassid);
uint8_t sclass_get_keeparch_goal(uint16_t sclassid,uint8_t archflag);
uint8_t sclass_get_create_labelmasks(uint16_t sclassid,uint32_t ***labelmasks);
uint8_t sclass_get_keeparch_labelmasks(uint16_t sclassid,uint8_t archflag,uint32_t ***labelmasks);
uint8_t sclass_is_simple_goal(uint16_t sclassid);
uint8_t sclass_is_admin_only(uint16_t sclassid);
// uint8_t sclass_has_any_labels(uint16_t sclassid);
uint8_t sclass_has_create_labels(uint16_t sclassid);
uint8_t sclass_has_keeparch_labels(uint16_t sclassid,uint8_t archflag);
uint16_t sclass_get_arch_delay(uint16_t sclassid);
uint8_t sclass_store(bio *fd);
int sclass_load(bio *fd,uint8_t mver,int ignoreflag);
void sclass_cleanup(void);
int sclass_init(void);

#endif
