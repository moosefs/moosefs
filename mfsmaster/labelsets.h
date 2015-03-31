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

#ifndef _LABELSETS_H_
#define _LABELSETS_H_

#include <inttypes.h>

#include "bio.h"

#define MAXLABELSETS 256


uint8_t labelset_setdescription(uint8_t labelid,uint8_t descrleng,const uint8_t *description);
uint8_t labelset_mr_setdescription(uint8_t labelid,const uint8_t *description);

uint32_t labelset_label_info(uint8_t *buff);
uint32_t labelset_label_set_info(uint8_t *buff);

uint16_t labelset_getsetid(uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay);
uint8_t labelset_mr_labelset(uint16_t labelsetid,uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay);
void labelset_incref(uint16_t labelsetid,uint8_t type);
void labelset_decref(uint16_t labelsetid,uint8_t type);
uint8_t labelset_get_create_mode(uint16_t labelsetid);
uint8_t labelset_get_create_goal(uint16_t labelsetid);
uint8_t labelset_get_keepmax_goal(uint16_t labelsetid);
uint8_t labelset_get_keeparch_goal(uint16_t labelsetid,uint8_t archflag);
uint8_t labelset_get_create_labelmasks(uint16_t labelsetid,uint32_t ***labelmasks);
uint8_t labelset_get_keeparch_labelmasks(uint16_t labelsetid,uint8_t archflag,uint32_t ***labelmasks);
uint8_t labelset_has_any_labels(uint16_t labelsetid);
uint8_t labelset_has_create_labels(uint16_t labelsetid);
uint8_t labelset_has_keeparch_labels(uint16_t labelsetid,uint8_t archflag);
uint8_t labelset_is_simple_goal(uint16_t labelsetid);
uint16_t labelset_get_arch_delay(uint16_t labelsetid);
void labelset_state_change(uint16_t oldlabelsetid,uint8_t oldarchflag,uint8_t oldrvc,uint16_t newlabelsetid,uint8_t newarchflag,uint8_t newrvc);
uint8_t labelset_store(bio *fd);
int labelset_load(bio *fd,uint8_t mver,int ignoreflag);
void labelset_cleanup(void);
int labelset_init(void);

#endif
