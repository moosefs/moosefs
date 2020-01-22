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

#ifndef _DICTIONARY_H_
#define _DICTIONARY_H_

#include <inttypes.h>

int dict_init(void);
void dict_cleanup(void);
void* dict_search(const uint8_t *data,uint32_t leng);
void* dict_insert(const uint8_t *data,uint32_t leng);
const uint8_t* dict_get_ptr(void *dptr);
uint32_t dict_get_leng(void *dptr);
uint32_t dict_get_hash(void *dptr);
void dict_dec_ref(void *dptr);
void dict_inc_ref(void *dptr);

#endif
