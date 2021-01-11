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

#ifndef _CFG_H_
#define _CFG_H_

#include <inttypes.h>

#define _CONFIG_MAKE_PROTOTYPE(fname,type) type cfg_get##fname(const char *name,const type def)

int cfg_load (const char *fname,int logundefined);
int cfg_reload (void);
void cfg_term (void);

int cfg_isdefined(const char *name);

_CONFIG_MAKE_PROTOTYPE(str,char*);
_CONFIG_MAKE_PROTOTYPE(num,int);
_CONFIG_MAKE_PROTOTYPE(uint8,uint8_t);
_CONFIG_MAKE_PROTOTYPE(int8,int8_t);
_CONFIG_MAKE_PROTOTYPE(uint16,uint16_t);
_CONFIG_MAKE_PROTOTYPE(int16,int16_t);
_CONFIG_MAKE_PROTOTYPE(uint32,uint32_t);
_CONFIG_MAKE_PROTOTYPE(int32,int32_t);
_CONFIG_MAKE_PROTOTYPE(uint64,uint64_t);
_CONFIG_MAKE_PROTOTYPE(int64,int64_t);
_CONFIG_MAKE_PROTOTYPE(double,double);

#endif
