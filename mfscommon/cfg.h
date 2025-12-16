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

#ifndef _CFG_H_
#define _CFG_H_

#include <inttypes.h>
#include <stdio.h>

#define _CONFIG_MAKE_PROTOTYPE(fname,type) type cfg_get##fname(const char *name,const type def)
#define _CONFIG_MAKE_PERIOD_PROTOTYPE(fname,type) type cfg_get##fname(const char *name,const char *def)

typedef struct cfg_buff {
	uint32_t leng;
	uint8_t data[1];
} cfg_buff;

int cfg_load (const char *fname,int logundefined);
int cfg_dangerous_options(void);
int cfg_reload (void);
void cfg_info(FILE *fd);
void cfg_term (void);

int cfg_isdefined(const char *name);
void cfg_use_option(const char *name,const char *value);

char* cfg_getdefaultstr(const char *name);
cfg_buff* cfg_getdefaultfile(const char *name,uint32_t maxleng);
int cfg_getdefaultfilemd5(const char *name,uint8_t txtmode,uint8_t digest[16]);

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
_CONFIG_MAKE_PERIOD_PROTOTYPE(speriod,uint32_t);
_CONFIG_MAKE_PERIOD_PROTOTYPE(hperiod,uint16_t);

#endif
