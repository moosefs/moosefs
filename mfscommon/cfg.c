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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef WIN32
#include "portable.h"
#else
#include <syslog.h>
#endif

#include "cfg.h"
#include "massert.h"
#ifdef WIN32
#include <stdarg.h>
static inline void mfs_arg_syslog(uint8_t level,const char *format,...) {
	va_list args;

	va_start(args, format);
	vprintf(format, args);
	va_end(args);
	printf("\n");
}
#else
#include "slogger.h"
#endif

typedef struct paramsstr {
	char *name;
	char *value;
	struct paramsstr *next;
} paramstr;

static char *cfgfname;
static paramstr *paramhead=NULL;
static int logundefined=0;

int cfg_reload (void) {
	FILE *fd;
	char linebuff[1000];
	uint32_t nps,npe,vps,vpe,i;
	uint8_t found;
	paramstr *tmp;

	fd = fopen(cfgfname,"r");
	if (fd==NULL) {
		mfs_arg_syslog(LOG_ERR,"cannot load config file: %s",cfgfname);
		return 0;
	}
	while (paramhead!=NULL) {
		tmp = paramhead;
		paramhead = tmp->next;
		free(tmp->name);
		free(tmp->value);
		free(tmp);
	}
	while (fgets(linebuff,999,fd)!=NULL) {
		linebuff[999]=0;
		if (linebuff[0]=='#') {
			continue;
		}
		i = 0;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		nps = i;
		while (linebuff[i]>32 && linebuff[i]<127 && linebuff[i]!='=') {
			i++;
		}
		npe = i;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		if (linebuff[i]!='=' || npe==nps) {
			if (linebuff[i]>32) {
				mfs_arg_syslog(LOG_WARNING,"bad definition in config file '%s': %s",cfgfname,linebuff);
			}
			continue;
		}
		i++;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		vps = i;
		while (linebuff[i]>=32) {
			i++;
		}
		while (i>vps && linebuff[i-1]==32) {
			i--;
		}
		vpe = i;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		if (linebuff[i]!='\0' && linebuff[i]!='\r' && linebuff[i]!='\n' && linebuff[i]!='#') {
			mfs_arg_syslog(LOG_WARNING,"bad definition in config file '%s': %s",cfgfname,linebuff);
			continue;
		}
		linebuff[npe]=0;
		linebuff[vpe]=0;
		found = 0;
		for (tmp = paramhead ; tmp && found==0; tmp=tmp->next) {
			if (strcmp(tmp->name,linebuff+nps)==0) {
				free(tmp->value);
				tmp->value = (char*)malloc(vpe-vps+1);
				memcpy(tmp->value,linebuff+vps,vpe-vps+1);
				found = 1;
			}
		}
		if (found==0) {
			tmp = (paramstr*)malloc(sizeof(paramstr));
			tmp->name = (char*)malloc(npe-nps+1);
			tmp->value = (char*)malloc(vpe-vps+1);
			memcpy(tmp->name,linebuff+nps,npe-nps+1);
			memcpy(tmp->value,linebuff+vps,vpe-vps+1);
			tmp->next = paramhead;
			paramhead = tmp;
		}
	}
	fclose(fd);
	return 1;
}

int cfg_load (const char *configfname,int _lu) {
	paramhead = NULL;
	logundefined = _lu;
	cfgfname = strdup(configfname);

	return cfg_reload();
}

int cfg_isdefined(const char *name) {
	paramstr *_cfg_tmp;
	for (_cfg_tmp = paramhead ; _cfg_tmp ; _cfg_tmp=_cfg_tmp->next) {
		if (strcmp(name,_cfg_tmp->name)==0) {
			return 1;
		}
	}
	return 0;
}

void cfg_term(void) {
	paramstr *i,*in;
	for (i = paramhead ; i ; i = in) {
		in = i->next;
		free(i->value);
		free(i->name);
		free(i);
	}
	free(cfgfname);
}

#define STR_TO_int(x) return strtol(x,NULL,0)
#define STR_TO_int32(x) return strtol(x,NULL,0)
#define STR_TO_uint32(x) return strtoul(x,NULL,0)
#define STR_TO_int64(x) return strtoll(x,NULL,0)
#define STR_TO_uint64(x) return strtoull(x,NULL,0)
#define STR_TO_double(x) return strtod(x,NULL)
#define STR_TO_charptr(x) { \
	char* _cfg_ret_tmp = strdup(x); \
	passert(_cfg_ret_tmp); \
	return _cfg_ret_tmp; \
}

#define COPY_int(x) return x
#define COPY_int32(x) return x
#define COPY_uint32(x) return x
#define COPY_int64(x) return x
#define COPY_uint64(x) return x
#define COPY_double(x) return x
#define COPY_charptr(x) { \
	char* _cfg_ret_tmp = strdup(x); \
	passert(_cfg_ret_tmp); \
	return _cfg_ret_tmp; \
}

#define _CONFIG_GEN_FUNCTION(fname,type,convname,format) \
type cfg_get##fname(const char *name,const type def) { \
	paramstr *_cfg_tmp; \
	for (_cfg_tmp = paramhead ; _cfg_tmp ; _cfg_tmp=_cfg_tmp->next) { \
		if (strcmp(name,_cfg_tmp->name)==0) { \
			STR_TO_##convname(_cfg_tmp->value); \
		} \
	} \
	if (logundefined) { \
		mfs_arg_syslog(LOG_NOTICE,"config: using default value for option '%s' - '" format "'",name,def); \
	} \
	COPY_##convname(def); \
}

_CONFIG_GEN_FUNCTION(str,char*,charptr,"%s")
_CONFIG_GEN_FUNCTION(num,int,int,"%d")
_CONFIG_GEN_FUNCTION(int8,int8_t,int32,"%"PRId8)
_CONFIG_GEN_FUNCTION(uint8,uint8_t,uint32,"%"PRIu8)
_CONFIG_GEN_FUNCTION(int16,int16_t,int32,"%"PRId16)
_CONFIG_GEN_FUNCTION(uint16,uint16_t,uint32,"%"PRIu16)
_CONFIG_GEN_FUNCTION(int32,int32_t,int32,"%"PRId32)
_CONFIG_GEN_FUNCTION(uint32,uint32_t,uint32,"%"PRIu32)
_CONFIG_GEN_FUNCTION(int64,int64_t,int64,"%"PRId64)
_CONFIG_GEN_FUNCTION(uint64,uint64_t,uint64,"%"PRIu64)
_CONFIG_GEN_FUNCTION(double,double,double,"%lf")
