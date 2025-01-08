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
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#ifdef WIN32
#include "portable.h"
#else
#endif

#include "cfg.h"
#include "massert.h"
#include "mfslog.h"
#include "timeparser.h"
#include "md5.h"

typedef struct paramsstr {
	char *name;
	char *value;
	struct paramsstr *next;
} paramstr;

static char *cfgfname;
static paramstr *paramhead=NULL;
static paramstr *usedhead=NULL;
static int logundefined=0;
static int dangerous=0;

int cfg_reload (void) {
	FILE *fd;
	char *linebuff;
	size_t lbsize;
	uint32_t nps,npe,vps,vpe,i;
	uint8_t found;
	paramstr *tmp;

	fd = fopen(cfgfname,"r");
	if (fd==NULL) {
		if (errno==ENOENT) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"main config file (%s) not found",cfgfname);
		} else {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't load main config file (%s), error",cfgfname);
		}
		return 0;
	}
	while (paramhead!=NULL) {
		tmp = paramhead;
		paramhead = tmp->next;
		free(tmp->name);
		free(tmp->value);
		free(tmp);
	}
	lbsize = 1000;
	linebuff = malloc(lbsize);
	while (getline(&linebuff,&lbsize,fd)!=-1) {
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
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"bad definition in config file '%s': %s",cfgfname,linebuff);
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
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"bad definition in config file '%s': %s",cfgfname,linebuff);
			continue;
		}
		linebuff[npe]=0;
		linebuff[vpe]=0;
		found = 0;
		if (npe-nps>=10 && memcmp(linebuff+nps,"DANGEROUS_",10)==0) {
			dangerous = 1;
		}
		for (tmp = paramhead ; tmp && found==0; tmp=tmp->next) {
			if (strcmp(tmp->name,linebuff+nps)==0) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"variable '%s' defined more than once in the config file (previous value: %s, current value: %s)",tmp->name,tmp->value,linebuff+vps);
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
	free(linebuff);
	fclose(fd);
	return 1;
}

void cfg_use_option(const char *name,const char *value) {
	paramstr *i,**inp;

	inp = &usedhead;

	while ((i=*inp)!=NULL) {
		if (strcmp(i->name,name)==0) {
			free(i->value);
			i->value = strdup(value);
			return;
		}
		inp = &(i->next);
	}
	i = malloc(sizeof(paramstr));
	i->name = strdup(name);
	i->value = strdup(value);
	i->next = NULL;
	*inp = i;
}

void cfg_info(FILE *fd) {
	paramstr *i;
	fprintf(fd,"[config]\n");
	for (i = usedhead ; i ; i = i->next) {
		fprintf(fd,"%s = %s\n",i->name,i->value);
	}
	fprintf(fd,"\n");
}

int cfg_load (const char *configfname,int _lu) {
	paramhead = NULL;
	usedhead = NULL;
	logundefined = _lu;
	dangerous = 0;
	cfgfname = strdup(configfname);

	return cfg_reload();
}

int cfg_dangerous_options(void) {
	return dangerous;
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
	paramhead = NULL;
	for (i = usedhead ; i ; i = in) {
		in = i->next;
		free(i->value);
		free(i->name);
		free(i);
	}
	usedhead = NULL;
	free(cfgfname);
}

static paramstr* cfg_findname(const char *name,uint8_t usedflag) {
	paramstr *psp;
	for (psp = (usedflag)?usedhead:paramhead ; psp!=NULL ; psp=psp->next) {
		if (strcmp(name,psp->name)==0) {
			return psp;
		}
	}
	return NULL;
}

char* cfg_getdefaultstr(const char *name) {
	paramstr *psp;

	psp = cfg_findname(name,0);
	if (psp==NULL) {
		psp = cfg_findname(name,1);
	}
	if (psp==NULL) {
		return NULL;
	}
	return strdup(psp->value);
}

cfg_buff* cfg_getdefaultfile(const char *name,uint32_t maxleng) {
	paramstr *psp;
	cfg_buff *ret;
	FILE *fd;
	unsigned long fsize;

	psp = cfg_findname(name,0);
	if (psp==NULL) {
		psp = cfg_findname(name,1);
	}
	if (psp==NULL) {
		return NULL;
	}
	fd = fopen(psp->value,"rb");
	if (fd==NULL) {
		return NULL;
	}
	fseek(fd,0L,SEEK_END);
	fsize = ftell(fd);
	fseek(fd,0,SEEK_SET);
	if (fsize>maxleng) {
		fclose(fd);
		return NULL;
	}
	ret = malloc(offsetof(cfg_buff,data)+fsize);
	passert(ret);
	if (fread(ret->data,1,fsize,fd)!=fsize) {
		free(ret);
		fclose(fd);
		return NULL;
	}
	fclose(fd);
	ret->leng = fsize;
	return ret;
}

int cfg_getdefaultfilemd5(const char *name,uint8_t txtmode,uint8_t digest[16]) {
	paramstr *psp;
	FILE *fd;
	char *linebuff,*sptr;
	uint8_t *binarybuff;
	unsigned long fsize;
	size_t lbsize;
	uint32_t s;
	md5ctx md5c;

	psp = cfg_findname(name,0);
	if (psp==NULL) {
		psp = cfg_findname(name,1);
	}
	if (psp==NULL) {
		return -1;
	}
	fd = fopen(psp->value,"rb");
	if (fd==NULL) {
		return -1;
	}
	md5_init(&md5c);
	if (txtmode) {
		lbsize = 10000;
		linebuff = malloc(lbsize);
		passert(linebuff);
		while (getline(&linebuff,&lbsize,fd)!=-1) {
			s=strlen(linebuff);
			while (s>0 && (linebuff[s-1]=='\r' || linebuff[s-1]=='\n' || linebuff[s-1]=='\t' || linebuff[s-1]==' ')) {
				s--;
			}
			if (s>0) {
				linebuff[s]=0;
				sptr = linebuff;
				while (*sptr==' ' || *sptr=='\t') {
					sptr++;
					s--;
				}
				if (*sptr!=0 && *sptr!='#') {
					md5_update(&md5c,(uint8_t*)sptr,s);
				}
			}
		}
		free(linebuff);
	} else {
		fseek(fd,0L,SEEK_END);
		fsize = ftell(fd);
		fseek(fd,0,SEEK_SET);
		if (fsize<=65536) {
			binarybuff = malloc(fsize);
			passert(binarybuff);
		} else {
			binarybuff = malloc(65536);
			passert(binarybuff);
			while (fsize>65536) {
				if (fread(binarybuff,1,65536,fd)!=65536) {
					free(binarybuff);
					fclose(fd);
					return -1;
				}
				md5_update(&md5c,binarybuff,65536);
				fsize -= 65536;
			}
		}
		if (fsize>0) {
			if (fread(binarybuff,1,fsize,fd)!=fsize) {
				free(binarybuff);
				fclose(fd);
				return -1;
			}
			md5_update(&md5c,binarybuff,fsize);
		}
		free(binarybuff);
	}
	fclose(fd);
	md5_final(digest,&md5c);
	return 0;
}

#define _CONFIG_GET_STRTOINT_FUNCTION(fname,type,convfn) \
static inline type str_to_##fname (char *x) { \
	char *e; \
	type r; \
	r = convfn(x,&e,0); \
	if (*e && *e!='\t' && *e!=' ') { \
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: number expected, got '%s'"); \
	} \
	return r; \
}

_CONFIG_GET_STRTOINT_FUNCTION(int,int,strtol)
_CONFIG_GET_STRTOINT_FUNCTION(int32,int32_t,strtol)
_CONFIG_GET_STRTOINT_FUNCTION(uint32,uint32_t,strtoul)
_CONFIG_GET_STRTOINT_FUNCTION(int64,int64_t,strtoll)
_CONFIG_GET_STRTOINT_FUNCTION(uint64,uint64_t,strtoull)

static inline double str_to_double (char *x) {
	char *e;
	double r;
	r = strtod(x,&e);
	if (*e && *e!='\t' && *e!=' ') { \
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: number expected, got '%s'");
	}
	return r;
}

static inline char* str_to_charptr (char *x) {
	char* _cfg_ret_tmp = strdup(x);
	passert(_cfg_ret_tmp);
	return _cfg_ret_tmp;
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
	paramstr *psp; \
	static char usedvalue[1000]; \
	psp = cfg_findname(name,0); \
	if (psp!=NULL) { \
		cfg_use_option(psp->name,psp->value); \
		return str_to_##convname(psp->value); \
	} \
	if (logundefined) { \
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"config: using default value for option '%s' - '" format "'",name,def); \
	} \
	snprintf(usedvalue,1000,format,def); \
	usedvalue[999] = 0; \
	cfg_use_option(name,usedvalue); \
	COPY_##convname(def); \
}

#define _CONFIG_GEN_PERIOD_FUNCTION(fname,type) \
type cfg_get##fname(const char *name,const char* def) { \
	paramstr *psp; \
	static char usedvalue[1000]; \
	uint32_t ret; \
	psp = cfg_findname(name,0); \
	if (psp!=NULL) { \
		switch (parse_##fname(psp->value,&ret)) { \
			case TPARSE_OK: \
				cfg_use_option(psp->name,psp->value); \
				return ret; \
			case TPARSE_UNEXPECTED_CHAR: \
				if (ret) { \
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: unexpected char '%c' in '%s = %s' - using defaults",(char)ret,psp->name,psp->value); \
				} else { \
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: unexpected end in '%s = %s' - using defaults",psp->name,psp->value); \
				} \
				break; \
			case TPARSE_VALUE_TOO_BIG: \
				if (ret) { \
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: value too big in section '%c' in '%s = %s' - using defaults",(char)ret,psp->name,psp->value); \
				} else { \
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: parsed value too big in '%s = %s' - using defaults",psp->name,psp->value); \
				} \
				break; \
		} \
	} \
	if (logundefined) { \
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"config: using default value for option '%s' - '%s'",name,def); \
	} \
	snprintf(usedvalue,1000,"%s",def); \
	usedvalue[999] = 0; \
	cfg_use_option(name,usedvalue); \
	if (parse_##fname(def,&ret)!=TPARSE_OK) { \
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"config: wrong default value for option '%s' - '%s' !!!",name,def); \
	} \
	return ret; \
}

// needed for autoamtic check: char * cfg_getstr(const char *,const char *);

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
_CONFIG_GEN_FUNCTION(double,double,double,"%.6lf")
_CONFIG_GEN_PERIOD_FUNCTION(speriod,uint32_t)
_CONFIG_GEN_PERIOD_FUNCTION(hperiod,uint16_t)

