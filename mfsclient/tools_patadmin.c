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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <inttypes.h>
#include <pwd.h>
#include <grp.h>
#include <sys/param.h>

#include "libmfstools.h"

#define DEFAULT_PATH "/mnt/mfs"

typedef struct _pattern {
	char glob_name[MAXPATTERNLENG];
	uint32_t euid;
	uint32_t egid;
	uint8_t priority;
	uint8_t omask;
	char sclass_name[MAXSCLASSNAMELENG];
	uint16_t trashretention;
	uint8_t seteattr;
	uint8_t clreattr;
} pattern;

#define	PATTERN_DEF_PRIORITY 128
#define	PATTERN_DEF_RETENTION (7*24)
#define	PATTERN_DEF_EATTR 0

static const char *eattrtab[EATTR_BITS]={EATTR_STRINGS};
static const char *eattrdesc[EATTR_BITS]={EATTR_DESCRIPTIONS};

void print_eattr(void) {
	int j;
	fprintf(stderr,"\nattributes:\n");
	for (j=0 ; j<EATTR_BITS ; j++) {
		if (eattrtab[j][0]) {
			fprintf(stderr," %s - %s\n",eattrtab[j],eattrdesc[j]);
		}
	}
}

int32_t ext_eattr_search(char *optarg_value) {
	uint32_t result=0;
	uint8_t i,bitoffset=0;
	uint8_t match,found;
	const char *all, *tabstr, *currpos=optarg_value;
	char c;

	while (*currpos) {
		while (*currpos==',' || *currpos==' ') {
			currpos++;
		}
		if (*currpos=='-') {
			bitoffset = 16;
			currpos++;
		} else if (*currpos=='+') {
			bitoffset = 8;
			currpos++;
		} else {
			bitoffset = 0;
		}
		found = 0;
		for (i=0 ; i<EATTR_BITS; i++) {
			all = currpos;
			tabstr = eattrtab[i];
			match = 1;
			while (match && (c=*all) && c!=',') {
				if (tolower(*all) != tolower(*tabstr)) {
					match = 0;
					break;
				}
				all++;
				tabstr++;
			}
			if (match) {
				if (found!=0) {
					fprintf(stderr,"ambiguous abbreviation found\n");
					return -1;
				} else {
					found = i+1;
				}
			}
		}
		if (found==0) {
			fprintf(stderr,"no such eattr\n");
			return -1;
		}
		bitoffset += (found-1);
		if ((result|(result>>8)|(result>>16)) & (1<<bitoffset)) {
			fprintf(stderr,"repeated eattr\n");
			return -1;
		}
		result |= 1<<bitoffset;
		while ((c=*currpos) && c!=',') {
			currpos++;
		}
	}
	//printf("parse eattr result: %08X\n\n", result);
	return result;
}

int print_pattern(pattern *pat) {
	printf("pattern: %s, ", pat->glob_name);

	if (pat->euid == PATTERN_EUGID_ANY) {
		printf("euid: ANY, ");
	} else {
		printf("euid: %"PRIu32", ", pat->euid);
	}

	if (pat->egid == PATTERN_EUGID_ANY) {
		printf("egid: ANY, ");
	} else {
		printf("egid: %"PRIu32", ", pat->egid);
	}
	printf("priority: %d", pat->priority);

	if (pat->omask & PATTERN_OMASK_SCLASS) {
		printf(", storage class: %s",pat->sclass_name);
	}

	if (pat->omask & PATTERN_OMASK_TRASHRETENTION) {
		char tbuff[20];
		snprint_hperiod(tbuff,20,pat->trashretention);
		tbuff[19]='\0';
		printf(", trash retention: %"PRIu16" (%s)",pat->trashretention,tbuff);
	}

	if (pat->omask & PATTERN_OMASK_EATTR) {
		uint8_t i, addcoma=0;
		printf(", eattr: ");
		i = EATTR_BITS;
		while (i--) {
			if (pat->seteattr & (1<<i)) {
				if (addcoma) {
					printf(",");
				}
				addcoma = 1;
				printf("+%s", eattrtab[i]);
			} else if (pat->clreattr & (1<<i)) {
				if (addcoma) {
					printf(",");
				}
				addcoma = 1;
				printf("-%s", eattrtab[i]);
			}
		}
	}
	printf("\n");
	return 0;
}

int add_pattern(const char *mfsmp, pattern *pat) {
	int32_t	leng;
	char cwdbuff[MAXPATHLEN];

	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	ps_comminit();
	ps_put8(0);							//fver
	ps_putstr(pat->glob_name);
	ps_put32(pat->euid);
	ps_put32(pat->egid);
	ps_put8(pat->priority);
	ps_put8(pat->omask);
	ps_putstr(pat->sclass_name);
	ps_put16(pat->trashretention);
	ps_put8(pat->seteattr);
	ps_put8(pat->clreattr);

	if ((leng = ps_send_and_receive(CLTOMA_PATTERN_ADD, MATOCL_PATTERN_ADD, NULL)) < 0) {
		goto error;
	}

	if (leng != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}
	uint8_t status;
	if ((status=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"pattern add '%s' error: %s\n", pat->glob_name, mfsstrerr(status));
		goto error;
	}
//OK:
	printf("pattern added:\n");
	print_pattern(pat);
	return 0;

error:
	reset_master();
	return -1;
}


int del_pattern(const char *mfsmp, pattern *pat) {
	int32_t	leng;
	char cwdbuff[MAXPATHLEN];

	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	if ((leng=strlen(pat->glob_name))==0 || leng>255) {
		fprintf(stderr,"improper pattern name\n");
		return -1;
	}

	ps_comminit();
	ps_put8(0);							//fver
	ps_putstr(pat->glob_name);
	ps_put32(pat->euid);
	ps_put32(pat->egid);

	if ((leng = ps_send_and_receive(CLTOMA_PATTERN_DELETE, MATOCL_PATTERN_DELETE, NULL)) < 0) {
		goto error;
	}

	uint8_t status;
	if (leng != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}
	if ((status=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"pattern delete '%s' error: %s\n", pat->glob_name, mfsstrerr(status));
		goto error;
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}

int list_pattern(const char *mfsmp) {
	uint8_t tmp8u;
	int32_t	leng;
	pattern pat;
	char cwdbuff[MAXPATHLEN];

	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	ps_comminit();
	ps_put8(0);

	if ((leng = ps_send_and_receive(CLTOMA_PATTERN_LIST, MATOCL_PATTERN_LIST, NULL)) < 0) {
		goto error;
	}

	if (leng==0) {
		printf("no patterns found\n");
	}

	while (leng>0) {
		if (!(tmp8u=ps_getstr(pat.glob_name))) { //empty name or reading error
			fprintf(stderr,"master query: wrong answer (glob name)\n");
			goto error;
		}
		leng -= tmp8u+1;
		if (leng<11) {
			fprintf(stderr,"master query: wrong answer (leng)\n");
			goto error;
		}
		pat.euid = ps_get32();
		pat.egid = ps_get32();
		pat.priority = ps_get8();
		pat.omask = ps_get8();
		leng -= 10;
		if (!(tmp8u=ps_getstr(pat.sclass_name))) { //empty name or reading error
			if (pat.omask & PATTERN_OMASK_SCLASS) {
				fprintf(stderr,"master query: wrong answer (sclass name)\n");
				goto error;
			}
		}
		leng -= tmp8u+1;
		if (leng<3) {
			goto error;
		}
		pat.trashretention = ps_get16();
		pat.seteattr = ps_get8();
		pat.clreattr = ps_get8();
		leng -= 4;
		if (ps_status()) {
			fprintf(stderr,"master query: wrong answer (end of stream)\n");
			goto error;
		}
		print_pattern(&pat);
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *createpattxt[] = {
	"create new pattern",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] -n pattern [-u uid] [-g gid] [-p priority] [-c class] [-t trash_retention] [-f eattr]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	" -n pattern - pattern that has to match (new or renamed) file name",
	" -u uid - optional: uid filter (if not present, uid will be ignored)",
	" -g gid - optional: gid filter (if not present, gid will be ignored)",
	" -p priority - optional: matching priority (0 to 255, higher matches first, default 128)",
	" -c class - optional: storage class to set if file name matches",
	" -t trash_retention - optional: trash retention to set if file name matches (see below)",
	" -f eattr - optional: extra attributes to change if file name matches, comma separated list, + means set, - means clear.",
	"",
	"Trash retention can be defined as a number of hours (integer) or a time period in one of two possible formats:",
	"first format: #.#T where T is one of: h-hours, d-days or w-weeks; fractions of hours will be rounded to full hours",
	"second format: #w#d#h, any number of definitions can be omitted, but the remaining definitions must be in order (so #w#h is still a valid definition, but #d#w is not);",
	"ranges: h: 0 to 23, d: 0 to 6, w is unlimited and the first definition is also always unlimited (i.e. for #d#h d will be unlimited)",
	NULL
};

static const char *deletepattxt[] = {
	"delete specified pattern",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] -n pattern [-u uid] [-g gid]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	" -n pattern - defined pattern to be removed",
	" -u uid - uid filter of pattern to be removed (if specified)",
	" -g gid - gid filter of pattern to be removed (if specified)",
	NULL
};

static const char *listpattxt[] = {
	"list all defined patterns",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

void createpatusage(void) {
	tcomm_print_help(createpattxt);
	print_eattr();
	exit(1);
}

void deletepatusage(void) {
	tcomm_print_help(deletepattxt);
	exit(1);
}

void listpatusage(void) {
	tcomm_print_help(listpattxt);
	exit(1);
}

int pattern_get_number(char *optarg_value,uint32_t *num) {
	const char *p;
	char c;
	uint64_t v;
	p = optarg_value;
	v = 0;
	while ((c=*p)!='\0') {
		if (c>='0' && c<='9') {
			v*=10;
			v+=c-'0';
			if (v>UINT32_MAX) {
				return -1;
			}
		} else {
			return -1;
		}
		p++;
	}
	*num = v;
	return 0;
}

int pattern_parse_options(int option,char *optarg_value,pattern *pat) {
	struct passwd pwd, *presult;
	struct group grp, *gresult;
	char pwgrbuff[16384];
	uint32_t v;

	switch (option) {
		case 'n':
			if (pat->glob_name[0]) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (strlen(optarg_value) >= MAXPATTERNLENG) {
				fprintf(stderr,"glob definition too long\n");
				return -1;
			}
			strcpy(pat->glob_name, optarg_value);
			break;
		case 'u':
			if (pat->euid != PATTERN_EUGID_ANY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (pattern_get_number(optarg_value,&(pat->euid))<0) {
				if (getpwnam_r(optarg_value, &pwd, pwgrbuff, 16384, &presult) !=0 ) {
					presult = NULL;
				}
				if (presult == NULL) {
					printf("can't find user named '%s'\n", optarg_value);
					return -1;
				}
				pat->euid = pwd.pw_uid;
			}
			break;
		case 'g':
			if (pat->egid != PATTERN_EUGID_ANY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (pattern_get_number(optarg_value,&(pat->egid))<0) {
				if (getgrnam_r(optarg_value, &grp, pwgrbuff, 16384, &gresult) !=0 ) {
					gresult = NULL;
				}
				if (gresult == NULL) {
					printf("can't find group named '%s'\n", optarg_value);
					return -1;
				}
				pat->egid = grp.gr_gid;

			}
			break;
		case 'p':
			if (pat->priority != PATTERN_DEF_PRIORITY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (pattern_get_number(optarg_value,&v)<0) {
				fprintf(stderr,"wrong priority value\n");
				return -1;
			}
			if (v > 255) {
				fprintf(stderr,"priority value too big\n");
				return -1;
			}
			pat->priority = v;
			break;
		case 'c':
			if (pat->omask & PATTERN_OMASK_SCLASS) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (strlen(optarg_value)>=MAXSCLASSNAMELENG) {
				fprintf(stderr,"sclass name too long\n");
				return -1;
			}
			strcpy(pat->sclass_name, optarg_value);
			pat->omask |= PATTERN_OMASK_SCLASS;
			break;
		case 't':
			if (pat->omask & PATTERN_OMASK_TRASHRETENTION) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (tparse_hours(optarg_value,&(pat->trashretention))<0) {
				fprintf(stderr,"bad trash retention\n");
				return -1;
			}
			pat->omask |= PATTERN_OMASK_TRASHRETENTION;
			break;
		case 'f':
			if (! (v = ext_eattr_search(optarg_value)) || v > 0xFFFFFF) {
				fprintf(stderr,"unknown flag\n");
				return -1;
			}
			if (v&0xFF) {
				fprintf(stderr,"eattr can only be set or cleared by matching to the pattern (use '+' or '-' prefix)\n");
				return -1;
			}
			pat->seteattr |= (v>>8)&0xFF;
			pat->clreattr |= (v>>16)&0xFF;
			pat->omask |= PATTERN_OMASK_EATTR;
			if ((pat->seteattr&pat->clreattr)) {
				fprintf(stderr,"set_eattr and clr_eattr sets with the same eattr\n");
				return -1;
			}
			break;
	}
	return 0;
}

int createpatexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	pattern pat;
	char *mountpoint;

	mountpoint = NULL;
	memset(&pat,0,sizeof(pattern));
	pat.euid = PATTERN_EUGID_ANY;
	pat.egid = PATTERN_EUGID_ANY;
	pat.priority = PATTERN_DEF_PRIORITY;
	pat.trashretention = PATTERN_DEF_RETENTION;
	pat.seteattr = PATTERN_DEF_EATTR;
	pat.clreattr = PATTERN_DEF_EATTR;

	while ((ch=getopt(argc,argv,"?M:n:u:g:p:c:t:f:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'n':
			case 'u':
			case 'g':
			case 'p':
			case 'c':
			case 't':
			case 'f':
				if (pattern_parse_options(ch,optarg,&pat)<0) {
					createpatusage();
				}
				break;
			default:
				createpatusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc>0 || pat.glob_name[0]==0) {
		createpatusage();
		return 1;
	}

	if (add_pattern(mountpoint,&pat)<0) {
		status = 1;
	}

	return status;
}

int deletepatexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	pattern pat;
	char *mountpoint;

	mountpoint = NULL;
	memset(&pat,0,sizeof(pattern));
	pat.euid = PATTERN_EUGID_ANY;
	pat.egid = PATTERN_EUGID_ANY;

	while ((ch=getopt(argc,argv,"?M:n:u:g:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'n':
			case 'u':
			case 'g':
				if (pattern_parse_options(ch,optarg,&pat)<0) {
					deletepatusage();
				}
				break;
			default:
				deletepatusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc>0 || pat.glob_name[0]==0) {
		deletepatusage();
		return 1;
	}

	if (del_pattern(mountpoint,&pat)<0) {
		status = 1;
	}

	return status;
}

int listpatexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;

	mountpoint = NULL;

	while ((ch=getopt(argc,argv,"?M:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			default:
				listpatusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc>0) {
		listpatusage();
		return 1;
	}

	if (list_pattern(mountpoint)<0) {
		status = 1;
	}

	return status;
}

static command commandlist[] = {
	{"create | make | add", createpatexe},
	{"delete | remove", deletepatexe},
	{"list", listpatexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
