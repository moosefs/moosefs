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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "libmfstools.h"

// #define QUOTA_TOTAL 1

static inline void print_aligned(uint8_t tsize,const char *full,const char *partial,const char *abrev) {
	uint8_t txtsize;
	uint8_t spaces;
	const char *txt;
	txtsize = strlen(full);
	if (tsize>=txtsize) {
		spaces = tsize - txtsize;
		txt = full;
	} else {
		txtsize = strlen(partial);
		if (tsize>=txtsize) {
			spaces = tsize - txtsize;
			txt = partial;
		} else {
			txtsize = strlen(abrev);
			if (tsize>=txtsize) {
				spaces = tsize - txtsize;
				txt = abrev;
			} else {
				spaces = tsize;
				txt = "";
			}
		}
	}
	while (spaces) {
		putchar(' ');
		spaces--;
	}
	printf("%s",txt);
}

static inline void print_percent(uint64_t current,uint64_t quota,uint8_t valid) {
	if (valid) {
		double perc;
		perc = current;
		perc *= 100.0;
		perc /= quota;
		if (perc>999.99) {
			printf(">999.99");
		} else {
			printf(" %6.2lf",perc);
		}
	} else {
		printf("      -");
	}
}

typedef struct _quota {
	uint8_t qflags;
	uint32_t graceperiod;
	uint32_t sinodes;
	uint64_t slength;
	uint64_t ssize;
	uint64_t srealsize;
	uint32_t hinodes;
	uint64_t hlength;
	uint64_t hsize;
	uint64_t hrealsize;
} quota;

#define QUOTA_SET 0
#define QUOTA_DEL 1
#define QUOTA_GET 2

int quota_control(const char *fname, uint8_t mode, quota *q) {
	uint32_t inode;
	uint8_t tsize;
	int32_t leng;
	uint8_t defaultgp;
	uint32_t curinodes;
	uint64_t curlength,cursize,currealsize;
	char graceperiodstr[20];
	int conn_status;

//	printf("set quota: %s (soft:%1X,i:%"PRIu32",l:%"PRIu64",w:%"PRIu64",r:%"PRIu64"),(hard:%1X,i:%"PRIu32",l:%"PRIu64",w:%"PRIu64",r:%"PRIu64")\n",fname,sflags,sinodes,slength,ssize,srealsize,hflags,hinodes,hlength,hsize,hrealsize);
	if (mode==QUOTA_GET) {
		q->qflags = 0;
	}

#ifdef QUOTA_TOTAL
	if (strcmp(fname,"*")==0) {
		conn_status = open_master_conn(".", NULL, NULL, NULL, 0, (q->qflags)?1:0);
		inode = 0;
	} else {
#endif
		conn_status = open_master_conn(fname, &inode, NULL, NULL, 0, (q->qflags)?1:0);
#ifdef QUOTA_TOTAL
	}
#endif

	if (conn_status<0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put8(q->qflags);
	if (mode==QUOTA_SET) {
		if (getmasterversion()<VERSION2INT(4,51,0)) { // older master can't understand "QUOTA_PERIOD_DONT_CHANGE" - just set to default
			if (q->graceperiod==QUOTA_PERIOD_DONT_CHANGE) {
				q->graceperiod = QUOTA_PERIOD_DEFAULT;
			}
		}
		if (getmasterversion()>=VERSION2INT(3,0,9)) {
			ps_put32(q->graceperiod);
		}
		ps_put32(q->sinodes);
		ps_put64(q->slength);
		ps_put64(q->ssize);
		ps_put64(q->srealsize);
		ps_put32(q->hinodes);
		ps_put64(q->hlength);
		ps_put64(q->hsize);
		ps_put64(q->hrealsize);
	}

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_QUOTACONTROL, MATOCL_FUSE_QUOTACONTROL, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=85 && leng!=89 && leng!=90) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}

	q->qflags = ps_get8();
	if (leng>=90) {
		defaultgp = ps_get8();
	} else {
		defaultgp = 0;
	}
	if (leng>=89) {
		q->graceperiod = ps_get32();
	} else {
		q->graceperiod = QUOTA_PERIOD_DEFAULT;
	}
	q->sinodes    = ps_get32();
	q->slength    = ps_get64();
	q->ssize      = ps_get64();
	q->srealsize  = ps_get64();
	q->hinodes    = ps_get32();
	q->hlength    = ps_get64();
	q->hsize      = ps_get64();
	q->hrealsize  = ps_get64();
	curinodes     = ps_get32();
	curlength     = ps_get64();
	cursize       = ps_get64();
	currealsize   = ps_get64();

	if ((q->graceperiod)!=QUOTA_PERIOD_DEFAULT) {
		snprint_speriod(graceperiodstr,20,q->graceperiod);
		graceperiodstr[19] = '\0';
		if (defaultgp) {
			printf("%s:\nsoft quota grace period: %s (default)\n", fname, graceperiodstr);
		} else {
			printf("%s:\nsoft quota grace period: %s\n", fname, graceperiodstr);
		}
	} else {
		printf("%s:\nsoft quota grace period: default\n", fname);
	}

	tsize = print_number_size(PNUM_NONE);
	printf("          | ");
	print_aligned(tsize,"current value","current","curr");
	printf(" | ");
	print_aligned(tsize,"soft quota","soft","s");
	printf(" | percent | ");
	print_aligned(tsize,"hard quota","hard","h");
	printf(" | percent |\n");
	print_number_quota(" inodes   | ",NULL,curinodes,PNUM_VALID);
	print_number_quota(" | "," | ",q->sinodes,((q->qflags)&QUOTA_FLAG_SINODES)?PNUM_VALID:PNUM_NONE);
	print_percent(curinodes,q->sinodes,(q->qflags&QUOTA_FLAG_SINODES));
	print_number_quota(" | "," | ",q->hinodes,((q->qflags)&QUOTA_FLAG_HINODES)?PNUM_VALID:PNUM_NONE);
	print_percent(curinodes,q->hinodes,(q->qflags&QUOTA_FLAG_HINODES));
	printf(" |\n");
	print_number_quota(" length   | ",NULL,curlength,PNUM_BYTES|PNUM_VALID);
	print_number_quota(" | "," | ",q->slength,((q->qflags)&QUOTA_FLAG_SLENGTH)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(curlength,q->slength,(q->qflags&QUOTA_FLAG_SLENGTH));
	print_number_quota(" | "," | ",q->hlength,((q->qflags)&QUOTA_FLAG_HLENGTH)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(curlength,q->hlength,(q->qflags&QUOTA_FLAG_HLENGTH));
	printf(" |\n");
	print_number_quota(" size     | ",NULL,cursize,PNUM_BYTES|PNUM_VALID);
	print_number_quota(" | "," | ",q->ssize,((q->qflags)&QUOTA_FLAG_SSIZE)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(cursize,q->ssize,(q->qflags&QUOTA_FLAG_SSIZE));
	print_number_quota(" | "," | ",q->hsize,((q->qflags)&QUOTA_FLAG_HSIZE)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(cursize,q->hsize,(q->qflags&QUOTA_FLAG_HSIZE));
	printf(" |\n");
	print_number_quota(" realsize | ",NULL,currealsize,PNUM_BYTES|PNUM_VALID);
	print_number_quota(" | "," | ",q->srealsize,((q->qflags)&QUOTA_FLAG_SREALSIZE)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(currealsize,q->srealsize,(q->qflags&QUOTA_FLAG_SREALSIZE));
	print_number_quota(" | "," | ",q->hrealsize,((q->qflags)&QUOTA_FLAG_HREALSIZE)?(PNUM_VALID|PNUM_BYTES):PNUM_NONE);
	print_percent(currealsize,q->hrealsize,(q->qflags&QUOTA_FLAG_HREALSIZE));
	printf(" |\n");

	if (defaultgp) { // set graceperiod for potential copy operation
		q->graceperiod = QUOTA_PERIOD_DEFAULT;
	}
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *getquotatxt[] = {
	"get quota for given directory (directories)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] directory [directory ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

static const char *setquotatxt[] = {
	"set quota for given directory (directories)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-p grace_period|-P] [-iI inodes] [-lL length] [-sS size] [-rR realsize] directory [directory ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	" -p - set grace period for soft quota",
	" -P - set grace period to master's default (this is default for a new quota if no option is set)",
	" -i/-I - set soft/hard limit for number of filesystem objects",
	" -l/-L - set soft/hard limit for sum of files lengths",
	" -s/-S - set soft/hard limit for sum of file sizes (chunk sizes)",
	" -r/-R - set soft/hard limit for estimated hdd usage (usually size multiplied by goal)",
	"",
	"Grace period can be defined as a number of seconds (integer) or a time period in one of two possible formats:",
	"first format: #.#T where T is one of: s-seconds, m-minutes, h-hours, d-days or w-weeks; fractions of seconds will be rounded to full seconds",
	"second format: #w#d#h#m#s, any number of definitions can be omitted, but the remaining definitions must be in order (so #d#m is still a valid definition, but #m#d is not);",
	"ranges: s,m: 0 to 59, h: 0 to 23, d: 0 to 6, w is unlimited and the first definition is also always unlimited (i.e. for #d#h#m d will be unlimited)",
	"",
	"All numbers can have decimal point and SI/IEC symbol prefix at the end",
	"decimal (SI): (k - 10^3 , M - 10^6 , G - 10^9 , T - 10^12 , P - 10^15 , E - 10^18)",
	"binary (IEC 60027): (Ki - 2^10 , Mi - 2^20 , Gi - 2^30 , Ti - 2^40 , Pi - 2^50 , Ei - 2^60 )",
	NULL
};

static const char *delquotatxt[] = {
	"delete quota for given directory (directories)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-iIlLsSrRaA] directory [directory ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	" -i/-I - delete inodes soft/hard quota",
	" -l/-L - delete length soft/hard quota",
	" -s/-S - delete size soft/hard quota",
	" -r/-R - delete real size soft/hard quota",
	" -a/-A - delete all soft/hard quotas",
	NULL
};

static const char *copyquotatxt[] = {
	"copy quota settings from one directory to another directory (directories)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] source_directory destination_directory [destination_directory ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

void getquotausage(void) {
	tcomm_print_help(getquotatxt);
	exit(1);
}

void setquotausage(void) {
	tcomm_print_help(setquotatxt);
	exit(1);
}

void delquotausage(void) {
	tcomm_print_help(delquotatxt);
	exit(1);
}

void copyquotausage(void) {
	tcomm_print_help(copyquotatxt);
	exit(1);
}

int getquotaexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	quota q;

	while ((ch=getopt(argc,argv,"?nhHkmgr"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			default:
				getquotausage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=0) {
		getquotausage();
		return 1;
	}

	while (argc>0) {
		if (quota_control(*argv, QUOTA_GET, &q)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int setquotaexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	uint64_t v;
	quota q;

	memset(&q,0,sizeof(q));
	q.graceperiod = QUOTA_PERIOD_DONT_CHANGE;

	if (getuid()) {
		fprintf(stderr,"only root can change quota\n");
		setquotausage();
	}

	while ((ch=getopt(argc,argv,"?nhHkmgPp:i:I:l:L:s:S:r:R:"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			case 'P':
				if (q.graceperiod!=QUOTA_PERIOD_DONT_CHANGE) {
					fprintf(stderr,"period defined more than once\n");
					setquotausage();
				}
				q.graceperiod = QUOTA_PERIOD_DEFAULT;
				break;
			case 'p':
				if (q.graceperiod!=QUOTA_PERIOD_DONT_CHANGE) {
					fprintf(stderr,"period defined more than once\n");
					setquotausage();
				}
				if (tparse_seconds(optarg,&(q.graceperiod))<0) {
					fprintf(stderr,"bad grace period\n");
					setquotausage();
				}
				if (q.graceperiod==QUOTA_PERIOD_DEFAULT || q.graceperiod==QUOTA_PERIOD_DONT_CHANGE) {
					fprintf(stderr,"wrong grace period\n");
					setquotausage();
				}
				break;
			case 'i':
				if (my_get_number(optarg, &v, UINT32_MAX, 0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					setquotausage();
				}
				q.sinodes = v;
				q.qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':
				if (my_get_number(optarg, &v, UINT32_MAX, 0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					setquotausage();
				}
				q.hinodes = v;
				q.qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad length limit\n");
					setquotausage();
				}
				q.slength = v;
				q.qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad length limit\n");
					setquotausage();
				}
				q.hlength = v;
				q.qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad size limit\n");
					setquotausage();
				}
				q.ssize = v;
				q.qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad size limit\n");
					setquotausage();
				}
				q.hsize = v;
				q.qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad real size limit\n");
					setquotausage();
				}
				q.srealsize = v;
				q.qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				if (my_get_number(optarg, &v, UINT64_MAX, 0)<0) {
					fprintf(stderr,"bad real size limit\n");
					setquotausage();
				}
				q.hrealsize = v;
				q.qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			default:
				setquotausage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=0) {
		setquotausage();
		return 1;
	}

	if (q.qflags==0 && q.graceperiod==QUOTA_PERIOD_DONT_CHANGE) {
		fprintf(stderr,"no quota options given, at least one is required\n");
		setquotausage();
		return 1;
	}

	while (argc>0) {
		if (quota_control(*argv,QUOTA_SET,&q)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

int delquotaexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	quota q;

	memset(&q,0,sizeof(q));

	if (getuid()) {
		fprintf(stderr,"only root can change quota\n");
		delquotausage();
	}

	while ((ch=getopt(argc,argv,"?nhHkmgiIlLsSrRaA"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			case 'i':
				q.qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':

				q.qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				q.qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				q.qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				q.qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				q.qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				q.qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				q.qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			case 'a':
				q.qflags |= QUOTA_FLAG_SALL;
				break;
			case 'A':
				q.qflags |= QUOTA_FLAG_HALL;
				break;
			default:
				delquotausage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=0) {
		delquotausage();
		return 1;
	}

	while (argc>0) {
		if (quota_control(*argv,QUOTA_DEL,&q)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

int copyquotaexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	quota q;

	while ((ch=getopt(argc,argv,"?nhHkmg"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			default:
				copyquotausage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		copyquotausage();
		return 1;
	}

	if (quota_control(*argv, QUOTA_GET, &q)<0) {
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		if (quota_control(*argv, QUOTA_SET, &q)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

static command commandlist[] = {
	{"get", getquotaexe},
	{"set", setquotaexe},
	{"del", delquotaexe},
	{"copy", copyquotaexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
