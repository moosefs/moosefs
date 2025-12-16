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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <unistd.h>

#include "libmfstools.h"

//TRASH RETENTION MANAGEMENT TOOLS
/* formats:
#     - number of seconds
#s    - number of seconds
#.#m  - number of minutes
#.#h  - number of hours
#.#d  - number of days
#.#w  - number of weeks */
static inline
uint32_t parse_period_old(char *str,char **endpos) {
	double base;
	double divisor;
	base = 0.0;
	while ((*str)>='0' && (*str)<='9') {
		base *= 10.0;
		base += (*str)-'0';
		str++;
	}
	if ((*str)=='.') {
		divisor = 0.1;
		str++;
		while ((*str)>='0' && (*str)<='9') {
			base += ((*str)-'0')*divisor;
			divisor /= 10.0;
			str++;
		}
	}
	while ((*str)==' ') {
		str++;
	}
	if ((*str)=='s') {
		str++;
	} else if ((*str)=='m') {
		str++;
		base *= 60.0;
	} else if ((*str)=='h') {
		str++;
		base *= 3600.0;
	} else if ((*str)=='d') {
		str++;
		base *= 86400.0;
	} else if ((*str)=='w') {
		str++;
		base *= 604800.0;
	}
	*endpos = str;
	if (base >= UINT32_MAX) {
		return UINT32_MAX;
	}
	if (base <= 0) {
		return 0;
	}
	return base;
}

int get_trashtime(const char *fname, uint32_t *trashtime, uint8_t mode) {
	uint32_t inode;
	int32_t leng;
	uint32_t fn,dn,i;
	uint32_t cnt;

	if (open_master_conn(fname, &inode, NULL, NULL, 0, 0) < 0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETTRASHRETENTION, MATOCL_FUSE_GETTRASHRETENTION, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng<8 || leng%8!=0) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n", fname);
		goto error;
	} else if (mode==GMODE_NORMAL && leng!=16) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n", fname);
		goto error;
	}

	if (mode==GMODE_NORMAL) {
		fn         = ps_get32();
		dn         = ps_get32();
		*trashtime = ps_get32();
		cnt        = ps_get32();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			fprintf(stderr,"%s: master query: wrong answer (fn,dn)\n", fname);
			goto error;
		}
		if (cnt!=1) {
			fprintf(stderr,"%s: master query: wrong answer (cnt)\n", fname);
			goto error;
		}
		printf("%s: %"PRIu32"\n",fname,*trashtime);
	} else {	//(mode!=GMODE_NORMAL)
		fn = ps_get32();
		dn = ps_get32();
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			*trashtime = ps_get32();
			cnt        = ps_get32();
			printf(" files with trashtime        %10"PRIu32" :", *trashtime);
			print_number_desc(" ","\n",cnt,PNUM_32BIT);
		}
		for (i=0 ; i<dn ; i++) {
			*trashtime = ps_get32();
			cnt        = ps_get32();
			printf(" directories with trashtime  %10"PRIu32" :", *trashtime);
			print_number_desc(" ","\n",cnt,PNUM_32BIT);
		}
	}
//OK:
	return 0;

error:
	reset_master();
	return -1;
}

int set_trashtime(const char *fname,uint32_t trashtime,uint8_t mode) {
	uint32_t inode,uid;
	int32_t leng;
	uint32_t changed,notchanged,notpermitted;

	if (open_master_conn(fname,&inode,NULL,NULL,0,1) < 0) {
		return -1;
	}
	uid = getuid();

	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	ps_put32(trashtime);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_SETTRASHRETENTION, MATOCL_FUSE_SETTRASHRETENTION, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=12) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}
	changed      = ps_get32();
	notchanged   = ps_get32();
	notpermitted = ps_get32();
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %"PRIu32"\n", fname, trashtime);
		} else {
			printf("%s: trashtime not changed\n", fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number_desc(" inodes with trashtime changed:     ","\n", changed, PNUM_32BIT);
		print_number_desc(" inodes with trashtime not changed: ","\n", notchanged, PNUM_32BIT);
		print_number_desc(" inodes with permission denied:     ","\n", notpermitted, PNUM_32BIT);
	}
//Ok:
	return 0;

error:
	reset_master();
	return -1;
}

//---------------------------------------------------------------------

static const char *gettrashtimetxt[] = {
	"get objects trashtime (how many seconds file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *settrashtimetxt[] = {
	"set objects trashtime (how many seconds file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] seconds[-|+] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	"",
	" SECONDS+ - increase trashtime to given value",
	" SECONDS- - decrease trashtime to given value",
	" SECONDS - just set trashtime to given value",
	NULL
};

static const char *copytrashtimetxt[] = {
	"copy objects trashtime (how many seconds file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] source_object object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

void gettrashtimeusage(void) {
	tcomm_print_help(gettrashtimetxt);
	exit(1);
}

void settrashtimeusage(void) {
	tcomm_print_help(settrashtimetxt);
	exit(1);
}

void copytrashtimeusage(void) {
	tcomm_print_help(copytrashtimetxt);
	exit(1);
}

int gettrashtimeexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint32_t trashtime;

	printf("mfsgettrashtime is deprecated, use mfsgettrashretention instead\n");

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
			case 'r':
				rflag = 1;
				break;
			default:
				gettrashtimeusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		gettrashtimeusage();
		return 1;
	}

	while (argc>0) {
		if (get_trashtime(*argv, &trashtime, rflag ? GMODE_RECURSIVE : GMODE_NORMAL)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int settrashtimeexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	int smode = SMODE_SET;
	char *p;
	uint32_t trashtime = 0;

	printf("mfssettrashtime is deprecated, use mfssettrashretention instead\n");

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
			case 'r':
				rflag = 1;
				break;
			default:
				settrashtimeusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		settrashtimeusage();
		return 1;
	}

	p = argv[0];

	if (p[0]>='0' && p[0]<='9') {
		trashtime = parse_period_old(p,&p);
	}
	while (p[0]==' ') {
		p++;
	}
	if (p[0]=='\0' || ((p[0]=='-' || p[0]=='+') && p[1]=='\0')) {
		if (p[0]=='-') {
			smode=SMODE_DECREASE;
		} else if (p[0]=='+') {
			smode=SMODE_INCREASE;
		}
	} else {
		fprintf(stderr,"trashtime should be given as number of seconds optionally followed by '-' or '+'\n");
		settrashtimeusage();
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		//same for: MFSSETTRASHRETENTION, MFSCOPYTRASHRETENTION
		if (set_trashtime(*argv,trashtime,(rflag)?(smode | SMODE_RMASK):smode)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int copytrashtimeexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint32_t trashtime;

	printf("mfscopytrashtime is deprecated, use mfscopytrashretention instead\n");

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
			case 'r':
				rflag = 1;
				break;
			default:
				copytrashtimeusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		copytrashtimeusage();
		return 1;
	}

	if (get_trashtime(*argv, &trashtime, GMODE_NORMAL)<0) {
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		if (set_trashtime(*argv, trashtime, SMODE_SET|((rflag) ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

static command commandlist[] = {
	{"get", gettrashtimeexe},
	{"set", settrashtimeexe},
	{"copy", copytrashtimeexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
