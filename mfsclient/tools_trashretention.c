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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <unistd.h>

#include "libmfstools.h"

//TRASH RETENTION MANAGEMENT TOOLS

int get_trashretention(const char *fname,uint32_t *trashretention, uint8_t mode) {
	uint32_t inode;
	uint32_t fn,dn,i;
	uint32_t cnt;
	int32_t leng;
	char tbuff[20];

	if (open_master_conn(fname,&inode,NULL,NULL,0,0) < 0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETTRASHRETENTION, MATOCL_FUSE_GETTRASHRETENTION, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(ps_get8()));
		goto error;
	} else if (leng<8 || leng%8!=0) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	} else if (mode==GMODE_NORMAL && leng!=16) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}

	if (mode==GMODE_NORMAL) {
		fn = ps_get32();
		dn = ps_get32();
		*trashretention = ps_get32()/3600;
		cnt = ps_get32();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			fprintf(stderr,"%s: master query: wrong answer (fn,dn)\n",fname);
			goto error;
		}
		if (cnt!=1) {
			fprintf(stderr,"%s: master query: wrong answer (cnt)\n",fname);
			goto error;
		}
		snprint_hperiod(tbuff,20,*trashretention);
		tbuff[19]='\0';
		printf("%s: %"PRIu32" (%s)\n",fname,*trashretention,tbuff);
	} else {
		fn = ps_get32();
		dn = ps_get32();
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			*trashretention = ps_get32()/3600;
			snprint_hperiod(tbuff,20,*trashretention);
			tbuff[19]='\0';
			cnt = ps_get32();
			printf(" files with trash retention   %10"PRIu32" (%s) :",*trashretention,tbuff);
			print_number_desc(" ","\n",cnt,PNUM_32BIT);
		}
		for (i=0 ; i<dn ; i++) {
			*trashretention = ps_get32()/3600;
			snprint_hperiod(tbuff,20,*trashretention);
			tbuff[19]='\0';
			cnt = ps_get32();
			printf(" directories with trash retention %10"PRIu32" (%s) :",*trashretention,tbuff);
			print_number_desc(" ","\n",cnt,PNUM_32BIT);
		}
	}
//ok:
	return 0;

error:
	reset_master();
	return -1;
}

int set_trashretention(const char *fname,uint32_t trashretention,uint8_t mode) {
	uint32_t inode,uid;
	uint32_t changed,notchanged,notpermitted;
	int32_t leng;

	if (open_master_conn(fname,&inode,NULL,NULL,0,1) < 0) {
		return -1;
	}
	uid = getuid();

	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	ps_put32(trashretention*3600);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_SETTRASHRETENTION, MATOCL_FUSE_SETTRASHRETENTION, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=12) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}
	changed = ps_get32();
	notchanged = ps_get32();
	notpermitted = ps_get32();
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			char tbuff[20];
			snprint_hperiod(tbuff,20,trashretention);
			tbuff[19]='\0';
			printf("%s: %"PRIu32" (%s)\n",fname,trashretention,tbuff);
		} else {
			printf("%s: trash retention not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);

		print_number_desc(" inodes with trash retention changed:     ","\n",changed,PNUM_32BIT);
		print_number_desc(" inodes with trash retention not changed: ","\n",notchanged,PNUM_32BIT);
		print_number_desc(" inodes with permission denied:           ","\n",notpermitted,PNUM_32BIT);
	}
//ok:
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *gettrashretentiontxt[] = {
	"get objects trash retention time (how long a file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *settrashretentiontxt[] = {
	"set objects trash retention time (how long a file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] TIME[-|+] object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	"",
	" TIME+ - increase trashretention to given value",
	" TIME- - decrease trashretention to given value",
	" TIME - just set trashretention to given value",
	"",
	"Trash retention (TIME) can be defined as a number of hours (integer) or a time period in one of two possible formats:",
	"first format: #.#T where T is one of: h-hours, d-days or w-weeks; fractions of hours will be rounded to full hours",
	"second format: #w#d#h, any number of definitions can be ommited, but the remaining definitions must be in order (so #w#h is still a valid definition, but #d#w is not);",
	"ranges: h: 0 to 23, d: 0 to 6, w is unlimited and the first definition is also always unlimited (i.e. for #d#h d will be unlimited)",
	NULL
};

static const char *copytrashretentiontxt[] = {
	"copy objects trash retention time (how long a file should be left in trash)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] source_object object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

void gettrashretentionusage(void) {
	tcomm_print_help(gettrashretentiontxt);
	exit(1);
}

void settrashretentionusage(void) {
	tcomm_print_help(settrashretentiontxt);
	exit(1);
}

void copytrashretentionusage(void) {
	tcomm_print_help(copytrashretentiontxt);
	exit(1);
}

int gettrashretentionexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint32_t trashretention;

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
				gettrashretentionusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		gettrashretentionusage();
		return 1;
	}

	while (argc>0) {
		if (get_trashretention(*argv, &trashretention, rflag ? GMODE_RECURSIVE : GMODE_NORMAL)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int settrashretentionexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	int smode = SMODE_SET;
	char *p;
	uint16_t trtmp;
	uint32_t trashretention = 0;

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
				settrashretentionusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		settrashretentionusage();
		return 1;
	}

	p = argv[0];

	if (*p) {
		while (*p) {
			p++;
		}
		p--;
		if (*p=='+') {
			smode = SMODE_INCREASE;
			*p = '\0';
		}
		if (*p=='-') {
			smode = SMODE_DECREASE;
			*p = '\0';
		}
	}
	if (tparse_hours(argv[0],&trtmp)<0) {
		fprintf(stderr,"bad trashretention\n");
		settrashretentionusage();
		return 1;
	}
	trashretention = trtmp;

	argc--;
	argv++;

	while (argc>0) {
		//same for: MFSSETTRASHRETENTION, MFSCOPYTRASHRETENTION
		if (set_trashretention(*argv,trashretention,(rflag)?(smode | SMODE_RMASK):smode)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int copytrashretentionexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint32_t trashretention;

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
				copytrashretentionusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		copytrashretentionusage();
		return 1;
	}

	if (get_trashretention(*argv, &trashretention, GMODE_NORMAL)<0) {
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		if (set_trashretention(*argv, trashretention, SMODE_SET|((rflag) ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

static command commandlist[] = {
	{"get", gettrashretentionexe},
	{"set", settrashretentionexe},
	{"copy", copytrashretentionexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
