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
#include <string.h>
#include <unistd.h>

#include "libmfstools.h"

int get_sclass(const char *fname, uint8_t *goal, char storage_class_name[MAXSCLASSNAMELENG], uint8_t mode) {
	uint32_t inode;
	int32_t	leng;
	uint8_t fn,dn,i;
	uint8_t scnleng;
	uint32_t cnt;

	if (open_master_conn(fname, &inode, NULL, NULL, 0, 0) < 0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETSCLASS, MATOCL_FUSE_GETSCLASS, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname, mfsstrerr(ps_get8()));
		goto error;
	} else if (leng<2) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}
	//seems to be right...
	if (mode==GMODE_NORMAL) {
		fn = ps_get8();
		dn = ps_get8();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			fprintf(stderr,"%s: master query: wrong answer (fn,dn)\n",fname);
			goto error;
		}// else (fn==0 && dn==1) || (fn==1 && dn==0)
		*goal = ps_get8();
		if (*goal==0) {
			fprintf(stderr,"%s: unsupported data format (upgrade master)\n",fname);
			goto error;
		} else if (*goal==0xFF) {
			scnleng = ps_get8();
			ps_getbytes((uint8_t*)storage_class_name, scnleng);
			storage_class_name[scnleng]=0;
		}
		cnt = ps_get32();
		if (cnt != 1) {
			fprintf(stderr,"%s: master query: wrong answer (cnt)\n",fname);
			goto error;
		}
		if (*goal==0xFF) {
			printf("%s: %s\n",fname,storage_class_name);
		} else {
			printf("%s: %"PRIu8"\n",fname,*goal);
		}
	} else {//mode != GMODE_NORMAL
		fn = ps_get8();
		dn = ps_get8();
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			*goal = ps_get8();
			if (*goal==0) {
				fprintf(stderr,"%s: unsupported data format (upgrade master)\n",fname);
				goto error;
			} else if (*goal==0xFF) {
				scnleng = ps_get8();
				ps_getbytes((uint8_t*)storage_class_name, scnleng);
				storage_class_name[scnleng]=0;
			}
			cnt = ps_get32();
			if (*goal==0xFF) {
				printf ("       files with storage class : %s :",storage_class_name);
			} else {
				printf ("       files with goal          : %"PRIu8, *goal);
			}
			print_number_desc("\n                          count : ","\n",cnt,PNUM_32BIT);
		}
		for (i=0 ; i<dn ; i++) {
			*goal = ps_get8();
			if (*goal==0) {
				fprintf(stderr,"%s: unsupported data format (upgrade master)\n",fname);
				goto error;
			} else if (*goal==0xFF) {
				scnleng = ps_get8();
				ps_getbytes((uint8_t*)storage_class_name, scnleng);
				storage_class_name[scnleng]=0;
			}
			cnt = ps_get32();
			if (*goal==0xFF) {
				printf (" directories with storage class : %s :",storage_class_name);
			} else {
				printf (" directories with goal          : %"PRIu8, *goal);
			}
			print_number_desc("\n                          count : ","\n",cnt,PNUM_32BIT); //32*SPACE
		}
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}


int set_sclass(const char *fname,uint8_t goal,const char src_storage_class_name[MAXSCLASSNAMELENG],const char storage_class_name[MAXSCLASSNAMELENG],uint8_t mode) {
	uint8_t nleng,snleng;
	uint32_t inode, uid;
	int32_t leng;
	uint32_t changed, notchanged, notpermitted, quotaexceeded;

	nleng = strlen(storage_class_name);
	if ((mode&SMODE_TMASK)==SMODE_EXCHANGE) {
		snleng = strlen(src_storage_class_name);
	} else {
		snleng = 0;
	}

	if (goal==0 || (goal>9 && goal<0xFF)) {
		printf("%s: set storage class unsupported mode (internal error)\n",fname);
		return -1;
	}

	if (open_master_conn(fname, &inode,NULL,NULL,0,1) < 0) {
		return -1;
	}
	if (goal==0xFF) {
		if (getmasterversion()<VERSION2INT(3,0,75)) {
			printf("%s: storage classes not supported (master too old)\n",fname);
			goto error;
		}
	}
	uid = getuid();

	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	ps_put8(goal);
	ps_put8(mode);
	if (goal==0xFF) {
		if ((mode&SMODE_TMASK)==SMODE_EXCHANGE) {
			ps_put8(snleng);
			ps_putbytes((uint8_t*)src_storage_class_name, snleng);
		}
		ps_put8(nleng);
		ps_putbytes((uint8_t*)storage_class_name,nleng);
	}
	if ((leng = ps_send_and_receive(CLTOMA_FUSE_SETSCLASS, MATOCL_FUSE_SETSCLASS, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=12 && leng!=16) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}

	changed		 = ps_get32();
	notchanged	 = ps_get32();
	notpermitted = ps_get32();
	if (leng==16) {
		quotaexceeded = ps_get32();
	} else {
		quotaexceeded = 0;
	}
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			if (goal==0xFF) {
				printf("%s: %s\n",fname,storage_class_name);
			} else {
				printf("%s: %"PRIu8"\n",fname,goal);
			}
		} else {
			if (goal==0xFF) {
				printf("%s: storage class not changed\n",fname);
			} else {
				printf("%s: goal not changed\n",fname);
			}
		}
	} else {
		printf("%s:\n",fname);
		if (goal==0xFF) {
			print_number_desc(" inodes with storage class changed:     ","\n", changed,     PNUM_32BIT);
			print_number_desc(" inodes with storage class not changed: ","\n", notchanged,  PNUM_32BIT);
		} else {
			print_number_desc(" inodes with goal changed:              ","\n", changed,     PNUM_32BIT);
			print_number_desc(" inodes with goal not changed:          ","\n", notchanged,  PNUM_32BIT);
		}
		print_number_desc(    " inodes with permission denied:         ","\n", notpermitted, PNUM_32BIT);
		if (leng==16) {
			print_number_desc(" inodes with quota exceeded:            ","\n", quotaexceeded,PNUM_32BIT);
		}
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *getsclasstxt[] = {
	"get objects storage class (desired number of copies / labels)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *setsclasstxt[] = {
	"set objects storage class (desired number of copies / labels)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] sclass_name object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *copysclasstxt[] = {
	"copy object storage class (desired number of copies / labels)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] source_object object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *xchgsclasstxt[] = {
	"exchange objects storage class (desired number of copies / labels)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] src_sclass_name dst_sclass_name object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

void getsclassusage(void) {
	tcomm_print_help(getsclasstxt);
	exit(1);
}

void setsclassusage(void) {
	tcomm_print_help(setsclasstxt);
	exit(1);
}

void copysclassusage(void) {
	tcomm_print_help(copysclasstxt);
	exit(1);
}

void xchgsclassusage(void) {
	tcomm_print_help(xchgsclasstxt);
	exit(1);
}

int getsclassexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	char storage_class_name[MAXSCLASSNAMELENG];
	uint8_t goal;

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
				getsclassusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		getsclassusage();
		return 1;
	}

	while (argc>0) {
		if (get_sclass(*argv, &goal, storage_class_name, rflag ? GMODE_RECURSIVE : GMODE_NORMAL)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int setsclassexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	char *p;
	int scl;
	char storage_class_name[MAXSCLASSNAMELENG];

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
				setsclassusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		setsclassusage();
		return 1;
	}

	p = argv[0];
	scl = strlen(p);
	if (scl>=MAXSCLASSNAMELENG) {
		printf("%s: storage class name too long\n", argv[0]);
		setsclassusage();
		return 1;
	}

	memcpy(storage_class_name,p,scl+1);

	argc--;
	argv++;

	while (argc>0) {
		if (set_sclass(*argv, 0xFF, storage_class_name, storage_class_name, SMODE_SET | (rflag ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int copysclassexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	char storage_class_name[MAXSCLASSNAMELENG];
	uint8_t goal;

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
				copysclassusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		copysclassusage();
		return 1;
	}

	if (get_sclass(*argv, &goal, storage_class_name, GMODE_NORMAL)<0) {
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		if (set_sclass(*argv, goal, storage_class_name, storage_class_name, SMODE_SET | (rflag ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int xchgsclassexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	char *p;
	int scl;
	char src_storage_class_name[MAXSCLASSNAMELENG];
	char dst_storage_class_name[MAXSCLASSNAMELENG];

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
				xchgsclassusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=2) {
		xchgsclassusage();
		return 1;
	}

	p = argv[0];
	scl = strlen(p);
	if (scl>=MAXSCLASSNAMELENG) {
		printf("%s: storage class name too long\n", p);
		xchgsclassusage();
		return 1;
	}

	memcpy(src_storage_class_name,p,scl+1);

	argc--;
	argv++;

	p = argv[0];

	scl = strlen(p);
	if (scl>=MAXSCLASSNAMELENG) {
		printf("%s: storage class name too long\n", p);
		xchgsclassusage();
		return 1;
	}

	memcpy(dst_storage_class_name,p,scl+1);

	argc--;
	argv++;

	while (argc>0) {
		if (set_sclass(*argv, 0xFF, src_storage_class_name, dst_storage_class_name, SMODE_SET | SMODE_EXCHANGE | (rflag ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

static command commandlist[] = {
	{"get", getsclassexe},
	{"set", setsclassexe},
	{"copy", copysclassexe},
	{"xchg", xchgsclassexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
