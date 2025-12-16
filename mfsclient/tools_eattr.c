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

#include "libmfstools.h"

//EXTRA ATTRIBUTES MANAGEMENT TOOLS

static const char
	*eattrtab[EATTR_BITS]={EATTR_STRINGS},
	*eattrdesc[EATTR_BITS]={EATTR_DESCRIPTIONS};

uint8_t eattr_search(char *optarg_value) {
	uint8_t i=0;

	while (i<EATTR_BITS) {
		if (strcmp(optarg_value, eattrtab[i])==0) {
			return (1<<i);
		}
		i++;
	}
	return 0;
}

/*int my_str_cmp(char *all, char *str) {	//check to the end of 'all'
	while (*all) {
		if (tolower(*all++) != tolower(*str++)) {
			return -1;
		}
	}
	return 0;
}
*/


void print_extra_attributes(void) {
	int j;
	fprintf(stderr,"\nattributes:\n");
	for (j=0 ; j<EATTR_BITS ; j++) {
		if (eattrtab[j][0]) {
			fprintf(stderr," %s - %s\n",eattrtab[j],eattrdesc[j]);
		}
	}
}

int get_eattr(const char *fname,uint8_t *eattr,uint8_t mode) {
	int32_t leng;
	uint32_t inode;
	uint32_t fcnt[EATTR_BITS];
	uint32_t dcnt[EATTR_BITS];
	uint32_t cnt;
	uint8_t fn,dn,i,j;

	if (open_master_conn(fname, &inode, NULL, NULL,0,0)<0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put8(mode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETEATTR, MATOCL_FUSE_GETEATTR, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng%5!=2) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	} else if (mode==GMODE_NORMAL && leng!=7) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}
	if (mode==GMODE_NORMAL) {
		fn	= ps_get8();
		dn	= ps_get8();
		*eattr = ps_get8();
		cnt	= ps_get32();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			fprintf(stderr,"%s: master query: wrong answer (fn,dn)\n",fname);
			goto error;
		}
		if (cnt!=1) {
			fprintf(stderr,"%s: master query: wrong answer (cnt)\n",fname);
			goto error;
		}
		printf("%s: ",fname);
		if (*eattr > 0) {
			cnt=0;
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					printf("%s%s",(cnt)?",":"",eattrtab[j]);
					cnt=1;
				}
			}
			printf("\n");
		} else {
			printf("-\n");
		}
	} else {
		for (j=0 ; j<EATTR_BITS ; j++) {
			fcnt[j]=0;
			dcnt[j]=0;
		}
		fn = ps_get8();
		dn = ps_get8();
		for (i=0 ; i<fn ; i++) {
			*eattr = ps_get8();
			cnt = ps_get32();
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					fcnt[j]+=cnt;
				}
			}
		}
		for (i=0 ; i<dn ; i++) {
			*eattr = ps_get8();
			cnt = ps_get32();
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					dcnt[j]+=cnt;
				}
			}
		}
		printf("%s:\n",fname);
		for (j=0 ; j<EATTR_BITS ; j++) {
			if (eattrtab[j][0]) {
				printf(" not directory nodes with attribute %16s :",eattrtab[j]);
				print_number_desc(" ","\n",fcnt[j],PNUM_32BIT);
				printf(" directories with attribute         %16s :",eattrtab[j]);
				print_number_desc(" ","\n",dcnt[j],PNUM_32BIT);
			} else {
				if (fcnt[j]>0) {
					printf(" not directory nodes with attribute      'unknown-%u' :",j);
					print_number_desc(" ","\n",fcnt[j],PNUM_32BIT);
				}
				if (dcnt[j]>0) {
					printf(" directories with attribute              'unknown-%u' :",j);
					print_number_desc(" ","\n",dcnt[j],PNUM_32BIT);
				}
			}
		}
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}

int set_eattr(const char *fname,uint8_t eattr,uint8_t mode) {
	int32_t leng;
	uint32_t inode,uid;
	uint32_t changed,notchanged,notpermitted;

	if (open_master_conn(fname, &inode, NULL, NULL, 0, 1)<0) {
		return -1;
	}
	uid = getuid();

	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	ps_put8(eattr);
	ps_put8(mode);

	if ((leng=ps_send_and_receive(CLTOMA_FUSE_SETEATTR, MATOCL_FUSE_SETEATTR, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=12) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}
	changed = ps_get32();
	notchanged = ps_get32();
	notpermitted = ps_get32();
	if ((mode&SMODE_RMASK)==0) {
		if (changed) {
			printf("%s: attribute(s) changed\n",fname);
		} else {
			printf("%s: attribute(s) not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number_desc(" inodes with attributes changed:     ","\n",changed,PNUM_32BIT);
		print_number_desc(" inodes with attributes not changed: ","\n",notchanged,PNUM_32BIT);
		print_number_desc(" inodes with permission denied:      ","\n",notpermitted,PNUM_32BIT);
	}
//OK
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *geteattrtxt[] = {
	"get objects extra attributes",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

static const char *seteattrtxt[] = {
	"set objects extra attributes",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] -f attrname [-f attrname ...] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	" -f - specify attribute to set",
	NULL
};

static const char *deleattrtxt[] = {
	"delete objects extra attributes",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] -a | -f attrname [-f attrname ...] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	" -a - delete all existing attributes",
	" -f - specify attribute to delete",
	NULL
};

static const char *copyeattrtxt[] = {
	"copy objects extra attributes",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-r] source_object object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	_RECURSIVEDESC_,
	NULL
};

void geteattrusage(void) {
	tcomm_print_help(geteattrtxt);
	exit(1);
}

void setdeleattrusage(int sdmode) {
	tcomm_print_help(sdmode?seteattrtxt:deleattrtxt);
	print_extra_attributes();
	exit(1);
}

void copyeattrusage(void) {
	tcomm_print_help(copyeattrtxt);
	exit(1);
}

int geteattrexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint8_t eattr;

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
				geteattrusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		geteattrusage();
		return 1;
	}

	while (argc>0) {
		if (get_eattr(*argv, &eattr, rflag ? GMODE_RECURSIVE : GMODE_NORMAL)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int setdeleattrexe(int argc,char *argv[],int sdmode) {
	int ch;
	int rflag = 0;
	int status = 0;
	int smode = SMODE_SET;
	uint8_t eattr;
	uint8_t tmp8;

	if (sdmode) {
		smode = SMODE_INCREASE;
	} else {
		smode = SMODE_DECREASE;
	}
	eattr = 0;

	while ((ch=getopt(argc,argv,(sdmode)?"?nhHkmgrf:":"?nhHkmgraf:"))!=-1) {
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
			case 'a':
				if (sdmode) {
					setdeleattrusage(sdmode);
				} else {
					eattr = 0xFF;
				}
				break;
			case 'f':
				if (! (tmp8 = eattr_search(optarg)) ) {
					fprintf(stderr,"unknown flag\n");
					setdeleattrusage(sdmode);
				}
				eattr |= tmp8;
				break;
			default:
				setdeleattrusage(sdmode);
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		setdeleattrusage(sdmode);
		return 1;
	}

	if (eattr==0) {
		fprintf(stderr,"no attribute(s) to %s\n",sdmode?"set":"delete");
		fprintf(stderr,"use -? for help\n");
		return 1;
	}

	while (argc>0) {
		if (set_eattr(*argv,eattr,(rflag)?(smode | SMODE_RMASK):smode)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

int seteattrexe(int argc,char *argv[]) {
	return setdeleattrexe(argc,argv,1);
}

int deleattrexe(int argc,char *argv[]) {
	return setdeleattrexe(argc,argv,0);
}

int copyeattrexe(int argc,char *argv[]) {
	int ch;
	int rflag = 0;
	int status = 0;
	uint8_t eattr;

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
				copyeattrusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		copyeattrusage();
		return 1;
	}

	if (get_eattr(*argv, &eattr, GMODE_NORMAL)<0) {
		return 1;
	}
	argc--;
	argv++;

	while (argc>0) {
		if (set_eattr(*argv, eattr, SMODE_SET|((rflag) ? SMODE_RMASK : 0))<0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

static command commandlist[] = {
	{"get", geteattrexe},
	{"set", seteattrexe},
	{"del", deleattrexe},
	{"copy", copyeattrexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
