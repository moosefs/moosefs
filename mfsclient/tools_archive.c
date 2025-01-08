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

//ARCHIVE MANAGEMENT TOOLS
int archive_control(const char *fname, uint8_t archcmd) {
	uint32_t inode,uid;
	int32_t leng;

	if (open_master_conn(fname, &inode, NULL, NULL, 0,1)<0) {
		return -1;
	}
	uid = getuid();

	ps_comminit();
	ps_put32(inode);
	ps_put8(archcmd);
	if (archcmd != ARCHCTL_GET) {
		ps_put32(uid);
	}

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_ARCHCTL, MATOCL_FUSE_ARCHCTL, fname)) < 0) {
		goto error;
	}

	if (leng == 1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	}
	if (archcmd == ARCHCTL_GET) {
		uint32_t archinodes,partinodes,notarchinodes;
		uint64_t archchunks,notarchchunks;
		if (leng != 28) {
			fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
			goto error;
		}
		archchunks    = ps_get64();
		notarchchunks = ps_get64();
		archinodes    = ps_get32();
		partinodes    = ps_get32();
		notarchinodes = ps_get32();
		if (archinodes+partinodes+notarchinodes==1) {
			if (archinodes == 1) {
				printf("%s: all chunks are archived\n", fname);
			} else if (notarchinodes==1) {
				printf("%s: all chunks are not archived\n", fname);
			} else {
				printf("%s: file is partially archived (archived chunks: %"PRIu64" ; not archived chunks: %"PRIu64")\n", fname, archchunks, notarchchunks);
			}
		} else {
			printf("%s:\n",fname);
			print_number_desc(" files with all chunks archived:     ","\n", archinodes, PNUM_32BIT);
			print_number_desc(" files with all chunks not archived: ","\n", notarchinodes, PNUM_32BIT);
			print_number_desc(" files partially archived:           ","\n", partinodes, PNUM_32BIT);
			print_number_desc(" archived chunks:                    ","\n", archchunks, PNUM_32BIT);
			print_number_desc(" not archived chunks:                ","\n", notarchchunks, PNUM_32BIT);
		}
	} else {
		uint64_t changed, notchanged;
		uint32_t notpermitted;
		if (leng != 20) {
			fprintf(stderr,"%s: master query: wrong answer (leng)\n", fname);
			goto error;
		}
		changed      = ps_get64();
		notchanged   = ps_get64();
		notpermitted = ps_get32();
		printf("%s:\n",fname);
		print_number_desc(" chunks changed:               ","\n", changed, PNUM_32BIT);
		print_number_desc(" chunks not changed:           ","\n", notchanged, PNUM_32BIT);
		print_number_desc(" files with permission denied: ","\n", notpermitted, PNUM_32BIT);
	}
//OK:
	return 0;

error:
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *checkarchivetxt[] = {
	"checks if archive flag is set or not (when directory is specified then command will check it recursivelly)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

static const char *setarchivetxt[] = {
	"set archive flags in chunks (recursivelly for directories) - moves files to archive (use 'archive' goal/labels instead of 'keep' goal/labels)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] object [object ...]",
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

static const char *clrarchivetxt[] = {
	"clear archive flags in chunks (recursivelly for directories) - moves files from archive (use 'keep' goal/labels instead of 'archive' goal/labels) - it also changes ctime, so files will move back to archive after time specified in mfssetgoal",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] object [object ...]"
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

void checkarchiveusage(void) {
	tcomm_print_help(checkarchivetxt);
	exit(1);
}

void setarchiveusage(void) {
	tcomm_print_help(setarchivetxt);
	exit(1);
}

void clrarchiveusage(void) {
	tcomm_print_help(clrarchivetxt);
	exit(1);
}

int commonarchiveexe(int argc,char *argv[],uint8_t archcmd,void (*commonusage)(void)) {
	int ch;
	int status = 0;

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
				commonusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		commonusage();
		return 1;
	}

	while (argc>0) {
		if (archive_control(*argv, archcmd)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int checkarchiveexe(int argc,char *argv[]) {
	return commonarchiveexe(argc,argv,ARCHCTL_GET,checkarchiveusage);
}

int setarchiveexe(int argc,char *argv[]) {
	return commonarchiveexe(argc,argv,ARCHCTL_SET,setarchiveusage);
}

int clrarchiveexe(int argc,char *argv[]) {
	return commonarchiveexe(argc,argv,ARCHCTL_CLR,clrarchiveusage);
}

static command commandlist[] = {
	{"check | chk", checkarchiveexe},
	{"set", setarchiveexe},
	{"clr", clrarchiveexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
