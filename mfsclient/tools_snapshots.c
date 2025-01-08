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
#include <sys/param.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>

#include "libmfstools.h"

//SNAPSHOT TOOLS

int append_file(const char *fname, const char *afname, int64_t slice_from, int64_t slice_to) {
	int32_t leng;
	uint32_t inode, ainode, uid, gid;
	uint32_t slice_from_abs, slice_to_abs;
	uint8_t flags;
	gid_t *grouplist;
	uint32_t i, gids;
	uint8_t addmaingroup;
	mode_t dmode,smode;

	if (slice_from < INT64_C(-0xFFFFFFFF) || slice_to > INT64_C(0xFFFFFFFF)
	|| slice_to < INT64_C(-0xFFFFFFFF) || slice_to > INT64_C(0xFFFFFFFF)) {
		printf("bad slice indexes\n");
		return -1;
	}

	flags = 0;
	if (slice_from<0) {
		slice_from_abs = -slice_from;
		flags |= APPEND_SLICE_FROM_NEG;
	} else {
		slice_from_abs = slice_from;
	}
	if (slice_to<0) {
		slice_to_abs = -slice_to;
		flags |= APPEND_SLICE_TO_NEG;
	} else {
		slice_to_abs = slice_to;
	}

	if (open_master_conn(fname, &inode, &dmode, NULL, 0, 1)<0) {
		return -1;
	}
	if (open_master_conn(afname, &ainode, &smode, NULL, 1, 1)<0) {
		fprintf(stderr,"(%s,%s): both elements must be on the same moosefs instance\n",fname,afname);
		return -1;
	}


	if ((slice_from!=0 || slice_to!=0x80000000) && getmasterversion()<VERSION2INT(3,0,92)) {//GaPaRestore20180419
		printf("slices not supported in your master - please upgrade it\n");
		return -1;
	}
	if ((smode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n", afname);
		return -1;
	}
	if ((dmode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n", fname);
		return -1;
	}

	grouplist = malloc(sizeof(gid_t)*NGROUPS_MAX);
	uid = getuid();
	gid = getgid();

	if (getmasterversion()>=VERSION2INT(2,0,0)) {
		gids = getgroups(NGROUPS_MAX, grouplist);
		addmaingroup = 1;
		for (i=0 ; i<gids ; i++) {
			if (grouplist[i]==gid) {
				addmaingroup = 0;
			}
		}
	} else {
		gids = 0;
		addmaingroup = 0;
	}

	ps_comminit();

	if (getmasterversion()>=VERSION2INT(3,0,92)) {
		ps_put8(flags);
		ps_put32(inode);
		ps_put32(ainode);
		ps_put32(slice_from_abs);
		ps_put32(slice_to_abs);
	}else{
		ps_put32(inode);
		ps_put32(ainode);
	}
	ps_put32(uid);
	if (getmasterversion()<VERSION2INT(2,0,0)) {
		ps_put32(gid);
	} else {
		ps_put32(addmaingroup+gids);
		if (addmaingroup) {
			ps_put32(gid);
		}
		for (i=0 ; i<gids ; i++) {
			ps_put32(grouplist[i]);
		}
	}

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_APPEND_SLICE, MATOCL_FUSE_APPEND_SLICE, fname)) < 0) {//cv->fd	?
		goto error;
	}

	if (leng!=1) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		goto error;
	} else if ((flags=ps_get8())!=MFS_STATUS_OK) {
		printf("%s: %s\n", fname, mfsstrerr(flags));
		goto error;
	}

	free(grouplist);

//OK
	return 0;

error:
	free(grouplist);
	reset_master();
	return -1;
}

int snapshot_ctl(const char *dstdir,const char *dstbase,const char *srcname,uint8_t smode) {
	uint32_t dstinode,srcinode,uid,gid;
	int32_t leng;
	gid_t *grouplist;
	uint32_t i,gids;
	uint8_t addmaingroup;
	uint32_t nleng;
	uint16_t umsk;

	umsk = umask(0);
	umask(umsk);
	nleng = strlen(dstbase);
	if (nleng>255) {
		printf("%s: name too long\n",dstbase);
		return -1;
	}
	if (open_master_conn(dstdir,&dstinode,NULL,NULL,0,1) < 0) {
		return -1;
	}
	if (srcname!=NULL) {
		if (open_master_conn(srcname,&srcinode,NULL,NULL,1,1) < 0) {
			fprintf(stderr,"(%s,%s): both elements must be on the same moosefs instance\n",dstdir,srcname);
			return -1;
		}
	} else {
		srcinode = 0;
	}
	grouplist = malloc(sizeof(gid_t)*NGROUPS_MAX);
	uid = getuid();
	gid = getgid();

	if (getmasterversion()>=VERSION2INT(2,0,0)) {
		gids = getgroups(NGROUPS_MAX,grouplist);
		addmaingroup = 1;
		for (i=0 ; i<gids ; i++) {
			if (grouplist[i]==gid) {
				addmaingroup = 0;
			}
		}
	} else {
		gids = 0;
		addmaingroup = 0;
	}

	ps_comminit();
	ps_put32(srcinode);
	ps_put32(dstinode);
	ps_putstr(dstbase);

	ps_put32(uid);
	if (getmasterversion()<VERSION2INT(2,0,0)) {
		ps_put32(gid);
	} else {
		ps_put32(addmaingroup+gids);
		if (addmaingroup) {
			ps_put32(gid);
		}
		for (i=0 ; i<gids ; i++) {
			ps_put32(grouplist[i]);
		}
	}
	ps_put8(smode);
	ps_put16(umsk);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_SNAPSHOT, MATOCL_FUSE_SNAPSHOT, dstdir)) < 0) {
		goto error;
	}

	if (leng!=1) {
		if (smode & SNAPSHOT_MODE_DELETE) {
			fprintf(stderr,"%s/%s: master query: wrong answer (leng)\n",dstdir,dstbase);
		} else {
			fprintf(stderr,"%s->%s/%s: master query: wrong answer (leng)\n",srcname,dstdir,dstbase);
		}
		goto error;
	}
	if ((umsk=ps_get8())) {
		if (smode & SNAPSHOT_MODE_DELETE) {
			fprintf(stderr,"%s/%s: %s\n", dstdir, dstbase, mfsstrerr(umsk));
		} else {
			fprintf(stderr,"%s->%s/%s: %s\n", srcname, dstdir, dstbase, mfsstrerr(umsk));
		}
		goto error;
	}

	free(grouplist);

//OK:
	return 0;

error:
	free(grouplist);
	reset_master();
	return -1;
}

int remove_snapshot(const char *dstname,uint8_t smode) {
	char dstpath[PATH_MAX+1],base[PATH_MAX+1],dir[PATH_MAX+1];

	if (realpath(dstname,dstpath)==NULL) {
		fprintf(stderr,"%s: realpath error on %s: %s\n",dstname,dstpath,strerr(errno));
	}
	memcpy(dir,dstpath,PATH_MAX+1);
	dirname_inplace(dir);
	if (bsd_basename(dstpath,base)<0) {
		fprintf(stderr,"%s: basename error\n",dstpath);
		return -1;
	}
	return snapshot_ctl(dir,base,NULL,smode | SNAPSHOT_MODE_DELETE);
}

int make_snapshot(const char *dstname,char * const *srcnames,uint32_t srcelements,uint8_t smode) {
	char to[PATH_MAX+1],base[PATH_MAX+1],dir[PATH_MAX+1];
	char src[PATH_MAX+1];
	struct stat sst,dst;
	int status;
	uint32_t i,l;

	if (stat(dstname,&dst)<0) {	// dst does not exist
		if (errno!=ENOENT) {
			fprintf(stderr,"%s: stat error: %s\n",dstname,strerr(errno));
			return -1;
		}
		if (srcelements>1) {
			fprintf(stderr,"can snapshot multiple elements only into existing directory\n");
			return -1;
		}
		if (lstat(srcnames[0],&sst)<0) {
			fprintf(stderr,"%s: lstat error: %s\n",srcnames[0],strerr(errno));
			return -1;
		}
		if (bsd_dirname(dstname,dir)<0) {
			fprintf(stderr,"%s: dirname error\n",dstname);
			return -1;
		}
		if (stat(dir,&dst)<0) {
			fprintf(stderr,"%s: stat error: %s\n",dir,strerr(errno));
			return -1;
		}
		if (realpath(dir,to)==NULL) {
			fprintf(stderr,"%s: realpath error on %s: %s\n",dir,to,strerr(errno));
			return -1;
		}
		if (bsd_basename(dstname,base)<0) {
			fprintf(stderr,"%s: basename error\n",dstname);
			return -1;
		}
		if (strlen(dstname)>0 && dstname[strlen(dstname)-1]=='/' && !S_ISDIR(sst.st_mode)) {
			fprintf(stderr,"directory %s does not exist\n",dstname);
			return -1;
		}
		return snapshot_ctl(to,base,srcnames[0],smode);
	} else {	// dst exists
		if (realpath(dstname,to)==NULL) {
			fprintf(stderr,"%s: realpath error on %s: %s\n",dstname,to,strerr(errno));
			return -1;
		}
		if (!S_ISDIR(dst.st_mode)) {	// dst id not a directory
			if (srcelements>1) {
				fprintf(stderr,"can snapshot multiple elements only into existing directory\n");
				return -1;
			}
			if (lstat(srcnames[0],&sst)<0) {
				fprintf(stderr,"%s: lstat error: %s\n",srcnames[0],strerr(errno));
				return -1;
			}
			memcpy(dir,to,PATH_MAX+1);
			dirname_inplace(dir);
			if (bsd_basename(to,base)<0) {
				fprintf(stderr,"%s: basename error\n",to);
				return -1;
			}
			return snapshot_ctl(dir,base,srcnames[0],smode);
		} else {	// dst is a directory
			status = 0;
			for (i=0 ; i<srcelements ; i++) {
				if (lstat(srcnames[i],&sst)<0) {
					fprintf(stderr,"%s: lstat error: %s\n",srcnames[i],strerr(errno));
					status=-1;
					continue;
				}
				if (!S_ISDIR(sst.st_mode)) {	// src is not a directory
					if (!S_ISLNK(sst.st_mode)) {	// src is not a symbolic link
						if (realpath(srcnames[i],src)==NULL) {
							fprintf(stderr,"%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							fprintf(stderr,"%s: basename error\n",src);
							status=-1;
							continue;
						}
					} else {	// src is a symbolic link
						if (bsd_basename(srcnames[i],base)<0) {
							fprintf(stderr,"%s: basename error\n",srcnames[i]);
							status=-1;
							continue;
						}
					}
					if (snapshot_ctl(to,base,srcnames[i],smode)<0) {
						status=-1;
					}
				} else {	// src is a directory
					l = strlen(srcnames[i]);
					if (l>0 && srcnames[i][l-1]!='/') {	// src is a directory and name has trailing slash
						if (realpath(srcnames[i],src)==NULL) {
							fprintf(stderr,"%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							fprintf(stderr,"%s: basename error\n",src);
							status=-1;
							continue;
						}
						if (snapshot_ctl(to,base,srcnames[i],smode)<0) {
							status=-1;
						}
					} else {	// src is a directory and name has not trailing slash
						memcpy(dir,to,PATH_MAX+1);
						dirname_inplace(dir);
						if (bsd_basename(to,base)<0) {
							fprintf(stderr,"%s: basename error\n",to);
							status=-1;
							continue;
						}
						if (snapshot_ctl(dir,base,srcnames[i],smode)<0) {
							status=-1;
						}
					}
				}
			}
			return status;
		}
	}
}

//----------------------------------------------------------------------

static const char *appendchunkstxt[] = {
	"append file chunks to another file. If destination file doesn't exist then it's created as empty file and then chunks are appended",
	"",
	"usage: "_EXENAME_" [-s slice_from:slice_to] snapshot_file file [file ...]",
	_QMARKDESC_,
	" -s - append only fragment of source file from chunk 'slice_from' to chunk 'slice_to'",
	NULL
};

static const char *makesnapshottxt[] = {
	"make snapshot (lazy copy)",
	"",
	"usage: "_EXENAME_" [-?] [-ocp] source_object [source_object ...] destination",
	_QMARKDESC_,
	" -o - allow to overwrite existing objects",
	" -c - 'cp' mode for attributes (create objects using current uid,gid,umask etc.)",
	" -p - preserve hardlinks",
	NULL
};

static const char *rmsnapshottxt[] = {
	"remove snapshot (quick rm -r)",
	"",
	"usage: "_EXENAME_" [-?] [-f] object [object ...]",
	_QMARKDESC_,
	" -f - remove as much as possible (according to access rights and snapshot flags)",
	NULL
};

void appendchunksusage(void) {
	tcomm_print_help(appendchunkstxt);
	exit(1);
}

void makesnapshotusage(void) {
	tcomm_print_help(makesnapshottxt);
	exit(1);
}

void rmsnapshotusage(void) {
	tcomm_print_help(rmsnapshottxt);
	exit(1);
}

int appendchunksexe(int argc,char *argv[]) {
	int ch;
	int i;
	int status = 0;
	int64_t slice_from,slice_to;
	char *appendfname;

	slice_from = 0;
	slice_to = (int64_t)0x80000000;

	while ((ch=getopt(argc,argv,"?s:"))!=-1) {
		switch (ch) {
			case 's':
				if (*optarg==':') {
					slice_from = 0;
					optarg++;
				} else {
					slice_from = strtoll(optarg, &optarg, 10);
					if (*optarg != ':') {
						fprintf(stderr,"bad slice definition\n");
						appendchunksusage();
					}
					optarg++;
				}
				if (*optarg == '\0') {
					slice_to = 0x80000000;
				} else {
					slice_to = strtoll(optarg, &optarg, 10);
				}
				if (*optarg != '\0') {
					fprintf(stderr,"bad slice definition\n");
					appendchunksusage();
				}

				break;
			default:
				appendchunksusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		appendchunksusage();
		return 1;
	}

	appendfname = argv[0];
	i = open(appendfname, O_RDWR | O_CREAT, 0666);
	if (i<0) {
		fprintf(stderr,"can't create/open file: %s\n",appendfname);
		exit(1);
	}
	close(i);
	argc--;
	argv++;

	while (argc>0) {
		if (append_file(appendfname, *argv, slice_from, slice_to)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int makesnapshotexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	uint8_t snapmode = 0;

	while ((ch=getopt(argc,argv,"?ocp"))!=-1) {
		switch (ch) {
			case 'o':
				snapmode |= SNAPSHOT_MODE_CAN_OVERWRITE;
				break;
			case 'c':
				snapmode |= SNAPSHOT_MODE_CPLIKE_ATTR;
				break;
			case 'p':
				snapmode |= SNAPSHOT_MODE_PRESERVE_HARDLINKS;
				break;
			default:
				makesnapshotusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		makesnapshotusage();
		return 1;
	}

	status = make_snapshot(argv[argc-1],argv,argc-1,snapmode);

	return status;
}

int rmsnapshotexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	uint8_t snapmode = 0;

	while ((ch=getopt(argc,argv,"?f"))!=-1) {
		switch (ch) {
			case 'f':
				snapmode |= SNAPSHOT_MODE_FORCE_REMOVAL;
				break;
			default:
				rmsnapshotusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		rmsnapshotusage();
		return 1;
	}

	while (argc>0) {
		if (remove_snapshot(*argv,snapmode)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

static command commandlist[] = {
	{"append", appendchunksexe},
	{"make", makesnapshotexe},
	{"rm", rmsnapshotexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
