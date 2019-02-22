/*
 * Copyright (C) 2019 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#if defined(HAVE_MLOCKALL)
#  if defined(HAVE_SYS_MMAN_H)
#    include <sys/mman.h>
#  endif
#  if defined(HAVE_SYS_RESOURCE_H)
#    include <sys/resource.h>
#  endif
#  if defined(RLIMIT_MEMLOCK) && defined(MCL_CURRENT) && defined(MCL_FUTURE)
#    define MFS_USE_MEMLOCK 1
#  endif
#endif

#if defined(HAVE_MALLOC_H)
#  include <malloc.h>
#endif
#if defined(M_ARENA_MAX) && defined(M_ARENA_TEST) && defined(HAVE_MALLOPT)
#  define MFS_USE_MALLOPT 1
#endif

#if defined(HAVE_LINUX_OOM_H)
#  include <linux/oom.h>
#  if defined(OOM_DISABLE) || defined(OOM_SCORE_ADJ_MIN)
#    define OOM_ADJUSTABLE 1
#  endif
#endif

#include <fuse.h>
#include <fuse_opt.h>
#include <fuse_lowlevel.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>
#include <unistd.h>
#include <syslog.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>

#include "mfs_fuse.h"
#include "mfs_meta_fuse.h"

#include "MFSCommunication.h"
#include "clocks.h"
#include "massert.h"
#include "portable.h"
#include "md5.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "csorder.h"
#include "sustained_inodes.h"
#include "sustained_stats.h"
#include "symlinkcache.h"
#include "negentrycache.h"
//#include "dircache.h"
#include "chunksdatacache.h"
#include "conncache.h"
#include "chunkrwlock.h"
#include "readdata.h"
#include "writedata.h"
#include "delayrun.h"
#include "csdb.h"
#include "stats.h"
#include "strerr.h"
#include "crc.h"
#include "processname.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " VERSSTR ", written by Jakub Kruszona-Zawadzki";

#if defined(__APPLE__)
#define DEFAULT_OPTIONS "allow_other,daemon_timeout=600,novncache"
// #define DEFAULT_OPTIONS "allow_other,default_permissions,daemon_timeout=600,iosize=65536,novncache"
#else
#define DEFAULT_OPTIONS "allow_other"
#endif

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn);

static struct fuse_lowlevel_ops mfs_meta_oper = {
	.init           = mfs_fsinit,
	.statfs         = mfs_meta_statfs,
	.lookup         = mfs_meta_lookup,
	.getattr        = mfs_meta_getattr,
	.setattr        = mfs_meta_setattr,
	.unlink         = mfs_meta_unlink,
	.rename         = mfs_meta_rename,
	.opendir        = mfs_meta_opendir,
	.readdir        = mfs_meta_readdir,
	.releasedir     = mfs_meta_releasedir,
	.open           = mfs_meta_open,
	.release        = mfs_meta_release,
	.read           = mfs_meta_read,
	.write          = mfs_meta_write,
//	.access         = mfs_meta_access,
};

static struct fuse_lowlevel_ops mfs_oper = {
	.init           = mfs_fsinit,
	.statfs         = mfs_statfs,
	.lookup         = mfs_lookup,
	.getattr        = mfs_getattr,
	.setattr        = mfs_setattr,
	.mknod          = mfs_mknod,
	.unlink         = mfs_unlink,
	.mkdir          = mfs_mkdir,
	.rmdir          = mfs_rmdir,
	.symlink        = mfs_symlink,
	.readlink       = mfs_readlink,
	.rename         = mfs_rename,
	.link           = mfs_link,
	.opendir        = mfs_opendir,
	.readdir        = mfs_readdir,
	.releasedir     = mfs_releasedir,
	.create         = mfs_create,
	.open           = mfs_open,
	.release        = mfs_release,
	.flush          = mfs_flush,
	.fsync          = mfs_fsync,
	.read           = mfs_read,
	.write          = mfs_write,
	.access         = mfs_access,
	.getxattr       = mfs_getxattr,
	.setxattr       = mfs_setxattr,
	.listxattr      = mfs_listxattr,
	.removexattr    = mfs_removexattr,
#if FUSE_VERSION >= 26
	.getlk		= mfs_getlk,
	.setlk		= mfs_setlk,
#endif
#if FUSE_VERSION >= 29
	.flock          = mfs_flock,
#endif
};

struct mfsopts {
	char *masterhost;
	char *masterport;
	char *bindhost;
	char *proxyhost;
	char *subfolder;
	char *password;
	char *passfile;
	char *md5pass;
	char *preferedlabels;
	unsigned nofile;
	signed nice;
	int mfssuid;
	int mfsdev;
#ifdef MFS_USE_MEMLOCK
	int memlock;
#endif
#ifdef MFS_USE_MALLOPT
	int limitarenas;
#endif
#if defined(__linux__) && defined(OOM_ADJUSTABLE)
	int allowoomkiller;
#endif
	int nostdmountoptions;
	int meta;
	int debug;
	int flattrash;
	int delayedinit;
	int mkdircopysgid;
	char *sugidclearmodestr;
	int sugidclearmode;
	char *cachemode;
	int cachefiles;
	int keepcache;
	int passwordask;
	int noxattrs;
	int noposixlocks;
	int nobsdlocks;
	int donotrememberpassword;
//	int xattraclsupport;
	unsigned writecachesize;
	unsigned readaheadsize;
	unsigned readaheadleng;
	unsigned readaheadtrigger;
	unsigned ioretries;
	unsigned timeout;
	unsigned logretry;
	double attrcacheto;
	double xattrcacheto;
	double entrycacheto;
	double direntrycacheto;
	double negentrycacheto;
	double groupscacheto;
	double fsyncmintime;
	int fsyncbeforeclose;
	int netdev; // only for ignoring '_netdev' option
};

static struct mfsopts mfsopts;
static char *defaultmountpoint = NULL;

static int custom_cfg;

enum {
	KEY_CFGFILE,
	KEY_META,
	KEY_HOST,
	KEY_PORT,
	KEY_BIND,
	KEY_PROXY,
	KEY_PATH,
	KEY_PASSWORDASK,
	KEY_NOSTDMOUNTOPTIONS,
	KEY_HELP,
	KEY_VERSION,
};

#define MFS_OPT(t, p, v) { t, offsetof(struct mfsopts, p), v }

static struct fuse_opt mfs_opts_stage1[] = {
	FUSE_OPT_KEY("mfscfgfile=",    KEY_CFGFILE),
	FUSE_OPT_KEY("-c ",            KEY_CFGFILE),
	FUSE_OPT_END
};

static struct fuse_opt mfs_opts_stage2[] = {
	MFS_OPT("mfsmaster=%s", masterhost, 0),
	MFS_OPT("mfsport=%s", masterport, 0),
	MFS_OPT("mfsbind=%s", bindhost, 0),
	MFS_OPT("mfsproxy=%s", proxyhost, 0),
	MFS_OPT("mfssubfolder=%s", subfolder, 0),
	MFS_OPT("mfspassword=%s", password, 0),
	MFS_OPT("mfspassfile=%s", passfile, 0),
	MFS_OPT("mfsmd5pass=%s", md5pass, 0),
	MFS_OPT("mfspreflabels=%s", preferedlabels, 0),
	MFS_OPT("mfsrlimitnofile=%u", nofile, 0),
	MFS_OPT("mfsnice=%d", nice, 0),
	MFS_OPT("mfssuid", mfssuid, 1),
	MFS_OPT("mfsdev", mfsdev, 1),
#ifdef MFS_USE_MEMLOCK
	MFS_OPT("mfsmemlock", memlock, 1),
#endif
#ifdef MFS_USE_MALLOPT
	MFS_OPT("mfslimitarenas=%u", limitarenas, 0),
#endif
#if defined(__linux__) && defined(OOM_ADJUSTABLE)
	MFS_OPT("mfsallowoomkiller", allowoomkiller, 1),
#endif
	MFS_OPT("mfswritecachesize=%u", writecachesize, 0),
	MFS_OPT("mfsreadaheadsize=%u", readaheadsize, 0),
	MFS_OPT("mfsreadaheadleng=%u", readaheadleng, 0),
	MFS_OPT("mfsreadaheadtrigger=%u", readaheadtrigger, 0),
	MFS_OPT("mfsioretries=%u", ioretries, 0),
	MFS_OPT("mfstimeout=%u", timeout, 0),
	MFS_OPT("mfslogretry=%u", logretry, 0),
	MFS_OPT("mfsdebug", debug, 1),
	MFS_OPT("mfsmeta", meta, 1),
	MFS_OPT("mfsflattrash", flattrash, 1),
	MFS_OPT("mfsdelayedinit", delayedinit, 1),
	MFS_OPT("mfsdonotrememberpassword", donotrememberpassword, 1),
	MFS_OPT("mfscachefiles", cachefiles, 1),
	MFS_OPT("mfsnoxattr", noxattrs, 1),
	MFS_OPT("mfsnoxattrs", noxattrs, 1),
	MFS_OPT("mfsnoposixlock", noposixlocks, 1),
	MFS_OPT("mfsnoposixlocks", noposixlocks, 1),
	MFS_OPT("mfsnobsdlock", nobsdlocks, 1),
	MFS_OPT("mfsnobsdlocks", nobsdlocks, 1),
	MFS_OPT("mfscachemode=%s", cachemode, 0),
	MFS_OPT("mfsmkdircopysgid=%u", mkdircopysgid, 0),
	MFS_OPT("mfssugidclearmode=%s", sugidclearmodestr, 0),
	MFS_OPT("mfsattrcacheto=%lf", attrcacheto, 0),
	MFS_OPT("mfsxattrcacheto=%lf", xattrcacheto, 0),
	MFS_OPT("mfsentrycacheto=%lf", entrycacheto, 0),
	MFS_OPT("mfsdirentrycacheto=%lf", direntrycacheto, 0),
	MFS_OPT("mfsnegentrycacheto=%lf", negentrycacheto, 0),
	MFS_OPT("mfsgroupscacheto=%lf", groupscacheto, 0),
//	MFS_OPT("mfsaclsupport", xattraclsupport, 1),
	MFS_OPT("mfsfsyncmintime=%lf", fsyncmintime, 0),
	MFS_OPT("mfsfsyncbeforeclose", fsyncbeforeclose, 1),
	MFS_OPT("_netdev", netdev, 1),

	FUSE_OPT_KEY("-m",             KEY_META),
	FUSE_OPT_KEY("--meta",         KEY_META),
	FUSE_OPT_KEY("-H ",            KEY_HOST),
	FUSE_OPT_KEY("-P ",            KEY_PORT),
	FUSE_OPT_KEY("-B ",            KEY_BIND),
	FUSE_OPT_KEY("-L ",            KEY_PROXY),
	FUSE_OPT_KEY("-S ",            KEY_PATH),
	FUSE_OPT_KEY("-p",             KEY_PASSWORDASK),
	FUSE_OPT_KEY("--password",     KEY_PASSWORDASK),
	FUSE_OPT_KEY("-n",             KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("--nostdopts",    KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("-V",             KEY_VERSION),
	FUSE_OPT_KEY("--version",      KEY_VERSION),
	FUSE_OPT_KEY("-h",             KEY_HELP),
	FUSE_OPT_KEY("--help",         KEY_HELP),
	FUSE_OPT_END
};

static void usage(const char *progname) {
	fprintf(stderr,"usage: %s [HOST[:PORT]:[PATH]] [options] mountpoint\n",progname);
	fprintf(stderr,"\n");
	fprintf(stderr,"general options:\n");
	fprintf(stderr,"    -o opt,[opt...]         mount options\n");
	fprintf(stderr,"    -h   --help             print help\n");
	fprintf(stderr,"    -V   --version          print version\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"MFS options:\n");
	fprintf(stderr,"    -c CFGFILE                  equivalent to '-o mfscfgfile=CFGFILE'\n");
	fprintf(stderr,"    -m   --meta                 equivalent to '-o mfsmeta'\n");
	fprintf(stderr,"    -H HOST                     equivalent to '-o mfsmaster=HOST'\n");
	fprintf(stderr,"    -P PORT                     equivalent to '-o mfsport=PORT'\n");
	fprintf(stderr,"    -B IP                       equivalent to '-o mfsbind=IP'\n");
	fprintf(stderr,"    -L IP                       equivalent to '-o mfsproxy=IP'\n");
	fprintf(stderr,"    -S PATH                     equivalent to '-o mfssubfolder=PATH'\n");
	fprintf(stderr,"    -p   --password             similar to '-o mfspassword=PASSWORD', but show prompt and ask user for password\n");
	fprintf(stderr,"    -n   --nostdopts            do not add standard MFS mount options: '-o " DEFAULT_OPTIONS ",fsname=MFS'\n");
	fprintf(stderr,"    -o mfscfgfile=CFGFILE       load some mount options from external file (if not specified then use default file: " ETC_PATH "/mfs/mfsmount.cfg or " ETC_PATH "/mfsmount.cfg)\n");
	fprintf(stderr,"    -o mfsdebug                 print some debugging information\n");
	fprintf(stderr,"    -o mfsmeta                  mount meta filesystem (trash etc.)\n");
	fprintf(stderr,"    -o mfsflattrash             use flat trash structure in meta\n");
	fprintf(stderr,"    -o mfsdelayedinit           connection with master is done in background - with this option mount can be run without network (good for being run from fstab / init scripts etc.)\n");
#if defined(__linux__)
	fprintf(stderr,"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 1)\n");
#else
	fprintf(stderr,"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 0)\n");
#endif
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
	fprintf(stderr,"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: EXT)\n");
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
	fprintf(stderr,"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: BSD)\n");
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
	fprintf(stderr,"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: OSX)\n");
#else
	fprintf(stderr,"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: NEVER)\n");
#endif
#if defined(__FreeBSD__)
	fprintf(stderr,"    -o mfscachemode=CMODE       set cache mode (see below ; default: DIRECT)\n");
#else
	fprintf(stderr,"    -o mfscachemode=CMODE       set cache mode (see below ; default: AUTO)\n");
#endif
	fprintf(stderr,"    -o mfscachefiles            (deprecated) equivalent to '-o mfscachemode=YES'\n");
	fprintf(stderr,"    -o mfsattrcacheto=SEC       set attributes cache timeout in seconds (default: 1.0)\n");
	fprintf(stderr,"    -o mfsxattrcacheto=SEC      set extended attributes (xattr) cache timeout in seconds (default: 30.0)\n");
	fprintf(stderr,"    -o mfsentrycacheto=SEC      set file entry cache timeout in seconds (default: 0.0)\n");
	fprintf(stderr,"    -o mfsdirentrycacheto=SEC   set directory entry cache timeout in seconds (default: 1.0)\n");
	fprintf(stderr,"    -o mfsnegentrycacheto=SEC   set negative entry cache timeout in seconds (default: 0.0)\n");
	fprintf(stderr,"    -o mfsgroupscacheto=SEC     set supplementary groups cache timeout in seconds (default: 300.0)\n");
	fprintf(stderr,"    -o mfsrlimitnofile=N        on startup mfsmount tries to change number of descriptors it can simultaneously open (default: 100000)\n");
	fprintf(stderr,"    -o mfsnice=N                on startup mfsmount tries to change his 'nice' value (default: -19)\n");
#ifdef MFS_USE_MEMLOCK
	fprintf(stderr,"    -o mfsmemlock               try to lock memory\n");
#endif
#ifdef MFS_USE_MALLOPT
	fprintf(stderr,"    -o mfslimitarenas=N         if N>0 then limit glibc malloc arenas (default: 4)\n");
#endif
#if defined(__linux__) && defined(OOM_ADJUSTABLE)
	fprintf(stderr,"    -o mfsallowoomkiller        do not disable out of memory killer\n");
#endif
	fprintf(stderr,"    -o mfsfsyncmintime=SEC      force fsync before last file close when file was opened/created at least SEC seconds earlier (default: 0.0 - always do fsync before close)\n");
	fprintf(stderr,"    -o mfswritecachesize=N      define size of write cache in MiB (default: 256)\n");
	fprintf(stderr,"    -o mfsreadaheadsize=N       define size of all read ahead buffers in MiB (default: 256)\n");
	fprintf(stderr,"    -o mfsreadaheadleng=N       define amount of bytes to be additionally read (default: 1048576)\n");
	fprintf(stderr,"    -o mfsreadaheadtrigger=N    define amount of bytes read sequentially that turns on read ahead (default: 10 * mfsreadaheadleng)\n");
	fprintf(stderr,"    -o mfsioretries=N           define number of retries before I/O error is returned (default: 30)\n");
	fprintf(stderr,"    -o mfstimeout=N             define maximum timeout in seconds before I/O error is returned (default: 0 - which means no timeout)\n");
	fprintf(stderr,"    -o mfslogretry=N            define minimal retry counter on which system will start log I/O messages (default: 5)\n");
	fprintf(stderr,"    -o mfsmaster=HOST           define mfsmaster location (default: " DEFAULT_MASTERNAME ")\n");
	fprintf(stderr,"    -o mfsport=PORT             define mfsmaster port number (default: " DEFAULT_MASTER_CLIENT_PORT ")\n");
	fprintf(stderr,"    -o mfsbind=IP               define source ip address for connections (default: NOT DEFINED - chosen automatically by OS)\n");
	fprintf(stderr,"    -o mfsproxy=IP              define listen ip address of local master proxy for communication with tools (default: 127.0.0.1)\n");
	fprintf(stderr,"    -o mfssubfolder=PATH        define subfolder to mount as root (default: /)\n");
	fprintf(stderr,"    -o mfspassword=PASSWORD     authenticate to mfsmaster with given password\n");
	fprintf(stderr,"    -o mfspassfile=FILENAME     authenticate to mfsmaster with password from given file\n");
	fprintf(stderr,"    -o mfsmd5pass=MD5           authenticate to mfsmaster using directly given md5 (only if mfspassword is not defined)\n");
	fprintf(stderr,"    -o mfsdonotrememberpassword do not remember password in memory - more secure, but when session is lost then new session is created without password\n");
	fprintf(stderr,"    -o mfspreflabels=LABELEXPR  specify preferred labels for choosing chunkservers during I/O\n");
	fprintf(stderr,"    -o mfsnoxattrs              turn off xattr support\n");
#if FUSE_VERSION >= 26
	fprintf(stderr,"    -o mfsnoposixlocks          turn off support for global posix locks (lockf + ioctl) - locks will work locally\n");
#endif
#if FUSE_VERSION >= 29
	fprintf(stderr,"    -o mfsnobsdlocks            turn off support for global BSD locks (flock) - locks will work locally\n");
#endif
	fprintf(stderr,"\n");
	fprintf(stderr,"CMODE can be set to:\n");
	fprintf(stderr,"    DIRECT                      forces direct io (bypasses cache)\n");
	fprintf(stderr,"    NO,NONE or NEVER            never allow files data to be kept in cache (safest but can reduce efficiency)\n");
	fprintf(stderr,"    YES or ALWAYS               always allow files data to be kept in cache (dangerous)\n");
	fprintf(stderr,"    AUTO                        file cache is managed by mfsmaster automatically (should be very safe and efficient)\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"SMODE can be set to:\n");
	fprintf(stderr,"    NEVER                       MFS will not change suid and sgid bit on chown\n");
	fprintf(stderr,"    ALWAYS                      clear suid and sgid on every chown - safest operation\n");
	fprintf(stderr,"    OSX                         standard behavior in OS X and Solaris (chown made by unprivileged user clear suid and sgid)\n");
	fprintf(stderr,"    BSD                         standard behavior in *BSD systems (like in OSX, but only when something is really changed)\n");
	fprintf(stderr,"    EXT                         standard behavior in most file systems on Linux (directories not changed, others: suid cleared always, sgid only when group exec bit is set)\n");
	fprintf(stderr,"    XFS                         standard behavior in XFS on Linux (like EXT but directories are changed by unprivileged users)\n");
	fprintf(stderr,"SMODE extra info:\n");
	fprintf(stderr,"    btrfs,ext2,ext3,ext4,hfs[+],jfs,ntfs and reiserfs on Linux work as 'EXT'.\n");
	fprintf(stderr,"    Only xfs on Linux works a little different. Beware that there is a strange\n");
	fprintf(stderr,"    operation - chown(-1,-1) which is usually converted by a kernel into something\n");
	fprintf(stderr,"    like 'chmod ug-s', and therefore can't be controlled by MFS as 'chown'\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"LABELEXPR grammar:\n");
	fprintf(stderr,"    LABELEXPR -> S ';' LABELEXPR | S\n");
	fprintf(stderr,"    S -> S '+' M | M\n");
	fprintf(stderr,"    M -> M L | L\n");
	fprintf(stderr,"    L -> 'a' .. 'z' | 'A' .. 'Z' | '(' S ')' | '[' S ']'\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"    Subexpressions should be placed in priority order.\n");
	fprintf(stderr,"    Up to nine subexpressions (priorities) can be specified.\n");
	fprintf(stderr,"\n");
}

static void mfs_opt_parse_cfg_file(const char *filename,int optional,struct fuse_args *outargs) {
	FILE *fd;
	char lbuff[1000],*p;

	fd = fopen(filename,"r");
	if (fd==NULL) {
		if (optional==0) {
			fprintf(stderr,"can't open cfg file: %s\n",filename);
			abort();
		}
		return;
	}
	custom_cfg = 1;
	while (fgets(lbuff,999,fd)) {
		if (lbuff[0]!='#' && lbuff[0]!=';') {
			lbuff[999]=0;
			for (p = lbuff ; *p ; p++) {
				if (*p=='\r' || *p=='\n') {
					*p=0;
					break;
				}
			}
			p--;
			while (p>=lbuff && (*p==' ' || *p=='\t')) {
				*p=0;
				p--;
			}
			p = lbuff;
			while (*p==' ' || *p=='\t') {
				p++;
			}
			if (*p) {
//				printf("add option: %s\n",p);
				if (*p=='-') {
					fuse_opt_add_arg(outargs,p);
				} else if (*p=='/') {
					if (defaultmountpoint) {
						free(defaultmountpoint);
					}
					defaultmountpoint = strdup(p);
				} else {
					fuse_opt_add_arg(outargs,"-o");
					fuse_opt_add_arg(outargs,p);
				}
			}
		}
	}
	fclose(fd);
}

static int mfs_opt_proc_stage1(void *data, const char *arg, int key, struct fuse_args *outargs) {
	struct fuse_args *defargs = (struct fuse_args*)data;
	(void)outargs;

	if (key==KEY_CFGFILE) {
		if (memcmp(arg,"mfscfgfile=",11)==0) {
			mfs_opt_parse_cfg_file(arg+11,0,defargs);
		} else if (arg[0]=='-' && arg[1]=='c') {
			mfs_opt_parse_cfg_file(arg+2,0,defargs);
		}
		return 0;
	}
	return 1;
}

// return value:
//   0 - discard this arg
//   1 - keep this arg for future processing
static int mfs_opt_proc_stage2(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)data;

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		return 1;
	case FUSE_OPT_KEY_NONOPT:
		return 1;
	case KEY_HOST:
		if (mfsopts.masterhost!=NULL) {
			free(mfsopts.masterhost);
		}
		mfsopts.masterhost = strdup(arg+2);
		return 0;
	case KEY_PORT:
		if (mfsopts.masterport!=NULL) {
			free(mfsopts.masterport);
		}
		mfsopts.masterport = strdup(arg+2);
		return 0;
	case KEY_BIND:
		if (mfsopts.bindhost!=NULL) {
			free(mfsopts.bindhost);
		}
		mfsopts.bindhost = strdup(arg+2);
		return 0;
	case KEY_PROXY:
		if (mfsopts.proxyhost!=NULL) {
			free(mfsopts.proxyhost);
		}
		mfsopts.proxyhost = strdup(arg+2);
		return 0;
	case KEY_PATH:
		if (mfsopts.subfolder!=NULL) {
			free(mfsopts.subfolder);
		}
		mfsopts.subfolder = strdup(arg+2);
		return 0;
	case KEY_PASSWORDASK:
		mfsopts.passwordask = 1;
		return 0;
	case KEY_META:
		mfsopts.meta = 1;
		return 0;
	case KEY_NOSTDMOUNTOPTIONS:
		mfsopts.nostdmountoptions = 1;
		return 0;
	case KEY_VERSION:
		fprintf(stderr, "MFS version %s\n",VERSSTR);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);

			fuse_opt_add_arg(&helpargs,outargs->argv[0]);
			fuse_opt_add_arg(&helpargs,"--version");
			fuse_parse_cmdline(&helpargs,NULL,NULL,NULL);
			fuse_mount(NULL,&helpargs);
		}
		exit(0);
	case KEY_HELP:
		usage(outargs->argv[0]);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);

			fuse_opt_add_arg(&helpargs,outargs->argv[0]);
			fuse_opt_add_arg(&helpargs,"-ho");
			fuse_parse_cmdline(&helpargs,NULL,NULL,NULL);
			fuse_mount("",&helpargs);
		}
		exit(1);
	default:
		fprintf(stderr, "internal error\n");
		abort();
	}
}

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn) {
	int *piped = (int*)userdata;
	char s;
	conn->max_write = 131072;
	conn->max_readahead = 131072;
#if defined(FUSE_CAP_BIG_WRITES) || defined(FUSE_CAP_DONT_MASK) || defined(FUSE_CAP_FLOCK_LOCKS) || defined(FUSE_CAP_POSIX_LOCKS)
	conn->want = 0;
#endif
#ifdef FUSE_CAP_BIG_WRITES
	conn->want |= FUSE_CAP_BIG_WRITES;
#endif
#ifdef FUSE_CAP_DONT_MASK
	conn->want |= FUSE_CAP_DONT_MASK;
#endif
#ifdef FUSE_CAP_FLOCK_LOCKS
	if (mfsopts.nobsdlocks==0) {
		conn->want |= FUSE_CAP_FLOCK_LOCKS;
	}
#endif
#ifdef FUSE_CAP_POSIX_LOCKS
	if (mfsopts.noposixlocks==0) {
		conn->want |= FUSE_CAP_POSIX_LOCKS;
	}
#endif
	if (piped[1]>=0) {
		s=0;
		if (write(piped[1],&s,1)!=1) {
			syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
		}
		close(piped[1]);
	}
}

uint32_t main_snprint_parameters(char *buff,uint32_t size) {
	uint32_t leng = 0;

#define bprintf(...) { if (leng<size) leng+=snprintf(buff+leng,size-leng,__VA_ARGS__); }
#define NOTNULL(a) (((a)==NULL)?"(not defined)":(a))
#define DIRECTOPT(name,string) bprintf(name ": %s\n",string);
#define BOOLOPT(name,opt) bprintf(name ": %s\n",(mfsopts.opt)?"(defined)":"(not defined)");
#define STROPT(name,opt) bprintf(name ": %s\n",NOTNULL(mfsopts.opt));
#define NUMOPT(name,format,opt) bprintf(name ": %" format "\n",(mfsopts.opt));
	STROPT("mfsmaster",masterhost);
	STROPT("mfsport",masterport);
	STROPT("mfsbind",bindhost);
	STROPT("mfsproxy",proxyhost);
	STROPT("mfssubfolder",subfolder);
	STROPT("mfspassword",password);
	STROPT("mfspassfile",passfile);
	STROPT("mfsmd5pass",md5pass);
	STROPT("mfspreflabels",preferedlabels);
	NUMOPT("mfsrlimitnofile","u",nofile);
	NUMOPT("mfsnice","d",nice);
	BOOLOPT("mfssuid",mfssuid);
	BOOLOPT("mfsdev",mfsdev);
#ifdef MFS_USE_MEMLOCK
	BOOLOPT("mfsmemlock",memlock);
#endif
#ifdef MFS_USE_MALLOPT
	NUMOPT("mfslimitarenas","u",limitarenas);
#endif
#if defined(__linux__) && defined(OOM_ADJUSTABLE)
	BOOLOPT("mfsallowoomkiller",allowoomkiller);
#endif
	NUMOPT("mfswritecachesize","u",writecachesize);
	NUMOPT("mfsreadaheadsize","u",readaheadsize);
	NUMOPT("mfsreadaheadleng","u",readaheadleng);
	NUMOPT("mfsreadaheadtrigger","u",readaheadtrigger);
	NUMOPT("mfsioretries","u",ioretries);
	NUMOPT("mfstimeout","u",timeout);
	NUMOPT("mfslogretry","u",logretry);
	BOOLOPT("mfsdebug",debug);
	BOOLOPT("mfsmeta",meta);
	BOOLOPT("mfsflattrash",flattrash);
	BOOLOPT("mfsdelayedinit",delayedinit);
	BOOLOPT("mfsdonotrememberpassword",donotrememberpassword);
	BOOLOPT("mfscachefiles",cachefiles);
	BOOLOPT("mfsnoxattrs",noxattrs);
	BOOLOPT("mfsnoposixlocks",noposixlocks);
	BOOLOPT("mfsnobsdlocks",nobsdlocks);
	STROPT("mfscachemode",cachemode);
	NUMOPT("mfsmkdircopysgid","u",mkdircopysgid);
	STROPT("mfssugidclearmode",sugidclearmodestr);
	NUMOPT("mfsattrcacheto",".3lf",attrcacheto);
	NUMOPT("mfsxattrcacheto",".3lf",xattrcacheto);
	NUMOPT("mfsentrycacheto",".3lf",entrycacheto);
	NUMOPT("mfsdirentrycacheto",".3lf",direntrycacheto);
	NUMOPT("mfsnegentrycacheto",".3lf",negentrycacheto);
	NUMOPT("mfsgroupscacheto",".3lf",groupscacheto);
	NUMOPT("mfsfsyncmintime",".3lf",fsyncmintime);
	BOOLOPT("mfsfsyncbeforeclose",fsyncbeforeclose);
	DIRECTOPT("working_keep_cache_mode",
			(mfsopts.keepcache==0)?"AUTO":
			(mfsopts.keepcache==1)?"YES":
			(mfsopts.keepcache==2)?"NO":
			(mfsopts.keepcache==3)?"DIRECT":
			"(unknown value)");
	DIRECTOPT("working_sugid_clear_mode",
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_NEVER)?"NEVER":
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_ALWAYS)?"ALWAYS":
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_OSX)?"OSX":
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_BSD)?"BSD":
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_EXT)?"EXT":
			(mfsopts.sugidclearmode==SUGID_CLEAR_MODE_XFS)?"XFS":
			"(unknown value)");
	DIRECTOPT("no_std_mount_options",(mfsopts.nostdmountoptions)?"(defined)":"(not defined)");
	return leng;
}

int mainloop(struct fuse_args *args,const char* mp,int mt,int fg,int argc_back,char **argv_back) {
	struct fuse_session *se;
	struct fuse_chan *ch;
	struct rlimit rls;
	int piped[2];
	char s;
	int err;
	int i;
	md5ctx ctx;
	uint8_t md5pass[16];

	if (mfsopts.passwordask && mfsopts.password==NULL && mfsopts.md5pass==NULL) {
		mfsopts.password = getpass("MFS Password:");
	}
	if (mfsopts.password) {
		md5_init(&ctx);
		md5_update(&ctx,(uint8_t*)(mfsopts.password),strlen(mfsopts.password));
		md5_final(md5pass,&ctx);
		memset(mfsopts.password,0,strlen(mfsopts.password));
	} else if (mfsopts.md5pass) {
		uint8_t *p = (uint8_t*)(mfsopts.md5pass);
		for (i=0 ; i<16 ; i++) {
			if (*p>='0' && *p<='9') {
				md5pass[i]=(*p-'0')<<4;
			} else if (*p>='a' && *p<='f') {
				md5pass[i]=(*p-'a'+10)<<4;
			} else if (*p>='A' && *p<='F') {
				md5pass[i]=(*p-'A'+10)<<4;
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
			if (*p>='0' && *p<='9') {
				md5pass[i]+=(*p-'0');
			} else if (*p>='a' && *p<='f') {
				md5pass[i]+=(*p-'a'+10);
			} else if (*p>='A' && *p<='F') {
				md5pass[i]+=(*p-'A'+10);
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
		}
		if (*p) {
			fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
			return 1;
		}
		memset(mfsopts.md5pass,0,strlen(mfsopts.md5pass));
	}

	if (mfsopts.delayedinit) {
		fs_init_master_connection(mfsopts.bindhost,mfsopts.masterhost,mfsopts.masterport,mfsopts.meta,mp,mfsopts.subfolder,(mfsopts.password||mfsopts.md5pass)?md5pass:NULL,mfsopts.donotrememberpassword,1);
	} else {
		if (fs_init_master_connection(mfsopts.bindhost,mfsopts.masterhost,mfsopts.masterport,mfsopts.meta,mp,mfsopts.subfolder,(mfsopts.password||mfsopts.md5pass)?md5pass:NULL,mfsopts.donotrememberpassword,0)<0) {
			return 1;
		}
	}
	memset(md5pass,0,16);

	if (fg==0) {
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY , LOG_DAEMON);
	} else {
#if defined(LOG_PERROR)
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
#else
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}

	i = mfsopts.nofile;
	while (1) {
		rls.rlim_cur = i;
		rls.rlim_max = i;
		if (setrlimit(RLIMIT_NOFILE,&rls)<0) {
			i /= 2;
			if (i<1000) {
				break;
			}
		} else {
			break;
		}
	}
	if (i != (int)(mfsopts.nofile)) {
		fprintf(stderr,"can't set open file limit to %u\n",mfsopts.nofile);
		if (i>=1000) {
			fprintf(stderr,"open file limit set to: %d\n",i);
		}
	}

	setpriority(PRIO_PROCESS,getpid(),mfsopts.nice);
#ifdef MFS_USE_MEMLOCK
	if (mfsopts.memlock) {
		rls.rlim_cur = RLIM_INFINITY;
		rls.rlim_max = RLIM_INFINITY;
		if (setrlimit(RLIMIT_MEMLOCK,&rls)<0) {
			mfsopts.memlock=0;
		}
	}
#endif

	piped[0] = piped[1] = -1;
	if (fg==0) {
		if (pipe(piped)<0) {
			fprintf(stderr,"pipe error\n");
			return 1;
		}
		err = fork();
		if (err<0) {
			fprintf(stderr,"fork error\n");
			return 1;
		} else if (err>0) {
			close(piped[1]);
			err = read(piped[0],&s,1);
			if (err==0) {
				s=1;
			}
			return s;
		}
		close(piped[0]);
		s=1;
	}


#ifdef MFS_USE_MEMLOCK
	if (mfsopts.memlock) {
		if (mlockall(MCL_CURRENT|MCL_FUTURE)==0) {
			syslog(LOG_NOTICE,"process memory was successfully locked in RAM");
		}
	}
#endif

/* glibc malloc tuning */
#ifdef MFS_USE_MALLOPT
	if (mfsopts.limitarenas) {
		if (!getenv("MALLOC_ARENA_MAX")) {
			syslog(LOG_NOTICE,"setting glibc malloc arena max to %u",mfsopts.limitarenas);
			mallopt(M_ARENA_MAX, mfsopts.limitarenas);
		}
		if (!getenv("MALLOC_ARENA_TEST")) {
			syslog(LOG_NOTICE,"setting glibc malloc arena test to %u",mfsopts.limitarenas);
			mallopt(M_ARENA_TEST, mfsopts.limitarenas);
		}
	} else {
		syslog(LOG_NOTICE,"setting glibc malloc arenas turned off");
	}
#endif /* glibc malloc tuning */

#if defined(__linux__) && defined(OOM_ADJUSTABLE)
	if (mfsopts.allowoomkiller==0) {
		FILE *fd;
		int dis;
		dis = 0;
#  if defined(OOM_SCORE_ADJ_MIN)
		fd = fopen("/proc/self/oom_score_adj","w");
		if (fd!=NULL) {
			fprintf(fd,"%d\n",OOM_SCORE_ADJ_MIN);
			fclose(fd);
			dis = 1;
#    if defined(OOM_DISABLE)
		} else {
			fd = fopen("/proc/self/oom_adj","w");
			if (fd!=NULL) {
				fprintf(fd,"%d\n",OOM_DISABLE);
				fclose(fd);
				dis = 1;
			}
#    endif
		}
#  elif defined(OOM_DISABLE)
		fd = fopen("/proc/self/oom_adj","w");
		if (fd!=NULL) {
			fprintf(fd,"%d\n",OOM_DISABLE);
			fclose(fd);
			dis = 1;
		}
#  endif
		if (dis) {
			syslog(LOG_NOTICE,"out of memory killer disabled");
		} else {
			syslog(LOG_NOTICE,"can't disable out of memory killer");
		}
	}
#endif

	syslog(LOG_NOTICE,"monotonic clock function: %s",monotonic_method());
	syslog(LOG_NOTICE,"monotonic clock speed: %"PRIu32" ops / 10 mili seconds",monotonic_speed());

	conncache_init(200);
	chunkrwlock_init();
	chunksdatacache_init();
	symlink_cache_init();
	negentry_cache_init(mfsopts.negentrycacheto);
//	dir_cache_init();
	fs_init_threads(mfsopts.ioretries,mfsopts.timeout);
	if (masterproxy_init(mfsopts.proxyhost)<0) {
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunksdatacache_term();
		chunkrwlock_term();
		conncache_term();
		return 1;
	}

//	fs_term();
//	negentry_cache_term();
//	symlink_cache_term();
//	chunksdatacache_term();
//	chunkrwlock_term();
//	conncache_term();
//	return 1;

	if (mfsopts.meta==0) {
		csdb_init();
		delay_init();
		read_data_init(mfsopts.readaheadsize*1024*1024,mfsopts.readaheadleng,mfsopts.readaheadtrigger,mfsopts.ioretries,mfsopts.timeout,mfsopts.logretry);
		write_data_init(mfsopts.writecachesize*1024*1024,mfsopts.ioretries,mfsopts.timeout,mfsopts.logretry);
	}

 	ch = fuse_mount(mp, args);
	if (ch==NULL) {
		fprintf(stderr,"error in fuse_mount\n");
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (mfsopts.meta==0) {
			write_data_term();
			read_data_term();
			delay_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunksdatacache_term();
		chunkrwlock_term();
		conncache_term();
		return 1;
	}

	if (mfsopts.meta) {
		mfs_meta_init(mfsopts.debug,mfsopts.entrycacheto,mfsopts.attrcacheto,mfsopts.flattrash);
		se = fuse_lowlevel_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), (void*)piped);
	} else {
		mfs_init(ch,mfsopts.debug,mfsopts.keepcache,mfsopts.direntrycacheto,mfsopts.entrycacheto,mfsopts.attrcacheto,mfsopts.xattrcacheto,mfsopts.groupscacheto,mfsopts.mkdircopysgid,mfsopts.sugidclearmode,1,mfsopts.fsyncmintime,mfsopts.noxattrs,mfsopts.noposixlocks,mfsopts.nobsdlocks); //mfsopts.xattraclsupport);
		se = fuse_lowlevel_new(args, &mfs_oper, sizeof(mfs_oper), (void*)piped);
	}
	if (se==NULL) {
		fuse_unmount(mp,ch);
		fprintf(stderr,"error in fuse_lowlevel_new\n");
		portable_usleep(100000);	// time for print other error messages by FUSE
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (mfsopts.meta==0) {
			write_data_term();
			read_data_term();
			delay_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunksdatacache_term();
		chunkrwlock_term();
		conncache_term();
		return 1;
	}

//	fprintf(stderr,"check\n");
	fuse_session_add_chan(se, ch);

	if (fuse_set_signal_handlers(se)<0) {
		fprintf(stderr,"error in fuse_set_signal_handlers\n");
		fuse_session_remove_chan(ch);
		fuse_session_destroy(se);
		fuse_unmount(mp,ch);
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (mfsopts.meta==0) {
			write_data_term();
			read_data_term();
			delay_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunksdatacache_term();
		chunkrwlock_term();
		conncache_term();
		return 1;
	}

	processname_init(argc_back,argv_back);

	if (mfsopts.debug==0 && fg==0) {
		setsid();
		setpgid(0,getpid());
		if ((i = open("/dev/null", O_RDWR, 0)) != -1) {
			(void)dup2(i, STDIN_FILENO);
			(void)dup2(i, STDOUT_FILENO);
			(void)dup2(i, STDERR_FILENO);
			if (i>2) close (i);
		}
	}

	sinodes_init(mp);
	sstats_init();

	{
		char pname[256];
		snprintf(pname,256,"mfsmount (mounted on: %s)",mp);
		pname[255] = 0;
		processname_set(pname);
	}

	if (mt) {
		err = fuse_session_loop_mt(se);
	} else {
		err = fuse_session_loop(se);
	}
	if (err) {
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
			}
			close(piped[1]);
		}
	}

	sstats_term();
	sinodes_term();

	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
	fuse_session_destroy(se);
	fuse_unmount(mp,ch);
	if (mfsopts.meta==0) {
		mfs_term();
		write_data_term();
		read_data_term();
		delay_term();
		csdb_term();
	}
	masterproxy_term();
	fs_term();
//	dir_cache_term();
	negentry_cache_term();
	symlink_cache_term();
	chunksdatacache_term();
	chunkrwlock_term();
	conncache_term();
	return err ? 1 : 0;
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg) {
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1) {
		return -1;
	}
	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos], sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

static unsigned int strncpy_remove_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',') {
			*dstbuff++ = c;
			l++;
		}
	}
	*dstbuff=0;
	return l;
}

#if HAVE_FUSE_VERSION
static unsigned int strncpy_escape_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',' && c!='\\') {
			*dstbuff++ = c;
			l++;
		} else {
			if (l+2<dstsize) {
				*dstbuff++ = '\\';
				*dstbuff++ = c;
				l+=2;
			} else {
				*dstbuff=0;
				return l;
			}
		}
	}
	*dstbuff=0;
	return l;
}
#endif

void remove_mfsmount_magic(struct fuse_args *args) {
	int i;
	for (i=1 ; i<args->argc ; i++) {
		if (strcmp(args->argv[i],"mfsmount_magic")==0) {
			if (i+1 < args->argc) {
				memmove(&args->argv[i],&args->argv[i+1],sizeof(char *)*(args->argc - i - 1));
			}
			args->argc--;
			return;
		}
	}
}

void make_fsname(struct fuse_args *args) {
	char fsnamearg[256];
	unsigned int l;
#if HAVE_FUSE_VERSION
	int libver;
	libver = fuse_version();
	if (libver >= 27) {
		l = snprintf(fsnamearg,256,"-osubtype=mfs%s,fsname=",(mfsopts.meta)?"meta":"");
		if (libver >= 28) {
			l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.masterport);
			if (mfsopts.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
				l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		} else {
			l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterport);
			if (mfsopts.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
				l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		}
	} else {
#else
		l = snprintf(fsnamearg,256,"-ofsname=mfs%s#",(mfsopts.meta)?"meta":"");
		l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterhost);
		if (l<255) {
			fsnamearg[l++]=':';
		}
		l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterport);
		if (mfsopts.subfolder[0]!='/') {
			if (l<255) {
				fsnamearg[l++]='/';
			}
		}
		if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
			l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.subfolder);
		}
		if (l>255) {
			l=255;
		}
		fsnamearg[l]=0;
#endif
#if HAVE_FUSE_VERSION
	}
#endif
	fuse_opt_insert_arg(args, 1, fsnamearg);
}

/* debug function
void dump_args(const char *prfx,struct fuse_args *args) {
	int i;
	for (i=0 ; i<args->argc ; i++) {
		printf("%s [%d]: %s\n",prfx,i,args->argv[i]);
	}
}
*/

char* password_read(const char *filename) {
	FILE *fd;
	char passwordbuff[1024];
	char *ret;
	int i;

	fd = fopen(filename,"r");
	if (fd==NULL) {
		fprintf(stderr,"error opening password file: %s\n",filename);
		return NULL;
	}
	if (fgets(passwordbuff,1024,fd)==NULL) {
		fprintf(stderr,"password file (%s) is empty\n",filename);
		fclose(fd);
		return NULL;
	}
	fclose(fd);
	passwordbuff[1023]=0;
	i = strlen(passwordbuff);
	while (i>0) {
		i--;
		if (passwordbuff[i]=='\n' || passwordbuff[i]=='\r') {
			passwordbuff[i]=0;
		} else {
			break;
		}
	}
	if (i==0) {
		fprintf(stderr,"first line in password file (%s) is empty\n",filename);
		return NULL;
	}
	ret = malloc(i+1);
	passert(ret);
	memcpy(ret,passwordbuff,i);
	memset(passwordbuff,0,1024);
	ret[i] = 0;
	return ret;
}

int main(int argc, char *argv[]) {
	int res;
	int mt,fg;
	int i;
	char *mountpoint;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args defaultargs = FUSE_ARGS_INIT(0, NULL);
	int argc_back = argc;
	char **argv_back = argv;

#if defined(SIGPIPE) && defined(SIG_IGN)
	signal(SIGPIPE,SIG_IGN);
#endif
	strerr_init();
	mycrc32_init();

	setenv("FUSE_THREAD_STACK","524288",0); // works good with 262144 but not 131072, so for safety we will use 524288

	memset(&mfsopts,0,sizeof(mfsopts));
	mfsopts.masterhost = NULL;
	mfsopts.masterport = NULL;
	mfsopts.bindhost = NULL;
	mfsopts.proxyhost = NULL;
	mfsopts.subfolder = NULL;
	mfsopts.password = NULL;
	mfsopts.passfile = NULL;
	mfsopts.md5pass = NULL;
	mfsopts.preferedlabels = NULL;
	mfsopts.nofile = 0;
	mfsopts.nice = -19;
	mfsopts.mfssuid = 0;
	mfsopts.mfsdev = 0;
#ifdef MFS_USE_MEMLOCK
	mfsopts.memlock = 0;
#endif
#ifdef MFS_USE_MALLOPT
	mfsopts.limitarenas = 4;
#endif
#if defined(__linux__) && defined(OOM_DISABLE)
	mfsopts.allowoomkiller = 0;
#endif
	mfsopts.nostdmountoptions = 0;
	mfsopts.meta = 0;
	mfsopts.flattrash = 0;
	mfsopts.debug = 0;
	mfsopts.delayedinit = 0;
#ifdef __linux__
	mfsopts.mkdircopysgid = 1;
#else
	mfsopts.mkdircopysgid = 0;
#endif
	mfsopts.sugidclearmodestr = NULL;
	mfsopts.donotrememberpassword = 0;
//	mfsopts.xattraclsupport = 0;
	mfsopts.cachefiles = 0;
	mfsopts.noxattrs = 0;
	mfsopts.noposixlocks = 0;
	mfsopts.nobsdlocks = 0;
	mfsopts.cachemode = NULL;
	mfsopts.writecachesize = 0;
	mfsopts.readaheadsize = 0;
	mfsopts.readaheadleng = 0;
	mfsopts.readaheadtrigger = 0;
	mfsopts.ioretries = 30;
	mfsopts.timeout = 0;
	mfsopts.logretry = 5;
	mfsopts.passwordask = 0;
	mfsopts.attrcacheto = 1.0;
	mfsopts.xattrcacheto = 30.0;
	mfsopts.entrycacheto = 0.0;
	mfsopts.direntrycacheto = 1.0;
	mfsopts.negentrycacheto = 0.0;
	mfsopts.groupscacheto = 300.0;
	mfsopts.fsyncbeforeclose = 0;
	mfsopts.fsyncmintime = 0.0;

	custom_cfg = 0;

//	dump_args("input_args",&args);

	if (args.argc>1) {
		uint32_t hostlen,portlen,colons;
		char *c,*portbegin;
		int optpos;
		// skip options in format '-o XXXX' and '-oXXXX'
		optpos = 1;
		while (optpos<args.argc) {
			c = args.argv[optpos];
			if (c[0]=='-' && c[1]=='o') {
				if (c[2]) {
					optpos++;
				} else {
					optpos+=2;
				}
			} else {
				break;
			}
		}
		if (optpos<args.argc) {
			// check if next arg matches to HOST[:PORT]:[PATH]
			c = args.argv[optpos];
			colons = 0;
			for (i=0 ; c[i] ; i++) {
				if (c[i]==':') {
					colons++;
				}
			}
			if (colons>0) {
				hostlen = 0;
				portlen = 0;
				portbegin = NULL;
				while (((*c)>='a' && (*c)<='z') || ((*c)>='A' && (*c)<='Z') || ((*c)>='0' && (*c)<='9') || (*c)=='-' || (*c)=='.') { // DNS chars
					c++;
					hostlen++;
				}
				if (hostlen>0) {
					if ((*c)==':' && colons>1) {
						c++;
						portbegin = c;
						while ((*c)>='0' && ((*c)<='9')) {
							c++;
							portlen++;
						}
					}
					if ((*c)==':') { // match found
						c++;
						if (*c) {
							mfsopts.subfolder = strdup(c);
						}
						mfsopts.masterhost = malloc(hostlen+1);
						memcpy(mfsopts.masterhost,args.argv[optpos],hostlen);
						mfsopts.masterhost[hostlen]=0;
						if (portbegin!=NULL && portlen>0) {
							mfsopts.masterport = malloc(portlen+1);
							memcpy(mfsopts.masterport,portbegin,portlen);
							mfsopts.masterport[portlen]=0;
						}
						for (i=optpos+1 ; i<args.argc ; i++) {
							args.argv[i-1] = args.argv[i];
						}
						args.argc--;
					}
				}
			}
		}
	}

//	dump_args("after_first_filter",&args);

	if (fuse_opt_parse(&args, &defaultargs, mfs_opts_stage1, mfs_opt_proc_stage1)<0) {
		exit(1);
	}

	if (custom_cfg==0) {
		int cfgfd;
		char *cfgfile;

		cfgfile=strdup(ETC_PATH "/mfs/mfsmount.cfg");
		if ((cfgfd = open(cfgfile,O_RDONLY))<0 && errno==ENOENT) {
			free(cfgfile);
			cfgfile=strdup(ETC_PATH "/mfsmount.cfg");
			if ((cfgfd = open(cfgfile,O_RDONLY))>=0) {
				fprintf(stderr,"default sysconf path has changed - please move mfsmount.cfg from "ETC_PATH"/ to "ETC_PATH"/mfs/\n");
			}
		}
		if (cfgfd>=0) {
			close(cfgfd);
		}
		mfs_opt_parse_cfg_file(cfgfile,1,&defaultargs);
		free(cfgfile);
	}

//	dump_args("parsed_defaults",&defaultargs);
//	dump_args("changed_args",&args);

	for (i=0 ; i<defaultargs.argc ; i++) {
		fuse_opt_add_arg(&args,defaultargs.argv[i]);
	}

//	dump_args("combined_args",&args);

	if (fuse_opt_parse(&args, &mfsopts, mfs_opts_stage2, mfs_opt_proc_stage2)<0) {
		exit(1);
	}

//	dump_args("combined_args_after_parse",&args);

	if (mfsopts.cachemode!=NULL && mfsopts.cachefiles) {
		fprintf(stderr,"mfscachemode and mfscachefiles options are exclusive - use only mfscachemode\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	if (mfsopts.cachemode==NULL) {
#if defined(__FreeBSD__)
		mfsopts.keepcache = 3;
#else
		mfsopts.keepcache = (mfsopts.cachefiles)?1:0;
#endif
	} else if (strcasecmp(mfsopts.cachemode,"AUTO")==0) {
		mfsopts.keepcache=0;
	} else if (strcasecmp(mfsopts.cachemode,"YES")==0 || strcasecmp(mfsopts.cachemode,"ALWAYS")==0) {
		mfsopts.keepcache=1;
	} else if (strcasecmp(mfsopts.cachemode,"NO")==0 || strcasecmp(mfsopts.cachemode,"NONE")==0 || strcasecmp(mfsopts.cachemode,"NEVER")==0) {
		mfsopts.keepcache=2;
	} else if (strcasecmp(mfsopts.cachemode,"DIRECT")==0) {
		mfsopts.keepcache=3;
	} else {
		fprintf(stderr,"unrecognized cachemode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (mfsopts.sugidclearmodestr==NULL) {
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_EXT;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_BSD;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_OSX;
#else
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_NEVER;
#endif
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"NEVER")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_NEVER;
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"ALWAYS")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_ALWAYS;
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"OSX")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_OSX;
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"BSD")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_BSD;
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"EXT")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_EXT;
	} else if (strcasecmp(mfsopts.sugidclearmodestr,"XFS")==0) {
		mfsopts.sugidclearmode = SUGID_CLEAR_MODE_XFS;
	} else {
		fprintf(stderr,"unrecognized sugidclearmode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (mfsopts.masterhost==NULL) {
		mfsopts.masterhost = strdup(DEFAULT_MASTERNAME);
	}
	if (mfsopts.masterport==NULL) {
		mfsopts.masterport = strdup(DEFAULT_MASTER_CLIENT_PORT);
	}
	if (mfsopts.proxyhost==NULL) {
		mfsopts.proxyhost = strdup("127.0.0.1");
	}
	if (mfsopts.subfolder==NULL) {
		mfsopts.subfolder = strdup("/");
	}
	if (mfsopts.passfile!=NULL) {
		if (mfsopts.password!=NULL||mfsopts.md5pass!=NULL) {
			fprintf(stderr,"mfspassfile option is mutually exclusive with mfspassword and mfsmd5pass\nsee: %s -h for help\n",argv[0]);
			return 1;
		}
		mfsopts.password = password_read(mfsopts.passfile);
		if (mfsopts.password==NULL) {
			return 1;
		}
	}
	if (mfsopts.nofile==0) {
		mfsopts.nofile=100000;
	}
	if (mfsopts.writecachesize==0) {
		mfsopts.writecachesize=256;
	}
	if (mfsopts.writecachesize<16) {
		fprintf(stderr,"write cache size too low (%u MiB) - increased to 16 MiB\n",mfsopts.writecachesize);
		mfsopts.writecachesize=16;
	}
	if (mfsopts.writecachesize>2048) {
		fprintf(stderr,"write cache size too big (%u MiB) - decresed to 2048 MiB\n",mfsopts.writecachesize);
		mfsopts.writecachesize=2048;
	}
	if (mfsopts.readaheadsize==0) {
		mfsopts.readaheadsize=256;
	}
	if (mfsopts.readaheadsize<16) {
		fprintf(stderr,"read ahead size too low (%u MiB) - increased to 16 MiB\n",mfsopts.readaheadsize);
		mfsopts.readaheadsize=16;
	}
	if (mfsopts.readaheadsize>2048) {
		fprintf(stderr,"read ahead size too big (%u MiB) - decresed to 2048 MiB\n",mfsopts.readaheadsize);
		mfsopts.readaheadsize=2048;
	}
	if (mfsopts.readaheadleng==0) {
		mfsopts.readaheadleng=0x100000;
	}
	if (mfsopts.readaheadleng<0x20000) {
		fprintf(stderr,"read ahead length too low (%u B) - increased to 128 KiB\n",mfsopts.readaheadleng);
		mfsopts.readaheadleng=0x20000;
	}
	if (mfsopts.readaheadleng>0x200000) {
		fprintf(stderr,"read ahead length too big (%u B) - decresed to 2 MiB\n",mfsopts.readaheadleng);
		mfsopts.readaheadleng=0x200000;
	}
	if (mfsopts.readaheadtrigger==0) {
		mfsopts.readaheadtrigger=mfsopts.readaheadleng*10;
	}

	if (mfsopts.nostdmountoptions==0) {
		fuse_opt_add_arg(&args, "-o" DEFAULT_OPTIONS);
	}

	if (mfsopts.fsyncbeforeclose) {
		mfsopts.fsyncmintime=0.0;
	}

	if (csorder_init(mfsopts.preferedlabels)<0) {
		fprintf(stderr,"error parsing preferred labels expression\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	make_fsname(&args);
	if (mfsopts.mfssuid) {
		fuse_opt_insert_arg(&args, 1, "-osuid");
	}
	if (mfsopts.mfsdev) {
		fuse_opt_insert_arg(&args, 1, "-odev");
	}
	remove_mfsmount_magic(&args);

//	dump_args("combined_args_before_fuse_parse_cmdline",&args);

	if (fuse_parse_cmdline(&args,&mountpoint,&mt,&fg)<0) {
		fprintf(stderr,"see: %s -h for help\n",argv[0]);
		return 1;
	}

	if (!mountpoint) {
		if (defaultmountpoint) {
			mountpoint = defaultmountpoint;
		} else {
			fprintf(stderr,"no mount point\nsee: %s -h for help\n",argv[0]);
			return 1;
		}
	}

	res = mainloop(&args,mountpoint,mt,fg,argc_back,argv_back);
	fuse_opt_free_args(&defaultargs);
	fuse_opt_free_args(&args);
	free(mfsopts.masterhost);
	free(mfsopts.masterport);
	if (mfsopts.bindhost) {
		free(mfsopts.bindhost);
	}
	if (mfsopts.proxyhost) {
		free(mfsopts.proxyhost);
	}
	free(mfsopts.subfolder);
	if (defaultmountpoint) {
		free(defaultmountpoint);
	}
	stats_term();
	strerr_term();
	return res;
}
