/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#if defined(HAVE_MLOCKALL) && defined(RLIMIT_MEMLOCK) && defined(MCL_CURRENT) && defined(MCL_FUTURE)
#  define MFS_USE_MEMLOCK 1
#endif

#if defined(HAVE_MALLOC_H)
#  include <malloc.h>
#endif
#if defined(M_ARENA_MAX) && defined(M_ARENA_TEST) && defined(HAVE_MALLOPT)
#  define MFS_USE_MALLOPT 1
#endif

#include <fuse.h>
#include <fuse_opt.h>
#include <fuse_lowlevel.h>
#include <sys/time.h>
#include <sys/resource.h>
#ifdef MFS_USE_MEMLOCK
#  include <sys/mman.h>
#endif
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
#include "chunkloccache.h"
#include "symlinkcache.h"
#include "negentrycache.h"
//#include "dircache.h"
#include "conncache.h"
#include "readdata.h"
#include "writedata.h"
#include "csdb.h"
#include "stats.h"
#include "strerr.h"
#include "crc.h"

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
	.statfs		= mfs_meta_statfs,
	.lookup		= mfs_meta_lookup,
	.getattr	= mfs_meta_getattr,
	.setattr	= mfs_meta_setattr,
	.unlink		= mfs_meta_unlink,
	.rename		= mfs_meta_rename,
	.opendir	= mfs_meta_opendir,
	.readdir	= mfs_meta_readdir,
	.releasedir	= mfs_meta_releasedir,
	.open		= mfs_meta_open,
	.release	= mfs_meta_release,
	.read		= mfs_meta_read,
	.write		= mfs_meta_write,
//	.access		= mfs_meta_access
};

static struct fuse_lowlevel_ops mfs_oper = {
	.init           = mfs_fsinit,
	.statfs		= mfs_statfs,
	.lookup		= mfs_lookup,
	.getattr	= mfs_getattr,
	.setattr	= mfs_setattr,
	.mknod		= mfs_mknod,
	.unlink		= mfs_unlink,
	.mkdir		= mfs_mkdir,
	.rmdir		= mfs_rmdir,
	.symlink	= mfs_symlink,
	.readlink	= mfs_readlink,
	.rename		= mfs_rename,
	.link		= mfs_link,
	.opendir	= mfs_opendir,
	.readdir	= mfs_readdir,
	.releasedir	= mfs_releasedir,
	.create		= mfs_create,
	.open		= mfs_open,
	.release	= mfs_release,
	.flush		= mfs_flush,
	.fsync		= mfs_fsync,
	.read		= mfs_read,
	.write		= mfs_write,
	.access		= mfs_access,
	.getxattr       = mfs_getxattr,
	.setxattr       = mfs_setxattr,
	.listxattr      = mfs_listxattr,
	.removexattr    = mfs_removexattr,
#if FUSE_VERSION >= 26
//	.getlk		= mfs_getlk,
//	.setlk		= mfs_setlk,
#endif
};

struct mfsopts {
	char *masterhost;
	char *masterport;
	char *bindhost;
	char *proxyhost;
	char *subfolder;
	char *password;
	char *md5pass;
	unsigned nofile;
	signed nice;
#ifdef MFS_USE_MEMLOCK
	int memlock;
#endif
#ifdef MFS_USE_MALLOPT
	int limitarenas;
#endif
	int nostdmountoptions;
	int meta;
	int debug;
	int delayedinit;
	int mkdircopysgid;
	char *sugidclearmodestr;
	int sugidclearmode;
	char *cachemode;
	int cachefiles;
	int keepcache;
	int passwordask;
	int donotrememberpassword;
//	int xattraclsupport;
	unsigned writecachesize;
	unsigned readaheadsize;
	unsigned readaheadleng;
	unsigned readaheadtrigger;
	unsigned ioretries;
	double attrcacheto;
	double xattrcacheto;
	double entrycacheto;
	double direntrycacheto;
	double negentrycacheto;
	double groupscacheto;
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
	MFS_OPT("mfsmd5pass=%s", md5pass, 0),
	MFS_OPT("mfsrlimitnofile=%u", nofile, 0),
	MFS_OPT("mfsnice=%d", nice, 0),
#ifdef MFS_USE_MEMLOCK
	MFS_OPT("mfsmemlock", memlock, 1),
#endif
#ifdef MFS_USE_MALLOPT
	MFS_OPT("mfslimitarenas=%u", limitarenas, 0),
#endif
	MFS_OPT("mfswritecachesize=%u", writecachesize, 0),
	MFS_OPT("mfsreadaheadsize=%u", readaheadsize, 0),
	MFS_OPT("mfsreadaheadleng=%u", readaheadleng, 0),
	MFS_OPT("mfsreadaheadtrigger=%u", readaheadtrigger, 0),
	MFS_OPT("mfsioretries=%u", ioretries, 0),
	MFS_OPT("mfsdebug", debug, 1),
	MFS_OPT("mfsmeta", meta, 1),
	MFS_OPT("mfsdelayedinit", delayedinit, 1),
	MFS_OPT("mfsdonotrememberpassword", donotrememberpassword, 1),
	MFS_OPT("mfscachefiles", cachefiles, 1),
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
	fprintf(stderr,
"usage: %s mountpoint [options]\n"
"\n", progname);
	fprintf(stderr,
"general options:\n"
"    -o opt,[opt...]         mount options\n"
"    -h   --help             print help\n"
"    -V   --version          print version\n"
"\n");
	fprintf(stderr,
"MFS options:\n"
"    -c CFGFILE                  equivalent to '-o mfscfgfile=CFGFILE'\n"
"    -m   --meta                 equivalent to '-o mfsmeta'\n"
"    -H HOST                     equivalent to '-o mfsmaster=HOST'\n"
"    -P PORT                     equivalent to '-o mfsport=PORT'\n"
"    -B IP                       equivalent to '-o mfsbind=IP'\n"
"    -L IP                       equivalent to '-o mfsproxy=IP'\n"
"    -S PATH                     equivalent to '-o mfssubfolder=PATH'\n"
"    -p   --password             similar to '-o mfspassword=PASSWORD', but show prompt and ask user for password\n"
"    -n   --nostdopts            do not add standard MFS mount options: '-o " DEFAULT_OPTIONS ",fsname=MFS'\n"
"    -o mfscfgfile=CFGFILE       load some mount options from external file (if not specified then use default file: " ETC_PATH "/mfs/mfsmount.cfg or " ETC_PATH "/mfsmount.cfg)\n"
"    -o mfsdebug                 print some debugging information\n"
"    -o mfsmeta                  mount meta filesystem (trash etc.)\n"
"    -o mfsdelayedinit           connection with master is done in background - with this option mount can be run without network (good for being run from fstab / init scripts etc.)\n"
#ifdef __linux__
"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 1)\n"
#else
"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 0)\n"
#endif
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: EXT)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: BSD)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: OSX)\n"
#else
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: NEVER)\n"
#endif
"    -o mfscachemode=CMODE       set cache mode (see below ; default: AUTO)\n"
"    -o mfscachefiles            (deprecated) equivalent to '-o mfscachemode=YES'\n"
// "    -o mfscachefiles            allow files data to be kept in cache (dangerous in network environment)\n"
"    -o mfsattrcacheto=SEC       set attributes cache timeout in seconds (default: 1.0)\n"
"    -o mfsxattrcacheto=SEC      set extended attributes (xattr) cache timeout in seconds (default: 30.0)\n"
"    -o mfsentrycacheto=SEC      set file entry cache timeout in seconds (default: 0.0)\n"
"    -o mfsdirentrycacheto=SEC   set directory entry cache timeout in seconds (default: 1.0)\n"
"    -o mfsnegentrycacheto=SEC   set negative entry cache timeout in seconds (default: 1.0)\n"
"    -o mfsgroupscacheto=SEC     set supplementary groups cache timeout in seconds (default: 300.0)\n"
"    -o mfsrlimitnofile=N        on startup mfsmount tries to change number of descriptors it can simultaneously open (default: 100000)\n"
"    -o mfsnice=N                on startup mfsmount tries to change his 'nice' value (default: -19)\n"
#ifdef MFS_USE_MEMLOCK
"    -o mfsmemlock               try to lock memory\n"
#endif
#ifdef MFS_USE_MALLOPT
"    -o mfslimitarenas=N         if N>0 then limit glibc malloc arenas (default: 8)\n"
#endif
"    -o mfswritecachesize=N      define size of write cache in MiB (default: 128)\n"
"    -o mfsreadaheadsize=N       define size of all read ahead buffers in MiB (default: 128)\n"
"    -o mfsreadaheadleng=N       define amount of bytes to be additionaly read (default: 1048576)\n"
"    -o mfsreadaheadtrigger=N    define amount of bytes read sequentially that turns on read ahead (default: 10 * mfsreadaheadleng)\n"
"    -o mfsioretries=N           define number of retries before I/O error is returned (default: 30)\n"
"    -o mfsmaster=HOST           define mfsmaster location (default: " DEFAULT_MASTERNAME ")\n"
"    -o mfsport=PORT             define mfsmaster port number (default: " DEFAULT_MASTER_CLIENT_PORT ")\n"
"    -o mfsbind=IP               define source ip address for connections (default: NOT DEFINED - choosen automatically by OS)\n"
"    -o mfsproxy=IP              define listen ip address of local master proxy for communication with tools (default: 127.0.0.1)\n"
"    -o mfssubfolder=PATH        define subfolder to mount as root (default: /)\n"
"    -o mfspassword=PASSWORD     authenticate to mfsmaster with password\n"
"    -o mfsmd5pass=MD5           authenticate to mfsmaster using directly given md5 (only if mfspassword is not defined)\n"
"    -o mfsdonotrememberpassword do not remember password in memory - more secure, but when session is lost then new session is created without password\n"
"\n");
	fprintf(stderr,
"CMODE can be set to:\n"
"    NO,NONE or NEVER            never allow files data to be kept in cache (safest but can reduce efficiency)\n"
"    YES or ALWAYS               always allow files data to be kept in cache (dangerous)\n"
"    AUTO                        file cache is managed by mfsmaster automatically (should be very safe and efficient)\n"
"\n");
	fprintf(stderr,
"SMODE can be set to:\n"
"    NEVER                       MFS will not change suid and sgid bit on chown\n"
"    ALWAYS                      clear suid and sgid on every chown - safest operation\n"
"    OSX                         standard behavior in OS X and Solaris (chown made by unprivileged user clear suid and sgid)\n"
"    BSD                         standard behavior in *BSD systems (like in OSX, but only when something is really changed)\n"
"    EXT                         standard behavior in most file systems on Linux (directories not changed, others: suid cleared always, sgid only when group exec bit is set)\n"
"    XFS                         standard behavior in XFS on Linux (like EXT but directories are changed by unprivileged users)\n"
"SMODE extra info:\n"
"    btrfs,ext2,ext3,ext4,hfs[+],jfs,ntfs and reiserfs on Linux work as 'EXT'.\n"
"    Only xfs on Linux works a little different. Beware that there is a strange\n"
"    operation - chown(-1,-1) which is usually converted by a kernel into something\n"
"    like 'chmod ug-s', and therefore can't be controlled by MFS as 'chown'\n"
"\n");
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
#if defined(FUSE_CAP_BIG_WRITES) || defined(FUSE_CAP_DONT_MASK)
	conn->want = 0;
#endif
#ifdef FUSE_CAP_BIG_WRITES
	conn->want |= FUSE_CAP_BIG_WRITES;
#endif
#ifdef FUSE_CAP_DONT_MASK
	conn->want |= FUSE_CAP_DONT_MASK;
#endif
	if (piped[1]>=0) {
		s=0;
		if (write(piped[1],&s,1)!=1) {
			syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
		}
		close(piped[1]);
	}
}

int main_thread_create(pthread_t *th,const pthread_attr_t *attr,void *(*fn)(void *),void *arg) {
	sigset_t oldset;
	sigset_t newset;
	int res;

	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	res = pthread_create(th,attr,fn,arg);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	return res;
}

int main_minthread_create(pthread_t *th,uint8_t detached,void *(*fn)(void *),void *arg) {
	static pthread_attr_t *thattr = NULL;
	static uint8_t thattr_detached;
	if (thattr == NULL) {
		size_t mystacksize;
		thattr = malloc(sizeof(pthread_attr_t));
		passert(thattr);
		zassert(pthread_attr_init(thattr));
#ifdef PTHREAD_STACK_MIN
		mystacksize = PTHREAD_STACK_MIN;
		if (mystacksize < 0x100000) {
			mystacksize = 0x100000;
		}
#else
		mystacksize = 0x100000;
#endif
		zassert(pthread_attr_setstacksize(thattr,mystacksize));
		thattr_detached = detached + 1; // make it different
	}
	if (detached != thattr_detached) {
		if (detached) {
			zassert(pthread_attr_setdetachstate(thattr,PTHREAD_CREATE_DETACHED));
		} else {
			zassert(pthread_attr_setdetachstate(thattr,PTHREAD_CREATE_JOINABLE));
		}
		thattr_detached = detached;
	}
	return main_thread_create(th,thattr,fn,arg);
}

int mainloop(struct fuse_args *args,const char* mp,int mt,int fg) {
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
		fprintf(stderr,"can't set open file limit to %d\n",mfsopts.nofile);
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
			syslog(LOG_NOTICE,"setting glibc malloc arena max to 8");
			mallopt(M_ARENA_MAX, mfsopts.limitarenas);
		}
		if (!getenv("MALLOC_ARENA_TEST")) {
			syslog(LOG_NOTICE,"setting glibc malloc arena test to 1");
			mallopt(M_ARENA_TEST, 1);
		}
	} else {
		syslog(LOG_NOTICE,"setting glibc malloc arenas turned off");
	}
#endif /* glibc malloc tuning */

	syslog(LOG_NOTICE,"monotonic clock function: %s",monotonic_method());
	syslog(LOG_NOTICE,"monotonic clock speed: %"PRIu32" ops / 10 mili seconds",monotonic_speed());

	conncache_init(200);
	chunkloc_cache_init();
	symlink_cache_init();
	negentry_cache_init(mfsopts.negentrycacheto);
//	dir_cache_init();
	fs_init_threads(mfsopts.ioretries);
	if (masterproxy_init(mfsopts.proxyhost)<0) {
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunkloc_cache_term();
		return 1;
	}

//	fs_term();
//	negentry_cache_term();
//	symlink_cache_term();
//	chunkloc_cache_term();
//	return 1;

	if (mfsopts.meta==0) {
		csdb_init();
		read_data_init(mfsopts.readaheadsize*1024*1024,mfsopts.readaheadleng,mfsopts.readaheadtrigger,mfsopts.ioretries);
		write_data_init(mfsopts.writecachesize*1024*1024,mfsopts.ioretries);
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
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunkloc_cache_term();
		return 1;
	}

	if (mfsopts.meta) {
		mfs_meta_init(mfsopts.debug,mfsopts.entrycacheto,mfsopts.attrcacheto);
		se = fuse_lowlevel_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), (void*)piped);
	} else {
		mfs_init(mfsopts.debug,mfsopts.keepcache,mfsopts.direntrycacheto,mfsopts.entrycacheto,mfsopts.attrcacheto,mfsopts.xattrcacheto,mfsopts.groupscacheto,mfsopts.mkdircopysgid,mfsopts.sugidclearmode,1); //mfsopts.xattraclsupport);
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
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunkloc_cache_term();
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
			csdb_term();
		}
		masterproxy_term();
		fs_term();
//		dir_cache_term();
		negentry_cache_term();
		symlink_cache_term();
		chunkloc_cache_term();
		return 1;
	}

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
	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
	fuse_session_destroy(se);
	fuse_unmount(mp,ch);
	if (mfsopts.meta==0) {
		write_data_term();
		read_data_term();
		csdb_term();
	}
	masterproxy_term();
	fs_term();
//	dir_cache_term();
	negentry_cache_term();
	symlink_cache_term();
	chunkloc_cache_term();
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

/*
void dump_args(const char *prfx,struct fuse_args *args) {
	int i;
	for (i=0 ; i<args->argc ; i++) {
		printf("%s [%d]: %s\n",prfx,i,args->argv[i]);
	}
}
*/

int main(int argc, char *argv[]) {
	int res;
	int mt,fg;
	char *mountpoint;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args defaultargs = FUSE_ARGS_INIT(0, NULL);

#if defined(SIGPIPE) && defined(SIG_IGN)
	signal(SIGPIPE,SIG_IGN);
#endif
	strerr_init();
	mycrc32_init();

	mfsopts.masterhost = NULL;
	mfsopts.masterport = NULL;
	mfsopts.bindhost = NULL;
	mfsopts.proxyhost = NULL;
	mfsopts.subfolder = NULL;
	mfsopts.password = NULL;
	mfsopts.md5pass = NULL;
	mfsopts.nofile = 0;
	mfsopts.nice = -19;
#ifdef MFS_USE_MEMLOCK
	mfsopts.memlock = 0;
#endif
#ifdef MFS_USE_MALLOPT
	mfsopts.limitarenas = 8;
#endif
	mfsopts.nostdmountoptions = 0;
	mfsopts.meta = 0;
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
	mfsopts.cachemode = NULL;
	mfsopts.writecachesize = 0;
	mfsopts.readaheadsize = 0;
	mfsopts.readaheadleng = 0;
	mfsopts.readaheadtrigger = 0;
	mfsopts.ioretries = 30;
	mfsopts.passwordask = 0;
	mfsopts.attrcacheto = 1.0;
	mfsopts.xattrcacheto = 30.0;
	mfsopts.entrycacheto = 0.0;
	mfsopts.direntrycacheto = 1.0;
	mfsopts.negentrycacheto = 1.0;
	mfsopts.groupscacheto = 300.0;

	custom_cfg = 0;

//	dump_args("input_args",&args);

	fuse_opt_add_arg(&defaultargs,"fakeappname");

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

	if (fuse_opt_parse(&defaultargs, &mfsopts, mfs_opts_stage2, mfs_opt_proc_stage2)<0) {
		exit(1);
	}

	if (fuse_opt_parse(&args, &mfsopts, mfs_opts_stage2, mfs_opt_proc_stage2)<0) {
		exit(1);
	}

//	dump_args("args_after_parse",&args);

	if (mfsopts.cachemode!=NULL && mfsopts.cachefiles) {
		fprintf(stderr,"mfscachemode and mfscachefiles options are exclusive - use only mfscachemode\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	if (mfsopts.cachemode==NULL) {
		mfsopts.keepcache=(mfsopts.cachefiles)?1:0;
	} else if (strcasecmp(mfsopts.cachemode,"AUTO")==0) {
		mfsopts.keepcache=0;
	} else if (strcasecmp(mfsopts.cachemode,"YES")==0 || strcasecmp(mfsopts.cachemode,"ALWAYS")==0) {
		mfsopts.keepcache=1;
	} else if (strcasecmp(mfsopts.cachemode,"NO")==0 || strcasecmp(mfsopts.cachemode,"NONE")==0 || strcasecmp(mfsopts.cachemode,"NEVER")==0) {
		mfsopts.keepcache=2;
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
	if (mfsopts.nofile==0) {
		mfsopts.nofile=100000;
	}
	if (mfsopts.writecachesize==0) {
		mfsopts.writecachesize=128;
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
		mfsopts.readaheadsize=128;
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
	if (mfsopts.readaheadleng>0x1000000) {
		fprintf(stderr,"read ahead length too big (%u B) - decresed to 16 MiB\n",mfsopts.readaheadleng);
		mfsopts.readaheadleng=0x1000000;
	}
	if (mfsopts.readaheadtrigger==0) {
		mfsopts.readaheadtrigger=mfsopts.readaheadleng*10;
	}

	if (mfsopts.nostdmountoptions==0) {
		fuse_opt_add_arg(&args, "-o" DEFAULT_OPTIONS);
	}


	make_fsname(&args);
	remove_mfsmount_magic(&args);

//	dump_args("args_before_fuse_parse_cmdline",&args);

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

	res = mainloop(&args,mountpoint,mt,fg);
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
