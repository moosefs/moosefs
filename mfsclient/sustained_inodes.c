/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

/* common */
#include <sys/stat.h>
#include <inttypes.h>
#include <syslog.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

// #define RINODES_DEBUG 1

#define RINODES_CHECK_INTERVAL_100MS 300

#ifdef RINODES_DEBUG
#include <stdio.h>
#else
#include "portable.h"
#include "mastercomm.h"
#include "lwthread.h"
#include "massert.h"
#include "strerr.h"
#include "slogger.h"
// #include "sustained_stats.h"

#ifndef HAVE___SYNC_FETCH_AND_OP
static pthread_mutex_t glock;
#endif
static pthread_t clthread;
static uint8_t term;
#endif


typedef struct _sinodes_ino {
	uint32_t inode;
	struct _sinodes_ino *next;
} sinodes_ino;

static sinodes_ino *lastlist,*currentlist;

#ifdef RINODES_DEBUG

static inline void sinodes_close(uint32_t inode) {
	printf("close inode: %"PRIu32"\n",inode);
}

static inline void sinodes_open(uint32_t inode) {
	printf("open inode: %"PRIu32"\n",inode);
}

#else

static inline void sinodes_close(uint32_t inode) {
	fs_forget_entry(inode);
//	sstats_deactivate(inode);
}

static inline void sinodes_open(uint32_t inode) {
	fs_add_entry(inode);
//	sstats_activate(inode);
}

#endif


void sinodes_process_inode(uint32_t inode) {
	sinodes_ino *ril,**rilp;
	rilp = &currentlist;
	while ((ril = *rilp)) {
		if (inode > ril->inode) {
			rilp = &(ril->next);
		} else if (inode == ril->inode) {
			return;
		} else {
			break;
		}
	}
	ril = malloc(sizeof(sinodes_ino));
	ril->inode = inode;
	ril->next = *rilp;
	*rilp = ril;
}

void sinodes_end(void) {
	sinodes_ino *rill,*ricl;

	rill = lastlist;
	ricl = currentlist;

	while (rill!=NULL || ricl!=NULL) {
		if (ricl==NULL || (rill!=NULL && rill->inode < ricl->inode)) {
			sinodes_close(rill->inode);
			rill = rill->next;
		} else if (rill==NULL || rill->inode > ricl->inode) {
			sinodes_open(ricl->inode);
			ricl = ricl->next;
		} else {
			rill = rill->next;
			ricl = ricl->next;
		}
	}
	for (rill = lastlist ; rill ; rill = ricl) {
		ricl = rill->next;
		free(rill);
	}
	lastlist = currentlist;
	currentlist = NULL;
}


#if defined(__linux__) || defined(__NetBSD__)
#include <dirent.h>
#elif defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/user.h>
#include <libproc.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/user.h>
#endif

static uint32_t mydevid;

void sinodes_pid_inodes(pid_t pid) {
	uint32_t devid;
	uint64_t inode;
//	printf("pid: %d\n",ki->ki_pid);

#if defined(__linux__) || defined(__NetBSD__)
	char path[100];
	struct stat st;
	snprintf(path,100,"/proc/%lld/cwd",(long long int)pid);
	if (stat(path,&st)>=0) {
		devid = st.st_dev;
		inode = st.st_ino;
		if (devid==mydevid) {
			sinodes_process_inode(inode);
		}
	}
#elif defined(__APPLE__)
	struct proc_vnodepathinfo vnpi;

	proc_pidinfo(pid, PROC_PIDVNODEPATHINFO, 0, &vnpi, sizeof(vnpi));

	devid = vnpi.pvi_cdir.vip_vi.vi_stat.vst_dev;
	inode = vnpi.pvi_cdir.vip_vi.vi_stat.vst_ino;
	if (devid==mydevid) {
		sinodes_process_inode(inode);
	}
#elif defined(__FreeBSD__)
	struct kinfo_file *kif;
	uint8_t *p;
	size_t len, olen;
	int name[4];
	int error;

	len = 0;
	name[0] = CTL_KERN;
	name[1] = KERN_PROC;
	name[2] = KERN_PROC_FILEDESC;
	name[3] = pid;
	p = NULL;
	if (sysctl(name, 4, NULL, &len, NULL, 0)<0) {
		if (errno!=ESRCH && errno!=EBUSY) {
			mfs_errlog(LOG_NOTICE,"sysctl(kern.proc.filedesc) error");
		}
		return;
	}
	if (len==0) {
		return;
	}
	do {
		len += len / 10;
		if (p!=NULL) {
			free(p);
		}
		p = malloc(len);
		if (p == NULL) {
			return;
		}
		olen = len;
		error = sysctl(name, 4, p, &len, NULL, 0);
	} while (error < 0 && errno == ENOMEM && olen == len);
	if (error<0) {
		if (errno!=ESRCH && errno!=EBUSY) {
			mfs_errlog(LOG_NOTICE,"sysctl(kern.proc.filedesc) error");
		}
		free(p);
		return;
	}
	kif = (struct kinfo_file*)p;
	do {
		kif = (struct kinfo_file*)((uint8_t*)(kif) + kif->kf_structsize);
	} while ((uint8_t*)kif < p+len);
	if ((uint8_t*)kif != p+len) {
		syslog(LOG_WARNING,"kinfo_file structure size mismatch\n");
		free(p);
		return;
	}
	kif = (struct kinfo_file*)p;
	do {
		if (kif->kf_fd==KF_FD_TYPE_CWD && kif->kf_type==KF_TYPE_VNODE) {
			devid = kif->kf_un.kf_file.kf_file_fsid;
			inode = kif->kf_un.kf_file.kf_file_fileid;
			if (devid==mydevid) {
				sinodes_process_inode(inode);
			}
		}
		kif = (struct kinfo_file*)((uint8_t*)(kif) + kif->kf_structsize);
	} while ((uint8_t*)kif < p+len);
	free(p);
/*
	libutil version:

	struct kinfo_file *kif;
	int i,no;

	kif = kinfo_getfile(pid,&no);

	for (i=0 ; i<no ; i++) {
		if (kif[i].kf_fd==KF_FD_TYPE_CWD && kif[i].kf_type==KF_TYPE_VNODE) {
			devid = kif[i].kf_un.kf_file.kf_file_fsid;
			inode = kif[i].kf_un.kf_file.kf_file_fileid;
			if (devid==mydevid) {
				sinodes_process_inode(inode);
			}
		}
	}
*/
#endif
}

void sinodes_all_pids(void) {
#if defined(__linux__) || defined(__NetBSD__)
	DIR *dd;
	struct dirent *de;
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
	struct dirent *destorage;
#endif
	const char *np;
	int pid;

	dd = opendir("/proc");
	if (dd==NULL) {
		return;
	}
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
	/* in new glibc readdir_r is obsoleted, so we should use readdir instead */
	destorage = (struct dirent*)malloc(sizeof(struct dirent)+pathconf("/proc",_PC_NAME_MAX)+1);
	if (destorage==NULL) {
		closedir(dd);
		return;
	}
	while (readdir_r(dd,destorage,&de)==0 && de!=NULL) {
#else
	while ((de = readdir(dd))!=NULL) {
#endif
		pid = 0;
		np = de->d_name;
		while (*np) {
			if (*np>='0' && *np<='9') {
				pid *= 10;
				pid += *np - '0';
			} else {
				pid = 0;
				break;
			}
			np++;
		}
		if (pid>0) {
			sinodes_pid_inodes(pid);
		}
	}
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
	free(destorage);
#endif
	closedir(dd);
#elif defined(__APPLE__) || defined(__FreeBSD__)
	struct kinfo_proc *ki, *p;
	size_t len, olen;
	int name[4];
	int cnt;
	int error;

	len = 0;
	name[0] = CTL_KERN;
	name[1] = KERN_PROC;
#if defined(KERN_PROC_PROC)
	name[2] = KERN_PROC_PROC;
#elif defined(KERN_PROC_ALL)
	name[2] = KERN_PROC_ALL;
#else
	name[2] = 0;
#endif
	name[3] = 0;
	p = NULL;
	if (sysctl(name, 4, NULL, &len, NULL, 0)<0) {
		mfs_errlog(LOG_NOTICE,"sysctl(kern.proc) error");
		return;
	}
	if (len==0) {
		return;
	}
	do {
		len += len / 10;
		if (p!=NULL) {
			free(p);
		}
		p = malloc(len);
		if (p == NULL) {
			return;
		}
		olen = len;
		error = sysctl(name, 4, p, &len, NULL, 0);
	} while (error < 0 && errno == ENOMEM && olen == len);
	if (error<0) {
		mfs_errlog(LOG_NOTICE,"sysctl(kern.proc) error");
		free(p);
		return;
	}
#if defined(__APPLE__)
	if ((len % sizeof(*p)) != 0) {
#else
	if ((len % sizeof(*p)) != 0 || p->ki_structsize != sizeof(*p)) {
#endif
		syslog(LOG_WARNING,"kinfo_proc structure size mismatch (len = %llu)", (long long unsigned int)len);
		free(p);
		return;
	}
	cnt = len / sizeof(*p);
	ki = p;
	while (cnt>0) {
#if defined(__APPLE__)
		sinodes_pid_inodes(ki->kp_proc.p_pid);
#else
		sinodes_pid_inodes(ki->ki_pid);
#endif
		ki++;
		cnt--;
	}
	free(p);
#endif
}

#include <stdio.h>

#ifdef RINODES_DEBUG
int main(int argc,char **argv) {
	struct stat st;
	if (argc!=2) {
		printf("usage: %s <mountpoint>\n",argv[0]);
		return -1;
	}
	if (stat(argv[1],&st)<0) {
		printf("stat '%s' error\n",argv[1]);
		return -1;
	}
	mydevid = st.st_dev;
	while (1) {
		sinodes_all_pids();
		sinodes_end();
		sleep(1);
	}
	return 0;
}
#else
void* sinodes_scanthread(void *arg) {
	struct stat st;
	uint32_t i;
	char *mountpoint = (char*)arg;

	st.st_ino = 1;
	while (stat(mountpoint,&st)<0 || st.st_ino!=1) {
		if (st.st_ino==1) {
			mfs_arg_errlog(LOG_WARNING,"can't stat my mountpoint (%s)",mountpoint);
		} else {
			st.st_ino=1;
		}
		sleep(1);
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&term,0)==1) {
			free(arg);
			return NULL;
		}
#else
		zassert(pthread_mutex_lock(&glock));
		if (term==1) {
			zassert(pthread_mutex_unlock(&glock));
			free(arg);
			return NULL;
		}
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	free(mountpoint);
	mountpoint = NULL;
//	syslog("stat %s : st.st_ino: %lu ; st.st_dev: %u\n",mountpoint,st.st_ino,st.st_dev);
	mydevid = st.st_dev;
	syslog(LOG_NOTICE,"my st_dev: %"PRIu32,mydevid);
	i = 0;
	while (1) {
		if (i>RINODES_CHECK_INTERVAL_100MS) {
			sinodes_all_pids();
			sinodes_end();
			i=0;
		} else {
			i++;
		}
		portable_usleep(100000);
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&term,0)==1) {
			return NULL;
		}
#else
		zassert(pthread_mutex_lock(&glock));
		if (term==1) {
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	return NULL;
}

void sinodes_term(void) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_or(&term,1);
#else
	zassert(pthread_mutex_lock(&glock));
	term = 1;
	zassert(pthread_mutex_unlock(&glock));
#endif
	pthread_join(clthread,NULL);
	sinodes_end();
#ifndef HAVE___SYNC_FETCH_AND_OP
	zassert(pthread_mutex_destroy(&glock));
#endif
}

void sinodes_init(const char *mp) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_and(&term,0);
#else
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_mutex_lock(&glock));
	term = 0;
	zassert(pthread_mutex_unlock(&glock));
#endif
	lwt_minthread_create(&clthread,0,sinodes_scanthread,strdup(mp));
}
#endif
