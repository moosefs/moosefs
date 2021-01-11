/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#if defined(HAVE_CONFIG_H)
#  include "config.h"
#endif

#if defined(__NetBSD__)
#  define _KMEMUSER
#endif

#include <sys/types.h>
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__)
#  include <sys/sysctl.h>
#endif
#if defined(__FreeBSD__)
#  include <sys/user.h>
#endif

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "massert.h"
#include "getgroups.h"
#include "portable.h"
#include "clocks.h"

static pthread_t main_thread;
static int keep_alive;

uint32_t get_groups(pid_t pid,gid_t gid,uint32_t **gidtab) {
#if defined(__linux__)
// Linux - supplementary groups are in file:
// /proc/<PID>/status
// line:
// Groups: <GID1>  <GID2> <GID3> ...
//
// NetBSD - supplementary groups are in file:
// /proc/<PID>/status
// as comma separated list of gids at end of (single) line.
	char proc_filename[50];
	char linebuff[4096];
	char *ptr;
	uint32_t gcount,n;
	gid_t g;
	FILE *fd;

	snprintf(proc_filename,50,"/proc/%d/status",pid);

	fd = fopen(proc_filename,"r");
	if (fd==NULL) {
		*gidtab = malloc(sizeof(uint32_t)*1);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		return 1;
	}
	while (fgets(linebuff,4096,fd)) {
		if (strncmp(linebuff,"Groups:",7)==0) {
			gcount = 1;
			ptr = linebuff+7;
			do {
				while (*ptr==' ' || *ptr=='\t') {
					ptr++;
				}
				if (*ptr>='0' && *ptr<='9') {
					g = strtoul(ptr,&ptr,10);
					if (g!=gid) {
						gcount++;
					}
				}
			} while (*ptr==' ' || *ptr=='\t');
			*gidtab = malloc(sizeof(uint32_t)*gcount);
			passert(*gidtab);
			(*gidtab)[0] = gid;
			n = 1;
			ptr = linebuff+7;
			do {
				while (*ptr==' ' || *ptr=='\t') {
					ptr++;
				}
				if (*ptr>='0' && *ptr<='9') {
					g = strtoul(ptr,&ptr,10);
					if (g!=gid) {
						(*gidtab)[n] = g;
						n++;
					}
				}
			} while ((*ptr==' ' || *ptr=='\t') && n<gcount);
			fclose(fd);
			return n;
		}
	}
	fclose(fd);
#elif defined(__sun__) || defined(__sun)
// Solaris - supplementary groups are in file:
// /proc/<PID>/cred
// binary format:
// euid:32 ruid:32 suid:32 egid:32 rgid:32 sgid:32 groups:32 gid_1:32 gid_2:32 ...
//
// the only problem ... only root can access this files for all processes !!!
	char proc_filename[50];
	uint32_t credbuff[1024];
	uint32_t gcount,gids,n;
	FILE *fd;

	snprintf(proc_filename,50,"/proc/%d/proc",pid);

	fd = fopen(proc_filename,"rb");
	if (fd==NULL) {
		*gidtab = malloc(sizeof(uint32_t)*1);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		return 1;
	}

	n = fread(credbuff,sizeof(uint32_t),1024,fd);

	fclose(fd);

	if (n<7) {
		*gidtab = malloc(sizeof(uint32_t)*1);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		return 1;
	}

	gcount = credbuff[6];
	if (gcount==n-7 && gcount>0) {
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (credbuff[n+7]!=gid) {
				gids++;
			}
		}

		*gidtab = malloc(sizeof(uint32_t)*gids);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (credbuff[n+7]!=gid) {
				(*gidtab)[gids] = credbuff[n+7];
				gids++;
			}
		}
		return gids;
	}
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__)
// BSD-like - supplementary groups can be obtained from sysctl:
// kern.proc.pid.<PID>
	int mibpath[4];
	struct kinfo_proc kp;
	size_t kplen;
	uint32_t gcount,gids,n;

#if defined(CTL_KERN) && defined(KERN_PROC) && defined(KERN_PROC_PID)
	mibpath[0] = CTL_KERN;
	mibpath[1] = KERN_PROC;
	mibpath[2] = KERN_PROC_PID;
#else
	kplen = 4;
	sysctlnametomib("kern.proc.pid", mibpath, &kplen);
#endif
	mibpath[3] = pid;

	kplen = sizeof(kp);
	memset(&kp,0,sizeof(kp));
	if (sysctl(mibpath,4,&kp,&kplen,NULL,0) == 0) {
#if defined(__APPLE__) || defined(__NetBSD__)
		gcount = kp.kp_eproc.e_ucred.cr_ngroups;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.kp_eproc.e_ucred.cr_groups[n]!=gid) {
				gids++;
			}
		}
		*gidtab = malloc(sizeof(uint32_t)*gids);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.kp_eproc.e_ucred.cr_groups[n]!=gid) {
				(*gidtab)[gids] = kp.kp_eproc.e_ucred.cr_groups[n];
				gids++;
			}
		}
		return gids;
#else /* FreeBSD */
		gcount = kp.ki_ngroups;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.ki_groups[n]!=gid) {
				gids++;
			}
		}
		*gidtab = malloc(sizeof(uint32_t)*gids);
		passert(*gidtab);
		(*gidtab)[0] = gid;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.ki_groups[n]!=gid) {
				(*gidtab)[gids] = kp.ki_groups[n];
				gids++;
			}
		}
		return gids;
#endif
	}
#endif
	(void)pid;
	*gidtab = malloc(sizeof(uint32_t)*1);
	passert(*gidtab);
	(*gidtab)[0] = gid;
	return 1;
}

#define HASHSIZE 65536
#define HASHFN(pid,uid,gid) (((pid*0x74BF4863+uid)*0xB435C489+gid)%(HASHSIZE))

static groups** groups_hashtab;
static double to;
static pthread_mutex_t glock;

static int debug_mode;

static inline void groups_remove(groups *g) {
	*(g->prev) = g->next;
	if (g->next) {
		g->next->prev = g->prev;
	}
	if (g->gidtab!=NULL) {
		free(g->gidtab);
	}
	free(g);
}

groups* groups_get_x(pid_t pid,uid_t uid,gid_t gid,uint8_t lockmode) {
	double t;
	uint32_t h;
	groups *g,*gn,*gf;
	if (debug_mode) {
		fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32")\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
	}
	zassert(pthread_mutex_lock(&glock));
	t = monotonic_seconds();
	h = HASHFN(pid,uid,gid);
//	fprintf(stderr,"groups_get hash: %"PRIu32"\n",h);
	for (gf = NULL,g = groups_hashtab[h] ; g!=NULL ; g = gn) {
		gn = g->next;
		if (g->time + to < t && lockmode==0 && g->locked==0 && g->lcnt==0) {
//			fprintf(stderr,"groups_get remove node (%"PRIu32",%"PRIu32",%"PRIu32") insert_time: %.3lf ; current_time: %.3lf ; timeout: %.3lf\n",g->pid,g->uid,g->gid,g->time,t,to);
			groups_remove(g);
		} else {
//			fprintf(stderr,"groups_get check node (%"PRIu32",%"PRIu32",%"PRIu32")\n",g->pid,g->uid,g->gid);
			if (g->pid==pid && g->uid==uid && g->gid==gid) {
				gf = g;
			}
		}
	}
	g = gf;
	if (g) {
		if (debug_mode) {
			fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - found data in cache\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
		}
		g->lcnt++;
		if (lockmode==1) {
			g->locked = 1;
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - lock cache\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
			}
		}
		if (g->lcnt==1 && g->locked==0 && g->uid==0) { // refresh groups for user 'root' - only root can change groups
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - refresh cache\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
			}
			if (g->gidtab) {
				free(g->gidtab);
			}
			g->gidcnt = get_groups(pid,gid,&(g->gidtab));
		}
		if (lockmode==2) {
			g->locked = 0;
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - unlock cache\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
			}
		}
	} else {
		g = malloc(sizeof(groups));
		g->time = t;
		g->pid = pid;
		g->uid = uid;
		g->gid = gid;
		g->lcnt = 1;
		if (lockmode==1) { // emergency case
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - emergency mode\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
			}
			g->gidtab = malloc(sizeof(uint32_t));
			g->gidtab[0] = gid;
			g->gidcnt = 1;
			g->locked = 1;
		} else {
			g->gidcnt = get_groups(pid,gid,&(g->gidtab));
			g->locked = 0;
		}
		g->next = groups_hashtab[h];
		if (g->next) {
			g->next->prev = &(g->next);
		}
		g->prev = groups_hashtab+h;
		groups_hashtab[h] = g;
//		fprintf(stderr,"groups_get insert node (%"PRIu32",%"PRIu32",%"PRIu32")\n",g->pid,g->uid,g->gid);
	}
	zassert(pthread_mutex_unlock(&glock));
	if (debug_mode) {
		fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32"):",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
		for (h=0 ; h<g->gidcnt ; h++) {
			fprintf(stderr,"%c%"PRIu32,(h==0)?'(':',',g->gidtab[h]);
		}
		if (g->gidcnt==0) {
			fprintf(stderr,"EMPTY\n");
		} else {
			fprintf(stderr,")\n");
		}
	}
	return g;
}

void groups_rel(groups* g) {
	zassert(pthread_mutex_lock(&glock));
	if (g->lcnt>0) {
		g->lcnt--;
	}
	zassert(pthread_mutex_unlock(&glock));
}

void* groups_cleanup_thread(void* arg) {
	static uint32_t h = 0;
	uint32_t i;
	double t;
	groups *g,*gn;
	int ka = 1;
	while (ka) {
		zassert(pthread_mutex_lock(&glock));
		t = monotonic_seconds();
		for (i=0 ; i<16 ; i++) {
			for (g = groups_hashtab[h] ; g!=NULL ; g = gn) {
				gn = g->next;
				if (g->time + to < t && g->locked==0 && g->lcnt==0) {
					groups_remove(g);
				}
			}
			h++;
			h%=HASHSIZE;
		}
		ka = keep_alive;
		zassert(pthread_mutex_unlock(&glock));
		portable_usleep(10000);
	}
	return arg;
}

/*
void* groups_debug_thread(void* arg) {
	uint32_t i,j,k;
	uint32_t l,u;
	groups *g;
	while (1) {
		zassert(pthread_mutex_lock(&glock));
		k = 0;
		l = 0;
		u = 0;
		for (i=0 ; i<HASHSIZE ; i++) {
			j = 0;
			if (groups_hashtab[i]!=NULL) {
				l++;
			}
			for (g = groups_hashtab[i] ; g!=NULL ; g = g->next) {
				j++;
				u++;
				fprintf(stderr,"hashpos: %"PRIu32" ; pid: %"PRIu32" ; uid: %"PRIu32" ; gid: %"PRIu32" ; time: %.6lf ; lcnt: %"PRIu32" ; locked: %"PRIu32" ; gidcnt: %"PRIu32"\n",i,g->pid,g->uid,g->gid,g->time,g->lcnt,g->locked,g->gidcnt);
			}
			if (j>k) {
				k=j;
			}
		}
		fprintf(stderr,"malloc cnt: %"PRIu32" ; free cnt: %"PRIu32" ; maxchain: %"PRIu32" ; used hashtab entries: %"PRIu32" ; data entries: %"PRIu32" ; avgchain: %.2lf / %.2lf\n",mallocs,frees,k,l,u,(double)(u)/(double)(HASHSIZE),(double)(u)/(double)(l));
		zassert(pthread_mutex_unlock(&glock));
		sleep(5);
	}
	return arg;
}
*/

void groups_term(void) {
	uint32_t i;
#ifdef __clang_analyzer__
	groups *gn;
#endif
	zassert(pthread_mutex_lock(&glock));
	keep_alive = 0;
	zassert(pthread_mutex_unlock(&glock));
	pthread_join(main_thread,NULL);
	zassert(pthread_mutex_lock(&glock));
	for (i=0 ; i<HASHSIZE ; i++) {
		while (groups_hashtab[i]) {
#ifdef __clang_analyzer__
			gn = groups_hashtab[i]->next;
#endif
			groups_remove(groups_hashtab[i]);
#ifdef __clang_analyzer__
			// groups_hashtab[i] is changed by using 'prev' pointer, so after groups_remove variable groups_hashtab[i] has different value and can be used in while !!!
			groups_hashtab[i] = gn;
#endif
		}
	}
	free(groups_hashtab);
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_mutex_destroy(&glock));
}

void groups_init(double _to,int dm) {
	uint32_t i;
	debug_mode = dm;
	zassert(pthread_mutex_init(&glock,NULL));
	groups_hashtab = malloc(sizeof(groups*)*HASHSIZE);
	passert(groups_hashtab);
	for (i=0 ; i<HASHSIZE ; i++) {
		groups_hashtab[i] = NULL;
	}
	to = _to;
	keep_alive = 1;
	pthread_create(&main_thread,NULL,groups_cleanup_thread,NULL);
//	pthread_create(&t,NULL,groups_debug_thread,NULL);
}

/*
#include <stdio.h>
#include "strerr.h"

int main(int argc,char *argv[]) {
	groups *g;
	pid_t pid;
	uid_t uid;
	gid_t gid;
	uint32_t n;

	strerr_init();
	if (argc==2) {
		pid = strtoul(argv[1],NULL,10);
		uid = getuid();
		gid = getgid();
	} else if (argc==4) {
		pid = strtoul(argv[1],NULL,10);
		uid = strtoul(argv[2],NULL,10);
		gid = strtoul(argv[3],NULL,10);
	} else {
		pid = getpid();
		uid = getuid();
		gid = getgid();
	}

	groups_init(1.0,0);
	printf("pid: %d ; uid: %d ; gid: %d\n",pid,uid,gid);
	g = groups_get(pid,uid,gid);
	for (n=0 ; n<g->gidcnt ; n++) {
		printf("gid_%"PRIu32": %d\n",n,g->gidtab[n]);
	}
	groups_rel(g);
	return 0;
}
*/
