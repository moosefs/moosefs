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

// #define DEBUGTHREAD 1

static pthread_t main_thread;
static int keep_alive;

#define HASHSIZE 65536
#define HASHFN(pid,uid,gid) (((pid*0x74BF4863+uid)*0xB435C489+gid)%(HASHSIZE))

typedef struct grcache {
	double time;
	pid_t pid;
	uid_t uid;
	gid_t gid;
	groups *g;
	struct grcache *next,**prev;
} grcache;

static grcache** groups_hashtab;
static double to;
static pthread_mutex_t glock;

static int debug_mode;

#ifdef DEBUGTHREAD
static pthread_t debug_thread;
static uint32_t malloc_cnt,free_cnt;
#endif

static inline groups* make_groups(gid_t gid,uint32_t gidcnt) {
	groups *ret;
#ifdef DEBUGTHREAD
	zassert(pthread_mutex_lock(&glock));
	malloc_cnt++;
	zassert(pthread_mutex_unlock(&glock));
#endif
	ret = malloc(sizeof(groups)+sizeof(uint32_t)*gidcnt);
	ret->lcnt = 1;
	ret->gidcnt = gidcnt;
	if (gidcnt>0) { // pro forma
		ret->gidtab = (uint32_t*)(ret+1); // first byte after 'groups' structure
		ret->gidtab[0] = gid;
	} else {
		ret->gidtab = NULL;
	}
	return ret;
}

static inline groups* get_groups(pid_t pid,gid_t gid) {
	groups *ret;
#if defined(__linux__)
// Linux - supplementary groups are in the file:
// /proc/<PID>/status
// line:
// Groups: <GID1>  <GID2> <GID3> ...
//
// NetBSD - supplementary groups are in the file:
// /proc/<PID>/status
// as comma separated list of gids at end of (single) line.
	char proc_filename[50];
	char *ptr;
	uint32_t gcount,n;
	gid_t g;
	FILE *fd;
	char *linebuff;
	size_t lbsize;

	snprintf(proc_filename,50,"/proc/%d/status",pid);

	fd = fopen(proc_filename,"r");
	if (fd==NULL) {
		return make_groups(gid,1);
	}
	linebuff = malloc(1024);
	lbsize = 1024;
	while (getline(&linebuff,&lbsize,fd)!=-1) {
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
			ret = make_groups(gid,gcount);
			n = 1;
			ptr = linebuff+7;
			do {
				while (*ptr==' ' || *ptr=='\t') {
					ptr++;
				}
				if (*ptr>='0' && *ptr<='9') {
					g = strtoul(ptr,&ptr,10);
					if (g!=gid) {
						ret->gidtab[n] = g;
						n++;
					}
				}
			} while ((*ptr==' ' || *ptr=='\t') && n<gcount);
			fclose(fd);
			free(linebuff);
			return ret;
		}
	}
	fclose(fd);
	free(linebuff);
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
		return make_groups(gid,1);
	}

	n = fread(credbuff,sizeof(uint32_t),1024,fd);

	fclose(fd);

	if (n<7) {
		return make_groups(gid,1);
	}

	gcount = credbuff[6];
	if (gcount==n-7 && gcount>0) {
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (credbuff[n+7]!=gid) {
				gids++;
			}
		}

		ret = make_groups(gid,gids);
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (credbuff[n+7]!=gid) {
				ret->gidtab[gids] = credbuff[n+7];
				gids++;
			}
		}
		return ret;
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
		ret = make_groups(gid,gids);
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.kp_eproc.e_ucred.cr_groups[n]!=gid) {
				ret->gidtab[gids] = kp.kp_eproc.e_ucred.cr_groups[n];
				gids++;
			}
		}
		return ret;
#else /* FreeBSD */
		gcount = kp.ki_ngroups;
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.ki_groups[n]!=gid) {
				gids++;
			}
		}
		ret = make_groups(gid,gids);
		gids = 1;
		for (n=0 ; n<gcount ; n++) {
			if (kp.ki_groups[n]!=gid) {
				ret->gidtab[gids] = kp.ki_groups[n];
				gids++;
			}
		}
		return ret;
#endif
	}
#endif
	(void)pid;
	return make_groups(gid,1);
}

static inline void groups_dump(groups *g) {
	uint32_t h;

	for (h=0 ; h<g->gidcnt ; h++) {
		fprintf(stderr,"%c%"PRIu32,(h==0)?'(':',',g->gidtab[h]);
	}
	if (g->gidcnt==0) {
		fprintf(stderr,"EMPTY\n");
	} else {
		fprintf(stderr,")\n");
	}
}

static inline void groups_decref(groups *g) {
	if (g->lcnt>0) {
		g->lcnt--;
	}
	if (g->lcnt==0) {
#ifdef DEBUGTHREAD
		free_cnt++;
#endif
		free(g);
	}
}

static inline void groups_remove(grcache *gc) {
	*(gc->prev) = gc->next;
	if (gc->next) {
		gc->next->prev = gc->prev;
	}
	groups_decref(gc->g);
#ifdef DEBUGTHREAD
	free_cnt++;
#endif
	free(gc);
}

// uid!=0 , cacheonly==0 
//    result in cache -> return
//    result not in cache -> get_groups and add to cache
// uid==0 , cacheonly==0
//    get_groups and add to cache (remove old if exists)
// cacheonly!=0
//    result in cache -> return
//    result not in cache -> return make_groups(gid,1)

groups* groups_get_common(pid_t pid,uid_t uid,gid_t gid,uint8_t cacheonly) {
	double t;
	uint32_t h;
	groups *g,*gf;
	grcache *gc,*gcn,*gcf;

	if (debug_mode) {
		fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32")\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
	}
	t = monotonic_seconds();
	zassert(pthread_mutex_lock(&glock));
	h = HASHFN(pid,uid,gid);
	gcf = NULL;
	for (gc = groups_hashtab[h] ; gc!=NULL ; gc = gcn) {
		gcn = gc->next;
		if (gc->time + to < t && cacheonly==0) {
			groups_remove(gc);
		} else {
			if (gc->pid==pid && gc->uid==uid && gc->gid==gid) {
				gcf = gc;
			}
		}
	}
	if (gcf) {
		gf = gcf->g;
		gf->lcnt++;
	} else {
		gf = NULL;
	}
	zassert(pthread_mutex_unlock(&glock));
	if (cacheonly) {
		if (gf!=NULL) {
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32"):",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
				groups_dump(gf);
			}
			return gf;
		} else {
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32") - emergency mode\n",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
			}
			return make_groups(gid,1);
		}
	} else {
		if (gf!=NULL && uid!=0) {
			if (debug_mode) {
				fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32"):",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
				groups_dump(gf);
			}
			return gf;
		}
	}
	// assert cacheonly==0
	g = get_groups(pid,gid);
	zassert(pthread_mutex_lock(&glock));
	if (gf!=NULL) {
		groups_decref(gf);
	}
	gcf = NULL;
	for (gc = groups_hashtab[h] ; gc!=NULL ; gc = gc->next) {
		if (gc->pid==pid && gc->uid==uid && gc->gid==gid) {
			gcf = gc;
		}
	}
	if (gcf) {
		groups_decref(gcf->g);
		gcf->g = g;
		g->lcnt++;
	} else {
#ifdef DEBUGTHREAD
		malloc_cnt++;
#endif
		gc = malloc(sizeof(grcache));
		gc->time = t;
		gc->pid = pid;
		gc->uid = uid;
		gc->gid = gid;
		gc->g = g;
		g->lcnt++;
		gc->next = groups_hashtab[h];
		if (gc->next) {
			gc->next->prev = &(gc->next);
		}
		gc->prev = groups_hashtab+h;
		groups_hashtab[h] = gc;
	}
	zassert(pthread_mutex_unlock(&glock));
	if (debug_mode) {
		fprintf(stderr,"groups_get(pid=%"PRIu32",uid=%"PRIu32",gid=%"PRIu32"):",(uint32_t)pid,(uint32_t)uid,(uint32_t)gid);
		groups_dump(g);
	}
	return g;
}

void groups_rel(groups* g) {
	zassert(pthread_mutex_lock(&glock));
	groups_decref(g);
	zassert(pthread_mutex_unlock(&glock));
}

void* groups_cleanup_thread(void* arg) {
	static uint32_t h = 0;
	uint32_t i;
	double t;
	grcache *gc,*gcn;
	int ka = 1;
	while (ka) {
		zassert(pthread_mutex_lock(&glock));
		t = monotonic_seconds();
		for (i=0 ; i<16 ; i++) {
			for (gc = groups_hashtab[h] ; gc!=NULL ; gc = gcn) {
				gcn = gc->next;
				if (gc->time + to < t) {
					groups_remove(gc);
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

#ifdef DEBUGTHREAD
void* groups_debug_thread(void* arg) {
	uint32_t i,j,k;
	uint32_t l,u;
	grcache *gc;
	int ka = 1;
	while (ka) {
		zassert(pthread_mutex_lock(&glock));
		k = 0;
		l = 0;
		u = 0;
		for (i=0 ; i<HASHSIZE ; i++) {
			j = 0;
			if (groups_hashtab[i]!=NULL) {
				l++;
			}
			for (gc = groups_hashtab[i] ; gc!=NULL ; gc = gc->next) {
				j++;
				u++;
				fprintf(stderr,"hashpos: %"PRIu32" ; pid: %"PRIu32" ; uid: %"PRIu32" ; gid: %"PRIu32" ; time: %.6lf ; lcnt: %"PRIu32" ; gidcnt: %"PRIu32" ; gidtab: ",i,gc->pid,gc->uid,gc->gid,gc->time,gc->g->lcnt,gc->g->gidcnt);
				groups_dump(gc->g);
			}
			if (j>k) {
				k=j;
			}
		}
		fprintf(stderr,"malloc cnt: %"PRIu32" ; free cnt: %"PRIu32" ; maxchain: %"PRIu32" ; used hashtab entries: %"PRIu32" ; data entries: %"PRIu32" ; avgchain: %.2lf / %.2lf\n",malloc_cnt,free_cnt,k,l,u,(double)(u)/(double)(HASHSIZE),(double)(u)/(double)(l));
		ka = keep_alive;
		zassert(pthread_mutex_unlock(&glock));
		sleep(5);
	}
	return arg;
}
#endif

void groups_term(void) {
	uint32_t i;
#ifdef __clang_analyzer__
	groups *gcn;
#endif
	zassert(pthread_mutex_lock(&glock));
	keep_alive = 0;
	zassert(pthread_mutex_unlock(&glock));
	pthread_join(main_thread,NULL);
#ifdef DEBUGTHREAD
	pthread_join(debug_thread,NULL);
#endif
	zassert(pthread_mutex_lock(&glock));
	for (i=0 ; i<HASHSIZE ; i++) {
		while (groups_hashtab[i]) {
#ifdef __clang_analyzer__
			gcn = groups_hashtab[i]->next;
#endif
			groups_remove(groups_hashtab[i]);
#ifdef __clang_analyzer__
			// groups_hashtab[i] is changed by using 'prev' pointer, so after groups_remove variable groups_hashtab[i] has different value and can be used in while !!!
			groups_hashtab[i] = gcn;
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
#ifdef DEBUGTHREAD
	pthread_create(&debug_thread,NULL,groups_debug_thread,NULL);
#endif
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
