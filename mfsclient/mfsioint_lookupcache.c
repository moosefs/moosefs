#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "clocks.h"
#include "mastercomm.h"

#include "mfsioint.h"


#define LCACHE_HENTRIES 64
#define LCACHE_QENTRIES 8

typedef struct _path_lookup_cache {
	uint32_t hash;
	uint8_t refresh_in_progress;
	pthread_cond_t cond;
	// key in hash
	uint32_t base_inode;
	uint32_t pleng;
	uint8_t path[MFS_PATH_MAX];
	// key not in hash
	uint32_t uid;
	uint32_t gidcnt;
	uint32_t gidtab[MFS_NGROUPS_MAX];
	// data
	uint32_t parent;
	uint32_t inode;
	uint8_t nleng;
	uint8_t name[256];
	uint8_t attr[ATTR_RECORD_SIZE];
	// timestamp
	double validts;
} path_lookup_cache;

static path_lookup_cache lcache[LCACHE_HENTRIES][LCACHE_QENTRIES];
static pthread_mutex_t lcache_lock[LCACHE_HENTRIES];

static double lcache_retention;

uint8_t lcache_path_normalize(uint32_t pleng,const uint8_t *path,uint32_t *rpleng,uint8_t rpath[MFS_PATH_MAX]) {
	uint32_t rleng;
	const uint8_t *pptr,*pend;
	uint32_t partlen;

	pptr = path;
	pend = path+pleng;
	partlen = 0;
	rleng = 0;
	while (*pptr && pptr<pend) {
		if (*pptr=='/') {
			if (partlen>0) {
				if (partlen==2 && rpath[rleng-1]=='.' && rpath[rleng-2]=='.') { // '..' - go up
					if (rleng<3) {
						return MFS_ERROR_EINVAL;
					}
					rleng-=3;
					while (rleng>0 && rpath[rleng-1]!='/') {
						rleng--;
					}
				} else if (partlen==1 && rpath[rleng-1]=='.') { // '.' - just ignore it
					rleng--;
				} else {
					if (rleng>=MFS_PATH_MAX) {
						return MFS_ERROR_ENAMETOOLONG;
					}
					rpath[rleng++] = '/';
				}
			}
			partlen = 0;
		} else {
			if (partlen>=MFS_NAME_MAX) {
				return MFS_ERROR_ENAMETOOLONG;
			}
			if (rleng>=MFS_PATH_MAX) {
				return MFS_ERROR_ENAMETOOLONG;
			}
			rpath[rleng++] = *pptr;
			partlen++;
		}
		pptr++;
	}
	if (rleng>=MFS_PATH_MAX) {
		return MFS_ERROR_ENAMETOOLONG;
	}
	rpath[rleng] = '\0';
	*rpleng = rleng;
	return MFS_STATUS_OK;
}

uint32_t lcache_hash(uint32_t base_inode,uint32_t pleng,uint8_t *path) {
	uint32_t hash;
	uint32_t i;

	hash = base_inode;
	for (i=0 ; i<pleng ; i++) {
		hash = hash*33+path[i];
	}
	return hash;
}

uint8_t lcache_path_lookup(uint32_t base_inode,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gidcnt,uint32_t *gidtab,uint32_t *parent_inode,uint32_t *last_inode,uint8_t *nleng,uint8_t name[256],uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t rpath[MFS_PATH_MAX];
	uint32_t rpleng;
	uint8_t status;
	uint32_t hash,hind,qind;
	path_lookup_cache *plc,*minplc;
	double ts;

	ts = monotonic_seconds();
	status = lcache_path_normalize(pleng,path,&rpleng,rpath);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	hash = lcache_hash(base_inode,rpleng,rpath);
	hind = hash % LCACHE_HENTRIES;
	pthread_mutex_lock(lcache_lock+hind);
	minplc = NULL;
	for (qind = 0 ; qind < LCACHE_QENTRIES ; qind++) {
		plc = &(lcache[hind][qind]);
		if (plc->hash==hash && plc->base_inode==base_inode && plc->pleng==rpleng && plc->uid==uid && plc->gidcnt==gidcnt && memcmp(plc->path,rpath,rpleng)==0 && memcmp(plc->gidtab,gidtab,sizeof(uint32_t)*gidcnt)==0) {
			while (plc->refresh_in_progress) {
				pthread_cond_wait(&(plc->cond),lcache_lock+hind);
			}
			if (plc->validts<=ts) {
				plc->refresh_in_progress = 1;
				pthread_mutex_unlock(lcache_lock+hind);
//				fprintf(stderr,"lookup invalid %s !!!\n",rpath);
				status = fs_path_lookup(base_inode,rpleng,rpath,uid,gidcnt,gidtab,&(plc->parent),&(plc->inode),&(plc->nleng),plc->name,plc->attr);
				ts = monotonic_seconds();
				pthread_mutex_lock(lcache_lock+hind);
				plc->refresh_in_progress = 0;
				pthread_cond_broadcast(&(plc->cond));
				if (status!=MFS_STATUS_OK) {
					pthread_mutex_unlock(lcache_lock+hind);
					return status;
				}
				plc->validts = ts + lcache_retention;
//			} else {
//				fprintf(stderr,"lookup valid %s !!!\n",rpath);
			}
			if (parent_inode!=NULL) {
				*parent_inode = plc->parent;
			}
			if (last_inode!=NULL) {
				*last_inode = plc->inode;
			}
			if (nleng!=NULL) {
				*nleng = plc->nleng;
			}
			memcpy(name,plc->name,plc->nleng);
			memcpy(attr,plc->attr,ATTR_RECORD_SIZE);
			pthread_mutex_unlock(lcache_lock+hind);
			return MFS_STATUS_OK;
		} else if (plc->refresh_in_progress==0) {
			if (minplc==NULL || plc->validts < minplc->validts) {
				minplc = plc;
			}
		}
	}
	if (minplc==NULL) { // all slots are busy - just ignore cache and make query
		pthread_mutex_unlock(lcache_lock+hind);
//		fprintf(stderr,"lookup busy %s !!!\n",rpath);
		return fs_path_lookup(base_inode,rpleng,rpath,uid,gidcnt,gidtab,parent_inode,last_inode,nleng,name,attr);
	}
	plc = minplc;
	plc->hash = hash;
	plc->refresh_in_progress = 1;
	plc->base_inode = base_inode;
	plc->pleng = rpleng;
	memcpy(plc->path,rpath,rpleng);
	plc->uid = uid;
	plc->gidcnt = gidcnt;
	memcpy(plc->gidtab,gidtab,sizeof(uint32_t)*gidcnt);
	pthread_mutex_unlock(lcache_lock+hind);
//	fprintf(stderr,"lookup new %s !!!\n",rpath);
	status = fs_path_lookup(base_inode,rpleng,rpath,uid,gidcnt,gidtab,&(plc->parent),&(plc->inode),&(plc->nleng),plc->name,plc->attr);
	ts = monotonic_seconds();
	pthread_mutex_lock(lcache_lock+hind);
	plc->refresh_in_progress = 0;
	pthread_cond_broadcast(&(plc->cond));
	if (status!=MFS_STATUS_OK) {
		plc->validts = ts;
		pthread_mutex_unlock(lcache_lock+hind);
		return status;
	}
	plc->validts = ts + lcache_retention;
	if (parent_inode!=NULL) {
		*parent_inode = plc->parent;
	}
	if (last_inode!=NULL) {
		*last_inode = plc->inode;
	}
	if (nleng!=NULL) {
		*nleng = plc->nleng;
	}
	memcpy(name,plc->name,plc->nleng);
	memcpy(attr,plc->attr,ATTR_RECORD_SIZE);
	pthread_mutex_unlock(lcache_lock+hind);
	return MFS_STATUS_OK;
}

void lcache_path_invalidate(uint32_t base_inode,uint32_t pleng,const uint8_t *path) {
	uint8_t rpath[MFS_PATH_MAX];
	uint32_t rpleng;
	uint8_t status;
	uint32_t hash,hind,qind;
	path_lookup_cache *plc;
	double ts;

	ts = monotonic_seconds();
	status = lcache_path_normalize(pleng,path,&rpleng,rpath);
	if (status!=MFS_STATUS_OK) {
		return;
	}
//	fprintf(stderr,"invalidate %s\n",rpath);
	hash = lcache_hash(base_inode,rpleng,rpath);
	hind = hash % LCACHE_HENTRIES;
	pthread_mutex_lock(lcache_lock+hind);
	for (qind = 0 ; qind < LCACHE_QENTRIES ; qind++) {
		plc = &(lcache[hind][qind]);
		if (plc->hash==hash && plc->base_inode==base_inode && plc->pleng==rpleng && memcmp(plc->path,rpath,rpleng)==0 && plc->refresh_in_progress==0) {
			plc->validts = ts;
		}
	}
	pthread_mutex_unlock(lcache_lock+hind);
}

// TODO: add another hashmap - now it should be fast enough without any other structures !!!
void lcache_inode_invalidate(uint32_t inode) {
	uint32_t hind,qind;
	path_lookup_cache *plc;
	double ts;

	ts = monotonic_seconds();
	for (hind = 0 ; hind < LCACHE_HENTRIES ; hind++) {
		pthread_mutex_lock(lcache_lock+hind);
		for (qind = 0 ; qind < LCACHE_QENTRIES ; qind++) {
			plc = &(lcache[hind][qind]);
			if (plc->inode==inode && plc->refresh_in_progress==0) {
				plc->validts = ts;
			}
		}
		pthread_mutex_unlock(lcache_lock+hind);
	}
}

void lcache_term(void) {
	uint32_t hind,qind;

	for (hind = 0 ; hind < LCACHE_HENTRIES ; hind++) {
		pthread_mutex_destroy(lcache_lock+hind);
		for (qind = 0 ; qind < LCACHE_QENTRIES ; qind++) {
			pthread_cond_destroy(&(lcache[hind][qind].cond));
		}
	}
}

int lcache_init(double lc_retention) {
	uint32_t hind,qind;
	double ts;

	ts = monotonic_seconds();

	for (hind = 0 ; hind < LCACHE_HENTRIES ; hind++) {
		for (qind = 0 ; qind < LCACHE_QENTRIES ; qind++) {
			lcache[hind][qind].validts = ts;
			if (pthread_cond_init(&(lcache[hind][qind].cond),NULL)<0) {
				return -1;
			}
		}
		if (pthread_mutex_init(lcache_lock+hind,NULL)<0) {
			return -1;
		}
	}
	lcache_retention = lc_retention;
	return 0;
}
