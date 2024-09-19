#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "globengine.h"
#include "storageclass.h"
#include "metadata.h"
#include "changelog.h"
#include "main.h"
#include "bio.h"
#include "mfslog.h"
#include "datapack.h"
#include "massert.h"

#define PATTERNS_MAX 1024

typedef struct _pattern {
	void *glob;
	uint8_t valid;
	uint8_t modified;
	uint8_t gnleng;
	uint8_t gname[256];
	uint32_t euid,egid;
	uint8_t priority;
	uint8_t omask;
	uint8_t scid;
	uint8_t seteattr,clreattr;
	uint16_t trashretention;
} pattern;

static pattern patterntab[PATTERNS_MAX];
static uint32_t validpatterns;

static inline int pattern_check_ugids(pattern *p,uint32_t uid,uint32_t gids,uint32_t *gid) {
	uint32_t i;
	if (p->euid!=PATTERN_EUGID_ANY && p->euid!=uid) {
		return 0;
	}
	if (p->egid==PATTERN_EUGID_ANY) {
		return 1;
	}
	for (i=0 ; i<gids ; i++) {
		if (p->egid==gid[i]) {
			return 1;
		}
	}
	return 0;
}

int patterns_compare(const void *a,const void *b) {
	const pattern *aa = ((const pattern*)a);
	const pattern *bb = ((const pattern*)b);
	if (aa->valid < bb->valid) {
		return 1;
	} else if (aa->valid > bb->valid) {
		return -1;
	} else if (aa->priority < bb->priority) {
		return 1;
	} else if (aa->priority > bb->priority) {
		return -1;
	} else if (aa->scid < bb->scid) {
		return -1;
	} else if (aa->scid > bb->scid) {
		return 1;
	} else {
		return strcmp((const char*)(aa->gname),(const char *)(bb->gname));
	}
}

void patterns_have_changed(void) {
	uint32_t i;

	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].modified) {
			if (patterntab[i].glob!=NULL) {
				glob_free(patterntab[i].glob);
			}
			if (patterntab[i].valid) {
				patterntab[i].glob = glob_new(patterntab[i].gname);
			} else {
				patterntab[i].glob = NULL;
			}
			patterntab[i].modified = 0;
		}
	}
	qsort(patterntab,PATTERNS_MAX,sizeof(pattern),patterns_compare);
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid==0) {
			validpatterns=i;
			return;
		}
	}
	validpatterns=PATTERNS_MAX;
}

uint8_t patterns_find_matching(uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t name[256],uint8_t *scid,uint16_t *trashretention,uint8_t *seteattr,uint8_t *clreattr) {
	uint32_t i;
	for (i=0 ; i<validpatterns ; i++) {
		if (pattern_check_ugids(patterntab+i,uid,gids,gid) && glob_match(patterntab[i].glob,name,nleng)) {
			*scid = patterntab[i].scid;
			*trashretention = patterntab[i].trashretention;
			*seteattr = patterntab[i].seteattr;
			*clreattr = patterntab[i].clreattr;
			return patterntab[i].omask;
		}
	}
	return 0;
}


void patterns_sclass_delete(uint8_t scid) {
	uint32_t i;

	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid && scid==patterntab[i].scid) {
			patterntab[i].valid = 0;
			patterntab[i].modified = 1;
		}
	}
	patterns_have_changed();
}

static inline uint8_t patterns_univ_add(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t priority,uint8_t omask,uint8_t scid,uint16_t trashretention,uint8_t seteattr,uint8_t clreattr,uint8_t mrflag) {
	uint32_t i;
	uint32_t npos;

	if (gnleng==0) {
		return MFS_ERROR_EINVAL;
	}
	if ((omask & PATTERN_OMASK_EATTR) && ((seteattr==0 && clreattr==0) || ((seteattr&clreattr)!=0))) {
		return MFS_ERROR_EINVAL;
	}
	npos = PATTERNS_MAX;
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid) {
			if (gnleng==patterntab[i].gnleng && euid==patterntab[i].euid && egid==patterntab[i].egid && memcmp(gname,patterntab[i].gname,gnleng)==0) {
				return MFS_ERROR_PATTERNEXISTS;
			}
		} else if (npos==PATTERNS_MAX) {
			npos = i;
		}
	}
	if (npos==PATTERNS_MAX) {
		return MFS_ERROR_PATLIMITREACHED;
	}
	patterntab[npos].valid = 1;
	patterntab[npos].modified = 1;
	patterntab[npos].gnleng = gnleng;
	memcpy(patterntab[npos].gname,gname,gnleng);
	patterntab[npos].gname[gnleng]=0;
	patterntab[npos].euid = euid;
	patterntab[npos].egid = egid;
	patterntab[npos].priority = priority;
	patterntab[npos].omask = omask;
	patterntab[npos].scid = scid;
	patterntab[npos].trashretention = trashretention;
	patterntab[npos].seteattr = seteattr;
	patterntab[npos].clreattr = clreattr;
	patterns_have_changed();
	if (mrflag==0) {
		changelog("%"PRIu32"|PATADD(%s,%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu8",%"PRIu16",%"PRIu8",%"PRIu8")",main_time(),changelog_escape_name(gnleng,gname),euid,egid,priority,omask,scid,trashretention,seteattr,clreattr);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t patterns_add(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t priority,uint8_t omask,uint8_t scnleng,const uint8_t *scname,uint16_t trashretention,uint8_t seteattr,uint8_t clreattr) {
	uint8_t scid;

	if (omask & PATTERN_OMASK_SCLASS) {
		scid = sclass_find_by_name(scnleng,scname);
		if (scid==0) {
			return MFS_ERROR_NOSUCHCLASS;
		}
	} else {
		scid = 0;
	}

	return patterns_univ_add(gnleng,gname,euid,egid,priority,omask,scid,trashretention,seteattr,clreattr,0);
}

uint8_t patterns_mr_add(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t priority,uint8_t omask,uint8_t scid,uint16_t trashretention,uint8_t seteattr,uint8_t clreattr) {
	return patterns_univ_add(gnleng,gname,euid,egid,priority,omask,scid,trashretention,seteattr,clreattr,1);
}

static inline uint8_t patterns_univ_delete(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t mrflag) {
	uint32_t i;
	uint8_t f;

	f = 0;
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid && gnleng==patterntab[i].gnleng && euid==patterntab[i].euid && egid==patterntab[i].egid && memcmp(gname,patterntab[i].gname,gnleng)==0) {
			patterntab[i].valid = 0;
			patterntab[i].modified = 1;
			f=1;
		}
	}
	if (f==0) {
		return MFS_ERROR_NOSUCHPATTERN;
	}
	patterns_have_changed();
	if (mrflag==0) {
		changelog("%"PRIu32"|PATDEL(%s,%"PRIu32",%"PRIu32")",main_time(),changelog_escape_name(gnleng,gname),euid,egid);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t patterns_delete(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid) {
	return patterns_univ_delete(gnleng,gname,euid,egid,0);
}

uint8_t patterns_mr_delete(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid) {
	return patterns_univ_delete(gnleng,gname,euid,egid,1);
}

uint32_t patterns_list(uint8_t *buff) {
	uint32_t i;
	uint32_t lsize;

	lsize = 0;
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid) {
			if (buff==NULL) {
				if (patterntab[i].omask & PATTERN_OMASK_SCLASS) {
					lsize += patterntab[i].gnleng + sclass_get_nleng(patterntab[i].scid) + 16;
				} else {
					lsize += patterntab[i].gnleng + 16;
				}
			} else {
				put8bit(&buff,patterntab[i].gnleng);
				memcpy(buff,patterntab[i].gname,patterntab[i].gnleng);
				buff += patterntab[i].gnleng;
				put32bit(&buff,patterntab[i].euid);
				put32bit(&buff,patterntab[i].egid);
				put8bit(&buff,patterntab[i].priority);
				put8bit(&buff,patterntab[i].omask);
				if (patterntab[i].omask & PATTERN_OMASK_SCLASS) {
					uint8_t scnleng;
					scnleng = sclass_get_nleng(patterntab[i].scid);
					put8bit(&buff,scnleng);
					memcpy(buff,sclass_get_name(patterntab[i].scid),scnleng);
					buff += scnleng;
					lsize += patterntab[i].gnleng + scnleng + 16;
				} else {
					put8bit(&buff,0);
					lsize += patterntab[i].gnleng + 16;
				}
				put16bit(&buff,patterntab[i].trashretention);
				put8bit(&buff,patterntab[i].seteattr);
				put8bit(&buff,patterntab[i].clreattr);
			}
		}
	}
	return lsize;
}

uint8_t patterns_store(bio *fd) {
	uint8_t databuff[256+4+4+1+1+1+2+1];
	uint8_t *ptr;
	uint8_t gnleng;
	int32_t wsize;
	uint32_t i;
	if (fd==NULL) {
		return 0x11;
	}
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		if (patterntab[i].valid) {
			ptr = databuff;
			gnleng = patterntab[i].gnleng;
			put8bit(&ptr,gnleng);
			put32bit(&ptr,patterntab[i].euid);
			put32bit(&ptr,patterntab[i].egid);
			put8bit(&ptr,patterntab[i].priority);
			put8bit(&ptr,patterntab[i].omask);
			put8bit(&ptr,patterntab[i].scid);
			put16bit(&ptr,patterntab[i].trashretention);
			put8bit(&ptr,patterntab[i].seteattr);
			put8bit(&ptr,patterntab[i].clreattr);
			memcpy(ptr,patterntab[i].gname,gnleng);
			ptr += gnleng;
//			wsize = 1+4+4+1+1+1+2+1+gnleng;
			wsize = ptr - databuff;
			if (bio_write(fd,databuff,wsize)!=wsize) {
				return 0xFF;
			}
		}
	}
	ptr = databuff;
	put8bit(&ptr,0);
	put32bit(&ptr,PATTERN_EUGID_ANY);
	put32bit(&ptr,PATTERN_EUGID_ANY);
	wsize = ptr - databuff;
	if (bio_write(fd,databuff,wsize)!=wsize) {
		return 0xFF;
	}
	return 0;
}

void patterns_cleanup(void) {
	uint32_t i;
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		patterntab[i].valid = 0;
		patterntab[i].modified = 1;
	}
	patterns_have_changed();
}

int patterns_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t databuff[262]; // MAX(1+4+4,1+1+2+2+1+255)
	const uint8_t *ptr;
	uint32_t i;
	uint8_t gnleng;
	uint32_t euid,egid;
	int32_t psize;

	if (mver>0x11) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading patterns data: unsupported format");
		return -1;
	}

	patterns_cleanup();
	i = 0;
	psize = (mver==0x10)?6:7;
	for (;;) {
		if (i>=PATTERNS_MAX) {
			if (ignoreflag) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"loading patterns data: too many patterns - ignore");
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading patterns data: too many patterns");
				return -1;
			}
		}
		if (bio_read(fd,databuff,(1+4+4))!=(1+4+4)) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading patterns data: read error");
			return -1;
		}
		ptr = databuff;
		gnleng = get8bit(&ptr);
		euid = get32bit(&ptr);
		egid = get32bit(&ptr);
		if (gnleng==0 && euid==PATTERN_EUGID_ANY && egid==PATTERN_EUGID_ANY) {
			break;
		}
		if (i<PATTERNS_MAX) {
			if (bio_read(fd,databuff,(psize+(int32_t)gnleng))!=(psize+(int32_t)gnleng)) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"loading patterns data: read error");
				return -1;
			}
			ptr = databuff;
			patterntab[i].gnleng = gnleng;
			patterntab[i].euid = euid;
			patterntab[i].egid = egid;
			patterntab[i].priority = get8bit(&ptr);
			patterntab[i].omask = get8bit(&ptr);
			patterntab[i].scid = get8bit(&ptr);
			patterntab[i].trashretention = get16bit(&ptr);
			patterntab[i].seteattr = get8bit(&ptr);
			if (mver==0x10) {
				patterntab[i].clreattr = 0;
			} else {
				patterntab[i].clreattr = get8bit(&ptr);
			}
			memcpy(patterntab[i].gname,ptr,gnleng);
			patterntab[i].gname[gnleng]=0;
			patterntab[i].valid = 1;
			patterntab[i].modified = 1;
		} else {
			bio_skip(fd,(uint32_t)(psize+gnleng));
		}
		i++;
	}
	patterns_have_changed();
	return 0;
}

int patterns_init(void) {
	uint32_t i;
	for (i=0 ; i<PATTERNS_MAX ; i++) {
		patterntab[i].glob = NULL;
		patterntab[i].valid = 0;
		patterntab[i].modified = 0;
	}
	validpatterns = 0;
	return 0;
}
