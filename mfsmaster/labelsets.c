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

#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "matocsserv.h"
#include "metadata.h"
#include "labelsets.h"
#include "slogger.h"
#include "datapack.h"
#include "changelog.h"
#include "bio.h"
#include "main.h"
#include "massert.h"

/* labels - descriptions */

#define LABELS 26

typedef struct _labeldescr {
	uint8_t descrleng;
	char *description;
} labeldescr;

static labeldescr labeldescriptions[LABELS];

static inline uint8_t labelset_univ_setdescription(uint8_t labelid,uint8_t descrleng,const uint8_t *description,uint8_t mr) {
	if (labelid>=LABELS) {
		return ERROR_EINVAL;
	}
	if (descrleng>128) {
		return ERROR_EINVAL;
	}
	if (labeldescriptions[labelid].description!=NULL) {
		free(labeldescriptions[labelid].description);
	}
	labeldescriptions[labelid].description = malloc(descrleng);
	passert(labeldescriptions[labelid].description);
	memcpy(labeldescriptions[labelid].description,description,descrleng);
	labeldescriptions[labelid].descrleng = descrleng;
	if (mr==0) {
		changelog("%"PRIu32"|LABELDESC(%"PRIu8",%s)",main_time(),labelid,changelog_escape_name(descrleng,description));
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t labelset_setdescription(uint8_t labelid,uint8_t descrleng,const uint8_t *description) {
	return labelset_univ_setdescription(labelid,descrleng,description,0);
}

uint8_t labelset_mr_setdescription(uint8_t labelid,const uint8_t *description) {
	return labelset_univ_setdescription(labelid,strlen((const char*)description),description,1);
}

#define GOALCLASSES 3

#define CHLOGSTRSIZE ((3*9*10*MASKORGROUP)+1)

/* label sets */

typedef struct _labelset {
	uint8_t create_mode;
	uint8_t create_labelscnt;
	uint8_t keep_labelscnt;
	uint8_t arch_labelscnt;
	uint8_t have_create_labels;
	uint8_t have_keep_labels;
	uint8_t have_arch_labels;
	uint32_t **create_labelmasks;
	uint32_t **keep_labelmasks;
	uint32_t **arch_labelmasks;
	uint16_t arch_delay;
	uint32_t labelsum;
	uint32_t files;
	uint32_t directories;
	uint64_t stdchunks[GOALCLASSES];
	uint64_t archchunks[GOALCLASSES];
} labelset;

#define FIRSTLABELID 10

static labelset labelsets[MAXLABELSETS];
static uint32_t firstneverused=0;

static int label_group_cmp(const void *a,const void *b) {
	const uint32_t *aa = *((const uint32_t**)a);
	const uint32_t *bb = *((const uint32_t**)b);
	uint32_t i;
	for (i=0 ; i<MASKORGROUP ; i++) {
		if (aa[i]<bb[i]) {
			return -1;
		}
		if (aa[i]>bb[i]) {
			return 1;
		}
	}
	return 0;
}

static int label_cmp(const void *a,const void *b) {
	uint32_t aa = *((const uint32_t*)a);
	uint32_t bb = *((const uint32_t*)b);
	return (aa<bb)?1:(aa>bb)?-1:0;
}

uint16_t labelset_getsetid(uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay) {
	uint32_t i,j,sum;
	uint32_t* create_labelmasks_tmp[9];
	uint32_t* keep_labelmasks_tmp[9];
	uint32_t* arch_labelmasks_tmp[9];
	uint8_t have_create_labels;
	uint8_t have_keep_labels;
	uint8_t have_arch_labels;
	uint32_t new = firstneverused;
	if (create_labelscnt>9 || create_labelscnt==0 || keep_labelscnt>9 || keep_labelscnt==0 || arch_labelscnt>9 || arch_labelscnt==0) {
		return 0;
	}
	sum = 0;
	have_create_labels = 0;
	for (i=0 ; i<create_labelscnt ; i++) {
		create_labelmasks_tmp[i] = create_labelmasks+i*MASKORGROUP;
		qsort(create_labelmasks_tmp[i],MASKORGROUP,sizeof(uint32_t),label_cmp);
		if (create_labelmasks_tmp[i][0]!=0) {
			have_create_labels = 1;
		}
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += create_labelmasks_tmp[i][j];
		}
	}
	have_keep_labels = 0;
	for (i=0 ; i<keep_labelscnt ; i++) {
		keep_labelmasks_tmp[i] = keep_labelmasks+i*MASKORGROUP;
		qsort(keep_labelmasks_tmp[i],MASKORGROUP,sizeof(uint32_t),label_cmp);
		if (keep_labelmasks_tmp[i][0]!=0) {
			have_keep_labels = 1;
		}
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += keep_labelmasks_tmp[i][j];
		}
	}
	have_arch_labels = 0;
	for (i=0 ; i<arch_labelscnt ; i++) {
		arch_labelmasks_tmp[i] = arch_labelmasks+i*MASKORGROUP;
		qsort(arch_labelmasks_tmp[i],MASKORGROUP,sizeof(uint32_t),label_cmp);
		if (arch_labelmasks_tmp[i][0]!=0) {
			have_arch_labels = 1;
		}
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += arch_labelmasks_tmp[i][j];
		}
	}
	if (have_create_labels==0 && have_keep_labels==0 && have_arch_labels==0 && create_labelscnt==keep_labelscnt && keep_labelscnt==arch_labelscnt) { // classic goal
		return create_labelscnt;
	}
	qsort(create_labelmasks_tmp,create_labelscnt,sizeof(uint32_t*),label_group_cmp);
	qsort(keep_labelmasks_tmp,keep_labelscnt,sizeof(uint32_t*),label_group_cmp);
	qsort(arch_labelmasks_tmp,arch_labelscnt,sizeof(uint32_t*),label_group_cmp);
	for (i=FIRSTLABELID ; i<firstneverused ; i++) {
		if (labelsets[i].create_mode==create_mode && labelsets[i].arch_delay==arch_delay && labelsets[i].create_labelscnt==create_labelscnt && labelsets[i].keep_labelscnt==keep_labelscnt && labelsets[i].arch_labelscnt==arch_labelscnt && labelsets[i].labelsum==sum) {
			for (j=0 ; j<create_labelscnt ; j++) {
				if (memcmp(labelsets[i].create_labelmasks[j],create_labelmasks_tmp[j],MASKORGROUP*sizeof(uint32_t))!=0) {
					break;
				}
			}
			if (j==create_labelscnt) {
				for (j=0 ; j<keep_labelscnt ; j++) {
					if (memcmp(labelsets[i].keep_labelmasks[j],keep_labelmasks_tmp[j],MASKORGROUP*sizeof(uint32_t))!=0) {
						break;
					}
				}
				if (j==keep_labelscnt) {
					for (j=0 ; j<arch_labelscnt ; j++) {
						if (memcmp(labelsets[i].arch_labelmasks[j],arch_labelmasks_tmp[j],MASKORGROUP*sizeof(uint32_t))!=0) {
							break;
						}
					}
					if (j==arch_labelscnt) {
						return i;
					}
				}
			}
		}
		if (new==firstneverused && (labelsets[i].files + labelsets[i].directories)==0) {
			new = i;
		}
	}
	if (new<MAXLABELSETS) {
		char chlogstr[CHLOGSTRSIZE];
		int chlogstrleng;

		if (labelsets[new].create_labelmasks) {
			for (i=0 ; i<labelsets[new].create_labelscnt ; i++) {
				free(labelsets[new].create_labelmasks[i]);
			}
			free(labelsets[new].create_labelmasks);
		}
		labelsets[new].create_labelmasks = malloc(create_labelscnt*sizeof(uint32_t*));
		passert(labelsets[new].create_labelmasks);
		for (i=0 ; i<create_labelscnt ; i++) {
			labelsets[new].create_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[new].create_labelmasks[i]);
			memcpy(labelsets[new].create_labelmasks[i],create_labelmasks_tmp[i],MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[new].create_labelscnt = create_labelscnt;
		labelsets[new].have_create_labels = have_create_labels;

		if (labelsets[new].keep_labelmasks) {
			for (i=0 ; i<labelsets[new].keep_labelscnt ; i++) {
				free(labelsets[new].keep_labelmasks[i]);
			}
			free(labelsets[new].keep_labelmasks);
		}
		labelsets[new].keep_labelmasks = malloc(keep_labelscnt*sizeof(uint32_t*));
		passert(labelsets[new].keep_labelmasks);
		for (i=0 ; i<keep_labelscnt ; i++) {
			labelsets[new].keep_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[new].keep_labelmasks[i]);
			memcpy(labelsets[new].keep_labelmasks[i],keep_labelmasks_tmp[i],MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[new].keep_labelscnt = keep_labelscnt;
		labelsets[new].have_keep_labels = have_keep_labels;

		if (labelsets[new].arch_labelmasks) {
			for (i=0 ; i<labelsets[new].arch_labelscnt ; i++) {
				free(labelsets[new].arch_labelmasks[i]);
			}
			free(labelsets[new].arch_labelmasks);
		}
		labelsets[new].arch_labelmasks = malloc(arch_labelscnt*sizeof(uint32_t*));
		passert(labelsets[new].arch_labelmasks);
		for (i=0 ; i<arch_labelscnt ; i++) {
			labelsets[new].arch_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[new].arch_labelmasks[i]);
			memcpy(labelsets[new].arch_labelmasks[i],arch_labelmasks_tmp[i],MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[new].arch_labelscnt = arch_labelscnt;
		labelsets[new].have_arch_labels = have_arch_labels;

		labelsets[new].create_mode = create_mode;
		labelsets[new].arch_delay = arch_delay;
		labelsets[new].labelsum = sum;
		labelsets[new].files = 0;
		labelsets[new].directories = 0;
		for (j=0 ; j<GOALCLASSES ; j++) {
			labelsets[new].stdchunks[j] = 0;
			labelsets[new].archchunks[j] = 0;
		}
		if (new==firstneverused) {
			firstneverused++;
		}
		chlogstrleng=0;
		for (i=0 ; i<create_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (chlogstrleng<CHLOGSTRSIZE) {
					chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",create_labelmasks_tmp[i][j]);
				}
			}
		}
		for (i=0 ; i<keep_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (chlogstrleng<CHLOGSTRSIZE) {
					chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",keep_labelmasks_tmp[i][j]);
				}
			}
		}
		for (i=0 ; i<arch_labelscnt ; i++) {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (chlogstrleng<CHLOGSTRSIZE) {
					chlogstrleng += snprintf(chlogstr+chlogstrleng,CHLOGSTRSIZE-chlogstrleng,"%"PRIu32",",arch_labelmasks_tmp[i][j]);
				}
			}
		}
		if (chlogstrleng>0) {
			chlogstr[chlogstrleng-1]='\0';
		} else {
			chlogstr[0]='0';
			chlogstr[1]='\0';
		}
		changelog("%"PRIu32"|LABELSET(%"PRIu32",W%"PRIu8",K%"PRIu8",A%"PRIu8",%"PRIu8",%"PRIu16",%s)",main_time(),new,create_labelscnt,keep_labelscnt,arch_labelscnt,create_mode,arch_delay,chlogstr);
		return new;
	}
	syslog(LOG_WARNING,"label sets count exceeded");
	return 0;
}

uint8_t labelset_mr_labelset(uint16_t labelsetid,uint8_t create_mode,uint8_t create_labelscnt,uint32_t *create_labelmasks,uint8_t keep_labelscnt,uint32_t *keep_labelmasks,uint8_t arch_labelscnt,uint32_t *arch_labelmasks,uint16_t arch_delay) {
	uint32_t i,j,sum;
	uint8_t have_create_labels;
	uint8_t have_keep_labels;
	uint8_t have_arch_labels;
	if (labelsetid<FIRSTLABELID || labelsetid>firstneverused) {
		syslog(LOG_WARNING,"labelset: wrong labelsetid");
		return ERROR_EINVAL;
	}
	if ((labelsets[labelsetid].files+labelsets[labelsetid].directories)!=0) {
		syslog(LOG_WARNING,"labelset: labelsetid not empty");
		return ERROR_MISMATCH;
	}
	if (labelsets[labelsetid].create_labelmasks) {
		for (i=0 ; i<labelsets[labelsetid].create_labelscnt ; i++) {
			free(labelsets[labelsetid].create_labelmasks[i]);
		}
		free(labelsets[labelsetid].create_labelmasks);
	}
	if (labelsets[labelsetid].keep_labelmasks) {
		for (i=0 ; i<labelsets[labelsetid].keep_labelscnt ; i++) {
			free(labelsets[labelsetid].keep_labelmasks[i]);
		}
		free(labelsets[labelsetid].keep_labelmasks);
	}
	if (labelsets[labelsetid].arch_labelmasks) {
		for (i=0 ; i<labelsets[labelsetid].arch_labelscnt ; i++) {
			free(labelsets[labelsetid].arch_labelmasks[i]);
		}
		free(labelsets[labelsetid].arch_labelmasks);
	}
	sum = 0;
	have_create_labels = 0;
	labelsets[labelsetid].create_labelmasks = malloc(create_labelscnt*sizeof(uint32_t*));
	passert(labelsets[labelsetid].create_labelmasks);
	for (i=0 ; i<create_labelscnt ; i++) {
		labelsets[labelsetid].create_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
		passert(labelsets[labelsetid].create_labelmasks[i]);
		memcpy(labelsets[labelsetid].create_labelmasks[i],create_labelmasks+i*MASKORGROUP,MASKORGROUP*sizeof(uint32_t));
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += create_labelmasks[i*MASKORGROUP+j];
			if (create_labelmasks[i*MASKORGROUP+j]!=0) {
				have_create_labels = 1;
			}
		}
	}
	have_keep_labels = 0;
	labelsets[labelsetid].keep_labelmasks = malloc(keep_labelscnt*sizeof(uint32_t*));
	passert(labelsets[labelsetid].keep_labelmasks);
	for (i=0 ; i<keep_labelscnt ; i++) {
		labelsets[labelsetid].keep_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
		passert(labelsets[labelsetid].keep_labelmasks[i]);
		memcpy(labelsets[labelsetid].keep_labelmasks[i],keep_labelmasks+i*MASKORGROUP,MASKORGROUP*sizeof(uint32_t));
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += keep_labelmasks[i*MASKORGROUP+j];
			if (keep_labelmasks[i*MASKORGROUP+j]!=0) {
				have_keep_labels = 1;
			}
		}
	}
	have_arch_labels = 0;
	labelsets[labelsetid].arch_labelmasks = malloc(arch_labelscnt*sizeof(uint32_t*));
	passert(labelsets[labelsetid].arch_labelmasks);
	for (i=0 ; i<arch_labelscnt ; i++) {
		labelsets[labelsetid].arch_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
		passert(labelsets[labelsetid].arch_labelmasks[i]);
		memcpy(labelsets[labelsetid].arch_labelmasks[i],arch_labelmasks+i*MASKORGROUP,MASKORGROUP*sizeof(uint32_t));
		for (j=0 ; j<MASKORGROUP ; j++) {
			sum += arch_labelmasks[i*MASKORGROUP+j];
			if (arch_labelmasks[i*MASKORGROUP+j]!=0) {
				have_arch_labels = 1;
			}
		}
	}
	labelsets[labelsetid].create_mode = create_mode;
	labelsets[labelsetid].arch_delay = arch_delay;
	labelsets[labelsetid].create_labelscnt = create_labelscnt;
	labelsets[labelsetid].keep_labelscnt = keep_labelscnt;
	labelsets[labelsetid].arch_labelscnt = arch_labelscnt;
	labelsets[labelsetid].have_create_labels = have_create_labels;
	labelsets[labelsetid].have_keep_labels = have_keep_labels;
	labelsets[labelsetid].have_arch_labels = have_arch_labels;
	labelsets[labelsetid].labelsum = sum;
	if (labelsetid==firstneverused) {
		firstneverused++;
	}
	meta_version_inc();
	return STATUS_OK;
}

void labelset_incref(uint16_t labelsetid,uint8_t type) {
	if (type==TYPE_DIRECTORY) {
		labelsets[labelsetid].directories++;
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		labelsets[labelsetid].files++;
	}
}

void labelset_decref(uint16_t labelsetid,uint8_t type) {
	if (type==TYPE_DIRECTORY) {
		labelsets[labelsetid].directories--;
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		labelsets[labelsetid].files--;
	}
}

uint8_t labelset_get_create_mode(uint16_t labelsetid) {
	return labelsets[labelsetid].create_mode;
}

uint8_t labelset_get_create_goal(uint16_t labelsetid) {
	return labelsets[labelsetid].create_labelscnt;
}

uint8_t labelset_get_keepmax_goal(uint16_t labelsetid) {
	if (labelsets[labelsetid].arch_labelscnt>labelsets[labelsetid].keep_labelscnt) {
		return labelsets[labelsetid].arch_labelscnt;
	} else {
		return labelsets[labelsetid].keep_labelscnt;
	}
}

uint8_t labelset_get_keeparch_goal(uint16_t labelsetid,uint8_t archflag) {
	if (archflag) {
		return labelsets[labelsetid].arch_labelscnt;
	} else {
		return labelsets[labelsetid].keep_labelscnt;
	}
}

uint8_t labelset_get_create_labelmasks(uint16_t labelsetid,uint32_t ***labelmasks) {
	*labelmasks = labelsets[labelsetid].create_labelmasks;
	return labelsets[labelsetid].create_labelscnt;
}

uint8_t labelset_get_keeparch_labelmasks(uint16_t labelsetid,uint8_t archflag,uint32_t ***labelmasks) {
	if (archflag) {
		*labelmasks = labelsets[labelsetid].arch_labelmasks;
		return labelsets[labelsetid].arch_labelscnt;
	} else {
		*labelmasks = labelsets[labelsetid].keep_labelmasks;
		return labelsets[labelsetid].keep_labelscnt;
	}
}

uint8_t labelset_is_simple_goal(uint16_t labelsetid) {
	if (labelsetid>=FIRSTLABELID) {
		return 0;
	} else {
		return 1;
	}
}

uint8_t labelset_has_any_labels(uint16_t labelsetid) {
	if (labelsetid>=FIRSTLABELID) {
		return (labelsets[labelsetid].have_create_labels|labelsets[labelsetid].have_keep_labels|labelsets[labelsetid].have_arch_labels);
	} else {
		return 0;
	}
}

uint8_t labelset_has_create_labels(uint16_t labelsetid) {
	if (labelsetid>=FIRSTLABELID) {
		return labelsets[labelsetid].have_create_labels;
	} else {
		return 0;
	}
}

uint8_t labelset_has_keeparch_labels(uint16_t labelsetid,uint8_t archflag) {
	if (labelsetid>=FIRSTLABELID) {
		if (archflag) {
			return labelsets[labelsetid].have_arch_labels;
		} else {
			return labelsets[labelsetid].have_keep_labels;
		}
	} else {
		return 0;
	}
}

uint16_t labelset_get_arch_delay(uint16_t labelsetid) {
	return labelsets[labelsetid].arch_delay;
}

void labelset_state_change(uint16_t oldlabelsetid,uint8_t oldarchflag,uint8_t oldrvc,uint16_t newlabelsetid,uint8_t newarchflag,uint8_t newrvc) {
	uint8_t refgoal,class;
	if (oldlabelsetid>0) {
		refgoal = oldarchflag?labelsets[oldlabelsetid].arch_labelscnt:labelsets[oldlabelsetid].keep_labelscnt;
		if (oldrvc > refgoal) {
			class = 2;
		} else if (oldrvc == refgoal) {
			class = 1;
		} else {
			class = 0;
		}
		if (oldarchflag==0) {
			labelsets[oldlabelsetid].stdchunks[class]--;
		} else {
			labelsets[oldlabelsetid].archchunks[class]--;
		}
	}
	if (newlabelsetid>0) {
		refgoal = newarchflag?labelsets[newlabelsetid].arch_labelscnt:labelsets[newlabelsetid].keep_labelscnt;
		if (newrvc > refgoal) {
			class = 2;
		} else if (newrvc == refgoal) {
			class = 1;
		} else {
			class = 0;
		}
		if (newarchflag==0) {
			labelsets[newlabelsetid].stdchunks[class]++;
		} else {
			labelsets[newlabelsetid].archchunks[class]++;
		}
	}
}

uint32_t labelset_label_info(uint8_t *buff) {
	uint32_t leng,i; /*,j,inodes; */
	if (buff==NULL) {
		leng = 2;
		for (i=0 ; i<LABELS ; i++) {
			leng += 4 + labeldescriptions[i].descrleng;
		}
		return leng;
	} else {
		put16bit(&buff,matocsserv_servers_count());
		for (i=0 ; i<LABELS ; i++) {
			put8bit(&buff,i);
			put8bit(&buff,labeldescriptions[i].descrleng);
			if (labeldescriptions[i].descrleng>0) {
				memcpy(buff,labeldescriptions[i].description,labeldescriptions[i].descrleng);
				buff+=labeldescriptions[i].descrleng;
			}
			put16bit(&buff,matocsserv_servers_with_label(i));
//			inodes = 0;
//			for (j=FIRSTLABELID ; j<firstneverused ; j++) {
//			}
//			put32bit(&buff,....);
		}
		return 0;
	}
}

uint32_t labelset_label_set_info(uint8_t *buff) {
	uint32_t leng,i,j,k;
	if (buff==NULL) {
		leng = 2;
		for (i=1 ; i<firstneverused ; i++) {
			if ((labelsets[i].files+labelsets[i].directories)>0) {
				leng += 18 + GOALCLASSES * 16;
				leng += ((uint32_t)labelsets[i].create_labelscnt) * ( MASKORGROUP * 4 + 2 );
				leng += ((uint32_t)labelsets[i].keep_labelscnt) * ( MASKORGROUP * 4 + 2 );
				leng += ((uint32_t)labelsets[i].arch_labelscnt) * ( MASKORGROUP * 4 + 2 );
			}
		}
		return leng;
	} else {
		chunk_labelset_can_be_fulfilled(0,NULL); // init server list
		put16bit(&buff,matocsserv_servers_count());
		for (i=1 ; i<firstneverused ; i++) {
			if ((labelsets[i].files+labelsets[i].directories)>0) {
				put8bit(&buff,i);
				put32bit(&buff,labelsets[i].files);
				put32bit(&buff,labelsets[i].directories);
				for (j=0 ; j<GOALCLASSES ; j++) {
					put64bit(&buff,labelsets[i].stdchunks[j]);
					put64bit(&buff,labelsets[i].archchunks[j]);
				}
				put8bit(&buff,labelsets[i].create_mode);
				put16bit(&buff,labelsets[i].arch_delay);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(labelsets[i].create_labelscnt,labelsets[i].create_labelmasks));
				put8bit(&buff,labelsets[i].create_labelscnt);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(labelsets[i].keep_labelscnt,labelsets[i].keep_labelmasks));
				put8bit(&buff,labelsets[i].keep_labelscnt);
				put8bit(&buff,chunk_labelset_can_be_fulfilled(labelsets[i].arch_labelscnt,labelsets[i].arch_labelmasks));
				put8bit(&buff,labelsets[i].arch_labelscnt);
				for (j=0 ; j<labelsets[i].create_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,labelsets[i].create_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(labelsets[i].create_labelmasks[j]));
				}
				for (j=0 ; j<labelsets[i].keep_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,labelsets[i].keep_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(labelsets[i].keep_labelmasks[j]));
				}
				for (j=0 ; j<labelsets[i].arch_labelscnt ; j++) {
					for (k=0 ; k<MASKORGROUP ; k++) {
						put32bit(&buff,labelsets[i].arch_labelmasks[j][k]);
					}
					put16bit(&buff,matocsserv_servers_with_labelsets(labelsets[i].arch_labelmasks[j]));
				}
			}
		}
		return 0;
	}
}

uint8_t labelset_store(bio *fd) {
	uint8_t databuff[5+3*(1+9*4*MASKORGROUP)];
	uint8_t *ptr;
	uint16_t i,j,k;
	int32_t wsize;
	if (fd==NULL) {
		return 0x15;
	}
	for (i=0 ; i<LABELS ; i++) {
		if (bio_write(fd,&(labeldescriptions[i].descrleng),1)!=1) {
			syslog(LOG_NOTICE,"write error");
			return 0xFF;
		}
		if (labeldescriptions[i].descrleng>0) {
			if (bio_write(fd,labeldescriptions[i].description,labeldescriptions[i].descrleng)!=labeldescriptions[i].descrleng) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
		}
	}
	ptr = databuff;
	put8bit(&ptr,MASKORGROUP);
	if (bio_write(fd,databuff,1)!=1) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}

	for (i=FIRSTLABELID ; i<firstneverused ; i++) {
		if ((labelsets[i].files+labelsets[i].directories)>0) {
			ptr = databuff;
			put16bit(&ptr,i);
			put8bit(&ptr,labelsets[i].create_mode);
			put16bit(&ptr,labelsets[i].arch_delay);
			put8bit(&ptr,labelsets[i].create_labelscnt);
			put8bit(&ptr,labelsets[i].keep_labelscnt);
			put8bit(&ptr,labelsets[i].arch_labelscnt);
			for (j=0 ; j<labelsets[i].create_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,labelsets[i].create_labelmasks[j][k]);
				}
			}
			for (j=0 ; j<labelsets[i].keep_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,labelsets[i].keep_labelmasks[j][k]);
				}
			}
			for (j=0 ; j<labelsets[i].arch_labelscnt ; j++) {
				for (k=0 ; k<MASKORGROUP ; k++) {
					put32bit(&ptr,labelsets[i].arch_labelmasks[j][k]);
				}
			}
			wsize = 8+(labelsets[i].create_labelscnt+labelsets[i].keep_labelscnt+labelsets[i].arch_labelscnt)*4*MASKORGROUP;
			if (bio_write(fd,databuff,wsize)!=wsize) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
		}
	}
	memset(databuff,0,8);
	if (bio_write(fd,databuff,8)!=8) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	return 0;
}

int labelset_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t *databuff = NULL;
	const uint8_t *ptr;
	uint32_t sum,labelmask;
	uint32_t chunkcount;
	uint16_t labelsetid;
	uint16_t arch_delay;
	uint8_t create_mode;
	uint8_t create_labelscnt;
	uint8_t keep_labelscnt;
	uint8_t arch_labelscnt;
	uint8_t have_create_labels;
	uint8_t have_keep_labels;
	uint8_t have_arch_labels;
	uint8_t descrleng;
	uint8_t i,j;
	uint8_t orgroup;
	uint8_t hdrleng;

	for (i=0 ; i<LABELS ; i++) {
		if (bio_read(fd,&descrleng,1)!=1) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_errlog(LOG_ERR,"loading labelset: read error");
			return -1;
		}
		if (descrleng>128) {
			mfs_syslog(LOG_ERR,"loading labelset: description too long");
			return -1;
		}
		if (labeldescriptions[i].description) {
			free(labeldescriptions[i].description);
		}
		if (descrleng>0) {
			labeldescriptions[i].description = malloc(descrleng);
			passert(labeldescriptions[i].description);
			labeldescriptions[i].descrleng = descrleng;
			if (bio_read(fd,labeldescriptions[i].description,descrleng)!=descrleng) {
				int err = errno;
				fputc('\n',stderr);
				errno = err;
				mfs_errlog(LOG_ERR,"loading labelset: read error");
				return -1;
			}
		} else {
			labeldescriptions[i].description = NULL;
			labeldescriptions[i].descrleng = 0;
		}
	}
	if (mver==0x10) {
		orgroup = 1;
	} else {
		if (bio_read(fd,&orgroup,1)!=1) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_errlog(LOG_ERR,"loading labelset: read error");
			return -1;
		}
		if (orgroup>MASKORGROUP) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading labelset: too many or-groups - ignore");
			} else {
				mfs_syslog(LOG_ERR,"loading labelset: too many or-groups");
				return -1;
			}
		}
	}
	if (orgroup<1) {
		mfs_syslog(LOG_ERR,"loading labelset: zero or-groups !!!");
		return -1;
	}
	databuff = malloc(3U*9U*4U*(uint32_t)orgroup);
	passert(databuff);
	hdrleng = (mver==0x12)?11:(mver<=0x13)?3:(mver<=0x14)?5:8;
	while (1) {
		if (bio_read(fd,databuff,hdrleng)!=hdrleng) {
			int err = errno;
			fputc('\n',stderr);
			errno = err;
			mfs_errlog(LOG_ERR,"loading labelset: read error");
			free(databuff);
			databuff=NULL;
			return -1;
		}
		ptr = databuff;
		labelsetid = get16bit(&ptr);
		if (mver>0x14) {
			create_mode = get8bit(&ptr);
			arch_delay = get16bit(&ptr);
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = get8bit(&ptr);
			arch_labelscnt = get8bit(&ptr);
			chunkcount = 0;
		} else if (mver>0x13) {
			create_mode = get8bit(&ptr);
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = get8bit(&ptr);
			arch_labelscnt = keep_labelscnt;
			arch_delay = 0;
			chunkcount = 0;
		} else {
			create_labelscnt = get8bit(&ptr);
			keep_labelscnt = create_labelscnt;
			arch_labelscnt = create_labelscnt;
			create_mode = CREATE_MODE_STD;
			arch_delay = 0;
			if (mver==0x12) {
				chunkcount = get32bit(&ptr);
				ptr+=4;
			} else {
				chunkcount = 0;
			}
		}
		if (labelsetid==0 && create_labelscnt==0 && keep_labelscnt==0 && arch_labelscnt==0 && chunkcount==0 && arch_delay==0) {
			break;
		}
		if (create_labelscnt==0 || create_labelscnt>9 || keep_labelscnt==0 || keep_labelscnt>9 || arch_labelscnt==0 || arch_labelscnt>9) {
			mfs_arg_syslog(LOG_ERR,"loading labelset: data format error (labelsetid: %"PRIu16" ; create_mode: %"PRIu8" ; create_labelscnt: %"PRIu8" ; keep_labelscnt: %"PRIu8" ; arch_labelscnt: %"PRIu8" ; arch_delay: %"PRIu16")",labelsetid,create_mode,create_labelscnt,keep_labelscnt,arch_labelscnt,arch_delay);
			free(databuff);
			databuff=NULL;
			return -1;
		}
		if (labelsetid<1 || labelsetid>=MAXLABELSETS) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading labelset: bad labelsetid - ignore");
				if (mver>0x14) {
					bio_skip(fd,(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup);
				} else if (mver>0x13) {
					bio_skip(fd,(create_labelscnt+keep_labelscnt)*4*orgroup);
				} else {
					bio_skip(fd,(create_labelscnt)*4*orgroup);
				}
				continue;
			} else {
				mfs_syslog(LOG_ERR,"loading labelset: bad labelsetid");
				free(databuff);
				databuff=NULL;
				return -1;
			}
		}
		if (labelsetid>=FIRSTLABELID) {
			if (mver>0x14) {
				if (bio_read(fd,databuff,(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup)!=(create_labelscnt+keep_labelscnt+arch_labelscnt)*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_errlog(LOG_ERR,"loading labelset: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			} else if (mver>0x13) {
				if (bio_read(fd,databuff,(create_labelscnt+keep_labelscnt)*4*orgroup)!=(create_labelscnt+keep_labelscnt)*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_errlog(LOG_ERR,"loading labelset: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			} else {
				if (bio_read(fd,databuff,create_labelscnt*4*orgroup)!=create_labelscnt*4*orgroup) {
					int err = errno;
					fputc('\n',stderr);
					errno = err;
					mfs_errlog(LOG_ERR,"loading labelset: read error");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			if (labelsets[labelsetid].create_labelmasks!=NULL || labelsets[labelsetid].keep_labelmasks!=NULL || labelsets[labelsetid].arch_labelmasks!=NULL) {
				if (ignoreflag) {
					mfs_syslog(LOG_ERR,"loading labelset: repeated labelsetid - ignore");
					if (chunkcount>0) {
						bio_skip(fd,chunkcount*8);
					}
					continue;
				} else {
					mfs_syslog(LOG_ERR,"loading labelset: repeated labelsetid");
					free(databuff);
					databuff=NULL;
					return -1;
				}
			}
			sum = 0;
			have_create_labels = 0;
			have_keep_labels = 0;
			have_arch_labels = 0;
			ptr = databuff;
			labelsets[labelsetid].create_labelmasks = malloc(create_labelscnt*sizeof(uint32_t*));
			passert(labelsets[labelsetid].create_labelmasks);
			for (i=0 ; i<create_labelscnt ; i++) {
				labelsets[labelsetid].create_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
				passert(labelsets[labelsetid].create_labelmasks[i]);
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (j<orgroup) {
						labelmask = get32bit(&ptr);
					} else {
						labelmask = 0;
					}
					sum += labelmask;
					labelsets[labelsetid].create_labelmasks[i][j] = labelmask;
					if (labelmask!=0) {
						have_create_labels = 1;
					}
				}
			}
			labelsets[labelsetid].keep_labelmasks = malloc(keep_labelscnt*sizeof(uint32_t*));
			passert(labelsets[labelsetid].keep_labelmasks);
			for (i=0 ; i<keep_labelscnt ; i++) {
				labelsets[labelsetid].keep_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
				passert(labelsets[labelsetid].keep_labelmasks[i]);
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (mver>0x13) {
						if (j<orgroup) {
							labelmask = get32bit(&ptr);
						} else {
							labelmask = 0;
						}
					} else {
						labelmask = labelsets[labelsetid].create_labelmasks[i][j];
					}
					sum += labelmask;
					labelsets[labelsetid].keep_labelmasks[i][j] = labelmask;
					if (labelmask!=0) {
						have_keep_labels = 1;
					}
				}
			}
			labelsets[labelsetid].arch_labelmasks = malloc(arch_labelscnt*sizeof(uint32_t*));
			passert(labelsets[labelsetid].arch_labelmasks);
			for (i=0 ; i<arch_labelscnt ; i++) {
				labelsets[labelsetid].arch_labelmasks[i] = malloc(MASKORGROUP*sizeof(uint32_t));
				passert(labelsets[labelsetid].arch_labelmasks[i]);
				for (j=0 ; j<MASKORGROUP ; j++) {
					if (mver>0x14) {
						if (j<orgroup) {
							labelmask = get32bit(&ptr);
						} else {
							labelmask = 0;
						}
					} else {
						labelmask = labelsets[labelsetid].keep_labelmasks[i][j];
					}
					sum += labelmask;
					labelsets[labelsetid].arch_labelmasks[i][j] = labelmask;
					if (labelmask!=0) {
						have_arch_labels = 1;
					}
				}
			}
			labelsets[labelsetid].create_mode = create_mode;
			labelsets[labelsetid].arch_delay = arch_delay;
			labelsets[labelsetid].create_labelscnt = create_labelscnt;
			labelsets[labelsetid].keep_labelscnt = keep_labelscnt;
			labelsets[labelsetid].arch_labelscnt = arch_labelscnt;
			labelsets[labelsetid].have_create_labels = have_create_labels;
			labelsets[labelsetid].have_keep_labels = have_keep_labels;
			labelsets[labelsetid].have_arch_labels = have_arch_labels;
			labelsets[labelsetid].labelsum = sum;
		}
		labelsets[labelsetid].files = 0;
		labelsets[labelsetid].directories = 0;
		for (j=0 ; j<GOALCLASSES ; j++) {
			labelsets[labelsetid].stdchunks[j] = 0;
			labelsets[labelsetid].archchunks[j] = 0;
		}
		if (labelsetid>=firstneverused) {
			firstneverused = labelsetid+1;
		}
		if (chunkcount>0) {
			bio_skip(fd,chunkcount*8);
		}
	}
	free(databuff);
	databuff=NULL;
	return 1;
}

void labelset_cleanup(void) {
	uint32_t i,j;

	for (i=0 ; i<LABELS ; i++) {
		if (labeldescriptions[i].description!=NULL) {
			free(labeldescriptions[i].description);
		}
		labeldescriptions[i].descrleng = 0;
		labeldescriptions[i].description = NULL;
	}
	for (i=0 ; i<MAXLABELSETS ; i++) {
		if (labelsets[i].create_labelmasks!=NULL) {
			for (j=0 ; j<labelsets[i].create_labelscnt ; j++) {
				free(labelsets[i].create_labelmasks[j]);
			}
			free(labelsets[i].create_labelmasks);
		}
		labelsets[i].create_labelscnt = 0;
		labelsets[i].create_labelmasks = NULL;
		if (labelsets[i].keep_labelmasks!=NULL) {
			for (j=0 ; j<labelsets[i].keep_labelscnt ; j++) {
				free(labelsets[i].keep_labelmasks[j]);
			}
			free(labelsets[i].keep_labelmasks);
		}
		labelsets[i].keep_labelscnt = 0;
		labelsets[i].keep_labelmasks = NULL;
		if (labelsets[i].arch_labelmasks!=NULL) {
			for (j=0 ; j<labelsets[i].arch_labelscnt ; j++) {
				free(labelsets[i].arch_labelmasks[j]);
			}
			free(labelsets[i].arch_labelmasks);
		}
		labelsets[i].arch_labelscnt = 0;
		labelsets[i].arch_labelmasks = NULL;
		labelsets[i].create_mode = CREATE_MODE_STD;
		labelsets[i].arch_delay = 0;
		labelsets[i].labelsum = 0;
		labelsets[i].files = 0;
		labelsets[i].directories = 0;
		for (j=0 ; j<GOALCLASSES ; j++) {
			labelsets[i].stdchunks[j] = 0;
			labelsets[i].archchunks[j] = 0;
		}
	}

	for (i=1 ; i<FIRSTLABELID ; i++) {
		labelsets[i].create_labelscnt = i;
		labelsets[i].create_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].create_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].create_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].create_labelmasks[j]);
			memset(labelsets[i].create_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[i].keep_labelscnt = i;
		labelsets[i].keep_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].keep_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].keep_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].keep_labelmasks[j]);
			memset(labelsets[i].keep_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[i].arch_labelscnt = i;
		labelsets[i].arch_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].arch_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].arch_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].arch_labelmasks[j]);
			memset(labelsets[i].arch_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
//		labelsets[i].create_mode = CREATE_MODE_STD;
//		labelsets[i].have_create_labels = 0;
//		labelsets[i].have_keep_labels = 0;
//		labelsets[i].labelsum = 0;
	}
	firstneverused = FIRSTLABELID;
}

int labelset_init(void) {
	uint32_t i,j;

	for (i=0 ; i<LABELS ; i++) {
		labeldescriptions[i].descrleng = 0;
		labeldescriptions[i].description = NULL;
	}

	for (i=0 ; i<MAXLABELSETS ; i++) {
		labelsets[i].create_mode = CREATE_MODE_STD;
		labelsets[i].have_create_labels = 0;
		labelsets[i].create_labelscnt = 0;
		labelsets[i].create_labelmasks = NULL;
		labelsets[i].have_keep_labels = 0;
		labelsets[i].keep_labelscnt = 0;
		labelsets[i].keep_labelmasks = NULL;
		labelsets[i].have_arch_labels = 0;
		labelsets[i].arch_labelscnt = 0;
		labelsets[i].arch_labelmasks = NULL;
		labelsets[i].arch_delay = 0;
		labelsets[i].labelsum = 0;
		labelsets[i].files = 0;
		labelsets[i].directories = 0;
		for (j=0 ; j<GOALCLASSES ; j++) {
			labelsets[i].stdchunks[j] = 0;
			labelsets[i].archchunks[j] = 0;
		}
	}

	for (i=1 ; i<FIRSTLABELID ; i++) {
		labelsets[i].create_labelscnt = i;
		labelsets[i].create_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].create_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].create_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].create_labelmasks[j]);
			memset(labelsets[i].create_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[i].keep_labelscnt = i;
		labelsets[i].keep_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].keep_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].keep_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].keep_labelmasks[j]);
			memset(labelsets[i].keep_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
		labelsets[i].arch_labelscnt = i;
		labelsets[i].arch_labelmasks = malloc(i*sizeof(uint32_t*));
		passert(labelsets[i].arch_labelmasks);
		for (j=0 ; j<i ; j++) {
			labelsets[i].arch_labelmasks[j] = malloc(MASKORGROUP*sizeof(uint32_t));
			passert(labelsets[i].arch_labelmasks[j]);
			memset(labelsets[i].arch_labelmasks[j],0,MASKORGROUP*sizeof(uint32_t));
		}
	}
	firstneverused = FIRSTLABELID;

	return 0;
}
