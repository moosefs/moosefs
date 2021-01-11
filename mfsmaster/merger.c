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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#include "slogger.h"
#include "sharedpointer.h"
#include "restore.h"
#include "clocks.h"

#define BSIZE 200000

typedef struct _hentry {
	FILE *fd;
	void *shfilename;
	char *buff;
	char *ptr;
	int64_t nextid;
} hentry;

static hentry *heap;
static uint32_t heapsize;
static int64_t maxidhole;
static uint64_t firstlv,lastlv;

#define PARENT(x) (((x)-1)/2)
#define CHILD(x) (((x)*2)+1)

void merger_heap_sort_down(void) {
	uint32_t l,r,m;
	uint32_t pos=0;
	hentry x;
	while (pos<heapsize) {
		l = CHILD(pos);
		r = l+1;
		if (l>=heapsize) {
			return;
		}
		m = l;
		if (r<heapsize && heap[r].nextid < heap[l].nextid) {
			m = r;
		}
		if (heap[pos].nextid <= heap[m].nextid) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[m];
		heap[m] = x;
		pos = m;
	}
}

void merger_heap_sort_up(void) {
	uint32_t pos=heapsize-1;
	uint32_t p;
	hentry x;
	while (pos>0) {
		p = PARENT(pos);
		if (heap[pos].nextid >= heap[p].nextid) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[p];
		heap[p] = x;
		pos = p;
	}
}


void merger_nextentry(uint32_t pos) {
	if (fgets(heap[pos].buff,BSIZE,heap[pos].fd)) {
		int64_t nextid = strtoll(heap[pos].buff,&(heap[pos].ptr),10);
		if (heap[pos].ptr[0]==':' && heap[pos].ptr[1]==' ') {
			heap[pos].ptr += 2;
		}
		if (heap[pos].nextid<0 || (nextid>heap[pos].nextid && nextid<heap[pos].nextid+maxidhole)) {
			heap[pos].nextid = nextid;
		} else {
			mfs_arg_syslog(LOG_WARNING,"found garbage at the end of file: %s (last correct id: %"PRIu64")\n",(char*)shp_get(heap[pos].shfilename),heap[pos].nextid);
			heap[pos].nextid = INT64_C(-1);
		}
	} else {
		heap[pos].nextid = INT64_C(-1);
	}
}

void merger_delete_entry(void) {
	if (heap[heapsize].fd) {
		fclose(heap[heapsize].fd);
	}
	if (heap[heapsize].shfilename!=NULL) {
		shp_dec(heap[heapsize].shfilename);
	}
	if (heap[heapsize].buff) {
		free(heap[heapsize].buff);
	}
}

void merger_new_entry(const char *filename) {
	// printf("add file: %s\n",filename);
	if ((heap[heapsize].fd = fopen(filename,"r"))!=NULL) {
		heap[heapsize].shfilename = shp_new(strdup(filename),free);
		heap[heapsize].buff = malloc(BSIZE);
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = INT64_C(-1);
		merger_nextentry(heapsize);
	} else {
		mfs_arg_syslog(LOG_WARNING,"can't open changelog file: %s\n",filename);
		heap[heapsize].shfilename = NULL;
		heap[heapsize].buff = NULL;
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = INT64_C(-1);
	}
}

int merger_start(uint32_t files,char **filenames,uint64_t maxhole,uint64_t minid,uint64_t maxid) {
	uint32_t i;
	heapsize = 0;
	heap = (hentry*)malloc(sizeof(hentry)*files);
	if (heap==NULL) {
		return -1;
	}
	for (i=0 ; i<files ; i++) {
		merger_new_entry(filenames[i]);
//		printf("file: %s / firstid: %"PRIu64"\n",filenames[i],heap[heapsize].nextid);
		if (heap[heapsize].nextid<0) {
			merger_delete_entry();
		} else {
			heapsize++;
			merger_heap_sort_up();
		}
	}
	maxidhole = maxhole;
	firstlv = minid;
	lastlv = maxid;
//	for (i=0 ; i<heapsize ; i++) {
//		printf("heap: %u / firstid: %"PRIu64"\n",i,heap[i].nextid);
//	}
	return 0;
}

int merger_loop(uint8_t verblevel) {
	int status;
	uint8_t perc,etaok;
	uint32_t eta;
	uint64_t st,cu;
	hentry h;

	//perc = 0;
	//eta = 0;
	//etaok = 0;
	st = monotonic_useconds();
	while (heapsize) {
		if ((heap[0].nextid%2497)==0 && lastlv>firstlv) {
			if (heap[0].nextid<(int64_t)firstlv) {
				perc = 0;
				eta = 0;
				etaok = 0;
				st = monotonic_useconds();
			} else if (heap[0].nextid>(int64_t)lastlv) {
				perc = 100;
				eta = 0;
				etaok = 1;
			} else {
				cu = monotonic_useconds();
				perc = (heap[0].nextid - firstlv) * 100 / (lastlv - firstlv);
				eta = ((lastlv - heap[0].nextid) * (cu - st) / (heap[0].nextid - firstlv)) / 1000000U;
				etaok = 1;
			}
			printf("progress: current change: %"PRIu64" (first:%"PRIu64" - last:%"PRIu64" - %"PRIu8"%%",heap[0].nextid,firstlv,lastlv,perc);
			if (etaok) {
				printf(" - ETA:%02u:%02us)\r",(unsigned int)(eta/60),(unsigned int)(eta%60));
			} else {
				printf(" - ETA:__:__s)\r");
			}
			fflush(stdout);
		}
//		printf("current id: %"PRIu64" / %s\n",heap[0].nextid,heap[0].ptr);
		if ((status=restore_file(heap[0].shfilename,heap[0].nextid,heap[0].ptr,verblevel))<0) {
			while (heapsize) {
				heapsize--;
				merger_delete_entry();
			}
			printf("\n");
			return status;
		}
		merger_nextentry(0);
		if (heap[0].nextid<0) {
			heapsize--;
			h = heap[0];
			heap[0] = heap[heapsize];
			heap[heapsize] = h;
			merger_delete_entry();
		}
		merger_heap_sort_down();
	}
	printf("progress: current change: %"PRIu64" (first:%"PRIu64" - last:%"PRIu64" - 100%% - ETA:finished)\n",lastlv,firstlv,lastlv);
	return 0;
}
