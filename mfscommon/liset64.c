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
#include <inttypes.h>

#include "liset64.h"
#include "mfsalloc.h"

typedef struct liset {
	int valid;
	uint64_t card;
	uint32_t ****bittab;
} liset;

static liset *settab = NULL;
static int settabsize = 0;
static int settabffree = 0;

int liset_new() {
	int i;
	for (i=0 ; i<settabffree ; i++) {
		if (settab[i].valid==0) {
			settab[i].valid=1;
			settab[i].bittab = NULL;
			return i;
		}
	}
	if (settabffree>=settabsize) {
		if (settab==NULL) {
			settabsize = 10;
			settab = (liset*)malloc(sizeof(liset)*settabsize);
		} else {
			settabsize *= 2;
			settab = (liset*)mfsrealloc((char*)settab,sizeof(liset)*settabsize);
		}
	}
	settab[settabffree].valid = 1;
	settab[settabffree].bittab = NULL;
	settab[settabffree].card = 0;
	return settabffree++;
}

void liset_clear(int setid) {
	liset *aset = settab+setid;
	uint32_t i,j,k;
	if (setid<0 || setid>=settabffree) return;
	if (aset->valid==0) return;
	if (aset->bittab!=NULL) {
		for (i=0 ; i<65536 ; i++) {
			if (aset->bittab[i]!=NULL) {
				for (j=0 ; j<65536 ; j++) {
					if (aset->bittab[i][j]!=NULL) {
						for (k=0 ; k<65536 ; k++) {
							if (aset->bittab[i][j][k]!=NULL) {
								free(aset->bittab[i][j][k]);
							}
						}
						free(aset->bittab[i][j]);
					}
				}
				free(aset->bittab[i]);
			}
		}
		free(aset->bittab);
		aset->bittab=NULL;
	}
	aset->card = 0;
}

void liset_remove(int setid) {
	liset *aset = settab+setid;
	if (setid<0 || setid>=settabffree) return;
	if (aset->valid==0) return;
	liset_clear(setid);
	aset->valid = 0;
}

static inline uint32_t* liset_getbset(liset *lset,uint64_t value,int m) {
	int i;
	uint16_t indx;
	uint32_t ****b0,***b1,**b2,*b3;
	b0 = lset->bittab;
	if (b0==NULL) {
		if (m==0) return NULL;
		b0 = lset->bittab = (uint32_t ****)malloc(sizeof(uint32_t ***)*65536);
		for (i=0 ; i<65536 ; i++) {
			b0[i]=NULL;
		}
	}
	indx = value>>48;
	b1 = b0[indx];
	if (b1==NULL) {
		if (m==0) return NULL;
		b1 = b0[indx] = (uint32_t ***)malloc(sizeof(uint32_t **)*65536);
		for (i=0 ; i<65536 ; i++) {
			b1[i] = NULL;
		}
	}
	indx = value>>32;
	b2 = b1[indx];
	if (b2==NULL) {
		if (m==0) return NULL;
		b2 = b1[indx] = (uint32_t **)malloc(sizeof(uint32_t*)*65536);
		for (i=0 ; i<65536 ; i++) {
			b2[i] = NULL;
		}
	}
	indx = value>>16;
	b3 = b2[indx];
	if (b3==NULL) {
		if (m==0) return NULL;
		b3 = b2[indx] = (uint32_t *)malloc(sizeof(uint32_t)*(65536/32));
		for (i=0 ; i<(65536/32) ; i++) {
			b3[i] = 0UL;
		}
	}
	return b3;
}

uint64_t liset_card(int setid) {
	liset *aset = settab+setid;
	if (setid<0 || setid>=settabffree) return 0;
	if (aset->valid==0) return 0;
	return aset->card;
}

int liset_addval(int setid,uint64_t data) {
	liset *aset = settab+setid;
	int pos;
	int ret = 0;
	uint32_t mask;
	uint32_t *bset;
	if (setid<0 || setid>=settabffree) return -1;
	if (aset->valid==0) return -1;
	bset = liset_getbset(aset,data,1);
	pos = (data&0xFFFF)/32;
	mask = (1<<(data&0x1F));
	ret = (bset[pos]&mask)?1:0;
	if (ret==0) {
		bset[pos]|=mask;
		aset->card++;
	}
	return ret;
}

int liset_delval(int setid,uint64_t data) {
	liset *aset = settab+setid;
	int pos;
	int ret = 0;
	uint32_t mask;
	uint32_t *bset;
	if (setid<0 || setid>=settabffree) return -1;
	if (aset->valid==0) return -1;
	bset = liset_getbset(aset,data,1);
	pos = (data&0xFFFF)/32;
	mask = (1<<(data&0x1F));
	ret = (bset[pos]&mask)?1:0;
	if (ret==1) {
		bset[pos]&=~mask;
		aset->card--;
	}
	return ret;
}

int liset_check(int setid,uint64_t data) {
	liset *aset = settab+setid;
	int pos;
	uint32_t mask;
	uint32_t *bset;
	if (setid<0 || setid>=settabffree) return -1;
	if (aset->valid==0) return -1;
	bset = liset_getbset(aset,data,0);
	if (bset==NULL) return 0;
	pos = (data&0xFFFF)/32;
	mask = (1<<(data&0x1F));
	return (bset[pos]&mask)?1:0;
}
