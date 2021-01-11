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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

#include "main.h"
#include "cfg.h"
#include "datapack.h"

typedef struct mlogentry {
	uint64_t chunkid;
	uint32_t inode;
	uint32_t indx;
	uint8_t type;
} mlogentry;

static mlogentry *mloghash;
static mlogentry *mloghashprev;
static uint32_t mloghashsize;
static uint32_t mloghashprevsize;
static uint32_t mloghashelements;
static uint32_t mloghashprevelements;
static uint32_t mloghashcapacity;
static uint8_t blocked;

void missing_log_insert(uint64_t chunkid,uint32_t inode,uint32_t indx,uint8_t type) {
	uint32_t hash,disp;
	if (blocked || mloghashelements>=mloghashcapacity || chunkid==0) {
		return;
	}
	hash = (((inode * 1363546567) ^ indx) * 985732289) ^ chunkid;
	disp = (((inode * 2345123993) + indx) * 746344009) + chunkid;
	hash %= mloghashsize;
	disp %= mloghashsize;
	disp |= 1;
	while (mloghash[hash].chunkid!=0) {
		if (mloghash[hash].chunkid==chunkid && mloghash[hash].inode==inode && mloghash[hash].indx==indx) {
			return;
		}
		hash += disp;
		hash %= mloghashsize;
	}
	mloghash[hash].chunkid = chunkid;
	mloghash[hash].inode = inode;
	mloghash[hash].indx = indx;
	mloghash[hash].type = type;
	mloghashelements++;
}

void missing_log_swap(void) {
	mlogentry *mloghashtmp;

	if (blocked) {
		blocked = 0;
	} else {
		if (mloghashsize == mloghashprevsize) {
			mloghashtmp = mloghashprev;
			mloghashprev = mloghash;
			mloghash = mloghashtmp;
		} else {
			free(mloghashprev);
			mloghashprev = mloghash;
			mloghash = malloc(sizeof(mlogentry)*mloghashsize);
		}
		memset(mloghash,0,sizeof(mlogentry)*mloghashsize);
		mloghashprevsize = mloghashsize;
		mloghashprevelements = mloghashelements;
		mloghashelements = 0;
	}
}

uint32_t missing_log_getdata(uint8_t *buff,uint8_t mode) {
	uint32_t i,j;
	if (buff==NULL) {
		return mloghashprevelements*((mode==0)?16:17);
	} else {
		j = 0;
		for (i=0 ; i<mloghashprevsize && j<mloghashprevelements ; i++) {
			if (mloghashprev[i].chunkid!=0) {
				put64bit(&buff,mloghashprev[i].chunkid);
				put32bit(&buff,mloghashprev[i].inode);
				put32bit(&buff,mloghashprev[i].indx);
				if (mode) {
					put8bit(&buff,mloghashprev[i].type);
				}
				j++;
			}
		}
		return 0;
	}
}

void missing_log_reload(void) {
	uint32_t ncapacity;

	ncapacity = cfg_getuint32("MISSING_LOG_CAPACITY",100000);

	if (ncapacity<1000) {
		syslog(LOG_WARNING,"MISSING_LOG_CAPACITY to low - increased to 1000");
	}
	if (ncapacity>1000000) {
		syslog(LOG_WARNING,"MISSING_LOG_CAPACITY to high - decreased to 1000000");
	}

	if (ncapacity != mloghashcapacity) {
		if (mloghash!=NULL) {
			free(mloghash);
		}
		for (mloghashsize=1024 ; mloghashsize<(ncapacity*3/2) ; mloghashsize<<=1) {
		}
		mloghash = malloc(sizeof(mlogentry)*mloghashsize);
		memset(mloghash,0,sizeof(mlogentry)*mloghashsize);
		mloghashelements = 0;
		if (mloghashcapacity==0) {
			blocked = 0;
		} else {
			blocked = 1;
		}
		mloghashcapacity = ncapacity;
	} 
}

int missing_log_init(void) {
	mloghash = NULL;
	mloghashprev = NULL;
	mloghashsize = 0;
	mloghashprevsize = 0;
	mloghashelements = 0;
	mloghashprevelements = 0;
	mloghashcapacity = 0;
	blocked = 1;
	missing_log_reload();
	main_reload_register(missing_log_reload);
	return 1;
}
