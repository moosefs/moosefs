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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "MFSCommunication.h"
#include "clocks.h"
#include "bitops.h"

#include "mfstest.h"

/* original crc32 code */

uint8_t bitcount_ref(uint32_t b) {
	uint8_t ret = 0;
	uint32_t i,mask;
	for (i=0,mask=1 ; i<32 ; i++,mask<<=1) {
		if (b&mask) {
			ret++;
		}
	}
	return ret;
}

uint8_t bitindex_ref(uint32_t b,uint8_t j) {
	uint32_t i,mask;
	for (i=0,mask=1 ; i<32 ; i++,mask<<=1) {
		if (b&mask) {
			if (j==0) {
				return i;
			} else {
				j--;
			}
		}
	}
	return 0xFF;
}

uint8_t bitrank_ref(uint32_t b,uint8_t j) {
	uint8_t ret = 0;
	uint32_t i,mask;
	for (i=0,mask=1 ; i<j ; i++,mask<<=1) {
		if (b&mask) {
			ret++;
		}
	}
	return ret;
}

uint32_t simple_pseudo_random(void) {
	static uint32_t u=1249853491;
	static uint32_t v=3456394786;

	v = 36969*(v & 65535) + (v >> 16);
	u = 18000*(u & 65535) + (u >> 16);

	return (v << 16) + u;
}

int main(void) {
//	uint8_t *speedtestblock;
//	uint8_t rblock[65536],sblock[65536],xblock[65536];
//	uint32_t i,j,s,crc,crc1,crc2;

	uint32_t i,b,bc1,bc2;
	uint8_t c;
	uint32_t *rblock;
	uint8_t *iblock;
	double st,en,corr,mybctime,refbctime;

	rblock = malloc(sizeof(uint32_t)*65536);
	iblock = malloc(sizeof(uint8_t)*65536);

	if (rblock==NULL || iblock==NULL) {
		return 99;
	}

	mfstest_init();

	mfstest_start(crc32);

	printf("bitops - bit count function\n");

	for (i=0 ; i<65536 ; i++) {
		b = simple_pseudo_random();
		mfstest_assert_uint8_eq(bitcount_ref(b),bitcount(b));
	}

	printf("bitops - bit index function\n");

	for (i=0 ; i<65536 ; i++) {
		b = simple_pseudo_random();
		c = bitcount_ref(b);
		c = simple_pseudo_random() % (c+1);
		rblock[i] = b;
		iblock[i] = c;
		mfstest_assert_uint8_eq(bitindex_ref(b,c),bitindex(b,c));
	}

	printf("bitops - bit rank function\n");

	for (i=0 ; i<65536 ; i++) {
		b = simple_pseudo_random();
		c = simple_pseudo_random()&0x1F;
		mfstest_assert_uint8_eq(bitrank_ref(b,c),bitrank(b,c));
	}

	printf("bitops speed\n");

	st = monotonic_seconds();
	corr = monotonic_seconds();
	corr -= st;

	bc1 = 0;
	st = monotonic_seconds();
	for (i=0 ; i<1024*1024*2 ; i++) {
		bc1 += bitcount(rblock[i&0xFFFF]);
	}
	en = monotonic_seconds();
	mybctime = (en-st)-corr;

	bc2 = 0;
	st = monotonic_seconds();
	for (i=0 ; i<1024*1024*2 ; i++) {
		bc2 += bitcount_ref(rblock[i&0xFFFF]);
	}
	en = monotonic_seconds();
	refbctime = (en-st)-corr;

	mfstest_assert_uint32_eq(bc1,bc2);
	printf("2M * bitcount ; mfs bitcount: %.2lfM/s ; simple bitcount: %.2lfM/s ; speedup: %.2lf\n",2.0/mybctime,2.0/refbctime,refbctime / mybctime);

	bc1 = 0;
	st = monotonic_seconds();
	for (i=0 ; i<1024*1024*2 ; i++) {
		bc1 += bitindex(rblock[i&0xFFFF],iblock[i&0xFFFF]);
	}
	en = monotonic_seconds();
	mybctime = (en-st)-corr;

	bc2 = 0;
	st = monotonic_seconds();
	for (i=0 ; i<1024*1024*2 ; i++) {
		bc2 += bitindex_ref(rblock[i&0xFFFF],iblock[i&0xFFFF]);
	}
	en = monotonic_seconds();
	refbctime = (en-st)-corr;

	mfstest_assert_uint32_eq(bc1,bc2);
	printf("2M * bitindex ; mfs bitindex: %.2lfM/s ; simple bitindex: %.2lfM/s ; speedup: %.2lf\n",2.0/mybctime,2.0/refbctime,refbctime / mybctime);

	mfstest_end();
	mfstest_return();

	free(iblock);
	free(rblock);
}
