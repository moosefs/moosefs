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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdlib.h>
#include "MFSCommunication.h"

#ifndef CRC_POLY
#define CRC_POLY 0xEDB88320
#endif

static uint32_t crc_table[16][256];

static void crc_generate_main_tables(void) {
	uint32_t c,poly,i;
	uint32_t j;

	poly = CRC_POLY;
	for (i=0; i<256; i++) {
		c=i;
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		crc_table[0][i] = c;
	}

	for (i=0; i<256; i++) {
		c = crc_table[0][i];
		for (j=1 ; j<16 ; j++) {
			c = crc_table[0][c&0xff]^(c>>8);
			crc_table[j][i] = c;
		}
	}
}

// based on code bublished by Stephan Brumme on his page: http://create.stephan-brumme.com/crc32/
// 16 byte slicing version by Bulat Ziganshin

#ifdef WORDS_BIGENDIAN
static inline uint32_t swap(uint32_t x) {
#if defined(__GNUC__) || defined(__clang__)
	return __builtin_bswap32(x);
#else
	return (x >> 24) |
	      ((x >>  8) & 0x0000FF00) |
	      ((x <<  8) & 0x00FF0000) |
	       (x << 24);
#endif
}
#endif

// #define CRC_PREFETCH 1

uint32_t mycrc32(uint32_t crc,const void* data,uint32_t leng) {
	const uint32_t *data4;
	const uint8_t *data1;
	uint32_t d0,d1,d2,d3;

#ifdef WORDS_BIGENDIAN
#define CRC_BLOCK { \
	d0 = *data4++ ^ swap(crc); \
	d1 = *data4++; \
	d2 = *data4++; \
	d3 = *data4++; \
	crc =	crc_table[ 0][ d3        & 0xFF] ^ \
		crc_table[ 1][(d3 >>  8) & 0xFF] ^ \
		crc_table[ 2][(d3 >> 16) & 0xFF] ^ \
		crc_table[ 3][(d3 >> 24) & 0xFF] ^ \
		crc_table[ 4][ d2        & 0xFF] ^ \
		crc_table[ 5][(d2 >>  8) & 0xFF] ^ \
		crc_table[ 6][(d2 >> 16) & 0xFF] ^ \
		crc_table[ 7][(d2 >> 24) & 0xFF] ^ \
		crc_table[ 8][ d1        & 0xFF] ^ \
		crc_table[ 9][(d1 >>  8) & 0xFF] ^ \
		crc_table[10][(d1 >> 16) & 0xFF] ^ \
		crc_table[11][(d1 >> 24) & 0xFF] ^ \
		crc_table[12][ d0        & 0xFF] ^ \
		crc_table[13][(d0 >>  8) & 0xFF] ^ \
		crc_table[14][(d0 >> 16) & 0xFF] ^ \
		crc_table[15][(d0 >> 24) & 0xFF];  \
}
#else
#define CRC_BLOCK { \
	d0 = *data4++ ^ crc; \
	d1 = *data4++; \
	d2 = *data4++; \
	d3 = *data4++; \
	crc =	crc_table[ 0][(d3 >> 24) & 0xFF] ^ \
		crc_table[ 1][(d3 >> 16) & 0xFF] ^ \
		crc_table[ 2][(d3 >>  8) & 0xFF] ^ \
		crc_table[ 3][ d3        & 0xFF] ^ \
		crc_table[ 4][(d2 >> 24) & 0xFF] ^ \
		crc_table[ 5][(d2 >> 16) & 0xFF] ^ \
		crc_table[ 6][(d2 >>  8) & 0xFF] ^ \
		crc_table[ 7][ d2        & 0xFF] ^ \
		crc_table[ 8][(d1 >> 24) & 0xFF] ^ \
		crc_table[ 9][(d1 >> 16) & 0xFF] ^ \
		crc_table[10][(d1 >>  8) & 0xFF] ^ \
		crc_table[11][ d1        & 0xFF] ^ \
		crc_table[12][(d0 >> 24) & 0xFF] ^ \
		crc_table[13][(d0 >> 16) & 0xFF] ^ \
		crc_table[14][(d0 >>  8) & 0xFF] ^ \
		crc_table[15][ d0        & 0xFF];  \
}
#endif

	crc = ~crc;
	data1 = (const uint8_t*)data;
	while (((unsigned long)data1&0x3) && leng!=0) {
		crc = (crc >> 8) ^ crc_table[0][(crc & 0xFF) ^ *data1++];
		leng--;
	}
	data4 = (const uint32_t*)data1;
#ifdef __clang__
	while (leng >= 16) {
		CRC_BLOCK
		leng -= 16;
	}
#else
	while (leng >= 64) {
#if defined(CRC_PREFETCH) && defined(__GNUC__)
		__builtin_prefetch(((const uint8_t*)data4)+256);
#endif
		CRC_BLOCK
		CRC_BLOCK
		CRC_BLOCK
		CRC_BLOCK
		leng -= 64;
	}
#endif
	data1 = (const uint8_t*)data4;
	while (leng!=0) {
		crc = (crc >> 8) ^ crc_table[0][(crc & 0xFF) ^ *data1++];
		leng--;
	}
	return ~crc;
}

/* crc_combine */

static uint32_t crc_combine_table[32][4][256];

static void crc_matrix_square(uint32_t sqr[32], uint32_t m[32]) {
	uint32_t i,j,s,v;
	for (i=0; i<32; i++) {
		for (j=0,s=0,v=m[i] ; v && j<32 ; j++, v>>=1) {
			if (v&1) {
				s^=m[j];
			}
		}
		sqr[i] = s;
	}
}

static void crc_generate_combine_tables(void) {
	uint32_t i,j,k,l,sum;
	uint32_t m1[32],m2[32],*mc,*m;
	m1[0]=CRC_POLY;
	j=1;
	for (i=1 ; i<32 ; i++) {
		m1[i]=j;
		j<<=1;
	}
	crc_matrix_square(m2,m1); // 1 bit -> 2 bits
	crc_matrix_square(m1,m2); // 2 bits -> 4 bits

	for (i=0 ; i<32 ; i++) {
		if (i&1) {
			crc_matrix_square(m1,m2);
			mc = m1;
		} else {
			crc_matrix_square(m2,m1);
			mc = m2;
		}
		for (j=0 ; j<4 ; j++) {
			for (k=0 ; k<256 ; k++) {
				sum = 0;
				l=k;
				m=mc+(j*8);
				while (l) {
					if (l&1) {
						sum ^= *m;
					}
					l>>=1;
					m++;
				}
				crc_combine_table[i][j][k]=sum;
			}
		}
	}
}

uint32_t mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2) {
	uint8_t i;

	/* add leng2 zeros to crc1 */
	i=0;
	while (leng2) {
		if (leng2&1) {
			crc1 = crc_combine_table[i][3][(crc1>>24)] \
			     ^ crc_combine_table[i][2][(crc1>>16)&0xFF] \
			     ^ crc_combine_table[i][1][(crc1>>8)&0xFF] \
			     ^ crc_combine_table[i][0][crc1&0xFF];
		}
		i++;
		leng2>>=1;
	};
	/* then combine crc1 and crc2 as output */
	return crc1^crc2;
}

void mycrc32_init(void) {
	crc_generate_main_tables();
	crc_generate_combine_tables();
}
