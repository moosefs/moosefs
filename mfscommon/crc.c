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
#include "MFSCommunication.h"

/* original crc32 code
uint32_t* crc32_reference_generate(void) {
	uint32_t *res;
	uint32_t crc, poly, i, j;

	res = (uint32_t*)malloc(sizeof(uint32_t)*256);
	poly = CRC_POLY;
	for (i=0 ; i<256 ; i++) {
		crc=i;
		for (j=0 ; j<8 ; j++) {
			if (crc & 1) {
				crc = (crc >> 1) ^ poly;
			} else {
				crc >>= 1;
			}
		}
		res[i] = crc;
//		printf("%08X,",(uint32_t)crc);
//		if ((i&3)==3) {
//			printf("\n");
//		}
	}
	return res;
}

uint32_t crc32_reference(uint32_t crc,uint8_t *block,uint32_t leng) {
	uint8_t c;
	static uint32_t *crc_table = NULL;

	if (crc_table==NULL) {
		crc_table = crc32_reference_generate();
	}

	crc^=0xFFFFFFFF;
	while (leng>0) {
		c = *block++;
		leng--;
		crc = ((crc>>8) & 0x00FFFFFF) ^ crc_table[ (crc^c) & 0xFF ];
	}
	return crc^0xFFFFFFFF;
}
*/

#define FASTCRC 1

#ifdef FASTCRC
#define BYTEREV(w) (((w)>>24)+(((w)>>8)&0xff00)+(((w)&0xff00)<<8)+(((w)&0xff)<<24))
static uint32_t crc_table[8][256];
#else
static uint32_t crc_table[256];
#endif

void crc_generate_main_tables(void) {
	uint32_t c,poly,i;
#ifdef FASTCRC
	uint32_t j;
#endif

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
#ifdef FASTCRC
#ifdef WORDS_BIGENDIAN
		crc_table[0][i] = BYTEREV(c);
#else /* little endian */
		crc_table[0][i] = c;
#endif
#else
		crc_table[i]=c;
#endif
	}

#ifdef FASTCRC
	for (i=0; i<256; i++) {
		c = crc_table[0][i];
		for (j=1 ; j<8 ; j++) {
#ifdef WORDS_BIGENDIAN
			c = crc_table[0][(c>>24)]^(c<<8);
			crc_table[j][i] = c;
#else /* little endian */
			c = crc_table[0][c&0xff]^(c>>8);
			crc_table[j][i] = c;
#endif
		}
	}
#endif
}

uint32_t mycrc32(uint32_t crc,const uint8_t *block,uint32_t leng) {
#ifdef FASTCRC
	const uint32_t *block4;
	uint32_t next;
#endif

#ifdef FASTCRC
#ifdef WORDS_BIGENDIAN
#define CRC_REORDER crc=(BYTEREV(crc))^0xFFFFFFFF
#define CRC_ONE_BYTE crc = crc_table[0][(crc >> 24) ^ *block++] ^ (crc << 8)
#define CRC_EIGHT_BYTES { \
	crc = (*block4++) ^ crc; \
	next = (*block4++); \
	crc = crc_table[0][next&0xFF] ^ crc_table[1][(next>>8)&0xFF] ^ crc_table[2][(next>>16)&0xFF] ^ crc_table[3][next>>24] ^ crc_table[4][crc&0xFF] ^ crc_table[5][(crc>>8)&0xFF] ^ crc_table[6][(crc>>16)&0xFF] ^ crc_table[7][crc>>24]; \
}
#else /* little endian */
#define CRC_REORDER crc^=0xFFFFFFFF
#define CRC_ONE_BYTE crc = crc_table[0][(crc ^ *block++) & 0xFF] ^ (crc >> 8)
#define CRC_EIGHT_BYTES { \
	crc = (*block4++) ^ crc; \
	next = (*block4++); \
	crc = crc_table[0][next>>24] ^ crc_table[1][(next>>16)&0xFF] ^ crc_table[2][(next>>8)&0xFF] ^ crc_table[3][next&0xFF] ^ crc_table[4][crc>>24] ^ crc_table[5][(crc>>16)&0xFF] ^ crc_table[6][(crc>>8)&0xFF] ^ crc_table[7][crc&0xFF]; \
}
#endif
	CRC_REORDER;
#ifdef WIN32
	while (leng && ((uintptr_t)block & 7)) {
#else
	while (leng && ((unsigned long int)block & 7)) {
#endif
		CRC_ONE_BYTE;
		leng--;
	}
	block4 = (const uint32_t*)block;
	while (leng>=64) {
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		CRC_EIGHT_BYTES;
		leng-=64;
	}
	while (leng>=8) {
		CRC_EIGHT_BYTES;
		leng-=8;
	}
	block = (const uint8_t*)block4;
	if (leng) do {
		CRC_ONE_BYTE;
	} while (--leng);
	CRC_REORDER;
	return crc;
#else
#define CRC_ONE_BYTE crc = (crc>>8)^crc_table[(crc^(*block++))&0xff]
	crc^=0xFFFFFFFF;
	while (leng>=8) {
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		leng-=8;
	}
	if (leng>0) do {
		CRC_ONE_BYTE;
	} while (--leng);
	return crc^0xFFFFFFFF;
#endif
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
