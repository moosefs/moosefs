/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifndef _DATAPACK_H_
#define _DATAPACK_H_

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <inttypes.h>
#include <string.h>

/* MFS data pack */

#define FAST_DATAPACK 1

#ifndef BSWAP64
#  define BSWAP64(x) ((((x)<<56)&0xFF00000000000000) | (((x)<<40)&0xFF000000000000) | (((x)<<24)&0xFF0000000000) | (((x)<<8)&0xFF00000000) | (((x)>>8)&0xFF000000) | (((x)>>24)&0xFF0000) | (((x)>>40)&0xFF00) | (((x)>>56)&0xFF))
#endif
#ifndef BSWAP32
#  define BSWAP32(x) ((((x)<<24)&0xFF000000) | (((x)<<8)&0xFF0000) | (((x)>>8)&0xFF00) | (((x)>>24)&0xFF))
#endif
#ifndef BSWAP16
#  define BSWAP16(x) ((((x)<<8)&0xFF00) | (((x)>>8)&0xFF))
#endif

static inline void put64bit(uint8_t **ptr,uint64_t val) {
#ifdef FAST_DATAPACK
#  ifndef WORDS_BIGENDIAN
	val = BSWAP64(val);
#  endif
	memcpy(*ptr,&val,8);
#else
	(*ptr)[0]=((val)>>56)&0xFF;
	(*ptr)[1]=((val)>>48)&0xFF;
	(*ptr)[2]=((val)>>40)&0xFF;
	(*ptr)[3]=((val)>>32)&0xFF;
	(*ptr)[4]=((val)>>24)&0xFF;
	(*ptr)[5]=((val)>>16)&0xFF;
	(*ptr)[6]=((val)>>8)&0xFF;
	(*ptr)[7]=(val)&0xFF;
#endif
	(*ptr)+=8;
}

static inline void put32bit(uint8_t **ptr,uint32_t val) {
#ifdef FAST_DATAPACK
#  ifndef WORDS_BIGENDIAN
	val = BSWAP32(val);
#  endif
	memcpy(*ptr,&val,4);
#else
	(*ptr)[0]=((val)>>24)&0xFF;
	(*ptr)[1]=((val)>>16)&0xFF;
	(*ptr)[2]=((val)>>8)&0xFF;
	(*ptr)[3]=(val)&0xFF;
#endif
	(*ptr)+=4;
}

static inline void put16bit(uint8_t **ptr,uint16_t val) {
#ifdef FAST_DATAPACK
#  ifndef WORDS_BIGENDIAN
	val = BSWAP16(val);
#  endif
	memcpy(*ptr,&val,2);
#else
	(*ptr)[0]=((val)>>8)&0xFF;
	(*ptr)[1]=(val)&0xFF;
#endif
	(*ptr)+=2;
}

static inline void put8bit(uint8_t **ptr,uint8_t val) {
	(*ptr)[0]=(val)&0xFF;
	(*ptr)++;
}

static inline uint64_t get64bit(const uint8_t **ptr) {
	uint64_t t64;
#ifdef FAST_DATAPACK
	memcpy(&t64,*ptr,8);
	(*ptr)+=8;
#  ifdef WORDS_BIGENDIAN
	return t64;
#  else
	return BSWAP64(t64);
#  endif
#else
	t64=((*ptr)[3]+256U*((*ptr)[2]+256U*((*ptr)[1]+256U*(*ptr)[0])));
	t64<<=32;
	t64|=(uint32_t)(((*ptr)[7]+256U*((*ptr)[6]+256U*((*ptr)[5]+256U*(*ptr)[4]))));
	(*ptr)+=8;
	return t64;
#endif
}

static inline uint32_t get32bit(const uint8_t **ptr) {
	uint32_t t32;
#ifdef FAST_DATAPACK
	memcpy(&t32,*ptr,4);
	(*ptr)+=4;
#  ifdef WORDS_BIGENDIAN
	return t32;
#  else
	return BSWAP32(t32);
#  endif
#else
	t32=((*ptr)[3]+256U*((*ptr)[2]+256U*((*ptr)[1]+256U*(*ptr)[0])));
	(*ptr)+=4;
	return t32;
#endif
}

static inline uint16_t get16bit(const uint8_t **ptr) {
	uint16_t t16;
#ifdef FAST_DATAPACK
	memcpy(&t16,*ptr,2);
	(*ptr)+=2;
#  ifdef WORDS_BIGENDIAN
	return t16;
#  else
	return BSWAP16(t16);
#  endif
#else
	t16=(*ptr)[1]+256U*(*ptr)[0];
	(*ptr)+=2;
	return t16;
#endif
}

static inline uint8_t get8bit(const uint8_t **ptr) {
	uint8_t t8;
	t8=(*ptr)[0];
	(*ptr)++;
	return t8;
}

#endif
