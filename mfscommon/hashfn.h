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

#ifndef _HASHFN_H_
#define _HASHFN_H_

#include <inttypes.h>
#include "datapack.h"
#include "config.h"

static uint32_t hash_primes_tab[]={0,
	         5U,         7U,        13U,        19U,        31U,        43U,        61U,        73U,
	       103U,       139U,       181U,       229U,       271U,       313U,       349U,       421U,
	       523U,       601U,       811U,      1021U,      1153U,      1279U,      1429U,      1609U,
	      1789U,      1999U,      2239U,      2551U,      2971U,      3301U,      3673U,      4051U,
	      4483U,      4933U,      5443U,      6091U,      6703U,      7459U,      8221U,      9241U,
	     10273U,     11353U,     12541U,     13831U,     15271U,     16831U,     18523U,     20443U,
	     22543U,     24919U,     27481U,     30271U,     33331U,     36781U,     40531U,     44623U,
	     49123U,     54403U,     60091U,     66109U,     72871U,     80209U,     88261U,     97159U,
	    106963U,    117673U,    129499U,    142591U,    156901U,    173023U,    190369U,    209569U,
	    230563U,    253639U,    279121U,    307093U,    337861U,    371929U,    409261U,    450259U,
	    495361U,    544963U,    599479U,    659611U,    725863U,    798649U,    878833U,    966871U,
	   1063849U,   1170583U,   1287751U,   1416631U,   1558309U,   1714159U,   1885603U,   2074201U,
	   2281663U,   2509963U,   2761009U,   3037423U,   3341269U,   3675403U,   4043191U,   4447609U,
	   4892581U,   5381863U,   5920171U,   6512281U,   7163539U,   7880011U,   8668063U,   9534871U,
	  10488631U,  11537683U,  12691909U,  13961359U,  15357571U,  16893391U,  18582871U,  20441251U,
	  22485481U,  24734263U,  27207811U,  29929093U,  32922313U,  36214561U,  39836413U,  43820263U,
	  48202423U,  53022919U,  58325473U,  64158301U,  70574173U,  77631889U,  85395511U,  93935539U,
	 103329253U, 113662183U, 125028583U, 137531941U, 151285231U, 166413943U, 183055489U, 201361051U,
	 221497189U, 243647059U, 268011829U, 294813019U, 324294769U, 356724493U, 392397169U, 431637379U,
	 474801913U, 522282421U, 574510669U, 631961821U, 695158363U, 764674201U, 841142149U, 925256641U,
	1017782533U,1119560881U,1231517461U,1354669819U,1490137813U,1639152091U,1803067309U,1983374539U,
	2181712441U,2399883949U,2639872969U,2903860369U,3194246623U,3513671971U,3865039309U,4251543451U};
static uint32_t hash_primes_tab_size=177;

static inline uint32_t hash_next_size(uint32_t i) {
	int32_t s=0,e=hash_primes_tab_size-1;
	int32_t c;
	while (e-s>1) {
		c = (s+e)/2;
		if (hash_primes_tab[c]<=i) {
			s = c;
		} else {
			e = c;
		}
	}
	return hash_primes_tab[e];
}

/* fast integer hash functions by Thomas Wang */
/* all of them pass the avalanche test */

/* They are not mutch better in standard collision test than stupid "X*prime"
 * functions, but calculation times are similar, so it should be safer to use
 * this functions */

/* 32bit -> 32bit */
static inline uint32_t hash32(uint32_t key) {
	key = ~key + (key << 15); // key = (key << 15) - key - 1;
	key = key ^ (key >> 12);
	key = key + (key << 2);
	key = key ^ (key >> 4);
	key = key * 2057; // key = (key + (key << 3)) + (key << 11);
	key = key ^ (key >> 16);
	return key;
}

/* 32bit -> 32bit - with 32bit multiplication (can be adjusted by other constant values, such as: 0xb55a4f09,0x165667b1,2034824023,2034824021 etc.) */
static inline uint32_t hash32mult(uint32_t key) { 
	key = (key ^ 61) ^ (key >> 16); 
	key = key + (key << 3); 
	key = key ^ (key >> 4); 
	key = key * 0x27d4eb2d; 
	key = key ^ (key >> 15); 
	return key; 
}

/* 64bit -> 32bit */
static inline uint32_t hash6432(uint64_t key) {
	key = (~key) + (key << 18); // key = (key << 18) - key - 1;
	key = key ^ (key >> 31);
	key = key * 21; // key = (key + (key << 2)) + (key << 4);
	key = key ^ (key >> 11);
	key = key + (key << 6);
	key = key ^ (key >> 22);
	return (uint32_t)key;
}

/* 64bit -> 64bit */
static inline uint64_t hash64(uint64_t key) {
	key = (~key) + (key << 21); // key = (key << 21) - key - 1;
	key = key ^ (key >> 24);
	key = (key + (key << 3)) + (key << 8); // key * 265
	key = key ^ (key >> 14);
	key = (key + (key << 2)) + (key << 4); // key * 21
	key = key ^ (key >> 28);
	key = key + (key << 31);
	return key;
}


/* buff to hash value functions */

/* buff -> 32bit ; FNV1a */

#define FNV32_INIT (UINT32_C(0x811c9dc5))

static inline uint32_t fnv32(const uint8_t *buf, uint32_t len, uint32_t hash) {
	const uint8_t *bufend = buf + len;

	while (buf < bufend) {
		hash ^= (uint32_t)buf[0];
		hash *= UINT32_C(0x01000193);
		buf++;
	}

	return hash;
}

/* buff -> 64bit ; FNV1a */

#define FNV64_INIT (UINT64_C(0xcbf29ce484222325))

static inline uint64_t fnv64(const uint8_t *buf, uint32_t len, uint64_t hash) {
	const uint8_t *bufend = buf + len;

	while (buf < bufend) {
		hash ^= (uint64_t)buf[0];
		hash *= UINT64_C(0x100000001b3);
		buf++;
	}

	return hash;
}

/* buff -> 32bit ; Murmur3 */

static inline uint32_t murmur3_32(const uint8_t *buf, uint32_t len, uint32_t hash) {
	static const uint32_t c1 = 0xcc9e2d51;
	static const uint32_t c2 = 0x1b873593;
	static const uint32_t r1 = 15;
	static const uint32_t r2 = 13;
	static const uint32_t m = 5;
	static const uint32_t n = 0xe6546b64;
	uint32_t i;
	uint32_t k;

	for (i = 0 ; i < len / 4 ; i++) {
		k = get32bit(&buf);
		k *= c1;
		k = (k << r1) | (k >> (32 - r1));
		k *= c2;

		hash ^= k;
		hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
	}

	k = 0;
	switch (len & 3) {
		case 3:
			k ^= (buf[2] << 16);
			nobreak;
		case 2:
			k ^= (buf[1] << 8);
			nobreak;
		case 1:
			k ^= buf[0];

			k *= c1;
			k = (k << r1) | (k >> (32 - r1));
			k *= c2;
			hash ^= k;
	}

	hash ^= len;
	hash ^= (hash >> 16);
	hash *= 0x85ebca6b;
	hash ^= (hash >> 13);
	hash *= 0xc2b2ae35;
	hash ^= (hash >> 16);

	return hash;
}

#endif
