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
#include <inttypes.h>

#include "datapack.h"

#include "mfstest.h"

int main(void) {
	uint64_t buff[2];
	uint8_t *wp;
	const uint8_t *rp;
	uint32_t i;

	mfstest_init();

	wp = (uint8_t*)buff;
	for (i=0 ; i<16 ; i++) {
		wp[i] = ((15-i)*0x10)+i;
	}

	mfstest_start(getbit_uneven);
	rp = (uint8_t*)buff;
	mfstest_assert_uint8_eq(get8bit(&rp),0xF0);
	mfstest_assert_uint16_eq(get16bit(&rp),0xE1D2);
	mfstest_assert_uint32_eq(get32bit(&rp),0xC3B4A596);
	mfstest_assert_uint64_eq(get64bit(&rp),0x8778695A4B3C2D1E);
	mfstest_end();

	mfstest_start(getbit_even);
	rp = (uint8_t*)buff;
	mfstest_assert_uint64_eq(get64bit(&rp),0xF0E1D2C3B4A59687);
	mfstest_assert_uint32_eq(get32bit(&rp),0x78695A4B);
	mfstest_assert_uint16_eq(get16bit(&rp),0x3C2D);
	mfstest_assert_uint8_eq(get8bit(&rp),0x1E);
	mfstest_end();

	mfstest_start(getbit_partials);
	rp = (uint8_t*)buff;
	mfstest_assert_uint64_eq(get56bit(&rp),0xF0E1D2C3B4A596);
	mfstest_assert_uint64_eq(get48bit(&rp),0x8778695A4B3C);
	rp = (uint8_t*)buff;
	mfstest_assert_uint64_eq(get40bit(&rp),0xF0E1D2C3B4);
	mfstest_assert_uint32_eq(get24bit(&rp),0xA59687);
	mfstest_end();



	wp = (uint8_t*)buff;
	for (i=0; i<16 ; i++) {
		wp[i] = 0;
	}

	put8bit(&wp,0xF0);
	put16bit(&wp,0xE1D2);
	put32bit(&wp,0xC3B4A596);
	put64bit(&wp,0x8778695A4B3C2D1E);
	put8bit(&wp,0x0F);

	mfstest_start(putbit_uneven);
	rp = (uint8_t*)buff;
	for (i=0 ; i<16 ; i++) {
		mfstest_assert_uint8_eq(rp[i],((15-i)*0x10)+i);
	}
	mfstest_end();

	wp = (uint8_t*)buff;
	for (i=0; i<16 ; i++) {
		wp[i] = 0;
	}

	put64bit(&wp,0xF0E1D2C3B4A59687);
	put32bit(&wp,0x78695A4B);
	put16bit(&wp,0x3C2D);
	put8bit(&wp,0x1E);
	put8bit(&wp,0x0F);

	mfstest_start(putbit_even);
	rp = (uint8_t*)buff;
	for (i=0 ; i<16 ; i++) {
		mfstest_assert_uint8_eq(rp[i],((15-i)*0x10)+i);
	}
	mfstest_end();

	mfstest_start(putbit_partial);
	wp = (uint8_t*)buff;
	for (i=0; i<16 ; i++) {
		wp[i] = 0;
	}

	put56bit(&wp,0xF0E1D2C3B4A596);
	put48bit(&wp,0x8778695A4B3C);
	put24bit(&wp,0x2D1E0F);

	rp = (uint8_t*)buff;
	for (i=0 ; i<16 ; i++) {
		mfstest_assert_uint8_eq(rp[i],((15-i)*0x10)+i);
	}

	wp = (uint8_t*)buff;
	for (i=0; i<16 ; i++) {
		wp[i] = 0;
	}

	put40bit(&wp,0xF0E1D2C3B4);
	put24bit(&wp,0xA59687);
	put40bit(&wp,0x78695A4B3C);
	put24bit(&wp,0x2D1E0F);

	rp = (uint8_t*)buff;
	for (i=0 ; i<16 ; i++) {
		mfstest_assert_uint8_eq(rp[i],((15-i)*0x10)+i);
	}
	mfstest_end();
	mfstest_return();
}

