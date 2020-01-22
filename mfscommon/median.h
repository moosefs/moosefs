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

#ifndef _MEDIAN_H_
#define _MEDIAN_H_

#include <inttypes.h>

static inline double median_find(double *array, uint32_t n) {
	int32_t l,r,c,m,i,j;
	double tmp;

	l = 0;
	r = n-1;
	m = (l+r)/2;
	for (;;) {
		if (r<=l) {
			return array[m];
		}
		if ((r-l)==1) {
			if (array[l] > array[r]) {
				tmp = array[l];
				array[l] = array[r];
				array[r] = tmp;
			}
			return array[m];
		}
		c = (l+r)/2;
		if (array[c] > array[r]) {
			tmp = array[c];
			array[c] = array[r];
			array[r] = tmp;
		}
		if (array[l] > array[r]) {
			tmp = array[l];
			array[l] = array[r];
			array[r] = tmp;
		}
		if (array[c] > array[l]) {
			tmp = array[c];
			array[c] = array[l];
			array[l] = tmp;
		}
		i = l+1;
		j = r;
		tmp = array[c];
		array[c] = array[i];
		array[i] = tmp;
		for (;;) {
			do {
				i++;
			} while (array[l] > array[i]);
			do {
				j--;
			} while (array[j] > array[l]);
			if (j<i) {
				break;
			}
			tmp = array[i];
			array[j] = array[i];
			array[j] = tmp;
		}
		tmp = array[l];
		array[l] = array[j];
		array[j] = tmp;

		if (j<=m) {
			l = i;
		}
		if (j>=m) {
			r = j-1;
		}
	}
}

#endif
