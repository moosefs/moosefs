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
#else
#  if defined(__APPLE__)
#    define HAVE_MACH_MACH_H 1
#    define HAVE_MACH_MACH_TIME_H 1
#    define HAVE_MACH_ABSOLUTE_TIME 1
#    define HAVE_MACH_TIMEBASE_INFO 1
#  endif
#  if defined(__linux__) || defined(__FreeBSD__) || defined(WIN32)
#    define HAVE_CLOCK_GETTIME 1
#  endif
#  if defined(__posix__)
#    define HAVE_SYS_TIME_H 1
#    define HAVE_GETTIMEOFDAY 1
#  endif
#endif

#include <time.h>

#if defined(HAVE_SYS_TIME_H)
#  include <sys/time.h>
#endif
#if defined(HAVE_MACH_MACH_H) && defined(HAVE_MACH_MACH_TIME_H)
#  include <mach/mach.h>
#  include <mach/mach_time.h>
#endif

#include <inttypes.h>

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
double monotonic_seconds() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC,&ts);
	return ts.tv_sec + (ts.tv_nsec * 0.000000001);
}

uint64_t monotonic_nseconds() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC,&ts);
	return (uint64_t)(ts.tv_sec)*UINT64_C(1000000000)+(uint64_t)(ts.tv_nsec);
}

uint64_t monotonic_useconds() {
	return monotonic_nseconds()/1000;
}

const char* monotonic_method() {
	return "clock_gettime";
}

uint32_t monotonic_speed(void) {
	uint32_t i;
	uint64_t st,en;
	i = 0;
	st = monotonic_nseconds() + 10000000;
	do {
		en = monotonic_nseconds();
		i++;
	} while (en < st);
	return i;
}

#elif defined(HAVE_MACH_ABSOLUTE_TIME) && defined(HAVE_MACH_TIMEBASE_INFO)
double monotonic_seconds() {
	uint64_t c;
	static double coef = 0.0;

	c = mach_absolute_time();
	if (coef==0.0) {
		mach_timebase_info_data_t sti;
		mach_timebase_info(&sti);
		coef = (double)(sti.numer);
		coef /= (double)(sti.denom);
		coef /= 1000000000.0;
	}
	return c * coef;
}

uint64_t monotonic_nseconds() {
	uint64_t c;
	static uint8_t i = 0;
	static mach_timebase_info_data_t sti;

	c = mach_absolute_time();
	if (i==0) {
		mach_timebase_info(&sti);
		i = 1;
	}
	return c * sti.numer / sti.denom;
}

uint64_t monotonic_useconds() {
	return monotonic_nseconds()/1000;
}

const char* monotonic_method() {
	(void)monotonic_seconds(); // init static variables
	(void)monotonic_nseconds(); // init static variables
	return "mach_absolute_time";
}

uint32_t monotonic_speed(void) {
	uint32_t i;
	uint64_t st,en;
	i = 0;
	st = monotonic_nseconds() + 10000000;
	do {
		en = monotonic_nseconds();
		i++;
	} while (en < st);
	return i;
}
#elif defined(HAVE_GETTIMEOFDAY)
double monotonic_seconds() {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return tv.tv_sec + (tv.tv_usec * 0.000001);
}

uint64_t monotonic_useconds() {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return (uint64_t)(tv.tv_sec)*UINT64_C(1000000)+(uint64_t)(tv.tv_usec);
}

uint64_t monotonic_nseconds() {
	return monotonic_useconds()*1000;
}

const char* monotonic_method() {
	return "gettimeofday";
}

uint32_t monotonic_speed(void) {
	uint32_t i;
	uint64_t st,en;
	i = 0;
	st = monotonic_useconds() + 10000;
	do {
		en = monotonic_useconds();
		i++;
	} while (en < st);
	return i;
}
#else
#error "Can't find valid time function"
#endif

#if 0
#include <unistd.h>
#include <stdio.h>
int main(void) {
	double st,en;
	uint64_t stusec,enusec;
	uint64_t stnsec,ennsec;

	printf("used method: %s\n",monotonic_method());
	st = monotonic_seconds();
	stusec = monotonic_useconds();
	stnsec = monotonic_nseconds();
	sleep(1);
	en = monotonic_seconds();
	enusec = monotonic_useconds();
	ennsec = monotonic_nseconds();
	printf("%.6lf ; %"PRIu64" ; %"PRIu64"\n",en-st,enusec-stusec,ennsec-stnsec);
}
#endif
