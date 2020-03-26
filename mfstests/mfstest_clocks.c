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

#include <sys/types.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include "clocks.h"
#include "portable.h"

#include "mfstest.h"

uint64_t wallclock_utime(void) {
	struct timeval tv;
	uint64_t usec;

	gettimeofday(&tv,NULL);
	usec = tv.tv_sec;
	usec *= 1000000;
	usec += tv.tv_usec;
	return usec;
}

int main(void) {
	double st,en;
	uint64_t stusec,enusec;
	uint64_t stnsec,ennsec;
	uint64_t wcstusec,wcenusec;

	if (strcmp(monotonic_method(),"time")==0) {
		printf("testing classic 'time(NULL)' clock doesn't make sense\n");
		return 77;
	}

	mfstest_init();

	mfstest_start(monotonic_clocks);

	printf("used method: %s\n",monotonic_method());
	st = monotonic_seconds();
	stusec = monotonic_useconds();
	stnsec = monotonic_nseconds();
	wcstusec = wallclock_utime();
	portable_usleep(10000);
	en = monotonic_seconds();
	enusec = monotonic_useconds();
	ennsec = monotonic_nseconds();
	wcenusec = wallclock_utime();
	en -= st;
	enusec -= stusec;
	ennsec -= stnsec;
	wcenusec -= wcstusec;
	printf("second: %.6lf ; %"PRIu64" ; %"PRIu64" ; %"PRIu64"\n",en,enusec,ennsec,wcenusec);

	mfstest_assert_double_ge(en,0.01);
	mfstest_assert_uint64_ge(enusec,10000);
	mfstest_assert_uint64_ge(ennsec,10000000);
	mfstest_assert_uint64_ge(wcenusec,10000);
	mfstest_assert_double_lt(en,0.02);
	mfstest_assert_uint64_lt(enusec,20000);
	mfstest_assert_uint64_lt(ennsec,20000000);
	mfstest_assert_uint64_lt(wcenusec,20000);

	mfstest_end();
	mfstest_return();
}
