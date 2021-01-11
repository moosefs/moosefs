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
	double st,en,secsum;
	uint64_t stusec,enusec,usecsum;
	uint64_t stnsec,ennsec,nsecsum;
	uint64_t wcstusec,wcenusec,wcusecsum;
	int i;

	if (strcmp(monotonic_method(),"time")==0) {
		printf("testing classic 'time(NULL)' clock doesn't make sense\n");
		return 77;
	}

	mfstest_init();

	mfstest_start(monotonic_clocks);

	printf("used method: %s\n",monotonic_method());
	secsum = 0.0;
	usecsum = 0;
	nsecsum = 0;
	wcusecsum = 0;
	for (i=0 ; i<10 ; i++) {
		st = monotonic_seconds();
		stusec = monotonic_useconds();
		stnsec = monotonic_nseconds();
		wcstusec = wallclock_utime();
		portable_usleep(10000);
		en = monotonic_seconds();
		enusec = monotonic_useconds();
		ennsec = monotonic_nseconds();
		wcenusec = wallclock_utime();
		secsum += en-st;
		usecsum += enusec-stusec;
		nsecsum += ennsec-stnsec;
		wcusecsum += wcenusec-wcstusec;
	}
	printf("second: %.6lf ; %"PRIu64" ; %"PRIu64" ; %"PRIu64"\n",secsum,usecsum,nsecsum,wcusecsum);

	mfstest_assert_double_ge(secsum,0.1);
	mfstest_assert_uint64_ge(usecsum,100000);
	mfstest_assert_uint64_ge(nsecsum,100000000);
	mfstest_assert_uint64_ge(wcusecsum,100000);
	mfstest_assert_double_lt(secsum,0.25);
	mfstest_assert_uint64_lt(usecsum,250000);
	mfstest_assert_uint64_lt(nsecsum,250000000);
	mfstest_assert_uint64_lt(wcusecsum,250000);

	mfstest_end();
	mfstest_return();
}
