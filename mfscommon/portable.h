/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _PORTABLE_H_
#define _PORTABLE_H_

#include <sys/select.h>
#include <time.h>
#include <inttypes.h>

#ifdef HAVE_NANOSLEEP

static inline void portable_usleep (uint64_t usec) {
	struct timespec req,rem;
	int s;
	req.tv_sec = usec / 1000000U;
	req.tv_nsec = (usec % 1000000U) * 1000U;
	do {
		s = nanosleep(&req,&rem);
		if (s<0) {
			req = rem;
		}
	} while (s<0);
}

#else

static inline void portable_usleep(uint64_t usec) {
	struct timeval tv;
	tv.tv_sec = usec/1000000;
	tv.tv_usec = usec%1000000;
	select(0, NULL, NULL, NULL, &tv);
}

#endif

#endif
