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

#ifndef _SLOGGER_H_
#define _SLOGGER_H_

#include <stdio.h>
#include <syslog.h>
#include <stdarg.h>
#include <errno.h>

#include "strerr.h"

#define mfs_syslog(priority,msg) {\
	syslog((priority),"%s",(msg)); \
	fprintf(stderr,"%s\n",(msg)); \
}

#define mfs_arg_syslog(priority,format, ...) {\
	syslog((priority),(format), __VA_ARGS__); \
	fprintf(stderr,format "\n", __VA_ARGS__); \
}

#define mfs_errlog(priority,msg) {\
	const char *_mfs_errstring = strerr(errno); \
	syslog((priority),"%s: %s", (msg) , _mfs_errstring); \
	fprintf(stderr,"%s: %s\n", (msg), _mfs_errstring); \
}

#define mfs_arg_errlog(priority,format, ...) {\
	const char *_mfs_errstring = strerr(errno); \
	syslog((priority),format ": %s", __VA_ARGS__ , _mfs_errstring); \
	fprintf(stderr,format ": %s\n", __VA_ARGS__ , _mfs_errstring); \
}

#define mfs_errlog_silent(priority,msg) syslog((priority),"%s: %s", msg, strerr(errno));
#define mfs_arg_errlog_silent(priority,format, ...) syslog((priority),format ": %s", __VA_ARGS__ , strerr(errno));

#endif
