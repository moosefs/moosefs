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

#ifndef _MASSERT_H_
#define _MASSERT_H_

#include <stdio.h>
#include <syslog.h>
#include <stdlib.h>
#include <errno.h>

#include "strerr.h"

#define massert(e,msg) ((e) ? (void)0 : (fprintf(stderr,"%s:%u - failed assertion '%s' : %s\n",__FILE__,__LINE__,#e,(msg)),syslog(LOG_ERR,"%s:%u - failed assertion '%s' : %s",__FILE__,__LINE__,#e,(msg)),abort()))
#define sassert(e) ((e) ? (void)0 : (fprintf(stderr,"%s:%u - failed assertion '%s'\n",__FILE__,__LINE__,#e),syslog(LOG_ERR,"%s:%u - failed assertion '%s'",__FILE__,__LINE__,#e),abort()))
#define passert(ptr) if (ptr==NULL) { \
		fprintf(stderr,"%s:%u - out of memory: %s is NULL\n",__FILE__,__LINE__,#ptr); \
		syslog(LOG_ERR,"%s:%u - out of memory: %s is NULL",__FILE__,__LINE__,#ptr); \
		abort(); \
	} else if (ptr==((void*)(-1))) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"%s:%u - mmap error on %s, error: %s",__FILE__,__LINE__,#ptr,_mfs_errorstring); \
		fprintf(stderr,"%s:%u - mmap error on %s, error: %s\n",__FILE__,__LINE__,#ptr,_mfs_errorstring); \
		abort(); \
	}
#define eassert(e) if (!(e)) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"%s:%u - failed assertion '%s', error: %s",__FILE__,__LINE__,#e,_mfs_errorstring); \
		fprintf(stderr,"%s:%u - failed assertion '%s', error: %s\n",__FILE__,__LINE__,#e,_mfs_errorstring); \
		abort(); \
	}
#define zassert(e) { \
	int _mfs_assert_ret = (e); \
	if (_mfs_assert_ret!=0) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"%s:%u - unexpected status, '%s' returned: %d (errno: %s)",__FILE__,__LINE__,#e,_mfs_assert_ret,_mfs_errorstring); \
		fprintf(stderr,"%s:%u - unexpected status, '%s' returned: %d (errno: %s)\n",__FILE__,__LINE__,#e,_mfs_assert_ret,_mfs_errorstring); \
		abort(); \
	} \
}

#endif
