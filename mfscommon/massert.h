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

#define massert(e,msg) ((e) ? (void)0 : (fprintf(stderr,"failed assertion '%s' : %s\n",#e,(msg)),syslog(LOG_ERR,"failed assertion '%s' : %s",#e,(msg)),abort()))
// #define passert(ptr) ((ptr!=NULL) ? (ptr!=(void*)(-1)) ? (void)0 :  : (fprintf(stderr,"out of memory: %s is NULL\n",#ptr),syslog(LOG_ERR,"out of memory: %s is NULL",#ptr),abort()))
#define sassert(e) ((e) ? (void)0 : (fprintf(stderr,"failed assertion '%s'\n",#e),syslog(LOG_ERR,"failed assertion '%s'",#e),abort()))
#define passert(ptr) if (ptr==NULL) { \
		fprintf(stderr,"out of memory: %s is NULL\n",#ptr); \
		syslog(LOG_ERR,"out of memory: %s is NULL",#ptr); \
		abort(); \
	} else if (ptr==((void*)(-1))) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"mmap error on %s, error: %s",#ptr,_mfs_errorstring); \
		fprintf(stderr,"mmap error on %s, error: %s\n",#ptr,_mfs_errorstring); \
		abort(); \
	}
#define eassert(e) if (!(e)) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"failed assertion '%s', error: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"failed assertion '%s', error: %s\n",#e,_mfs_errorstring); \
		abort(); \
	}
#define zassert(e) if ((e)!=0) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"unexpected status, '%s' returned: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"unexpected status, '%s' returned: %s\n",#e,_mfs_errorstring); \
		abort(); \
	}

#endif
