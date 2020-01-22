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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef WIN32
#include <signal.h>
#endif

#include <stdlib.h>
#include <inttypes.h>
#include <pthread.h>

#include "massert.h"

#ifndef WIN32
int lwt_thread_create(pthread_t *th,const pthread_attr_t *attr,void *(*fn)(void *),void *arg) {
	sigset_t oldset;
	sigset_t newset;
	int res;

	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
#ifdef SIGINFO
	sigaddset(&newset, SIGINFO);
#endif
#ifdef SIGPIPE
	sigaddset(&newset, SIGPIPE);
#endif
#ifdef SIGTSTP
	sigaddset(&newset, SIGTSTP);
#endif
#ifdef SIGTTIN
	sigaddset(&newset, SIGTTIN);
#endif
#ifdef SIGTTOU
	sigaddset(&newset, SIGTTOU);
#endif
#ifdef SIGUSR1
	sigaddset(&newset, SIGUSR1);
#endif
#ifdef SIGUSR2
	sigaddset(&newset, SIGUSR2);
#endif
#ifdef SIGALRM
	sigaddset(&newset, SIGALRM);
#endif
#ifdef SIGVTALRM
	sigaddset(&newset, SIGVTALRM);
#endif
#ifdef SIGPROF
	sigaddset(&newset, SIGPROF);
#endif
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	res = pthread_create(th,attr,fn,arg);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	return res;
}
#endif

int lwt_minthread_create(pthread_t *th,uint8_t detached,void *(*fn)(void *),void *arg) {
	static pthread_attr_t *thattr = NULL;
	static uint8_t thattr_detached;
	if (thattr == NULL) {
		size_t mystacksize;
		thattr = malloc(sizeof(pthread_attr_t));
		passert(thattr);
		zassert(pthread_attr_init(thattr));
#ifdef PTHREAD_STACK_MIN
		mystacksize = PTHREAD_STACK_MIN;
		if (mystacksize < 0x20000) {
			mystacksize = 0x20000;
		}
#else
		mystacksize = 0x20000;
#endif
		zassert(pthread_attr_setstacksize(thattr,mystacksize));
		thattr_detached = detached + 1; // make it different
	}
	if (detached != thattr_detached) {
		if (detached) {
			zassert(pthread_attr_setdetachstate(thattr,PTHREAD_CREATE_DETACHED));
		} else {
			zassert(pthread_attr_setdetachstate(thattr,PTHREAD_CREATE_JOINABLE));
		}
		thattr_detached = detached;
	}
#ifdef WIN32
	return pthread_create(th,thattr,fn,arg);
#else
	return lwt_thread_create(th,thattr,fn,arg);
#endif
}
