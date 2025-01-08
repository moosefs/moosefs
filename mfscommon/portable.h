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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _PORTABLE_H_
#define _PORTABLE_H_

#include <time.h>
#include <inttypes.h>
#ifdef WIN32
#define FD_SETSIZE 8192
#include <winsock2.h>
#include <windows.h>
#include <errno.h>
#include <stdio.h>
#include <sys/timeb.h>
#define strdup _strdup
#else
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef WIN32
typedef SSIZE_T ssize_t;
#endif

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

#ifdef WIN32

static inline void portable_usleep(uint64_t usec) {
	Sleep((DWORD)((usec+999)/1000));
}

static inline int gettimeofday(struct timeval* t, void* timezone) {
	struct _timeb timebuffer;
	_ftime(&timebuffer);
	t->tv_sec = (long)timebuffer.time;
	t->tv_usec = 1000 * timebuffer.millitm;
	return 0;
}

#else


static inline void portable_usleep(uint64_t usec) {
	struct timeval tv;
	tv.tv_sec = usec/1000000;
	tv.tv_usec = usec%1000000;
	select(0, NULL, NULL, NULL, &tv);
}

#endif /* WIN32 */

#endif /* HAVE_NANOSLEEP */

static inline void portable_sleep(uint64_t sec) {
	portable_usleep(sec*1000000);
}

#ifdef WIN32

static inline int poll(struct pollfd* fdarray, unsigned int nfds, int to) {
	return WSAPoll(fdarray, nfds, to);
}

/* emulate pipe via sockets */

static inline int pipe(int handles[2]) {
	SOCKET s, tmp_sock;
	struct sockaddr_in serv_addr;
	int len = sizeof(serv_addr);

	handles[0] = handles[1] = -1;

	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s<0 || s==INVALID_SOCKET) {
		printf("socket error: %u\n",GetLastError());
		return -1;
	}

	memset((void *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(0);
	serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (bind(s, (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR) {
		printf("bind error: %u\n",GetLastError());
		closesocket(s);
		return -1;
	}
	if (listen(s, 1) == SOCKET_ERROR) {
		printf("listen error: %u\n",GetLastError());
		closesocket(s);
		return -1;
	}
	if (getsockname(s, (SOCKADDR *) &serv_addr, &len) == SOCKET_ERROR) {
		printf("getsockname error: %u\n",GetLastError());
		closesocket(s);
		return -1;
	}

	tmp_sock = socket(AF_INET, SOCK_STREAM, 0);
	if (tmp_sock<0 || tmp_sock==INVALID_SOCKET) {
		printf("socket error: %u\n",GetLastError());
		closesocket(s);
		return -1;
	}
	handles[1] = (int)tmp_sock;

	if (connect(handles[1], (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR) {
		printf("connect error: %u\n",GetLastError());
		closesocket(s);
		return -1;
	}
	if ((tmp_sock = accept(s, (SOCKADDR *) &serv_addr, &len)) == INVALID_SOCKET) {
		printf("accept error: %u\n",GetLastError());
		closesocket(handles[1]);
		handles[1] = -1;
		closesocket(s);
		return -1;
	}
	handles[0] = (int)tmp_sock;
	closesocket(s);
	return 0;
}

#endif

static inline int close_pipe(int handles[2]) {
	int res=0;
#ifdef WIN32
	if (closesocket(handles[0])<0) {
		res = -1;
	}
	if (closesocket(handles[1])<0) {
		res = -1;
	}
#else
	if (close(handles[0])<0) {
		res = -1;
	}
	if (close(handles[1])<0) {
		res = -1;
	}
#endif
	return res;
}

#ifdef WIN32

/* define structure iovec */

struct iovec {
	char *iov_base;
	size_t iov_len;
};

#endif

#ifdef WIN32
static inline ssize_t universal_read(int sock,void *buff,size_t size) {
	ssize_t i;
	i = recv(sock,(char*)buff,(int)size,0);
	if (i<0) {
		switch (WSAGetLastError()) {
			case WSAEWOULDBLOCK:
				errno = EAGAIN;
				break;
			case WSAECONNRESET:
				i=0;
				break;
		}
	}
	return i;
}

static inline ssize_t universal_write(int sock,const void *buff,size_t size) {
	ssize_t i;
	i = send(sock,(const char*)buff,(int)size,0);
	if (i<0) {
		switch (WSAGetLastError()) {
			case WSAEWOULDBLOCK:
				errno = EAGAIN;
				break;
			case WSAECONNRESET:
				i=0;
				break;
		}
	}
	return i;
}
#else
#define universal_read(a,b,c) read(a,b,c)
#define universal_write(a,b,c) write(a,b,c)
#endif

#endif /* _PORTABLE_H_ */
