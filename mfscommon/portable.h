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
#else
#include <sys/select.h>
#include <unistd.h>
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
	Sleep((usec+999)/1000);
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

#ifdef WIN32

/* emulate poll via select */

struct pollfd {
	int fd;
	short events;
	short revents;
};

#define POLLIN          0x0001          /* any readable data available */
#define POLLPRI         0x0002          /* OOB/Urgent readable data */
#define POLLOUT         0x0004          /* file descriptor is writeable */
#define POLLERR         0x0008          /* some poll error occurred */
#define POLLHUP         0x0010          /* file descriptor was "hung up" */
#define POLLNVAL        0x0020          /* requested events "invalid" */
#define POLLRDNORM      0x0040          /* non-OOB/URG data available */
#define POLLWRNORM      POLLOUT         /* no write type differentiation */
#define POLLRDBAND      0x0080          /* OOB/Urgent readable data */
#define POLLWRBAND      0x0100          /* OOB/Urgent data can be written */

static inline int poll(struct pollfd *fdarray,unsigned int nfds,int to) {
	unsigned int i,maxfd;
	struct timeval tv;
	int r;
	fd_set fdin,fdout,fderr;
	FD_ZERO(&fdin);
	FD_ZERO(&fdout);
	FD_ZERO(&fderr);
	maxfd = 0;
	for (i=0 ; i<nfds ; i++) {
		if (fdarray[i].events & (POLLIN|POLLPRI|POLLRDNORM|POLLRDBAND)) {
			FD_SET(fdarray[i].fd,&fdin);
			if (fdarray[i].fd > maxfd) {
				maxfd = fdarray[i].fd;
			}
		}
		if (fdarray[i].events & (POLLOUT|POLLWRNORM|POLLWRBAND)) {
			FD_SET(fdarray[i].fd,&fdout);
			if (fdarray[i].fd > maxfd) {
				maxfd = fdarray[i].fd;
			}
		}
		fdarray[i].revents = 0;
	}
	tv.tv_sec = to / 1000;
	tv.tv_usec = (to % 1000) * 1000;
	r = select(maxfd+1,&fdin,&fdout,&fderr,&tv);
	if (r == SOCKET_ERROR) {
		fprintf(stderr,"select error: %u\n",WSAGetLastError());
		return -1;
	}
	if (r == 0) {
		return 0;
	}
	r = 0;
	for (i=0 ; i<nfds ; i++) {
		if (FD_ISSET(fdarray[i].fd,&fdin)) {
			fdarray[i].revents |= POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
		}
		if (FD_ISSET(fdarray[i].fd,&fdout)) {
			fdarray[i].revents |= POLLOUT | POLLWRNORM | POLLWRBAND;
		}
		if (FD_ISSET(fdarray[i].fd,&fderr)) {
			fdarray[i].revents |= POLLERR;
		}
		if (fdarray[i].revents != 0) {
			r++;
		}
	}
	return r;
}

/* ignore syslog */

#define LOG_EMERG 7
#define LOG_ALERT 6
#define LOG_CRIT 5
#define LOG_ERR 4
#define LOG_WARNING 3
#define LOG_NOTICE 2
#define LOG_INFO 1
#define LOG_DEBUG 0

#include <stdarg.h>
static inline void syslog(uint8_t level,const char *format,...) {
	va_list args;

	va_start(args, format);
	vfprintf(stderr,format, args);
	va_end(args);
	fprintf(stderr,"\n");
}
//static inline void syslog(int type,const char *msg,...) {
//	(void)type;
//	(void)msg;
//}

/* emulate pipe via sockets */

static inline int pipe(int handles[2]) {
	int s, tmp_sock;
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
	handles[1] = tmp_sock;

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
	handles[0] = tmp_sock;
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
	i = recv(sock,(char*)buff,size,0);
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
	i = send(sock,(const char*)buff,size,0);
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
