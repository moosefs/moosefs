/*
 * Copyright (C) 2019 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifdef WIN32
# include "portable.h" // poll wrapper
//#include <winsock2.h>
//#include <windows.h>
# include <Ws2def.h>
# include <Ws2tcpip.h>
# include <mstcpip.h>
#else
# include <sys/socket.h>
# include <sys/un.h>
# ifdef HAVE_POLL_H
#  include <poll.h>
# else
#  include <sys/poll.h>
# endif
# include <netinet/in.h>
# include <netinet/tcp.h>
# include <arpa/inet.h>
# include <netdb.h>
#endif
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include "sockets.h"
#include "clocks.h"

/* Acid's simple socket library - ver 5.0 */

/* ---------------SOCK ADDR--------------- */

#define RRDNS_MAXADDR 256

typedef struct sockets_rrdns {
	uint32_t iptab[RRDNS_MAXADDR];
	uint16_t porttab[RRDNS_MAXADDR];
	uint16_t datacnt,datapos;
} sockets_rrdns;

static inline int sockaddrnumfill(struct sockaddr_in *sa,uint32_t ip,uint16_t port) {
	memset(sa,0,sizeof(struct sockaddr_in));
	sa->sin_family = AF_INET;
	sa->sin_port = htons(port);
	sa->sin_addr.s_addr = htonl(ip);
#ifdef HAVE_SOCKADDR_SIN_LEN
	sa->sin_len = sizeof(struct sockaddr_in);
#endif
	return 0;
}


static inline int sockaddrfill(struct sockaddr_in *sa,const char *hostname,const char *service,int family,int socktype,int passive) {
	struct addrinfo hints, *res, *reshead;
	uint32_t n,r;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = family;
	hints.ai_socktype = socktype;
	if (passive) {
		hints.ai_flags = AI_PASSIVE;
	}
	if (hostname && hostname[0]=='*') {
		hostname=NULL;
	}
	if (service && service[0]=='*') {
		service=NULL;
	}
	if (getaddrinfo(hostname,service,&hints,&reshead)) {
		return -1;
	}

	n = 0;
	for (res = reshead ; res ; res=res->ai_next) {
		if (res->ai_family==family && res->ai_socktype==socktype && res->ai_addrlen==sizeof(struct sockaddr_in)) {
			n++;
		}
	}

	if (n>0) {
#ifdef WIN32
		r = rand()%n;
#else
		r = random()%n;
#endif
	} else {
		r = 0;
	}

	for (res = reshead ; res ; res=res->ai_next) {
		if (res->ai_family==family && res->ai_socktype==socktype && res->ai_addrlen==sizeof(struct sockaddr_in)) {
			if (r==0) {
				*sa = *((struct sockaddr_in*)(res->ai_addr));
				freeaddrinfo(reshead);
				return 0;
			} else {
				r--;
			}
		}
	}
	freeaddrinfo(reshead);
	return -1;
}


static inline int sockresolve(const char *hostname,const char *service,uint32_t *ip,uint16_t *port,int family,int socktype,int passive) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa,hostname,service,family,socktype,passive)<0) {
		return -1;
	}
	if (ip!=(void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port!=(void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

#ifndef WIN32
static inline int sockaddrpathfill(struct sockaddr_un *sa,const char *path) {
	size_t pl;
	pl = strlen(path);
	if (pl >= sizeof(sa->sun_path)) { // overflow
		return -1;
	}
	memset(sa,0,sizeof(struct sockaddr_un));
	sa->sun_family = AF_LOCAL;
	memcpy(sa->sun_path,path,pl);
	sa->sun_path[pl]='\0';
	return 0;
}
#endif

/* ---------- SOCKET UNIVERSAL ----------- */

static inline int descnonblock(int sock) {
#ifdef WIN32
	u_long yes = 1;
	return ioctlsocket(sock, FIONBIO, &yes);
#else
#ifdef O_NONBLOCK
	int flags = fcntl(sock, F_GETFL, 0);
	if (flags == -1) {
		return -1;
	}
	return fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#else
	int yes = 1;
	return ioctl(sock, FIONBIO, &yes);
#endif
#endif
}

static inline int sockgetstatus(int sock) {
	socklen_t arglen = sizeof(int);
	int rc = 0;
	if (getsockopt(sock,SOL_SOCKET,SO_ERROR,(void *)&rc,&arglen) < 0) {
		rc=errno;
	}
	errno=rc;
	return rc;
}

/* ----------- STRAM UNIVERSAL ----------- */

static inline int32_t streamtoread(int sock,void *buff,uint32_t leng,uint32_t msecto) {
	uint32_t rcvd=0;
	int i;
	struct pollfd pfd;
	double s,c;
	uint32_t msecpassed;

	s = 0.0;
	pfd.fd = sock;
	pfd.events = POLLIN;
	pfd.revents = 0;
	while (1) {
#ifdef WIN32
//		printf("recv data from %u (%u) ... ",sock,leng-rcvd);fflush(stdout);
		i = recv(sock,((uint8_t*)buff)+rcvd,leng-rcvd,0);
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
//		printf("got status %d %d %s %d\n",i,errno,strerr(errno),WSAGetLastError());fflush(stdout);
#else
		i = read(sock,((uint8_t*)buff)+rcvd,leng-rcvd);
#endif
		if (i==0) {
#ifdef ECONNRESET
			errno = ECONNRESET;
#endif
			return rcvd;
		}
		if (i>0) {
			rcvd += i;
		} else if (ERRNO_ERROR) {
			return -1;
		}
		if (pfd.revents & POLLHUP) {
			return rcvd;
		}
		if (rcvd>=leng) {
			break;
		}
		if (s==0.0) {
			s = monotonic_seconds();
			msecpassed = 0;
		} else {
			c = monotonic_seconds();
			msecpassed = (c-s)*1000.0;
			if (msecpassed>=msecto) {
				errno = ETIMEDOUT;
				return -1;
			}
		}
		pfd.revents = 0;
		if (poll(&pfd,1,msecto-msecpassed)<0) {
			if (errno!=EINTR) {
				return -1;
			} else {
				continue;
			}
		}
		if (pfd.revents & POLLERR) {
			return -1;
		}
		if ((pfd.revents & POLLIN)==0) {
			errno = ETIMEDOUT;
			return -1;
		}
	}
	return rcvd;
}

static inline int32_t streamtowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto) {
	uint32_t sent=0;
	int32_t i;
	struct pollfd pfd;
	double s,c;
	uint32_t msecpassed;

	s = 0.0;
	pfd.fd = sock;
	pfd.events = POLLOUT;
	pfd.revents = 0;
	while (1) {
#ifdef WIN32
//		printf("send data to %u (%u) ... ",sock,leng-sent);fflush(stdout);
		i = send(sock,((uint8_t*)buff)+sent,leng-sent,0);
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
//		printf("got status %d %d %s %d\n",i,errno,strerr(errno),WSAGetLastError());fflush(stdout);
#else
		i = write(sock,((uint8_t*)buff)+sent,leng-sent);
#endif
		if (i==0) {
			return 0;
		}
		if (i>0) {
			sent += i;
		} else if (ERRNO_ERROR) {
			return -1;
		}
		if (sent>=leng) {
			break;
		}
		if (s==0.0) {
			s = monotonic_seconds();
			msecpassed = 0;
		} else {
			c = monotonic_seconds();
			msecpassed = (c-s)*1000.0;
			if (msecpassed>=msecto) {
				errno = ETIMEDOUT;
				return -1;
			}
		}
		pfd.revents = 0;
		if (poll(&pfd,1,msecto-msecpassed)<0) {
			if (errno!=EINTR) {
				return -1;
			} else {
				continue;
			}
		}
		if (pfd.revents & (POLLHUP|POLLERR)) {
			return -1;
		}
		if ((pfd.revents & POLLOUT)==0) {
			errno = ETIMEDOUT;
			return -1;
		}
	}
	return sent;
}

static inline int32_t streamtoforward(int srcsock,int dstsock,void *buff,uint32_t leng,uint32_t rcvd,uint32_t sent,uint32_t msecto) {
	int32_t i;
	struct pollfd pfd[2];
	double s,c;
	uint32_t msecpassed;
	s = 0.0;
	pfd[0].fd = srcsock;
	pfd[0].events = POLLIN;
	pfd[0].revents = 0;
	pfd[1].fd = dstsock;
	pfd[1].events = POLLOUT;
	pfd[1].revents = 0;
	while (1) {
		if (rcvd<leng) {
#ifdef WIN32
			i = recv(srcsock,((uint8_t*)buff)+rcvd,leng-rcvd,0);
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
#else
			i = read(srcsock,((uint8_t*)buff)+rcvd,leng-rcvd);
#endif
			if (i==0) {
				leng = rcvd;
			}
			if (i>0) {
				rcvd += i;
			} else if (ERRNO_ERROR) {
				return -1;
			}
		}
		if (pfd[0].revents & POLLHUP) {
			leng = rcvd;
		}
		if (rcvd>sent) {
#ifdef WIN32
			i = send(dstsock,((uint8_t*)buff)+sent,rcvd-sent,0);
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
#else
			i = write(dstsock,((uint8_t*)buff)+sent,rcvd-sent);
#endif
			if (i>0) {
				sent += i;
			} else if (ERRNO_ERROR) {
				return -1;
			}
		}
		if (rcvd>=leng && sent>=leng) {
			break;
		}
		if (s==0.0) {
			s = monotonic_seconds();
			msecpassed = 0;
		} else {
			c = monotonic_seconds();
			msecpassed = (c-s)*1000.0;
			if (msecpassed>=msecto) {
				errno = ETIMEDOUT;
				return -1;
			}
		}
		pfd[0].revents = 0;
		pfd[1].revents = 0;
		if (rcvd==leng) { // only wait for write
			if (poll(pfd+1,1,msecto-msecpassed)<0) {
				if (errno!=EINTR) {
					return -1;
				} else {
					continue;
				}
			}
			if (pfd[1].revents & (POLLERR|POLLHUP)) {
				return -1;
			}
			pfd[0].revents = 0;
		} else if (rcvd==sent) { // only wait for read
			if (poll(pfd,1,msecto-msecpassed)<0) {
				if (errno!=EINTR) {
					return -1;
				} else {
					continue;
				}
			}
			if (pfd[0].revents & POLLERR) {
				return -1;
			}
			pfd[1].revents = 0;
		} else {
			if (poll(pfd,2,msecto-msecpassed)<0) {
				if (errno!=EINTR) {
					return -1;
				} else {
					continue;
				}
			}
			if ((pfd[0].revents & POLLERR) || (pfd[1].revents & (POLLERR|POLLHUP))) {
				return -1;
			}
		}
		if ((pfd[0].revents & (POLLIN|POLLHUP))==0 && (pfd[1].revents & POLLOUT)==0) {
			errno = ETIMEDOUT;
			return -1;
		}
	}
	return leng;
}

static inline int streamtoaccept(int lsock,uint32_t msecto) {
	int i;
	double s,c;
	uint32_t msecpassed;
	struct pollfd pfd;
	i = accept(lsock,(struct sockaddr *)NULL,0);
	if (i>=0) {
		return i;
	} else if (ERRNO_ERROR) {
		return -1;
	}
	s = monotonic_seconds();
	msecpassed = 0;
	while (1) {
		pfd.fd = lsock;
		pfd.events = POLLIN;
		pfd.revents = 0;
		if (poll(&pfd,1,msecto-msecpassed)>=0) {
			break;
		}
		if (errno==EINTR) {
			c = monotonic_seconds();
			msecpassed = (c-s)*1000.0;
			if (msecpassed>=msecto) {
				errno = ETIMEDOUT;
				return -1;
			}
			continue;
		} else {
			return -1;
		}
	}
	if (pfd.revents & (POLLHUP|POLLERR)) {
		return -1;
	}
	if (pfd.revents & POLLIN) {
		return accept(lsock,(struct sockaddr *)NULL,0);
	}
	errno = ETIMEDOUT;
	return -1;
}

static inline int streamaccept(int lsock) {
	int sock;
	sock=accept(lsock,(struct sockaddr *)NULL,0);
	if (sock<0) {
		return -1;
	}
	return sock;
}

/* -------------- UNIVERSAL -------------- */

int univnonblock(int fd) {
	return descnonblock(fd);
}

int32_t univtoread(int fd,void *buff,uint32_t leng,uint32_t msecto) {
	return streamtoread(fd,buff,leng,msecto);
}

int32_t univtowrite(int fd,const void *buff,uint32_t leng,uint32_t msecto) {
	return streamtowrite(fd,buff,leng,msecto);
}

int32_t univtoforward(int srcfd,int dstfd,void *buff,uint32_t leng,uint32_t rcvd,uint32_t sent,uint32_t msecto) {
	return streamtoforward(srcfd,dstfd,buff,leng,rcvd,sent,msecto);
}

/* ----------------- TCP ----------------- */

int tcpsetacceptfilter(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	bzero(&afa, sizeof(afa));
	strcpy(afa.af_name, "dataready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#elif TCP_DEFER_ACCEPT
	int v = 1;

	return setsockopt(sock, IPPROTO_TCP, TCP_DEFER_ACCEPT, &v, sizeof(v));
#else
	(void)sock;
	errno=ENOTSUP;
	return -1;
#endif
}

int tcpsocket(void) {
	return socket(AF_INET,SOCK_STREAM,0);
}

int tcpnonblock(int sock) {
	return descnonblock(sock);
}

int tcpgetstatus(int sock) {
	return sockgetstatus(sock);
}


int tcpresolve(const char *hostname,const char *service,uint32_t *ip,uint16_t *port,int passive) {
	return sockresolve(hostname,service,ip,port,AF_INET,SOCK_STREAM,passive);
}

int tcpreuseaddr(int sock) {
	int yes=1;
	return setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,(char*)&yes,sizeof(int));
}

int tcpnodelay(int sock) {
	int yes=1;
	return setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,(char*)&yes,sizeof(int));
}

int tcpaccfhttp(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	bzero(&afa, sizeof(afa));
	strcpy(afa.af_name, "httpready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	errno=EINVAL;
	return -1;
#endif
}

int tcpaccfdata(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	bzero(&afa, sizeof(afa));
	strcpy(afa.af_name, "dataready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	errno=EINVAL;
	return -1;
#endif
}

int tcpstrbind(int sock,const char *hostname,const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa,hostname,service,AF_INET,SOCK_STREAM,1)<0) {
		return -1;
	}
	if (bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	return 0;
}

int tcpnumbind(int sock,uint32_t ip,uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa,ip,port);
	if (bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	return 0;
}

int tcpstrconnect(int sock,const char *hostname,const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa,hostname,service,AF_INET,SOCK_STREAM,0)<0) {
		return -1;
	}
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
#ifdef WIN32
	if (WSAGetLastError()==WSAEWOULDBLOCK) {
		errno = EINPROGRESS;
	}
#endif
	if (errno == EINPROGRESS) {
		return 1;
	}
	return -1;
}

int tcpnumconnect(int sock,uint32_t ip,uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa,ip,port);
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
#ifdef WIN32
	if (WSAGetLastError()==WSAEWOULDBLOCK) {
		errno = EINPROGRESS;
	}
#endif
	if (errno == EINPROGRESS) {
		return 1;
	}
	return -1;
}

int tcpstrtoconnect(int sock,const char *hostname,const char *service,uint32_t msecto) {
	struct sockaddr_in sa;
	if (descnonblock(sock)<0) {
		return -1;
	}
	if (sockaddrfill(&sa,hostname,service,AF_INET,SOCK_STREAM,0)<0) {
		return -1;
	}
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
#ifdef WIN32
	if (WSAGetLastError()==WSAEWOULDBLOCK) {
		errno = EINPROGRESS;
	}
#endif
	if (errno == EINPROGRESS) {
		double s,c;
		uint32_t msecpassed;
		struct pollfd pfd;
		s = monotonic_seconds();
		msecpassed = 0;
		while (1) {
			pfd.fd = sock;
			pfd.events = POLLOUT;
			pfd.revents = 0;
			if (poll(&pfd,1,msecto-msecpassed)>=0) {
				break;
			}
			if (errno==EINTR) {
				c = monotonic_seconds();
				msecpassed = (c-s)*1000.0;
				if (msecpassed>=msecto) {
					errno = ETIMEDOUT;
					return -1;
				}
				continue;
			} else {
				return -1;
			}
		}
		if (pfd.revents & (POLLHUP|POLLERR)) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			return sockgetstatus(sock);
		}
		errno=ETIMEDOUT;
	}
	return -1;
}

int tcpnumtoconnect(int sock,uint32_t ip,uint16_t port,uint32_t msecto) {
	struct sockaddr_in sa;
	if (descnonblock(sock)<0) {
		return -1;
	}
	sockaddrnumfill(&sa,ip,port);
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
#ifdef WIN32
	if (WSAGetLastError()==WSAEWOULDBLOCK) {
		errno = EINPROGRESS;
	}
#endif
	if (errno == EINPROGRESS) {
		double s,c;
		uint32_t msecpassed;
		struct pollfd pfd;
		s = monotonic_seconds();
		msecpassed = 0;
		while (1) {
			pfd.fd = sock;
			pfd.events = POLLOUT;
			pfd.revents = 0;
			if (poll(&pfd,1,msecto-msecpassed)>=0) {
				break;
			}
			if (errno==EINTR) {
				c = monotonic_seconds();
				msecpassed = (c-s)*1000.0;
				if (msecpassed>=msecto) {
					errno = ETIMEDOUT;
					return -1;
				}
				continue;
			} else {
				return -1;
			}
		}
		if (pfd.revents & (POLLHUP|POLLERR)) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			return sockgetstatus(sock);
		}
		errno=ETIMEDOUT;
	}
	return -1;
}

int tcpstrlisten(int sock,const char *hostname,const char *service,uint16_t queue) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa,hostname,service,AF_INET,SOCK_STREAM,1)<0) {
		return -1;
	}
	if (bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int tcpnumlisten(int sock,uint32_t ip,uint16_t port,uint16_t queue) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa,ip,port);
	if (bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int tcpgetpeer(int sock,uint32_t *ip,uint16_t *port) {
	struct sockaddr_in sa;
	socklen_t leng;
	leng=sizeof(sa);
	if (getpeername(sock,(struct sockaddr *)&sa,&leng)<0) {
		return -1;
	}
	if (ip!=(void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port!=(void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

int tcpgetmyaddr(int sock,uint32_t *ip,uint16_t *port) {
	struct sockaddr_in sa;
	socklen_t leng;
	leng=sizeof(sa);
	if (getsockname(sock,(struct sockaddr *)&sa,&leng)<0) {
		return -1;
	}
	if (ip!=(void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port!=(void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

int tcpclose(int sock) {
	// make sure that all pending data in the output buffer will be sent
#ifdef WIN32
	shutdown(sock,SD_SEND);
	return closesocket(sock);
#else
	shutdown(sock,SHUT_WR);
	return close(sock);
#endif
}

int32_t tcptoread(int sock,void *buff,uint32_t leng,uint32_t msecto) {
	return streamtoread(sock,buff,leng,msecto);
}

int32_t tcptowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto) {
	return streamtowrite(sock,buff,leng,msecto);
}

int32_t tcptoforward(int srcsock,int dstsock,void *buff,uint32_t leng,uint32_t rcvd,uint32_t sent,uint32_t msecto) {
	return streamtoforward(srcsock,dstsock,buff,leng,rcvd,sent,msecto);
}

int tcptoaccept(int lsock,uint32_t msecto) {
	return streamtoaccept(lsock,msecto);
}

int tcpaccept(int lsock) {
	return streamaccept(lsock);
}

/*
int32_t tcpread(int sock,void *buff,uint32_t leng) {
	uint32_t rcvd=0;
	int i;
	while (rcvd<leng) {
		i = read(sock,((uint8_t*)buff)+rcvd,leng-rcvd);
		if (i<=0) {
			return i;
		}
		rcvd+=i;
	}
	return rcvd;
}

int32_t tcpwrite(int sock,const void *buff,uint32_t leng) {
	uint32_t sent=0;
	int i;
	while (sent<leng) {
		i = write(sock,((const uint8_t*)buff)+sent,leng-sent);
		if (i<=0) {
			return i;
		}
		sent+=i;
	}
	return sent;
}
*/

/* ----------------- UDP ----------------- */

int udpsocket(void) {
	return socket(AF_INET,SOCK_DGRAM,0);
}

int udpnonblock(int sock) {
	return descnonblock(sock);
}

int udpgetstatus(int sock) {
	return sockgetstatus(sock);
}


int udpresolve(const char *hostname,const char *service,uint32_t *ip,uint16_t *port,int passive) {
	return sockresolve(hostname,service,ip,port,AF_INET,SOCK_DGRAM,passive);
}

int udpnumlisten(int sock,uint32_t ip,uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa,ip,port);
	return bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in));
}

int udpstrlisten(int sock,const char *hostname,const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa,hostname,service,AF_INET,SOCK_DGRAM,1)<0) {
		return -1;
	}
	return bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_in));
}

int udpwrite(int sock,uint32_t ip,uint16_t port,const void *buff,uint16_t leng) {
	struct sockaddr_in sa;
	if (leng>512) {
		return -1;
	}
	sockaddrnumfill(&sa,ip,port);
	return sendto(sock,buff,leng,0,(struct sockaddr *)&sa,sizeof(struct sockaddr_in));
}

int udpread(int sock,uint32_t *ip,uint16_t *port,void *buff,uint16_t leng) {
	socklen_t templeng;
	struct sockaddr tempaddr;
	struct sockaddr_in *saptr;
	int ret;
	ret = recvfrom(sock,buff,leng,0,&tempaddr,&templeng);
	if (templeng==sizeof(struct sockaddr_in)) {
		saptr = ((struct sockaddr_in*)&tempaddr);
		if (ip!=(void *)0) {
			*ip = ntohl(saptr->sin_addr.s_addr);
		}
		if (port!=(void *)0) {
			*port = ntohs(saptr->sin_port);
		}
	}
	return ret;
}

int udpclose(int sock) {
#ifdef WIN32
	return closesocket(sock);
#else
	return close(sock);
#endif
}

/* ----------------- UNIX ---------------- */

#ifndef WIN32

int unixsocket(void) {
	return socket(AF_UNIX,SOCK_STREAM,0);
}

int unixnonblock(int sock) {
	return descnonblock(sock);
}

int unixgetstatus(int sock) {
	return sockgetstatus(sock);
}

int unixconnect(int sock,const char *path) {
	struct sockaddr_un sa;

	if (sockaddrpathfill(&sa,path)<0) {
		return -1;
	}
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_un)) >= 0) {
		return 0;
	}
	if (errno == EINPROGRESS) {
		return 1;
	}
	return -1;
}

int unixtoconnect(int sock,const char *path,uint32_t msecto) {
	struct sockaddr_un sa;

	if (descnonblock(sock)<0) {
		return -1;
	}
	if (sockaddrpathfill(&sa,path)<0) {
		return -1;
	}
	if (connect(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_un)) >= 0) {
		return 0;
	}
	if (errno == EINPROGRESS) {
		double s,c;
		uint32_t msecpassed;
		struct pollfd pfd;
		s = monotonic_seconds();
		msecpassed = 0;
		while (1) {
			pfd.fd = sock;
			pfd.events = POLLOUT;
			pfd.revents = 0;
			if (poll(&pfd,1,msecto-msecpassed)>=0) {
				break;
			}
			if (errno==EINTR) {
				c = monotonic_seconds();
				msecpassed = (c-s)*1000.0;
				if (msecpassed>=msecto) {
					errno = ETIMEDOUT;
					return -1;
				}
				continue;
			} else {
				return -1;
			}
		}
		if (pfd.revents & (POLLHUP|POLLERR)) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			return sockgetstatus(sock);
		}
		errno=ETIMEDOUT;
	}
	return -1;
}

int unixlisten(int sock,const char *path,int queue) {
	struct sockaddr_un sa;

	if (sockaddrpathfill(&sa,path)<0) {
		return -1;
	}
	if (bind(sock,(struct sockaddr *)&sa,sizeof(struct sockaddr_un)) < 0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int32_t unixtoread(int sock,void *buff,uint32_t leng,uint32_t msecto) {
	return streamtoread(sock,buff,leng,msecto);
}

int32_t unixtowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto) {
	return streamtowrite(sock,buff,leng,msecto);
}

int32_t unixtoforward(int srcsock,int dstsock,void *buff,uint32_t leng,uint32_t rcvd,uint32_t sent,uint32_t msecto) {
	return streamtoforward(srcsock,dstsock,buff,leng,rcvd,sent,msecto);
}

int unixtoaccept(int lsock,uint32_t msecto) {
	return streamtoaccept(lsock,msecto);
}

int unixaccept(int lsock) {
	return streamaccept(lsock);
}

#endif
