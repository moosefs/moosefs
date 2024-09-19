/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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

#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
// #include <stdlib.h>
// #include <unistd.h>
// #include <stdarg.h>
#include <errno.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "datapack.h"
#include "sockets.h"
#include "strerr.h"

static uint64_t msupervisor_getutime(void) {
	struct timeval tv;
	uint64_t usectime;

	gettimeofday(&tv,NULL);
	usectime = tv.tv_sec;
	usectime *= 1000000;
	usectime += tv.tv_usec;

	return usectime;
}

int msupervisor_simple(const char *masterhost,const char *masterport,const char *bindhost,uint8_t debug,uint8_t store) {
	uint32_t mip;
	uint16_t mport;
	uint32_t bindip;
	char strip[16];
	int sock;
	uint8_t packetbuff[40];
	const uint8_t *rptr;
	uint8_t *wptr;
	uint32_t type,pleng;
	uint32_t s1,s2,s3;
	uint64_t metaversion;
	uint64_t metaid;
	uint64_t utimestamp;
	uint64_t utimestart;
	int64_t usecping;

	sock = -1;

	if (bindhost) {
		if (tcpresolve(bindhost,NULL,&bindip,NULL,1)<0) {
//			fprintf(stderr,"can't resolve bindhost '%s'\n",bindhost);
//			return 1;
			bindip = 0;
		}
	} else {
		bindip = 0;
	}

	if (tcpresolve(masterhost,masterport,&mip,&mport,0)<0) {
		fprintf(stderr,"can't resolve master '%s:%s'\n",masterhost,masterport);
		return 1;
	}

	snprintf(strip,16,"%u.%u.%u.%u",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);

	if (debug) {
		printf("master ip and port numbers: %s:%"PRIu16"\n",strip,mport);
	}

	sock = tcpsocket();
	if (sock<0) {
		fprintf(stderr,"create socket, error: %s",strerr(errno));
		goto err;
	}
	if (tcpnonblock(sock)<0) {
		fprintf(stderr,"set nonblock, error: %s",strerr(errno));
		goto err;
	}
	if (bindip>0) {
		if (tcpnumbind(sock,bindip,0)<0) {
			fprintf(stderr,"can't bind socket to given ip, error: %s",strerr(errno));
			goto err;
		}
	}
	if (tcpnumtoconnect(sock,mip,mport,1500)<0) {
		fprintf(stderr,"can't connect to master (%s:%"PRIu16"), error: %s\n",strip,mport,strerr(errno));
		goto err;
	}

	if (debug) {
		printf("connected to the master\n");
	}

	utimestart = msupervisor_getutime();

	wptr = packetbuff;
	put32bit(&wptr,ANTOMA_REGISTER);
	put32bit(&wptr,7);
	put8bit(&wptr,4);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	put16bit(&wptr,10);
	if (tcptowrite(sock,packetbuff,15,1000,2000)!=15) {
		fprintf(stderr,"socket write error: %s\n",strerr(errno));
		goto err;
	}

	do {
		if (tcptoread(sock,packetbuff,8,1000,2000)!=8) {
			fprintf(stderr,"socket read error: %s\n",strerr(errno));
			goto err;
		}
		rptr = packetbuff;
		type = get32bit(&rptr);
		pleng = get32bit(&rptr);
	} while (type==ANTOAN_NOP && pleng==0);

	if (type!=MATOAN_STATE) {
		fprintf(stderr,"got wrong answer (type ; got:%"PRIu32" ; expected:%d)\n",type,MATOAN_STATE);
		goto err;
	}
	if (pleng!=20 && pleng!=28 && pleng!=40) {
		fprintf(stderr,"got wrong answer (size ; got:%"PRIu32" ; expected:20/28/40)\n",pleng);
		goto err;
	}

	if (tcptoread(sock,packetbuff,pleng,1000,2000)!=(int32_t)pleng) {
		fprintf(stderr,"socket read error: %s\n",strerr(errno));
		goto err;
	}

	usecping = msupervisor_getutime() - utimestart;

	rptr = packetbuff;
	s1 = get32bit(&rptr);
	s2 = get32bit(&rptr);
	s3 = get32bit(&rptr);
	if (s1!=UINT32_C(0xFFFFFFFF) || s2!=0 || s3!=0) {
		fprintf(stderr,"got wrong answer (incompatible master version)\n");
		goto err;
	}

	metaversion = get64bit(&rptr);
	if (pleng>=28) {
		metaid = get64bit(&rptr);
	} else {
		metaid = 0;
	}
	if (pleng>=40) {
		utimestamp = get64bit(&rptr);
	} else {
		utimestamp = 0;
	}

	if (store) { // send store message and exit
		wptr = packetbuff;
		put32bit(&wptr,ANTOMA_STORE_METADATA);
		put32bit(&wptr,0);

		if (tcptowrite(sock,packetbuff,8,1000,2000)!=8) {
			fprintf(stderr,"socket write error: %s\n",strerr(errno));
			goto err;
		}

		printf("store command has been sent to the master\n");

		tcpclose(sock);
		return 0;
	}
	tcpclose(sock);
	sock = -1;

	if (usecping<0) {
		printf("time went backward (%.3lf s) - skipping time check\n",(double)usecping/-1000000.0);
		usecping = 0;
	}
	if (usecping>500000) {
		printf("it took too much time to get answer from the master (%.3lf s) - skipping time check\n",(double)usecping/1000000.0);
		usecping = 0;
	}

	if (usecping>0) {
		uint64_t utimediff;

		utimestart += usecping/2;
		if (utimestart > utimestamp) {
			utimediff = utimestart - utimestamp;
		} else {
			utimediff = utimestamp - utimestart;
		}

		if (utimediff > 1000000) {
			printf("detected time desync between local time and the master (%.3lf s) - consider time synchronization\n",(double)(utimediff)/1000000.0);
		}
	}

	printf("MASTER %s:%"PRIu16" (meta id: 0x%016"PRIX64" ; meta version: %"PRIu64")\n",strip,mport,metaid,metaversion);

	return 0;
err:
	if (sock>=0) {
		tcpclose(sock);
	}
	return 1;
}
