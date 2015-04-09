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

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pcap.h>

#include "MFSCommunication.h"

typedef struct _userdata {
	uint8_t bytesskip;
} userdata;

struct _mfscmd {
	uint32_t command;
	const char *commandstr;
} cmdtab[]={
#include "commands.h"
	{0,NULL}};

typedef struct _mfscommand {
	uint32_t command;
	char *commandstr;
	uint8_t colorcode;
	uint8_t display;
} mfscommand;

static mfscommand *mfscmdtab = NULL;
static uint32_t mfscmdtableng = 0;

int commands_cmp(const void *a,const void *b) {
	const mfscommand *aa = (const mfscommand*)a;
	const mfscommand *bb = (const mfscommand*)b;
	return (aa->command>bb->command)?1:(aa->command<bb->command)?-1:0;
}

void commands_convert(void) {
	uint32_t i;
	uint8_t ccode;
	char *p;
	mfscmdtableng = 0;
	for (i=0 ; cmdtab[i].commandstr!=NULL ; i++) {
		mfscmdtableng++;
	}
	if (mfscmdtableng==0) {
		mfscmdtab = NULL;
		return;
	}
	mfscmdtab = malloc(sizeof(mfscommand)*mfscmdtableng);
	for (i=0 ; cmdtab[i].commandstr!=NULL ; i++) {
		mfscmdtab[i].command = cmdtab[i].command;
		mfscmdtab[i].commandstr = strdup(cmdtab[i].commandstr);
		p = mfscmdtab[i].commandstr;
		ccode = 0;
		if (p[0]!=0 && p[1]!=0 && p[2]=='T' && p[3]=='O') {
			if (p[0]=='A' && p[1]=='N') {
				if (p[4]=='A' && p[5]=='N') {
					ccode = 8; /* ANTOAN */
				} else if (p[4]=='C' && p[5]=='L') {
					ccode = 13; /* ANTOCL */
				} else if (p[4]=='C' && p[5]=='S') {
					ccode = 14; /* ANTOCS */
				} else if (p[4]=='M' && p[5]=='A') {
					ccode = 1; /* ANTOMA */
				}
			} else if (p[0]=='C' && p[1]=='L') {
				if (p[4]=='A' && p[5]=='N') {
					ccode = 5; /* CLTOAN */
				} else if (p[4]=='C' && p[5]=='S') {
					ccode = 11; /* CLTOCS */
				} else if (p[4]=='M' && p[5]=='A') {
					ccode = 10; /* CLTOMA */
				}
			} else if (p[0]=='C' && p[1]=='S') {
				if (p[4]=='A' && p[5]=='N') {
					ccode = 6; /* CSTOAN */
				} else if (p[4]=='C' && p[5]=='L') {
					ccode = 3; /* CSTOCL */
				} else if (p[4]=='M' && p[5]=='A') {
					ccode = 4; /* CSTOMA */
				}
			} else if (p[0]=='M' && p[1]=='A') {
				if (p[4]=='A' && p[5]=='N') {
					ccode = 9; /* MATOAN */
				} else if (p[4]=='C' && p[5]=='L') {
					ccode = 2; /* MATOCL */
				} else if (p[4]=='C' && p[5]=='S') {
					ccode = 12; /* MATOCS */
				}
			}
		}
		mfscmdtab[i].colorcode = ccode;
		mfscmdtab[i].display = 1;
	}
	qsort(mfscmdtab,mfscmdtableng,sizeof(mfscommand),commands_cmp);
}

const char* commands_find(uint32_t cmd,uint8_t *color,uint8_t *display) {
	int32_t first,last,middle;
	first = 0;
	last = mfscmdtableng-1;
	middle = (first+last)/2;

	while (first<=last) {
		if (mfscmdtab[middle].command<cmd) {
			first = middle + 1;
		} else if (mfscmdtab[middle].command>cmd)  {
			last = middle - 1;
		} else {
			*color = mfscmdtab[middle].colorcode;
			*display = mfscmdtab[middle].display;
			return mfscmdtab[middle].commandstr;
		}
		middle = (first+last)/2;
	}
	*color = 0;
	*display = 0;
	return NULL;
}

void commands_exclude(const char *opt) {
	char *str;
	char *p;
	uint32_t i;

	str = strdup(opt);
	for (p=strtok(str," ,;") ; p!=NULL ; p=strtok(NULL," ,;")) {
		for (i=0 ; i<mfscmdtableng ; i++) {
			if (strcmp(p,mfscmdtab[i].commandstr)==0) {
				mfscmdtab[i].display = 0;
			}
		}
	}
	free(str);
}

void commands_onlyuse(const char *opt) {
	char *str;
	char *p;
	uint32_t i;
	static uint8_t ft = 1;

	if (ft) { // do it once
		for (i=0 ; i<mfscmdtableng ; i++) {
			mfscmdtab[i].display = 0;
		}
		ft = 0;
	}

	str = strdup(opt);
	for (p=strtok(str," ,;") ; p!=NULL ; p=strtok(NULL," ,;")) {
		for (i=0 ; i<mfscmdtableng ; i++) {
			if (strcmp(p,mfscmdtab[i].commandstr)==0) {
				mfscmdtab[i].display = 1;
			}
		}
	}
	free(str);
}





#define COLOR_PAYLOAD_ADDR "\033[38;5;159m"
#define COLOR_PAYLOAD_HEX "\033[38;5;228m"
#define COLOR_TIMESTAMP "\033[38;5;231m"
#define COLOR_ADDR_SRCHI "\033[38;5;156m"
#define COLOR_ADDR_DSTHI "\033[38;5;217m"
#define COLOR_ADDR_SRCLO "\033[38;5;194m"
#define COLOR_ADDR_DSTLO "\033[38;5;224m"
#define COLOR_WRONGPACKET "\033[38;5;199m"
#define COLOR_NEWCONN "\033[38;5;116m"
#define COLOR_CLOSECONN "\033[38;5;100m"
#define COLOR_DATA "\033[38;5;180m"
#define COLOR_CLEAR "\033(B\033[m"

static const char* color_tab[] = {
	"\033[30m",
	"\033[31m",
	"\033[32m",
	"\033[33m",
	"\033[34m",
	"\033[35m",
	"\033[36m",
	"\033[37m",
	"\033[90m",
	"\033[91m",
	"\033[92m",
	"\033[93m",
	"\033[94m",
	"\033[95m",
	"\033[96m",
	"\033[97m",
	NULL
};

void hexdump(const uint8_t *ptr,uint32_t len) {
	uint8_t eol;
	uint32_t i;
	eol = 0;
	for (i=0 ; i<len ; i++) {
		eol = 1;
		if ((i&0x1F)==0) {
			printf(COLOR_PAYLOAD_ADDR "\t0x%05X:" COLOR_PAYLOAD_HEX,i);
		}
		printf(" %02X",ptr[i]);
		if ((i&0x1F)==0x1F) {
			printf(COLOR_CLEAR "\n");
			eol = 0;
		}
	}
	if (eol) {
		printf(COLOR_CLEAR "\n");
	}
}

// stupid implementation 
typedef struct _connection {
	uint32_t srcip,dstip;
	uint16_t srcport,dstport;
	uint32_t seq;
	struct _connection *next;
} connection;

static connection *connhead = NULL;

static inline connection** packet_find(uint32_t srcip,uint16_t srcport,uint32_t dstip,uint16_t dstport) {
	connection *c,**cp;
	cp = &connhead;
	while ((c = *cp)) {
		if (c->srcip==srcip && c->dstip==dstip && c->srcport==srcport && c->dstport==dstport) {
			return cp;
		}
		cp = &(c->next);
	}
	return NULL;
}

static inline void print_info(const struct timeval *ts,const uint8_t *ip,uint16_t srcport,uint16_t dstport) {
	printf(COLOR_TIMESTAMP "%ld.%06u : ",(long)(ts->tv_sec),(unsigned)(ts->tv_usec));
	printf("%s%3u.%3u.%3u.%3u : %5" PRIu16 COLOR_CLEAR " -> %s%3u.%3u.%3u.%3u : %5"PRIu16 COLOR_CLEAR " ",(srcport<49152)?COLOR_ADDR_SRCLO:COLOR_ADDR_SRCHI,ip[12],ip[13],ip[14],ip[15],srcport,(dstport<49152)?COLOR_ADDR_DSTLO:COLOR_ADDR_DSTHI,ip[16],ip[17],ip[18],ip[19],dstport);
}

void parse_packet(u_char *args, const struct pcap_pkthdr *header, const u_char *packet) {
	userdata *ud = (userdata*)args;
	const uint8_t *ip;
	const uint8_t *tcp;
	const uint8_t *payload;
	uint32_t iplen;
	uint32_t iphdrlen;
	uint32_t tcplen;
	uint32_t tcphdrlen;
	uint32_t payloadlen;
	uint32_t seqno,skip;
	uint32_t srcip,dstip;
	uint16_t srcport,dstport;
	uint32_t mfscmd,mfslen;
	uint8_t ccode,display;
	const char *commandstr;
	connection *c,**cp;

//	printf("%ld.%06u: capleng = %u ; leng = %u (%u + %u)\n",(long)(header->ts.tv_sec),(unsigned)(header->ts.tv_usec),header->caplen,header->len,ud->bytesskip,header->len-ud->bytesskip);
	if (ud->bytesskip>=header->caplen) { // no data
		return;
	}
	ip = packet+ud->bytesskip;
	iplen = header->caplen-ud->bytesskip;
//	hexdump(ip,iplen);
//	printf("\n");
	if ((ip[0]&0xF0)!=0x40) {
		return;	// this is not IPv4 packet - ignore
	}
	if (iplen<20) {
		return; // packet truncated on IP header - ignore
	}
	if (ip[9]!=6) {
		return; // this is not TCP packet - ignore
	}
	iphdrlen = 4 * (ip[0] & 0xF);
	if (iplen < iphdrlen) {
		return; // packet truncated on IP header (options) - ignore
	}
	tcp = ip + iphdrlen;
	tcplen = iplen - iphdrlen;
//	hexdump(tcp,tcplen);
//	printf("\n");
	if (tcplen<20) {
		return; // packet truncated on TCP header - ignore
	}
	tcphdrlen = 4 * (tcp[12] >> 4);
	if (tcplen < tcphdrlen) {
		return; // packet truncated on TCP header (options) - ignore
	}
	srcip = ((ip[12]*256U+ip[13])*256U+ip[14])*256U+ip[15];
	dstip = ((ip[16]*256U+ip[17])*256U+ip[18])*256U+ip[19];
	srcport = tcp[0]*256U+tcp[1];
	dstport = tcp[2]*256U+tcp[3];
	seqno = ((tcp[4]*256U+tcp[5])*256U+tcp[6])*256U+tcp[7];
	cp = packet_find(srcip,srcport,dstip,dstport);
	if (tcp[13]&0x2) { // SYN
		print_info(&(header->ts),ip,srcport,dstport);
		printf(COLOR_NEWCONN "... new connection ..." COLOR_CLEAR "\n");
	}
	if (tcp[13]&0x5) { // RST | FIN
		print_info(&(header->ts),ip,srcport,dstport);
		printf(COLOR_CLOSECONN "... close connection ..." COLOR_CLEAR "\n");
		if (cp!=NULL) {
			c = *cp;
			*cp = c->next;
			free(c);
		}
		return;
	}
	payload = tcp + tcphdrlen;
	payloadlen = tcplen - tcphdrlen;
	if (cp!=NULL) {
		c = *cp;
		if (c->seq > seqno) {
			skip = c->seq - seqno;
			print_info(&(header->ts),ip,srcport,dstport);
			printf(COLOR_DATA "... data in packet ..." COLOR_CLEAR "\n");
		} else {
			skip = 0;
		}
		if (skip < payloadlen) {
			payload += skip;
			payloadlen -= skip;
		} else {
			return;
		}
	}
//	hexdump(payload,payloadlen);
	while (payloadlen>=8) {
		mfscmd = ((payload[0]*256U+payload[1])*256U+payload[2])*256U+payload[3];
		mfslen = ((payload[4]*256U+payload[5])*256U+payload[6])*256U+payload[7];
		commandstr = commands_find(mfscmd,&ccode,&display);
		if (commandstr && mfslen<=100000000) {
			if (display) {
				print_info(&(header->ts),ip,srcport,dstport);
				if (ccode>=1 && ccode<=15) {
					printf("%s",color_tab[ccode]);
				}
				printf("%s",commandstr);
				if (ccode) {
					printf(COLOR_CLEAR);
				}
				printf("\n");
				if (payloadlen-8<=128) {
					if (mfslen < payloadlen-8) {
						hexdump(payload+8,mfslen);
					} else {
						hexdump(payload+8,payloadlen-8);
					}
				} else {
					if (mfslen < 128) {
						hexdump(payload+8,mfslen);
					} else {
						hexdump(payload+8,128);
						printf("\t(...)\n");
					}
				}
			}
			if (mfslen + 8 == payloadlen) {
				if (cp!=NULL) {
					c = *cp;
					*cp = c->next;
					free(c);
				}
				payloadlen = 0;
				payload = NULL;
			} else if (mfslen + 8 < payloadlen) {
				payloadlen -= (mfslen + 8);
				payload += (mfslen + 8);
				seqno += (mfslen + 8);
			} else {
				if (cp!=NULL) {
					c = *cp;
					c->seq = seqno + mfslen + 8;
				} else {
					c = malloc(sizeof(connection));
					c->srcip = srcip;
					c->dstip = dstip;
					c->srcport = srcport;
					c->dstport = dstport;
					c->seq = seqno + mfslen + 8;
					c->next = connhead;
					connhead = c;
				}
				payloadlen = 0;
				payload = NULL;
			}
		} else {
			print_info(&(header->ts),ip,srcport,dstport);
			printf(COLOR_WRONGPACKET "... not mfs packet (%u:%u) ..." COLOR_CLEAR "\n",mfscmd,mfslen);
			if (cp!=NULL) {
				c = *cp;
				*cp = c->next;
				free(c);
			}
			return;
		}
	}
	return;
}

void usage(const char *appname) {
	fprintf(stderr,"usage: %s [-i interface] [-f pcap_filter] [-c packet count] [-e commands] [-o commands]\n\t-e: do not display this commands\n\t-o: when present only this commands will be displayed\n",appname);
}

int main(int argc, char **argv) {
	char ch;
	char errbuf[PCAP_ERRBUF_SIZE];
	char *dev;
	char *filter;
	int32_t packetcnt;
	bpf_u_int32 devnet;
	bpf_u_int32 devmask;
	pcap_t *handle;
	int datalink;
	struct bpf_program fp;
	userdata udm;

	commands_convert();

	dev = NULL;
	filter = NULL;
	packetcnt = -1;

	while ((ch = getopt(argc, argv, "si:f:c:e:o:?")) != -1) {
		switch (ch) {
			case 's':
				if (filter) {
					free(filter);
				}
				filter = strdup("port 9419 or port 9420 or port 9421 or port 9422");
				break;
			case 'i':
				if (dev) {
					free(dev);
				}
				dev = strdup(optarg);
				break;
			case 'f':
				if (filter) {
					free(filter);
				}
				filter = strdup(optarg);
				break;
			case 'c':
				packetcnt = strtol(optarg,NULL,0);
				break;
			case 'e':
				commands_exclude(optarg);
				break;
			case 'o':
				commands_onlyuse(optarg);
				break;
			default:
				usage(argv[0]);
				return 1;
		}
	}

	if (dev==NULL) {
		dev = pcap_lookupdev(errbuf);
		if (dev == NULL) {
			fprintf(stderr, "Couldn't find default device: %s\n", errbuf);
			return 1;
		}
	}


	if (pcap_lookupnet(dev, &devnet, &devmask, errbuf)<0) {
		printf("Device: %s\n",dev);
		fprintf(stderr, "Couldn't get netmask for device %s: %s\n", dev, errbuf);
		devnet = 0;
		devmask = 0;
	} else {
//		printf("Device: %s (%u.%u.%u.%u/%u.%u.%u.%u)\n", dev, (devnet>>24)&0xFF,(devnet>>16)&0xFF,(devnet>>8)&0xFF,devnet&0xFF,(devmask>>24)&0xFF,(devmask>>16)&0xFF,(devmask>>8)&0xFF,devmask&0xFF);	// BIG/LITTLE ENDIAN ???
		printf("Device: %s (%u.%u.%u.%u/%u.%u.%u.%u)\n", dev, devnet&0xFF,(devnet>>8)&0xFF,(devnet>>16)&0xFF,(devnet>>24)&0xFF,devmask&0xFF,(devmask>>8)&0xFF,(devmask>>16)&0xFF,(devmask>>24)&0xFF);
	}

	handle = pcap_open_live(dev, 100000, 1, 1000, errbuf);
	if (handle == NULL) {
		fprintf(stderr, "Couldn't open device %s: %s\n", dev, errbuf);
		return 1;
	}

	datalink = pcap_datalink(handle);
	if (datalink == DLT_EN10MB) {
		udm.bytesskip = 14;
	} else if (datalink == DLT_NULL) {
		udm.bytesskip = 4;
	} else if (datalink == DLT_RAW) {
		udm.bytesskip = 0;
	} else {
		fprintf(stderr, "%s is not an Ethernet (type: %s)\n", dev, pcap_datalink_val_to_name(datalink));
		return 1;
	}

	if (filter!=NULL) {
		if (pcap_compile(handle, &fp, filter, 0, devnet)<0) {
			fprintf(stderr, "Couldn't parse filter %s: %s\n", filter, pcap_geterr(handle));
			return 1;
		}

		if (pcap_setfilter(handle, &fp)<0) {
			fprintf(stderr, "Couldn't install filter %s: %s\n", filter, pcap_geterr(handle));
			return 1;
		}
	}

	pcap_loop(handle,packetcnt,parse_packet,(void*)(&udm));

	pcap_freecode(&fp);
	pcap_close(handle);

	printf("\nCapture complete.\n");
	return 0;
}

