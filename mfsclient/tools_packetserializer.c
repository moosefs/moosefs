#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#ifdef GLOB_TOOLS

#include "mfsstrerr.h"
#include "datapack.h"
#include "tools_main.h"
#include "tools_packetserializer.h"

#else

#include "mfsstrerr.h"
#include "datapack.h"
#include "sockets.h"
#include "tools_common.h"
#include "tools_packetserializer.h"

#endif

#define MAX_RECONECTION	3

#define INIT_BUFF_SIZE (64*1024)

#define QUERY_HEADCONST 8

typedef struct ps_data_s {
	uint8_t	*wbuff;
	uint8_t *rbuff;
	uint8_t	*wptr;
	const uint8_t *rptr;
	uint32_t wbuffsize;
	uint32_t rbuffsize;
	uint32_t recdatasize;
//	uint32_t command;		//last command sent
	//uint8_t msgid;		//last MsgId - for future use, currently always zero
	uint32_t recanswerid;	//received answer identifier - mast be "ANTOAN_NOP" or according to command
	uint16_t status;
	uint8_t auxbuff[QUERY_HEADCONST];	//
} ps_data_t;

static ps_data_t ps_data = {
	.wbuff = NULL,
	.rbuff = NULL,
	.wptr = NULL,
	.rptr = NULL,
	.wbuffsize = 0,
	.rbuffsize = 0,
	.recdatasize = 0,
	.recanswerid = 0,
	.status = 0,
};

static void rbuffexpand(uint32_t newsize) {
	uint8_t	*newbuff;

	ps_data.rbuffsize = newsize;
	if ((newbuff=realloc(ps_data.rbuff, ps_data.rbuffsize)) == NULL) {
		fprintf(stderr,"memory allocation error!\n");
		exit(1);
	}
	ps_data.rptr = (ps_data.rptr-ps_data.rbuff)+newbuff;
	ps_data.rbuff = newbuff;
}

static void wbuffexpand(uint32_t newsize) {
	uint8_t	*newbuff;

	ps_data.wbuffsize = newsize;
	if ((newbuff=realloc(ps_data.wbuff, ps_data.wbuffsize)) == NULL) {
		fprintf(stderr,"memory allocation error!\n");
		exit(1);
	}
	ps_data.wptr = (ps_data.wptr-ps_data.wbuff)+newbuff;
	ps_data.wbuff = newbuff;
}

void ps_comminit(void) {
	if (ps_data.wbuffsize==0 || ps_data.wbuff==NULL) {
		if ((ps_data.wbuff=malloc(INIT_BUFF_SIZE)) == NULL) {
			fprintf(stderr,"memory allocation error!\n");
			exit(1);
		}
		ps_data.wbuffsize = INIT_BUFF_SIZE;
	}
	if (ps_data.rbuffsize==0 || ps_data.rbuff==NULL) {
		if ((ps_data.rbuff=malloc(INIT_BUFF_SIZE)) == NULL) {
			fprintf(stderr,"memory allocation error!\n");
			exit(1);
		}
		ps_data.rbuffsize = INIT_BUFF_SIZE;
	}
	ps_data.wptr = ps_data.wbuff+QUERY_HEADCONST;
	ps_data.rptr = NULL;
	ps_put32(0);			// msgid
	ps_data.status = 0;		// clear error
}

int32_t ps_bytesleft(void) {
	return ps_data.recdatasize - (ps_data.rptr - ps_data.rbuff);
}

int32_t ps_send_and_receive(uint32_t cmdid, uint32_t reqansid, const char *error_prefix) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint8_t reconn_cnt;

	if (ps_data.status!=0 || ps_data.wptr==NULL || ps_data.wbuff==NULL || ps_data.rbuff==NULL) {
		if (error_prefix!=NULL) {
			fprintf(stderr,"%s: wrong data to send!\n",error_prefix);
		} else {
			fprintf(stderr,"wrong data to send!\n");
		}
		ps_data.status = 1;
		return -1;
	}

//	ps_data.command = cmdid;
	reconn_cnt = 0;
	while (1) {
		if (master_socket()<0) {
			if (master_reconnect()<0) {
				ps_data.status = 1;
				return -1;
			}
			reconn_cnt++;
		}
		wptr = ps_data.wbuff;
		put32bit(&wptr, cmdid);
		put32bit(&wptr, (ps_data.wptr-ps_data.wbuff)-QUERY_HEADCONST);
		if (tcpwrite(master_socket(), ps_data.wbuff, ps_data.wptr-ps_data.wbuff) != (ps_data.wptr-ps_data.wbuff)) {
			if (reconn_cnt < MAX_RECONECTION) {
				if (master_reconnect()<0) {
					ps_data.status = 1;
					return -1;
				}
				reconn_cnt++;
				continue;
			} else {
				if (error_prefix!=NULL) {
					fprintf(stderr,"%s: master query: send error\n",error_prefix);
				} else {
					fprintf(stderr,"master query: send error\n");
				}
				ps_data.status = 1;
				return -1;
			}
		}

		do {
			//header of received answer
			if (tcpread(master_socket(), ps_data.auxbuff, QUERY_HEADCONST) != QUERY_HEADCONST) {
				if (reconn_cnt < MAX_RECONECTION) {
					if (master_reconnect()<0) {
						ps_data.status = 1;
						return -1;
					}
					reconn_cnt++;
					continue;
				} else {
					if (error_prefix!=NULL) {
						fprintf(stderr,"%s: master query: receive error (header)\n",error_prefix);
					} else {
						fprintf(stderr,"master query: receive error (header)\n");
					}
					ps_data.status = 1;
					return -1;
				}
			}
			rptr = ps_data.auxbuff;
			ps_data.recanswerid = get32bit(&rptr);
			ps_data.recdatasize = get32bit(&rptr);
		} while (ps_data.recanswerid == ANTOAN_NOP);

		if (ps_data.recanswerid != reqansid) {
			if (error_prefix!=NULL) {
				fprintf(stderr,"%s: master query: wrong answer (type)\n",error_prefix);
			} else {
				fprintf(stderr,"master query: wrong answer (type)\n");
			}
			ps_data.status = 1;
			return -1;
		}

		if (ps_data.recdatasize > ps_data.rbuffsize) {
			rbuffexpand(ps_data.recdatasize);
		}

//		printf("recdatasize: %u\n",ps_data.recdatasize);
//		printf("tcpread: %u\n",tcpread(fd, ps_data.buff, ps_data.recdatasize));
//		printf("errno: %d\n",errno);

		if (tcpread(master_socket(), ps_data.rbuff, ps_data.recdatasize) != (int32_t)ps_data.recdatasize) {
			if (reconn_cnt < MAX_RECONECTION) {
				if (master_reconnect()<0) {
					ps_data.status = 1;
					return -1;
				}
				reconn_cnt++;
				continue;
			} else {
				if (error_prefix!=NULL) {
					fprintf(stderr,"%s: master query: receive error (payload)\n",error_prefix);
				} else {
					fprintf(stderr,"master query: receive error (payload)\n");
				}
				ps_data.status = 1;
				return -1;
			}
		}
		break;
	}

	ps_data.wptr = NULL;
	ps_data.rptr = ps_data.rbuff;
	if (ps_get32()) {	// msgid != 0 ?
		if (error_prefix!=NULL) {
			fprintf(stderr,"%s: master query: wrong answer (queryid)\n",error_prefix);
		} else {
			fprintf(stderr,"master query: wrong answer (queryid)\n");
		}
		ps_data.status = 1;
		return -1;
	}
	return ps_data.recdatasize-(ps_data.rptr-ps_data.rbuff);	//bytes left to read
}

int ps_status(void) {
	return ps_data.status;
}

//safe version of puts into common buffer
void ps_put8(uint8_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(1U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put8bit(&ps_data.wptr, val);
}

void ps_put16(uint16_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(2U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put16bit(&ps_data.wptr, val);
}

void ps_put24(uint32_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(3U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put24bit(&ps_data.wptr, val);
}

void ps_put32(uint32_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(4U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put32bit(&ps_data.wptr, val);
}

void ps_put40(uint64_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(5U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put40bit(&ps_data.wptr, val);
}

void ps_put48(uint64_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(6U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put48bit(&ps_data.wptr, val);
}

void ps_put56(uint64_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(7U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put56bit(&ps_data.wptr, val);
}

void ps_put64(uint64_t val) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(8U+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand((ps_data.wbuffsize*3)/2);
	}
	put64bit(&ps_data.wptr, val);
}

//--------------------------------------------------

uint8_t ps_get8(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(1U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get8bit(&ps_data.rptr);
}

uint16_t ps_get16(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(2U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get16bit(&ps_data.rptr);
}

uint32_t ps_get24(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(3U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get24bit(&ps_data.rptr);
}

uint32_t ps_get32(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(4U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get32bit(&ps_data.rptr);
}

uint64_t ps_get40(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(5U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get40bit(&ps_data.rptr);
}

uint64_t ps_get48(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(6U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get48bit(&ps_data.rptr);
}

uint64_t ps_get56(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(7U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get56bit(&ps_data.rptr);
}

uint64_t ps_get64(void) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(8U+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return 0;
	}
	return get64bit(&(ps_data.rptr));
}

//safe version of gets into common buffer
void ps_dummyget(uint16_t skip) {	//skip bytes in data buffer
	if (ps_data.recdatasize < (uint32_t)(skip+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		ps_data.rptr = ps_data.rbuff + ps_data.recdatasize;
		return;
	}
	if (ps_data.rptr == NULL) {
		ps_data.status = 1;
	} else {
		ps_data.rptr += skip;
	}
}

//--------------------------------------------------

void ps_putbytes(const uint8_t *src, uint32_t leng) {
	if (ps_data.wptr == NULL) {
		ps_data.status = 1;
		return;
	}
	if (ps_data.wbuffsize < (uint32_t)(leng+ps_data.wptr-ps_data.wbuff)) {
		wbuffexpand(leng+(ps_data.wbuffsize*3)/2);
	}
	memcpy(ps_data.wptr,src,leng);
	ps_data.wptr += leng;
}

uint8_t ps_putstr(const char *str) {	//returns number of written bytes or zero if error
	uint32_t len;

	if (str==NULL || (len=strlen(str)) > 255) {// || len==0)
		ps_data.status = 1;
		return 0;
	}
	ps_put8((uint8_t)len);
	ps_putbytes((const uint8_t*)str, len);
	if (ps_data.status) {
		len = 0;
	}
	return len;
}

void ps_getbytes(uint8_t *dest, uint32_t leng) {
	if (ps_data.rptr==NULL || ps_data.recdatasize < (uint32_t)(leng+ps_data.rptr-ps_data.rbuff)) {
		fprintf(stderr,"Error - receive buffer overrun!\n");
		ps_data.status = 1;
		return;
	}
	memcpy(dest,ps_data.rptr,leng);
	ps_data.rptr += leng;
}

uint8_t ps_getstr(char str[256]) {	//returns read bytes count or zero if error
	uint8_t len;

	len = ps_get8();
	ps_getbytes((uint8_t *)str, len);
	if (ps_data.status) {
		len = 0;
	}
	str[len]='\0';
	return len;
}
