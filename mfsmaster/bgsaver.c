/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#include <stddef.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <poll.h>
#include <errno.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"
#include "datapack.h"
#include "crc.h"
#include "main.h"
#include "cfg.h"
#include "slogger.h"
#include "massert.h"
#include "sockets.h"
#include "mfsalloc.h"
#include "processname.h"
#include "clocks.h"
#include "errno.h"

#define MAXLOGNUMBER 1000U

enum {BGSAVER_ALIVE,BGSAVER_START,BGSAVER_WRITE,BGSAVER_FINISH,BGSAVER_DONE,BGSAVER_CHANGELOG,BGSAVER_CHANGELOG_ACK,BGSAVER_ROTATELOG,BGSAVER_TERMINATE};

enum {FREE,DATA,KILL}; // bgsaverconn.mode

#define PIPE_READ 0
#define PIPE_WRITE 1

#define MAX_STATUS_SIZE 100

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t data[1];
} out_packetstruct;

typedef struct in_packetstruct {
	struct in_packetstruct *next;
	uint32_t type,leng;
	uint8_t data[1];
} in_packetstruct;

typedef struct bgsaverconn {
	int data_pipe[2];
	int status_pipe[2];
	uint8_t mode;
	int32_t pdescpos_r,pdescpos_w;
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;
	void *ud;
	void (*donefn)(void*,int);
} bgsaverconn;

static bgsaverconn *bgsaversingleton=NULL;

static uint32_t BackLogsNumber;

static uint32_t changelog_delay = 0;
static double bgsaver_last_activity;
static double bgsaver_last_check;
static uint32_t bgsaver_last_check_count;
static uint32_t bgsaver_last_report;
static uint8_t terminating;
static uint8_t termdelay;

// child sync part

int32_t writeall(int sock,uint8_t *buff,uint32_t leng) {
	struct pollfd pfd;
	uint32_t bsent;
	int res;
	bsent = 0;
	while (bsent<leng) {
		res = write(sock,buff+bsent,leng-bsent);
		if (res<=0) {
			if (ERRNO_ERROR) {
				return -1;
			} else {
				pfd.fd = sock;
				pfd.revents = 0;
				pfd.events = POLLOUT;
				if (poll(&pfd,1,100)<0) {
					if (errno!=EINTR) {
						return -1;
					}
				}
			}
			res = 0;
		}
		bsent += res;
	}
	return bsent;
}

int32_t readall(int sock,uint8_t *buf,uint32_t leng) {
	struct pollfd pfd;
	uint32_t brecv;
	int res;
	brecv = 0;
	while (brecv<leng) {
		res = read(sock,buf+brecv,leng-brecv);
		if (res<=0) {
			if (ERRNO_ERROR) {
				return -1;
			} else {
				pfd.fd = sock;
				pfd.revents = 0;
				pfd.events = POLLIN;
				if (poll(&pfd,1,100)<0) {
					if (errno!=EINTR) {
						return -1;
					}
				}
			}
			res = 0;
		}
		brecv += res;
	}
	return brecv;
}


void bgsaver_worker(void) {
	bgsaverconn *eptr = bgsaversingleton;
	struct pollfd pfd;
	uint8_t auxbuff[12],*wptr;
	const uint8_t *rptr;
	int32_t l;
	uint32_t cmd;
	uint32_t leng;
	uint8_t *buff;
	uint32_t buffsize;
	uint64_t woffset;
	uint32_t wleng,wcrc;
	uint32_t speedlimit;
	uint64_t bytes;
	double starttime;
	double last_alive_send;
	uint8_t status;
	ssize_t ret;
	int fd;
	int logfd;
	int lf;
	char *chlogbuff;
	uint32_t chlogbuffsize;
	uint32_t chloglostcnt;
	uint32_t timestamp;
	static uint32_t last_timestamp;

	buff = NULL;
	buffsize = 0;
	fd = -1;
	bytes = 0;
	starttime = 0.0;
	speedlimit = 0;

	logfd = -1;
	chlogbuff = NULL;
	chlogbuffsize = 0;
	chloglostcnt = 0;

	pfd.fd = eptr->data_pipe[PIPE_READ];
	last_alive_send = monotonic_seconds();
	timestamp = 0;
	last_timestamp = 0;

	lf = open(".bgwriter.lock",O_RDWR,0666);
	if (lf<0) {
		mfs_errlog(LOG_ERR,"background data writer - can't open lockfile");
		goto err;
	}
	if (lockf(lf,F_TLOCK,0)<0) {
		mfs_errlog(LOG_ERR,"background data writer - can't get lock on lockfile");
		goto err;
	}

	for (;;) {
		if (last_alive_send+1.0<monotonic_seconds()) {
			wptr = auxbuff;
			put32bit(&wptr,BGSAVER_ALIVE);
			put32bit(&wptr,0);
			writeall(eptr->status_pipe[PIPE_WRITE],auxbuff,8);
			last_alive_send = monotonic_seconds();
		}
		pfd.revents = 0;
		pfd.events = POLLIN;
		poll(&pfd,1,100);
		if ((pfd.revents&(POLLERR|POLLHUP))!=0) {
			mfs_errlog_silent(LOG_ERR,"background data writer - HUP/ERR detected on data pipe");
			goto err;
		}
		if ((pfd.revents&POLLIN)==0) {
			continue;
		}
		l = readall(eptr->data_pipe[PIPE_READ],auxbuff,8);
		if (l!=8) {
			mfs_errlog_silent(LOG_ERR,"background data writer - reading pipe error");
			goto err;
		}
		rptr = auxbuff;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
		if (leng<=10000000) {
			if (leng>buffsize) {
				uint32_t newleng = (leng*3)/2;
				if (buff!=NULL) {
					free(buff);
				}
				buff = malloc(newleng);
				if (buff!=NULL) {
					buffsize = leng;
				} else {
					syslog(LOG_ERR,"background data writer - out of memory (alloc size: %"PRIu32")",newleng);
					goto err;
				}
			}
			l = readall(eptr->data_pipe[PIPE_READ],buff,leng);
		} else {
			syslog(LOG_ERR,"background data writer - packet too long (packet size: %"PRIu32")",leng);
			goto err;
		}
		if (l!=(int32_t)leng) {
			mfs_errlog_silent(LOG_ERR,"background data writer - reading pipe error");
			goto err;
		}
		switch (cmd) {
			case BGSAVER_START:
				if (leng!=4) {
					syslog(LOG_ERR,"background data writer - leng error (BGSAVER_START packet)");
					status = 0;
				} else {
					rptr = buff;
					speedlimit = get32bit(&rptr);
					if (fd>=0) {
						close(fd);
					}
					fd = open("metadata_download.tmp",O_WRONLY | O_TRUNC | O_CREAT,0666);
					if (fd<0) {
						mfs_errlog_silent(LOG_ERR,"background data writer - error opening 'metadata_download.tmp'");
						status = 0;
					} else {
						bytes = 0;
						starttime = monotonic_seconds();
						status = 1;
					}
				}
				break;
			case BGSAVER_WRITE:
				if (leng<16) {
					status = 0;
				} else {
					rptr = buff;
					woffset = get64bit(&rptr);
					wleng = get32bit(&rptr);
					wcrc = get32bit(&rptr);
					if (wleng!=leng-16) {
						syslog(LOG_ERR,"background data writer - leng error (BGSAVER_WRITE packet)");
						status = 0;
					} else {
#ifdef HAVE_PWRITE
						ret = pwrite(fd,rptr,wleng,woffset);
#else /* HAVE_PWRITE */
						lseek(fd,woffset,SEEK_SET);
						ret = write(fd,rptr,wleng);
#endif /* HAVE_PWRITE */
						if (ret!=(ssize_t)wleng) {
							mfs_errlog_silent(LOG_ERR,"background data writer - error writing 'metadata_download.tmp'");
							status = 0;
						} else if (wcrc != mycrc32(0,rptr,wleng)) {
							syslog(LOG_ERR,"background data writer - crc error (BGSAVER_WRITE packet)");
							status = 0;
						} else {
							if (speedlimit>0) {
								double seconds_passed = monotonic_seconds() - starttime;
								double expected_seconds;
								bytes += wleng;
								expected_seconds = (double)bytes / (double)speedlimit;
								if (expected_seconds>seconds_passed) {
									usleep((expected_seconds - seconds_passed) * 1000000);
								}
							}
							status = 1;
						}
					}
				}
				if (status==0) {
					close(fd);
					fd = -1;
				}
				break;
			case BGSAVER_FINISH:
				if (leng!=0) {
					syslog(LOG_ERR,"background data writer - leng error (BGSAVER_FINISH packet)");
					status = 0;
				} else {
					status = 1;
					if (fsync(fd)<0) {
						mfs_errlog_silent(LOG_ERR,"background data writer - error syncing 'metadata_download.tmp'");
						status = 0;
					}
					if (close(fd)<0) {
						mfs_errlog_silent(LOG_ERR,"background data writer - error closing 'metadata_download.tmp'");
						status = 0;
					}
					fd = -1;
				}
				break;
			case BGSAVER_CHANGELOG:
				status = 0;
				if (leng>=12) {
					uint64_t version;

					rptr = buff;
					version = get64bit(&rptr);
					timestamp = get32bit(&rptr);
					leng -= 12;

					if (logfd<0) {
						logfd = open("changelog.0.mfs",O_WRONLY | O_CREAT | O_APPEND,0666);
					}
					if (logfd>=0) {
						if (leng+50>chlogbuffsize) {
							if (chlogbuff==NULL) {
								chlogbuff = malloc(leng+500);
								if (chlogbuff==NULL) {
									chlogbuffsize = 0;
								} else {
									chlogbuffsize = leng+500;
								}
							} else {
								char *prevptr = chlogbuff;
								chlogbuff = realloc(chlogbuff,leng+500);
								if (chlogbuff==NULL) {
									free(prevptr);
									chlogbuffsize = 0;
								} else {
									chlogbuffsize = leng+500;
								}
							}
						}
						if (chlogbuff!=NULL) {
							wleng = snprintf(chlogbuff,chlogbuffsize,"%"PRIu64": %s\n",version,rptr);
							if (write(logfd,chlogbuff,wleng)==(ssize_t)wleng) {
								status = 1;
							} else {
								mfs_errlog_silent(LOG_ERR,"background data writer - error writing 'changelog.0.mfs'");
							}
						}
					} else {
						mfs_errlog_silent(LOG_ERR,"background data writer - error opening 'changelog.0.mfs'");
					}
				}
				break;
			case BGSAVER_ROTATELOG:
				if (leng!=0) {
					status = 0;
				} else {
					char logname1[100],logname2[100];
					uint32_t i;

					status = 1;
					if (logfd>=0) {
						if (fsync(logfd)<0) {
							mfs_errlog_silent(LOG_ERR,"background data writer - error syncing 'changelog.0.mfs'");
							status = 0;
						}
						if (close(logfd)<0) {
							mfs_errlog_silent(LOG_ERR,"background data writer - error closing 'changelog.0.mfs'");
							status = 0;
						}
						logfd=-1;
					}
					if (BackLogsNumber>0) {
						for (i=BackLogsNumber ; i>0 ; i--) {
							snprintf(logname1,100,"changelog.%"PRIu32".mfs",i);
							snprintf(logname2,100,"changelog.%"PRIu32".mfs",i-1);
							if (rename(logname2,logname1)<0) {
								if (errno!=ENOENT) {
									mfs_arg_errlog_silent(LOG_ERR,"background data writer - error renaming '%s'->'%s'",logname2,logname1);
								}
							}
						}
					} else {
						if (unlink("changelog.0.mfs")<0) {
							if (errno!=ENOENT) {
								mfs_errlog_silent(LOG_ERR,"background data writer - error deleting 'changelog.0.mfs'");
							}
						}
					}
				}
				break;
			case BGSAVER_TERMINATE:
				syslog(LOG_NOTICE,"background data writer - terminating");
				if (logfd>=0) {
					fsync(logfd);
					close(logfd);
				}
				if (fd>=0) {
					syslog(LOG_NOTICE,"background data writer - removing unfinished metadata file");
					close(fd);
					unlink("metadata_download.tmp");
				}
				goto err;
				break; // just silent compiler warnings
			default:
				syslog(LOG_ERR,"background data writer - got unrecognized command (%"PRIu32")",cmd);
				goto err;
		}
		if (cmd==BGSAVER_START || cmd==BGSAVER_WRITE || cmd==BGSAVER_FINISH) { // status required
			if (status==0) {
				unlink("metadata_download.tmp");
			}
			wptr = auxbuff;
			put32bit(&wptr,BGSAVER_DONE);
			put32bit(&wptr,1);
			*wptr = status;
			writeall(eptr->status_pipe[PIPE_WRITE],auxbuff,9);
		} else if (cmd==BGSAVER_CHANGELOG || cmd==BGSAVER_ROTATELOG) { // status only used for logging
			if (status==0) {
				if (chloglostcnt==0) {
					syslog(LOG_WARNING,"changelog lost !!!");
				} else if (chloglostcnt==100000) {
					syslog(LOG_WARNING,"next 100000 changelogs are lost !!!");
					chloglostcnt = 0;
				}
				chloglostcnt++;
			}
			if (status==1) {
				if (cmd==BGSAVER_CHANGELOG && timestamp>=last_timestamp) {
					last_timestamp = timestamp;
					wptr = auxbuff;
					put32bit(&wptr,BGSAVER_CHANGELOG_ACK);
					put32bit(&wptr,4);
					put32bit(&wptr,timestamp);
					writeall(eptr->status_pipe[PIPE_WRITE],auxbuff,12);
				}
				chloglostcnt = 0;
			}
		}
	}
err:
	if (buff!=NULL) {
		free(buff);
	}
	if (chlogbuff!=NULL) {
		free(chlogbuff);
	}
	if (lf>=0) {
		if (lockf(lf,F_ULOCK,0)<0) { // pro forma
			syslog(LOG_NOTICE,"background data writer - error removing lock from lockfile");
		}
		close(lf);
	}
	syslog(LOG_NOTICE,"background data writer - exiting");
	exit(0);
}




// async part

uint8_t* bgsaver_createpacket(bgsaverconn *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

void bgsaver_cancel(void) {
	bgsaverconn *eptr = bgsaversingleton;

	if (eptr==NULL || eptr->mode!=DATA) {
		return;
	}

	bgsaver_createpacket(eptr,BGSAVER_FINISH,0);
	eptr->ud = NULL;
	eptr->donefn = NULL;
}


void bgsaver_open(uint32_t speedlimit,void *ud,void (*donefn)(void*,int)) {
	bgsaverconn *eptr = bgsaversingleton;
	uint8_t *buff;

	if (eptr==NULL || eptr->mode!=DATA) {
		donefn(ud,-1);
		return;
	}

	buff = bgsaver_createpacket(eptr,BGSAVER_START,4);
	put32bit(&buff,speedlimit);
	eptr->ud = ud;
	eptr->donefn = donefn;
}

void bgsaver_store(const uint8_t *data,uint64_t offset,uint32_t leng,uint32_t crc,void *ud,void (*donefn)(void*,int)) {
	bgsaverconn *eptr = bgsaversingleton;
	uint8_t *buff;

	if (eptr==NULL || eptr->mode!=DATA) {
		donefn(ud,-1);
		return;
	}

	buff = bgsaver_createpacket(eptr,BGSAVER_WRITE,16+leng);
	put64bit(&buff,offset);
	put32bit(&buff,leng);
	put32bit(&buff,crc);
	memcpy(buff,data,leng);
	eptr->ud = ud;
	eptr->donefn = donefn;
}

void bgsaver_close(void *ud,void (*donefn)(void*,int)) {
	bgsaverconn *eptr = bgsaversingleton;

	if (eptr==NULL || eptr->mode!=DATA) {
		donefn(ud,-1);
		return;
	}

	bgsaver_createpacket(eptr,BGSAVER_FINISH,0);
	eptr->ud = ud;
	eptr->donefn = donefn;
}

void bgsaver_done(bgsaverconn *eptr,const uint8_t *data,uint32_t length) {
	void *ud;
	void (*donefn)(void*,int);

	ud = eptr->ud;
	donefn = eptr->donefn;
	eptr->donefn = NULL;
	eptr->ud = NULL;

	if (length!=1) {
		syslog(LOG_WARNING,"mallformed packet from bgworker");
		eptr->mode = KILL;
		if (donefn!=NULL) {
			donefn(ud,-1);
		}
		return;
	}
	if (donefn!=NULL) {
		donefn(ud,*data);
	}
}

void bgsaver_changelog(uint64_t version,const char *message) {
	bgsaverconn *eptr = bgsaversingleton;
	uint32_t l;
	uint8_t *buff;

	if (terminating) {
		syslog(LOG_WARNING,"changelog received during termination - changelog line lost");
	}
	if (eptr==NULL || eptr->mode!=DATA) {
		syslog(LOG_WARNING,"problems with data write subprocess detected - changelog line lost - force termination");
		main_exit();
		return;
	}
	l = strlen(message);

	buff = bgsaver_createpacket(eptr,BGSAVER_CHANGELOG,8+4+l+1);
	put64bit(&buff,version);
	put32bit(&buff,main_time());
	memcpy(buff,message,l+1); // copy message with ending zero
}

void bgsaver_rotatelog(void) {
	bgsaverconn *eptr = bgsaversingleton;

	if (eptr==NULL || eptr->mode!=DATA) {
		return;
	}

	bgsaver_createpacket(eptr,BGSAVER_ROTATELOG,0);
}

void bgsaver_changelog_ack(bgsaverconn *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	uint32_t timestamp,timestamp_ack;

	if (length!=4) {
		syslog(LOG_WARNING,"mallformed packet from bgworker");
		eptr->mode = KILL;
		return;
	}
	rptr = data;
	timestamp_ack = get32bit(&rptr);
	timestamp = main_time();
	if (timestamp > timestamp_ack) {
		changelog_delay = timestamp - timestamp_ack - 1;
	} else {
		changelog_delay = 0;
	}
}

void bgsaver_alive(bgsaverconn *eptr,const uint8_t *data,uint32_t length) {
	(void)eptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_WARNING,"mallformed packet from bgworker");
		eptr->mode = KILL;
		return;
	}
	bgsaver_last_activity = monotonic_seconds();
}

void bgsaver_gotpacket(bgsaverconn *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case BGSAVER_DONE:
			bgsaver_done(eptr,data,length);
			break;
		case BGSAVER_CHANGELOG_ACK:
			bgsaver_changelog_ack(eptr,data,length);
			break;
		case BGSAVER_ALIVE:
			bgsaver_alive(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
	}
}

void bgsaver_read(bgsaverconn *eptr) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;

	if (eptr == NULL) {
		if (readbuff != NULL) {
			free(readbuff);
		}
		readbuff = NULL;
		readbuffsize = 0;
		return;
	}

	if (readbuffsize==0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		passert(readbuff);
	}

	rbleng = 0;
	err = 0;
	hup = 0;
	for (;;) {
		i = read(eptr->status_pipe[PIPE_READ],readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
			}
			break;
		} else {
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = mfsrealloc(readbuff,readbuffsize);
				passert(readbuff);
			} else {
				break;
			}
		}
	}

	rbpos = 0;
	while (rbpos<rbleng) {
		if ((rbleng-rbpos)>=eptr->input_bytesleft) {
			memcpy(eptr->input_startptr,readbuff+rbpos,eptr->input_bytesleft);
			i = eptr->input_bytesleft;
		} else {
			memcpy(eptr->input_startptr,readbuff+rbpos,rbleng-rbpos);
			i = rbleng-rbpos;
		}
		rbpos += i;
		eptr->input_startptr+=i;
		eptr->input_bytesleft-=i;

		if (eptr->input_bytesleft>0) {
			break;
		}

		if (eptr->input_packet == NULL) {
			ptr = eptr->input_hdr;
			type = get32bit(&ptr);
			leng = get32bit(&ptr);

			if (leng>MAX_STATUS_SIZE) {
				syslog(LOG_WARNING,"bgworker packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MAX_STATUS_SIZE,type);
				eptr->input_end = 1;
				return;
			}

			eptr->input_packet = malloc(offsetof(in_packetstruct,data)+leng);
			passert(eptr->input_packet);
			eptr->input_packet->next = NULL;
			eptr->input_packet->type = type;
			eptr->input_packet->leng = leng;

			eptr->input_startptr = eptr->input_packet->data;
			eptr->input_bytesleft = leng;
		}

		if (eptr->input_bytesleft>0) {
			continue;
		}

		if (eptr->input_packet != NULL) {
			*(eptr->inputtail) = eptr->input_packet;
			eptr->inputtail = &(eptr->input_packet->next);
			eptr->input_packet = NULL;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
		}
	}

	if (hup) {
		syslog(LOG_NOTICE,"connection was reset by bgworker");
		eptr->input_end = 1;
	} else if (err) {
		mfs_errlog_silent(LOG_NOTICE,"read from bgworker error");
		eptr->input_end = 1;
	}
}

void bgsaver_parse(bgsaverconn *eptr) {
	in_packetstruct *ipack;

	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL) {
		bgsaver_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
		eptr->inputhead = ipack->next;
		free(ipack);
		if (eptr->inputhead==NULL) {
			eptr->inputtail = &(eptr->inputhead);
		}
	}
	if (eptr->mode==DATA && eptr->inputhead==NULL && eptr->input_end) {
		eptr->mode = KILL;
	}
}

void bgsaver_write(bgsaverconn *eptr) {
	out_packetstruct *opack;
	int32_t i;
#ifdef HAVE_WRITEV
	struct iovec iovtab[100];
	uint32_t iovdata;
	uint32_t leng;
	uint32_t left;

	for (;;) {
		leng = 0;
		for (iovdata=0,opack=eptr->outputhead ; iovdata<100 && opack!=NULL ; iovdata++,opack=opack->next) {
			iovtab[iovdata].iov_base = opack->startptr;
			iovtab[iovdata].iov_len = opack->bytesleft;
			leng += opack->bytesleft;
		}
		if (iovdata==0) {
			return;
		}
		i = writev(eptr->data_pipe[PIPE_WRITE],iovtab,iovdata);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		left = i;
		while (left>0 && eptr->outputhead!=NULL) {
			opack = eptr->outputhead;
			if (opack->bytesleft>left) {
				opack->startptr+=left;
				opack->bytesleft-=left;
				left = 0;
			} else {
				left -= opack->bytesleft;
				eptr->outputhead = opack->next;
				if (eptr->outputhead==NULL) {
					eptr->outputtail = &(eptr->outputhead);
				}
				free(opack);
			}
		}
		if ((uint32_t)i < leng) {
			return;
		}
	}
#else
	for (;;) {
		opack = eptr->outputhead;
		if (opack==NULL) {
			return;
		}
		i=write(eptr->data_pipe[PIPE_WRITE],opack->startptr,opack->bytesleft);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		opack->startptr+=i;
		opack->bytesleft-=i;
		if (opack->bytesleft>0) {
			return;
		}
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}


void bgsaver_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	bgsaverconn *eptr = bgsaversingleton;

	eptr->pdescpos_r = -1;
	eptr->pdescpos_w = -1;
	if (eptr->mode==FREE || eptr->data_pipe[1]<0 || eptr->status_pipe[0]<0) {
		return;
	}

	if (eptr->mode==DATA && eptr->input_end==0) {
		pdesc[pos].fd = eptr->status_pipe[0];
		pdesc[pos].events = POLLIN;
		eptr->pdescpos_r = pos;
		pos++;
	}

	if (eptr->mode==DATA && eptr->outputhead!=NULL) {
		pdesc[pos].fd = eptr->data_pipe[1];
		pdesc[pos].events = POLLOUT;
		eptr->pdescpos_w = pos;
		pos++;
	}

	*ndesc = pos;
}

void bgsaver_disconnection_check(void) {
	bgsaverconn *eptr = bgsaversingleton;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	if (eptr->mode == KILL) {
		close(eptr->data_pipe[PIPE_WRITE]);
		close(eptr->status_pipe[PIPE_READ]);
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
		eptr->data_pipe[PIPE_WRITE] = -1;
		eptr->status_pipe[PIPE_READ] = -1;
		eptr->input_packet = NULL;
		eptr->inputhead = NULL;
		eptr->inputtail = &(eptr->inputhead);
		eptr->outputhead = NULL;
		eptr->outputtail = &(eptr->outputhead);
		eptr->mode = FREE;
		if (terminating==0) {
			syslog(LOG_ERR,"connection lost with background data writer - exiting");
			main_exit();
		}
	}
}

void bgsaver_serve(struct pollfd *pdesc) {
	bgsaverconn *eptr = bgsaversingleton;

	if (eptr->pdescpos_r>=0) {
		if ((pdesc[eptr->pdescpos_r].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode==DATA) {
			bgsaver_read(eptr);
		}
		if (pdesc[eptr->pdescpos_r].revents & (POLLERR|POLLHUP)) {
			eptr->input_end = 1;
		}
		bgsaver_parse(eptr);
	}
	if (eptr->pdescpos_w>=0) {
		if ((((pdesc[eptr->pdescpos_w].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos_w].revents & POLLOUT)) && (eptr->mode==DATA)) {
			bgsaver_write(eptr);
		}
	}
	bgsaver_disconnection_check();
}

void bgsaver_alive_check(void) {
	double now = monotonic_seconds();

	if (bgsaver_last_check + 5.0 < now) { // after long loop
		bgsaver_last_check_count = 0;
	} else if (bgsaver_last_check_count<5) {
		bgsaver_last_check_count++;
	}
	bgsaver_last_check = now;

	if (bgsaver_last_check_count >= 5) { // check is active
		if (now - bgsaver_last_activity > bgsaver_last_report + 50) {
			bgsaver_last_report += 50;
			if (bgsaver_last_report<300) {
				syslog(LOG_WARNING,"background data writer is not responding (last ping received more than %u seconds ago)",bgsaver_last_report);
			} else {
				syslog(LOG_ERR,"background data writer is not responding (last ping received more than %u seconds ago) - terminating",bgsaver_last_report);
				main_exit();
			}
		} else if (now - bgsaver_last_activity < 5.0) {
			bgsaver_last_report = 0;
		}
	}
}

void bgsaver_reload(void) {
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	if (BackLogsNumber>MAXLOGNUMBER) {
		syslog(LOG_WARNING,"BACK_LOGS value too big !!!");
		BackLogsNumber = MAXLOGNUMBER;
	}
}

void bgsaver_termcheck(void) {
	bgsaverconn *eptr = bgsaversingleton;

	termdelay++;
	if (termdelay>2 && terminating==0) {
		terminating = 1;
		if (eptr->mode==DATA) {
			bgsaver_createpacket(eptr,BGSAVER_TERMINATE,0);
		}
	}
}

void bgsaver_wantexit(void) {

	main_time_register(1,0,bgsaver_termcheck);
}

int bgsaver_canexit(void) {
	bgsaverconn *eptr = bgsaversingleton;
	return (eptr->mode==FREE||bgsaver_last_report>0)?1:0;
}

void bgsaver_term(void) {
	bgsaverconn *eptr = bgsaversingleton;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	if (eptr->mode!=FREE) {
		close(eptr->data_pipe[PIPE_WRITE]);
		close(eptr->status_pipe[PIPE_READ]);
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
	}

	bgsaver_read(NULL); // free internal read buffer

	free(eptr);
	bgsaversingleton = NULL;
}

int bgsaver_init(void) {
	int lf;
	int e;
	bgsaverconn *eptr;

	lf = open(".bgwriter.lock",O_RDWR|O_CREAT,0666);
	if (lf<0) {
		mfs_errlog(LOG_ERR,"can't create bgsaver lockfile");
		return -1;
	}
	if (lockf(lf,F_TEST,0)<0) {
		mfs_errlog(LOG_ERR,"bgsaver lock exists");
		close(lf);
		return -1;
	}
	close(lf);

	bgsaver_reload();

	eptr = bgsaversingleton = malloc(sizeof(bgsaverconn));
	passert(eptr);

	if (pipe(eptr->data_pipe)<0) {
		mfs_errlog(LOG_ERR,"can't create pipe");
		return -1;
	}
	if (pipe(eptr->status_pipe)<0) {
		mfs_errlog(LOG_ERR,"can't create pipe");
		return -1;
	}
	univnonblock(eptr->data_pipe[PIPE_READ]);
	univnonblock(eptr->data_pipe[PIPE_WRITE]);
	univnonblock(eptr->status_pipe[PIPE_READ]);
	univnonblock(eptr->status_pipe[PIPE_WRITE]);
	e = fork();
	if (e<0) {
		mfs_errlog(LOG_ERR,"fork error");
		return -1;
	}
	if (e==0) {
		int f;
		f = open("/dev/null", O_RDWR, 0);
		close(STDIN_FILENO);
		sassert(dup(f)==STDIN_FILENO);
		close(STDOUT_FILENO);
		sassert(dup(f)==STDOUT_FILENO);
		close(STDERR_FILENO);
		sassert(dup(f)==STDERR_FILENO);
		processname_set("mfsmaster (data writer)");
		close(eptr->data_pipe[PIPE_WRITE]);
		close(eptr->status_pipe[PIPE_READ]);
		bgsaver_worker();
		exit(0);
	}
	close(eptr->data_pipe[PIPE_READ]);
	close(eptr->status_pipe[PIPE_WRITE]);

	eptr->mode = DATA;
	eptr->input_end = 0;
	eptr->input_bytesleft = 8;
	eptr->input_startptr = eptr->input_hdr;
	eptr->input_packet = NULL;
	eptr->inputhead = NULL;
	eptr->inputtail = &(eptr->inputhead);
	eptr->outputhead = NULL;
	eptr->outputtail = &(eptr->outputhead);
	eptr->pdescpos_r = -1;
	eptr->pdescpos_w = -1;

	terminating = 0;
	bgsaver_last_activity = monotonic_seconds();
	bgsaver_last_check = monotonic_seconds();
	bgsaver_last_check_count = 0;
	bgsaver_last_report = 0;

	termdelay = 0;
	main_wantexit_register(bgsaver_wantexit);
	main_time_register(1,0,bgsaver_alive_check);
	main_canexit_register(bgsaver_canexit);
	main_reload_register(bgsaver_reload);
	main_destruct_register(bgsaver_term);
	main_poll_register(bgsaver_desc,bgsaver_serve);
	return 0;
}
