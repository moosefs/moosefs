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

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/file.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>

#ifdef HAVE_LINUX_NBD_H
#include <linux/nbd.h>
#else
#define NBD_REQUEST_MAGIC 0x25609513
#define NBD_REPLY_MAGIC 0x67446698
enum {
	NBD_CMD_READ = 0,
	NBD_CMD_WRITE = 1,
	NBD_CMD_DISC = 2,
	NBD_CMD_FLUSH = 3,
	NBD_CMD_TRIM = 4
};
#define NBD_SET_SOCK    _IO( 0xab, 0 )
#define NBD_SET_BLKSIZE _IO( 0xab, 1 )
#define NBD_SET_SIZE    _IO( 0xab, 2 )
#define NBD_DO_IT       _IO( 0xab, 3 )
#define NBD_CLEAR_SOCK  _IO( 0xab, 4 )
#define NBD_CLEAR_QUE   _IO( 0xab, 5 )
#define NBD_PRINT_DEBUG _IO( 0xab, 6 )
#define NBD_SET_SIZE_BLOCKS     _IO( 0xab, 7 )
#define NBD_DISCONNECT  _IO( 0xab, 8 )
#define NBD_SET_TIMEOUT _IO( 0xab, 9 )
#define NBD_SET_FLAGS   _IO( 0xab, 10)
#endif

#ifdef HAVE_LINUX_FS_H
#include <linux/fs.h>
#else
#define BLKGETSIZE64 _IOR(0x12,114,size_t)
#endif

#include "mfsio.h"
#include "lwthread.h"
#include "datapack.h"
#include "portable.h"
#include "squeue.h"
#include "workers.h"
#include "strerr.h"
#include "massert.h"
#include "sizestr.h"
#include "sockets.h"
#include "processname.h"
// #include "idstr.h"

#define READ_TOMS 1000
#define WRITE_TOMS 1000

enum {
	MFSNBD_NOP,
	MFSNBD_STOP,
	MFSNBD_ADD,
	MFSNBD_REMOVE,
	MFSNBD_LIST,
	MFSNBD_RESIZE
};

enum {
	MFSNBD_OK,
	MFSNBD_ERROR
};

#define NBD_LINK_PREFIX "/dev/mfs/"
#define NBD_LINK_PREFIX_LENG 9

#define NBD_ERR_SIZE 200

static int NbdTimeout = 1800;

/*

NAME:
	leng:8 data:8*leng
PATH:
	leng:16 data:8*leng

MFSNBD_STOP:
-> -
<- status:8

MFSNBD_ADD:
-> mfspath:PATH device:NAME linkname:NAME size:64
<- status:8 answer:NAME

MFSNBD_REMOVE:
-> mfspath:PATH device:NAME linkname:NAME
<- status:8 answer:NAME

MFSNBD_LIST:
-> -
<- devices:8 devices * [ mfspath:PATH device:NAME linkname:NAME size:64 ]

MFSNBD_RESIZE:
-> mfspath:PATH device:NAME linkname:NAME size:64
<- status:8

*/

#define FLAG_READONLY 1
#define FLAG_IGNORELOCK 2

typedef struct nbdcommon {
	char *linkname;
	char *nbddevice;
	char *mfsfile;
	uint64_t fsize;
	uint32_t bsize;
	uint32_t flags;
	int sp[2];
	int mfsfd;
	int nbdfd;
	pthread_t ctrl_thread;
	pthread_t recv_thread;
	pthread_t send_thread;
	void *aqueue; // per bdev answer queues
} nbdcommon;

typedef struct nbdrequest {
	nbdcommon *nbdcp;
	uint8_t handle[8];
	uint64_t offset;
	uint32_t length;
	uint16_t cmd;
	uint16_t cmdflags;
	uint32_t status;
	uint8_t data[1];
} nbdrequest;

typedef struct _bdlist {
	nbdcommon *nbdcp;
	struct _bdlist *next;
} bdlist;

static void *workers_set;

static bdlist *bdhead;

static mfscfg mcfg;

#ifdef NBD_DEBUG
static const char* nbd_cmd_str(uint32_t cmd) {
	switch (cmd) {
		case NBD_CMD_READ:
			return "READ";
		case NBD_CMD_WRITE:
			return "WRITE";
		case NBD_CMD_DISC:
			return "DISC";
		case NBD_CMD_FLUSH:
			return "FLUSH";
		case NBD_CMD_TRIM:
			return "TRIM";
	}
	return "???";
}
#endif

int32_t writeall(int sock,uint8_t *buff,uint32_t leng) {
	uint32_t bsent;
	int res;
	bsent = 0;
	while (bsent<leng) {
		res = write(sock,buff+bsent,leng-bsent);
		if (res<0) {
			return -1;
		}
		bsent += res;
	}
	return bsent;
}

int32_t readall(int sock,uint8_t *buf,uint32_t leng) {
	uint32_t brecv;
	int res;
	brecv = 0;
	while (brecv<leng) {
		res = read(sock,buf+brecv,leng-brecv);
		if (res<=0) {
			return -1;
		}
		brecv += res;
	}
	return brecv;
}

int32_t skipall(int sock,uint32_t leng) {
	static uint8_t skipbuff[16384];
	uint32_t brecv;
	int res;
	brecv = 0;
	while (brecv<leng) {
		if (leng-brecv>16384) {
			res = read(sock,skipbuff,16384);
		} else {
			res = read(sock,skipbuff,leng-brecv);
		}
		if (res<=0) {
			return -1;
		}
		brecv += res;
	}
	return brecv;
}

void nbd_worker_fn(void *data,uint32_t workerscnt) {
	nbdrequest *r = (nbdrequest*)data;
	nbdcommon *nbdcp = r->nbdcp;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"worker function for %s got request (cmd:%s)",nbdcp->nbddevice,nbd_cmd_str(r->cmd));
	(void)workerscnt;
	switch (r->cmd) {
		case NBD_CMD_READ:
			if (r->offset + r->length > nbdcp->fsize) {
				r->status = EOVERFLOW;
			} else {
				if (mfs_pread(nbdcp->mfsfd,r->data,r->length,r->offset)<0) {
					r->status = errno;
				} else {
					r->status = errno;
				}
			}
			break;
		case NBD_CMD_WRITE:
			if (nbdcp->flags & FLAG_READONLY) {
				r->status = EROFS;
			} else if (r->offset + r->length > nbdcp->fsize) {
				r->status = EOVERFLOW;
			} else {
				if (mfs_pwrite(nbdcp->mfsfd,r->data,r->length,r->offset)<0) {
					r->status = errno;
				} else {
					r->status = 0;
				}
			}
			break;
		case NBD_CMD_DISC:
			mfs_fsync(nbdcp->mfsfd);
			r->status = 0;
			break;
#ifdef NBD_CMD_FLUSH
		case NBD_CMD_FLUSH:
			if (nbdcp->flags & FLAG_READONLY) {
				r->status = EROFS;
			} else if (mfs_fsync(nbdcp->mfsfd)<0) {
				r->status = errno;
			} else {
				r->status = 0;
			}
#endif
		default:
			// ignore other commands
			r->status = 0;
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"worker function for %s enqueue status %u (cmd:%s)",nbdcp->nbddevice,r->status,nbd_cmd_str(r->cmd));
	squeue_put(nbdcp->aqueue,r);
}

void* receive_thread(void *arg) {
	uint8_t commbuff[28];
	const uint8_t *rptr;
	const uint8_t *handleptr;
	uint8_t *wptr;
	uint32_t bytesread;
	uint32_t magic;
	uint16_t cmdflags;
	uint32_t cmd;
	uint64_t offset;
	uint32_t length;
	int res;
	nbdrequest *r;
	nbdcommon *nbdcp = (nbdcommon*)arg;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"receive thread for %s started",nbdcp->nbddevice);
	bytesread = 0;
	for (;;) {
		res = read(nbdcp->sp[0],commbuff+bytesread,28-bytesread);
		if (res<=0) {
			// disconnect - simulate NBD_CMD_DISC
			wptr = commbuff;
			put32bit(&wptr,NBD_REQUEST_MAGIC);
			put32bit(&wptr,NBD_CMD_DISC);
			memset(wptr,0,20);
			bytesread = 28;
		} else {
			bytesread += res;
		}
		if (bytesread<28) {
			continue;
		}
		bytesread = 0;
		rptr = commbuff;
		magic = get32bit(&rptr);
		cmdflags = get16bit(&rptr);
		cmd = get16bit(&rptr);
		handleptr = rptr;
		rptr += 8; // skip handle
		offset = get64bit(&rptr);
		length = get32bit(&rptr);
		if (magic!=NBD_REQUEST_MAGIC) { // desync - simulate NBD_CMD_DISC
			cmd = NBD_CMD_DISC;
		}
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"receive thread for %s got request (cmd:%s)",nbdcp->nbddevice,nbd_cmd_str(cmd));
		if (cmd==NBD_CMD_WRITE || cmd==NBD_CMD_READ) {
			r = (nbdrequest*)malloc(offsetof(nbdrequest,data)+length);
		} else {
			// r = (nbdrequest*)malloc(offsetof(nbdrequest,data)); <- this is how this line should look like, but compilers "know" better than programmers how to create good code nowadays
			r = (nbdrequest*)malloc(sizeof(nbdrequest)); // seriously - dynamic field in data structure is too hard to grasp for you (compiler makers) ???
		}
		passert(r);
		r->nbdcp = nbdcp;
		memcpy(r->handle,handleptr,8);
		r->offset = offset;
		r->length = length;
		r->cmd = cmd;
		r->cmdflags = cmdflags;
		if (cmd==NBD_CMD_WRITE) {
			readall(nbdcp->sp[0],r->data,length);
		}
//		nbd_worker_fn(r,0);
		workers_newjob(workers_set,r);
		if (cmd==NBD_CMD_DISC) {
			return NULL;
		}
	}
}

void* send_thread(void *arg) {
	uint8_t commbuff[16];
	uint8_t *wptr;
	nbdrequest *r;
	void *data;
	nbdcommon *nbdcp = (nbdcommon*)arg;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"send thread for %s started",nbdcp->nbddevice);
	wptr = commbuff;
	put32bit(&wptr,NBD_REPLY_MAGIC);
	for (;;) {
		squeue_get(nbdcp->aqueue,&data);
		if (data==NULL) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"send thread for %s ending (data==NULL)",nbdcp->nbddevice);
			return NULL;
		}
		r = (nbdrequest*)data;
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"send thread for %s got status %u for request (cmd:%s)",nbdcp->nbddevice,r->status,nbd_cmd_str(r->cmd));
		wptr = commbuff+4;
		put32bit(&wptr,r->status);
		memcpy(wptr,r->handle,8);
		writeall(nbdcp->sp[0],commbuff,16);
		if (r->cmd==NBD_CMD_READ && r->status==0) {
			writeall(nbdcp->sp[0],r->data,r->length);
		}
		if (r->cmd==NBD_CMD_DISC) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"send thread for %s ending (cmd:DISC)",nbdcp->nbddevice);
			return NULL;
		}
		free(r);
	}
}

void* nbd_controller_thread(void *arg) {
	nbdcommon *nbdcp = ((nbdcommon*)(arg));
	int err;

	err = ioctl(nbdcp->nbdfd, NBD_SET_SOCK, nbdcp->sp[1]);
	if (err<0) {
		ioctl(nbdcp->nbdfd, NBD_CLEAR_QUE);
		ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
//		exit(1);
		return NULL;
	}
#if defined NBD_SET_FLAGS && defined NBD_FLAG_SEND_FLUSH && defined NBD_CMD_FLUSH
	err = ioctl(nbdcp->nbdfd, NBD_SET_FLAGS, NBD_FLAG_SEND_FLUSH);
	if (err<0) {
		ioctl(nbdcp->nbdfd, NBD_CLEAR_QUE);
		ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
//		exit(1);
		return NULL;
	}
#endif
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"controller thread for %s performs DO_IT ioctl",nbdcp->nbddevice);
//	fprintf(stderr,"waiting for peer to finish ...\n");
	ioctl(nbdcp->nbdfd, NBD_DO_IT); // this will wait
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"controller thread for %s finished",nbdcp->nbddevice);
//	fprintf(stderr,"... finished\n");
	ioctl(nbdcp->nbdfd, NBD_CLEAR_QUE);
	ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
	return NULL;
}

static uint8_t term;

void termhandle(int signo) {
	term = 1;
	(void)signo;
}

void set_signals(void) {
	struct sigaction sa;

#ifdef SA_RESTART
	sa.sa_flags = SA_RESTART;
#else
	sa.sa_flags = 0;
#endif
	sigemptyset(&sa.sa_mask);
	sa.sa_handler = termhandle;
	sigaction(SIGTERM,&sa,(struct sigaction *)0);
	sigaction(SIGINT,&sa,(struct sigaction *)0);
	sa.sa_handler = SIG_IGN;
	sigaction(SIGPIPE,&sa,(struct sigaction *)0);
}

void make_daemon(void) {
	int f;
	int pipefd[2];

	fflush(stdout);
	fflush(stderr);

	if (pipe(pipefd)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"daemonize, pipe error: %s",strerror(errno));
		exit(1);
	}

	f = fork();
	if (f<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"daemonize, first fork error: %s",strerror(errno));
		exit(1);
	}
	if (f>0) {
		int status;
		char buf;
		close(pipefd[1]); // close unused write end
		while (read(pipefd[0], &buf, 1) > 0) {
			status = write(STDOUT_FILENO, &buf, 1); // ignore status
		}
		waitpid(f,&status,0);
		exit(0);
	}
	if (chdir("/")<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't change working directory to '/': %s",strerror(errno));
	}
	setsid();
	setpgid(0,getpid());
	f = fork();
	if (f<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"daemonize, second fork error: %s",strerror(errno));
		exit(1);
	}
	if (f>0) {
		close(pipefd[0]);
		close(pipefd[1]);
		exit(0);
	}

	set_signals();
	close(pipefd[0]);

	f = open("/dev/null", O_RDWR, 0);
	close(STDIN_FILENO);
	sassert(dup(f)==STDIN_FILENO);
	close(STDOUT_FILENO);
	sassert(dup(f)==STDOUT_FILENO);
	close(STDERR_FILENO);
	sassert(dup(pipefd[1])==STDERR_FILENO);
	close(f);
	close(pipefd[1]);
}

static inline char charconv(char c) {
	if ((c>='0' && c<='9') || (c>='A' && c<='Z') || (c>='a' && c<='z') || (c=='.') || (c=='-')) {
		return c;
	} else {
		return '_';
	}
}

char* linkname_generate(char *linkname,const char *masterhost,const char *masterport,const char *filename) {
	uint32_t mhl,mpl,fnl,l,i;
	char *ln;

	if (linkname!=NULL) {
		fnl = strlen(linkname);

		ln = malloc(fnl+NBD_LINK_PREFIX_LENG+1);
		passert(ln);

		memcpy(ln,NBD_LINK_PREFIX,NBD_LINK_PREFIX_LENG);
		l = 9;
		for (i=0 ; i<fnl ; i++) {
			ln[l++] = charconv(linkname[i]);
		}
		free(linkname);
	} else {
		mhl = strlen(masterhost);
		mpl = strlen(masterport);
		fnl = strlen(filename);

		ln = malloc(mhl+mpl+fnl+3+1+NBD_LINK_PREFIX_LENG);
		passert(ln);

		memcpy(ln,NBD_LINK_PREFIX,NBD_LINK_PREFIX_LENG);
		l=9;
		for (i=0 ; i<mhl ; i++) {
			ln[l++] = charconv(masterhost[i]);
		}
		ln[l++] = '_';
		for (i=0 ; i<mpl ; i++) {
			ln[l++] = charconv(masterport[i]);
		}
		ln[l++] = '_';
		for (i=0 ; i<fnl ; i++) {
			ln[l++] = charconv(filename[i]);
		}
	}
	ln[l++] = '\0';
	if (l>256+NBD_LINK_PREFIX_LENG) { // we don't want names longer than 255 chars
		ln[256+NBD_LINK_PREFIX_LENG]='\0';
	}
	return ln;
}

char* find_free_nbddevice(void) {
	int nbdfd;
	char devicename[50];
	uint64_t size;
	uint32_t i;
	int err;

	for (i=0;;i++) {
		snprintf(devicename,50,"/dev/nbd%u",i);
		devicename[49] = 0;
		nbdfd = open(devicename,O_RDWR);
		if (nbdfd<0) {
			if (errno==ENOENT) {
				return NULL;
			}
		} else {
#ifdef IOCTL_INTOP
			err = ioctl(nbdfd,(int)BLKGETSIZE64,&size);
#else
			err = ioctl(nbdfd,BLKGETSIZE64,&size);
#endif
			close(nbdfd);
			if (err<0) {
				return NULL;
			}
			if (size==0) {
				return strdup(devicename);
			}
		}
	}
	return NULL;
}

int nbd_linktest(nbdcommon *nbdcp) {
	uint64_t size;
	int err,fd;

	fd = open(nbdcp->linkname,O_RDWR);
	if (fd>=0) {
#ifdef IOCTL_INTOP
		err = ioctl(fd,(int)BLKGETSIZE64,&size);
#else
		err = ioctl(fd,BLKGETSIZE64,&size);
#endif
		close(fd);
		if (err>=0 && size>0) {
			return -1;
		}
	}
	return 0;
}

char* nbd_packet_to_str(const uint8_t *pstr,uint32_t pleng) {
	char *r;
	if (pleng==0) {
		return NULL;
	}
	r = malloc(pleng+1);
	passert(r);
	memcpy(r,pstr,pleng);
	r[pleng]=0;
	return r;
}

void nbd_force_partition_reread(nbdcommon *nbdcp) {
	int err,status;
	err = fork(); // reread partition tables - needs to be done in separate process
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"fork error: %s",strerror(errno)); // ignore this
	} else {
		if (err==0) { // child
			err = open(nbdcp->nbddevice,O_RDONLY);
			if (err<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"error opening %s: %s",nbdcp->nbddevice,strerror(errno));
			} else {
				close(err);
			}
			exit(0);
		} else {
			waitpid(err,&status,0);
		}
	}
}

int nbd_start(nbdcommon *nbdcp,char errmsg[NBD_ERR_SIZE]) {
	uint64_t size;
	int omode,lmode;
	int err;
	struct stat stbuf;

#define nbd_start_err_msg(format, ...) {\
	mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,(format), __VA_ARGS__); \
	snprintf(errmsg,NBD_ERR_SIZE,(format), __VA_ARGS__); \
}

	errmsg[0] = 0;
	err = socketpair(AF_UNIX, SOCK_STREAM, 0, nbdcp->sp);

	if (err<0) {
		nbd_start_err_msg("can't create socket pair: %s",strerror(errno));
		goto err1;
	}

	if (nbdcp->nbddevice==NULL) {
		nbdcp->nbddevice = find_free_nbddevice();
	}

	if (nbdcp->nbddevice==NULL) {
		nbd_start_err_msg("%s","can't find free NBD device");
		goto err2;
	}

	nbdcp->nbdfd = open(nbdcp->nbddevice,O_RDWR);
	if (nbdcp->nbdfd<0) {
		nbd_start_err_msg("error opening %s: %s",nbdcp->nbddevice,strerror(errno));
		goto err2;
	}

#ifdef IOCTL_INTOP
	err = ioctl(nbdcp->nbdfd,(int)BLKGETSIZE64,&size);
#else
	err = ioctl(nbdcp->nbdfd,BLKGETSIZE64,&size);
#endif
	if (err<0) {
		nbd_start_err_msg("can't obtain size of block device (%s): %s",nbdcp->nbddevice,strerror(errno));
		goto err3;
	}
	if (size>0) {
		nbd_start_err_msg("it seems that block device (%s) is already mapped",nbdcp->nbddevice);
		goto err3;
	}

	if (nbdcp->flags & FLAG_READONLY) {
		omode = O_RDONLY;
		lmode = LOCK_SH;
	} else {
		omode = O_RDWR;
		lmode = LOCK_EX;
	}
	if (nbdcp->fsize!=0) {
		omode |= O_CREAT;
	}
	nbdcp->mfsfd = mfs_open(nbdcp->mfsfile,omode,0666);
	if (nbdcp->mfsfd<0) {
		nbd_start_err_msg("error opening MFS file %s: %s",nbdcp->mfsfile,strerror(errno));
		goto err3;
	}

	if (mfs_flock(nbdcp->mfsfd,lmode|LOCK_NB)<0) {
		if ((nbdcp->flags & FLAG_IGNORELOCK) == 0) {
			if (errno==EROFS) {
				nbd_start_err_msg("MFS file %s is on a read-only filesystem - only read-only devices can be used and lock ignoring must be on (options -r and -i)",nbdcp->mfsfile);
			} else {
				nbd_start_err_msg("MFS file %s is locked (likely mapped elsewhere)",nbdcp->mfsfile);
			}
			goto err4;
		}
	}

	if (nbdcp->fsize==0) {
		if (mfs_fstat(nbdcp->mfsfd,&stbuf)<0) {
			nbd_start_err_msg("can't stat MFS file '%s': %s",nbdcp->mfsfile,strerror(errno));
			goto err5;
		}
		nbdcp->fsize = stbuf.st_size;
	}

	nbdcp->fsize = nbdcp->bsize * (nbdcp->fsize/nbdcp->bsize);

	if (nbdcp->fsize==0) {
		nbd_start_err_msg("%s","file size too low (less than one 4k block)");
		goto err5;
	}

	err = ioctl(nbdcp->nbdfd, NBD_SET_BLKSIZE, nbdcp->bsize);
	if (err<0) {
		nbd_start_err_msg("error setting block device block size (%s): %s",nbdcp->nbddevice,strerror(errno));
		goto err5;
	}

	err = ioctl(nbdcp->nbdfd, NBD_SET_SIZE_BLOCKS, nbdcp->fsize / nbdcp->bsize);
	if (err<0) {
		nbd_start_err_msg("error setting block device number of blocks (%s): %s",nbdcp->nbddevice,strerror(errno));
		goto err5;
	}

	err = ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
	if (err<0) {
		nbd_start_err_msg("error clearing socket for NBD device (%s): %s",nbdcp->nbddevice,strerror(errno));
		goto err5;
	}

	err = ioctl(nbdcp->nbdfd, NBD_SET_TIMEOUT, NbdTimeout);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error setting timeout for NBD device (%s): %s",nbdcp->nbddevice,strerror(errno));
	}

	nbdcp->aqueue = squeue_new(0);
	if (nbdcp->aqueue==NULL) {
		nbd_start_err_msg("%s","can't create queue");
		goto err5;
	}

	err = lwt_minthread_create(&(nbdcp->ctrl_thread),0,nbd_controller_thread,nbdcp);
	if (err<0) {
		nbd_start_err_msg("can't create controller thread: %s",strerror(errno));
		goto err6;
	}

	err = lwt_minthread_create(&(nbdcp->send_thread),0,send_thread,nbdcp);
	if (err<0) {
		nbd_start_err_msg("can't create send thread: %s",strerror(errno));
		goto err7;
	}

	err = lwt_minthread_create(&(nbdcp->recv_thread),0,receive_thread,nbdcp);
	if (err<0) {
		nbd_start_err_msg("can't create receive thread: %s",strerror(errno));
		goto err8;
	}

	err = mkdir(NBD_LINK_PREFIX,0777); // ignore status
	err = unlink(nbdcp->linkname); // ignore status
	err = symlink(nbdcp->nbddevice,nbdcp->linkname);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't create nbd device symlink %s->%s: %s",nbdcp->linkname,nbdcp->nbddevice,strerror(errno));
	}
	return 0;

err8:
	squeue_close(nbdcp->aqueue);
	pthread_join(nbdcp->send_thread,NULL);
err7:
	ioctl(nbdcp->nbdfd, NBD_CLEAR_QUE);
	ioctl(nbdcp->nbdfd, NBD_DISCONNECT);
	ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
	pthread_join(nbdcp->ctrl_thread,NULL);
err6:
	squeue_delete(nbdcp->aqueue);
err5:
	mfs_flock(nbdcp->mfsfd,LOCK_UN); // just in case
err4:
	mfs_close(nbdcp->mfsfd);
err3:
	close(nbdcp->nbdfd);
err2:
	close(nbdcp->sp[0]);
	close(nbdcp->sp[1]);
err1:
	return -1;
}

void nbd_stop(nbdcommon *nbdcp) {
	int err;

	err = ioctl(nbdcp->nbdfd, NBD_CLEAR_QUE);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%s: ioctl (NBD_CLEAR_QUE) failed: %s",nbdcp->nbddevice,strerror(errno));
	}
	err = ioctl(nbdcp->nbdfd, NBD_DISCONNECT);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%s: ioctl (NBD_DISCONNECT) failed: %s",nbdcp->nbddevice,strerror(errno));
	}
	err = ioctl(nbdcp->nbdfd, NBD_CLEAR_SOCK);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%s: ioctl (NBD_CLEAR_SOCK) failed: %s",nbdcp->nbddevice,strerror(errno));
	}

	pthread_join(nbdcp->recv_thread,NULL);
	pthread_join(nbdcp->send_thread,NULL);
	pthread_join(nbdcp->ctrl_thread,NULL);

	squeue_delete(nbdcp->aqueue);

	mfs_flock(nbdcp->mfsfd,LOCK_UN); // just in case
	mfs_close(nbdcp->mfsfd);

	err = unlink(nbdcp->linkname);
	if (err<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't remove nbd device symlink %s->%s: %s",nbdcp->linkname,nbdcp->nbddevice,strerror(errno));
	}
	close(nbdcp->nbdfd);
}

void nbd_free(nbdcommon *nbdcp) {
	if (nbdcp->linkname) {
		free(nbdcp->linkname);
	}
	if (nbdcp->nbddevice) {
		free(nbdcp->nbddevice);
	}
	if (nbdcp->mfsfile) {
		free(nbdcp->mfsfile);
	}
	free(nbdcp);
}

static uint8_t nbd_match(nbdcommon *nbdcp,uint32_t pleng,const uint8_t *path,uint32_t dleng,const uint8_t *device,uint32_t nleng,const uint8_t *name) {
	if (pleng>0 && (pleng!=strlen(nbdcp->mfsfile) || memcmp(nbdcp->mfsfile,path,pleng))) {
		return 0;
	} else if (dleng>0 && (dleng!=strlen(nbdcp->nbddevice) || memcmp(nbdcp->nbddevice,device,dleng))) {
		return 0;
	} else if (nleng>0 && (nleng!=strlen(nbdcp->linkname+NBD_LINK_PREFIX_LENG) || memcmp(nbdcp->linkname+NBD_LINK_PREFIX_LENG,name,nleng))) {
		return 0;
	}
	return 1;
}

static uint8_t nbd_auto_maps(const char *cfgfname) {
	const uint8_t *path,*device,*name;
	uint16_t pleng;
	uint8_t dleng,nleng;
	uint64_t size;
	uint32_t bsize;
	uint32_t flags;
	static bdlist *bdl;
	char *ptr;
	const char *eptr;
	char *flagstr;
	char *lbuff;
	size_t lbsize;
	char ans[NBD_ERR_SIZE];
	FILE *cfd;

	cfd = fopen(cfgfname,"r");
	if (cfd==NULL) {
		fprintf(stderr,"can't open auto mappings file: %s\n",cfgfname);
		return 0;
	}

	lbsize = 10000;
	lbuff = malloc(lbsize);
	// <path> [ (<device> | '*') [ (<linkname> | '*') [ (<size> | '*') [ (<bsize> | '*') [ ('ro' | 'rw') ]]]]]
	while (getline(&lbuff,&lbsize,cfd)!=-1) {
		for (ptr=lbuff ; ptr[0]!='\r' && ptr[0]!='\n' && ptr[0] ; ptr++) {}
		ptr[0] = 0;
		if (lbuff[0]==';' || lbuff[0]=='#' || lbuff[0]==0) {
			continue;
		}
		ptr = lbuff;

		size = 0;
		bsize = 0;
		flags = 0;

		// mfs file name
		path = (uint8_t *)ptr;
		pleng = 0;
		while (*ptr!=' ' && *ptr!='\t' && *ptr) {
			ptr++;
			pleng++;
		}
		while (*ptr==' ' || *ptr=='\t') {
			ptr++;
		}

		// device name
		device = (uint8_t *)ptr;
		dleng = 0;
		while (*ptr!=' ' && *ptr!='\t' && *ptr) {
			ptr++;
			dleng++;
		}
		while (*ptr==' ' || *ptr=='\t') {
			ptr++;
		}
		if (dleng==1 && device[0]=='*') {
			dleng = 0;
		}

		// link name
		name = (uint8_t*)ptr;
		nleng = 0;
		while (*ptr!=' ' && *ptr!='\t' && *ptr) {
			ptr++;
			nleng++;
		}
		while (*ptr==' ' || *ptr=='\t') {
			ptr++;
		}
		if (nleng==1 && name[0]=='*') {
			nleng = 0;
		}

		// size
		if (*ptr) {
			if (*ptr=='*' && (ptr[1]==' ' || ptr[1]=='\t' || ptr[1]==0)) {
				ptr = ptr+1;
			} else {
				size = sizestrtod(ptr,&eptr);
				ptr = (char*)eptr;
				if (*ptr=='B') {
					ptr++;
				}
			}
			if (*ptr!=' ' && *ptr!='\t' && *ptr) {
				fprintf(stderr,"error parsing device size\n");
				free(lbuff);
				fclose(cfd);
				return 0;
			}
			while (*ptr==' ' || *ptr=='\t') {
				ptr++;
			}
		}

		// bsize
		if (*ptr) {
			if (*ptr=='*' && (ptr[1]==' ' || ptr[1]=='\t' || ptr[1]==0)) {
				ptr = ptr+1;
			} else {
				bsize = sizestrtod(ptr,&eptr);
				ptr = (char*)eptr;
				if (*ptr=='B') {
					ptr++;
				}
			}
			if (*ptr!=' ' && *ptr!='\t' && *ptr) {
				fprintf(stderr,"error parsing block size\n");
				free(lbuff);
				fclose(cfd);
				return 0;
			}
			while (*ptr==' ' || *ptr=='\t') {
				ptr++;
			}
		}

		// flags (ro/rw ; lock/nolock)
		while ((flagstr = strsep(&ptr,", \t")) != NULL) {
			if (flagstr[0]) {
				if (strcmp(flagstr,"ro")==0) {
					flags |= FLAG_READONLY;
				} else if (strcmp(flagstr,"rw")==0) {
					flags &= ~FLAG_READONLY;
				} else if (strcmp(flagstr,"nolock")==0) {
					flags |= FLAG_IGNORELOCK;
				} else if (strcmp(flagstr,"lock")==0) {
					flags &= ~FLAG_IGNORELOCK;
				} else {
					fprintf(stderr,"unknown flag: %s - ignored\n",flagstr);
				}
			}
		}

		bdl = malloc(sizeof(bdlist));
		passert(bdl);
		bdl->nbdcp = malloc(sizeof(nbdcommon));
		passert(bdl->nbdcp);
		bdl->nbdcp->mfsfile = nbd_packet_to_str(path,pleng);
		bdl->nbdcp->nbddevice = nbd_packet_to_str(device,dleng);
		bdl->nbdcp->linkname = nbd_packet_to_str(name,nleng);
		bdl->nbdcp->fsize = size;
		bdl->nbdcp->bsize = bsize;
		bdl->nbdcp->flags = flags;
		bdl->nbdcp->linkname = linkname_generate(bdl->nbdcp->linkname,mcfg.masterhost,mcfg.masterport,bdl->nbdcp->mfsfile);
		if (nbd_linktest(bdl->nbdcp)<0) {
			fprintf(stderr,"link exists\n");
			nbd_free(bdl->nbdcp);
			free(bdl);
			free(lbuff);
			fclose(cfd);
			return 0;
		} else {
			if (nbd_start(bdl->nbdcp,ans)<0) {
				ans[NBD_ERR_SIZE-1]=0;
				fprintf(stderr,"%s\n",ans);
				nbd_free(bdl->nbdcp);
				free(bdl);
				free(lbuff);
				fclose(cfd);
				return 0;
			} else {
				fprintf(stderr,"started block device: (%s->%s : MFS:/%s : %.3lfGiB)\n",bdl->nbdcp->linkname,bdl->nbdcp->nbddevice,bdl->nbdcp->mfsfile,bdl->nbdcp->fsize/(1024.0*1024.0*1024.0));
				bdl->next = bdhead;
				bdhead = bdl;
			}
		}
	}

	free(lbuff);
	fclose(cfd);

	for (bdl=bdhead ; bdl!=NULL ; bdl=bdl->next) {
		nbd_force_partition_reread(bdl->nbdcp);
	}

	return 1;
}

/* cli <-> daemon communication */

/* daemon handlers */

void nbd_handle_nop(int sock,const uint8_t *buff,uint32_t leng) {
	uint8_t ans[8],*wptr;

	(void)buff;
	(void)leng;
	wptr = ans;
	put32bit(&wptr,MFSNBD_NOP);
	put32bit(&wptr,0);
	unixtowrite(sock,ans,8,1000,1000); // ignore status
}

void nbd_handle_stop_daemon(int sock,const uint8_t *buff,uint32_t leng) {
	uint8_t ans[9],*wptr;

	(void)buff;
	(void)leng;
	term = 1;
	wptr = ans;
	put32bit(&wptr,MFSNBD_STOP);
	put32bit(&wptr,1);
	put8bit(&wptr,(leng==0)?MFSNBD_OK:MFSNBD_ERROR);
	unixtowrite(sock,ans,9,1000,1000); // ignore status
}

void nbd_handle_add_device(int sock,const uint8_t *buff,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t ans[10+NBD_ERR_SIZE],*wptr;
	uint32_t msglen;
	uint8_t status;
	const uint8_t *path,*device,*name;
	uint16_t pleng;
	uint8_t dleng,nleng;
	uint64_t size;
	uint32_t bsize;
	uint32_t flags;
	static bdlist *bdl;

	wptr = ans;
	put32bit(&wptr,MFSNBD_ADD);
	put32bit(&wptr,0);
	rptr = buff;
	if (leng<20U) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	pleng = get16bit(&rptr);
	path = rptr;
	rptr += pleng;
	if (leng<20U+pleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	dleng = get8bit(&rptr);
	device = rptr;
	rptr += dleng;
	if (leng<20U+pleng+dleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	nleng = get8bit(&rptr);
	name = rptr;
	rptr += nleng;
	if (leng!=20U+pleng+dleng+nleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	size = get64bit(&rptr);
	bsize = get32bit(&rptr);
	flags = get32bit(&rptr);
	if (pleng==0) {
		msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"empty filename");
		status = MFSNBD_ERROR;
	} else {
		bdl = malloc(sizeof(bdlist));
		passert(bdl);
		bdl->nbdcp = malloc(sizeof(nbdcommon));
		passert(bdl->nbdcp);
		bdl->nbdcp->mfsfile = nbd_packet_to_str(path,pleng);
		bdl->nbdcp->nbddevice = nbd_packet_to_str(device,dleng);
		bdl->nbdcp->linkname = nbd_packet_to_str(name,nleng);
		bdl->nbdcp->fsize = size;
		bdl->nbdcp->bsize = bsize;
		bdl->nbdcp->flags = flags;
		bdl->nbdcp->linkname = linkname_generate(bdl->nbdcp->linkname,mcfg.masterhost,mcfg.masterport,bdl->nbdcp->mfsfile);
		if (nbd_linktest(bdl->nbdcp)<0) {
			msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"link exists");
			status = MFSNBD_ERROR;
			nbd_free(bdl->nbdcp);
			free(bdl);
		} else {
			if (nbd_start(bdl->nbdcp,(char*)(ans+10))<0) {
				ans[9+NBD_ERR_SIZE]=0;
				msglen = strlen((char*)(ans+10));
				status = MFSNBD_ERROR;
				nbd_free(bdl->nbdcp);
				free(bdl);
			} else {
				nbd_force_partition_reread(bdl->nbdcp);
				msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"started block device: (%s->%s : MFS:/%s : %.3lfGiB)",bdl->nbdcp->linkname,bdl->nbdcp->nbddevice,bdl->nbdcp->mfsfile,bdl->nbdcp->fsize/(1024.0*1024.0*1024.0));
				status = MFSNBD_OK;
				bdl->next = bdhead;
				bdhead = bdl;
			}
		}
	}
	wptr = ans+4;
	if (msglen>NBD_ERR_SIZE) {
		msglen=NBD_ERR_SIZE;
	}
	put32bit(&wptr,msglen+2);
	put8bit(&wptr,status);
	put8bit(&wptr,msglen);
	unixtowrite(sock,ans,10+msglen,1000,1000);
	return;
}

void nbd_handle_remove_device(int sock,const uint8_t *buff,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t ans[10+NBD_ERR_SIZE],*wptr;
	uint32_t msglen;
	const uint8_t *path,*device,*name;
	uint16_t pleng;
	uint8_t dleng,nleng;
	static bdlist *bdl,**bdlp;
	uint8_t found;

	wptr = ans;
	put32bit(&wptr,MFSNBD_REMOVE);
	put32bit(&wptr,0);
	rptr = buff;
	if (leng<4U) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	pleng = get16bit(&rptr);
	path = rptr;
	rptr += pleng;
	if (leng<4U+pleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	dleng = get8bit(&rptr);
	device = rptr;
	rptr += dleng;
	if (leng<4U+pleng+dleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	nleng = get8bit(&rptr);
	name = rptr;
	rptr += nleng;
	if (leng!=4U+pleng+dleng+nleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	found = 0;
	msglen = 0;
	bdlp = &bdhead;
	while (found==0 && (bdl=*bdlp)!=NULL) {
		if (nbd_match(bdl->nbdcp,pleng,path,dleng,device,nleng,name)) {
			msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"stop block device: (%s->%s : MFS:/%s : %.3lfGiB)",bdl->nbdcp->linkname,bdl->nbdcp->nbddevice,bdl->nbdcp->mfsfile,bdl->nbdcp->fsize/(1024.0*1024.0*1024.0));
			nbd_stop(bdl->nbdcp);
			nbd_free(bdl->nbdcp);
			*bdlp = bdl->next;
			free(bdl);
			found = 1;
		} else {
			bdlp = &(bdl->next);
		}
	}
	if (found==0) {
		msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"device not found");
	}
	wptr = ans+4;
	if (msglen>NBD_ERR_SIZE) {
		msglen=NBD_ERR_SIZE;
	}
	put32bit(&wptr,msglen+2);
	put8bit(&wptr,(found)?MFSNBD_OK:MFSNBD_ERROR);
	put8bit(&wptr,msglen);
	unixtowrite(sock,ans,10+msglen,1000,1000);
	return;
}

void nbd_handle_list_devices(int sock,const uint8_t *buff,uint32_t leng) {
	uint8_t dcnt;
	uint32_t dsize;
	uint32_t pleng,dleng,nleng;
	static bdlist *bdl;
	uint8_t *ans,*wptr;

	(void)buff;
	if (leng!=0) {
		ans = malloc(8);
		passert(ans);
		wptr = ans;
		put32bit(&wptr,MFSNBD_LIST);
		put32bit(&wptr,0);
		unixtowrite(sock,ans,8,1000,1000);
		free(ans);
		return;
	}
	dcnt = 0;
	dsize = 1;
	for (bdl=bdhead ; bdl!=NULL ; bdl=bdl->next) {
		pleng = strlen(bdl->nbdcp->mfsfile);
		dleng = strlen(bdl->nbdcp->nbddevice);
		nleng = strlen(bdl->nbdcp->linkname+NBD_LINK_PREFIX_LENG);
		if (pleng>65535) {
			pleng = 65535;
		}
		if (dleng>255) {
			dleng = 255;
		}
		if (nleng>255) {
			nleng = 255;
		}
		dsize += pleng + dleng + nleng + 20;
		dcnt++;
	}
	ans = malloc(8+dsize);
	passert(ans);
	wptr = ans;
	put32bit(&wptr,MFSNBD_LIST);
	put32bit(&wptr,dsize);
	put8bit(&wptr,dcnt);
	for (bdl=bdhead ; bdl!=NULL ; bdl=bdl->next) {
		pleng = strlen(bdl->nbdcp->mfsfile);
		dleng = strlen(bdl->nbdcp->nbddevice);
		nleng = strlen(bdl->nbdcp->linkname+NBD_LINK_PREFIX_LENG);
		if (pleng>65535) {
			pleng = 65535;
		}
		if (dleng>255) {
			dleng = 255;
		}
		if (nleng>255) {
			nleng = 255;
		}
		put16bit(&wptr,pleng);
		memcpy(wptr,bdl->nbdcp->mfsfile,pleng);
		wptr+=pleng;
		put8bit(&wptr,dleng);
		memcpy(wptr,bdl->nbdcp->nbddevice,dleng);
		wptr+=dleng;
		put8bit(&wptr,nleng);
		memcpy(wptr,bdl->nbdcp->linkname+NBD_LINK_PREFIX_LENG,nleng);
		wptr+=nleng;
		put64bit(&wptr,bdl->nbdcp->fsize);
		put32bit(&wptr,bdl->nbdcp->bsize);
		put32bit(&wptr,bdl->nbdcp->flags);
	}
	unixtowrite(sock,ans,8+dsize,1000,1000);
	free(ans);
	return;
}

void nbd_handle_resize_device(int sock,const uint8_t *buff,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t ans[10+NBD_ERR_SIZE],*wptr;
	uint32_t msglen;
	const uint8_t *path,*device,*name;
	uint16_t pleng;
	uint8_t dleng,nleng;
	uint64_t size,tsize;
	static bdlist *bdl;
	uint8_t found;

	wptr = ans;
	put32bit(&wptr,MFSNBD_RESIZE);
	put32bit(&wptr,0);
	rptr = buff;
	if (leng<12U) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	pleng = get16bit(&rptr);
	path = rptr;
	rptr += pleng;
	if (leng<12U+pleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	dleng = get8bit(&rptr);
	device = rptr;
	rptr += dleng;
	if (leng<12U+pleng+dleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	nleng = get8bit(&rptr);
	name = rptr;
	rptr += nleng;
	if (leng!=12U+pleng+dleng+nleng) {
		unixtowrite(sock,ans,8,1000,1000);
		return;
	}
	size = get64bit(&rptr);
	found = 0;
	msglen = 0;
	for (bdl=bdhead ; found==0 && bdl!=NULL ; bdl=bdl->next) {
		if (nbd_match(bdl->nbdcp,pleng,path,dleng,device,nleng,name)) {
			if (size==0) {
				struct stat stbuf;
				if (mfs_fstat(bdl->nbdcp->mfsfd,&stbuf)<0) {
					msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"can't stat MFS file '%s': %s",bdl->nbdcp->mfsfile,strerror(errno));
				} else {
					size = stbuf.st_size;
				}
			}
			if (size>0) {
				size = bdl->nbdcp->bsize * (size/bdl->nbdcp->bsize);
				if (size==0) {
					msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"file size too low (less than block size (%uB))",bdl->nbdcp->bsize);
				}
			}
			if (size>0) {
				if (ioctl(bdl->nbdcp->nbdfd, NBD_SET_SIZE_BLOCKS, size / bdl->nbdcp->bsize)<0) {
					msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"error setting block device number of blocks (%s): %s",bdl->nbdcp->nbddevice,strerror(errno));
				} else {
#ifdef IOCTL_INTOP
					if (ioctl(bdl->nbdcp->nbdfd,(int)BLKGETSIZE64,&tsize)<0) {
#else
					if (ioctl(bdl->nbdcp->nbdfd,BLKGETSIZE64,&tsize)<0) {
#endif
						msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"error testing block device size (%s): %s",bdl->nbdcp->nbddevice,strerror(errno));
					} else {
						if (tsize != size) {
							msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"can't resize block device - kernel 4.18+ is needed");
						} else {
							msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"change size of block device: (%s->%s : MFS:/%s : %.3lfGiB) -> %.3lfGiB",bdl->nbdcp->linkname,bdl->nbdcp->nbddevice,bdl->nbdcp->mfsfile,bdl->nbdcp->fsize/(1024.0*1024.0*1024.0),size/(1024.0*1024.0*1024.0));
							bdl->nbdcp->fsize = size;
						}
					}
				}
			}
			found = 1;
		}
	}
	if (found==0) {
		msglen = snprintf((char*)(ans+10),NBD_ERR_SIZE,"device not found");
	}
	wptr = ans+4;
	if (msglen>NBD_ERR_SIZE) {
		msglen=NBD_ERR_SIZE;
	}
	put32bit(&wptr,msglen+2);
	put8bit(&wptr,(found)?MFSNBD_OK:MFSNBD_ERROR);
	put8bit(&wptr,msglen);
	unixtowrite(sock,ans,10+msglen,1000,1000);
	return;
}

void nbd_handle_request(int sock) {
	uint32_t cmd,leng;
	uint8_t hdr[8];
	uint8_t *buff;
	const uint8_t *rptr;

	buff = NULL;
	if (unixtoread(sock,hdr,8,READ_TOMS,READ_TOMS)!=8) {
		goto err;
	}
	rptr = hdr;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (leng>100000) {
		goto err;
	}
	if (leng>0) {
		buff = malloc(leng);
		if (buff==NULL) {
			goto err;
		}
	} else {
		buff = NULL;
	}
	if (unixtoread(sock,buff,leng,READ_TOMS,READ_TOMS)!=(int32_t)leng) {
		goto err;
	}
	switch (cmd) {
		case MFSNBD_NOP:
			nbd_handle_nop(sock,buff,leng);
			break;
		case MFSNBD_STOP:
			nbd_handle_stop_daemon(sock,buff,leng);
			break;
		case MFSNBD_ADD:
			nbd_handle_add_device(sock,buff,leng);
			break;
		case MFSNBD_REMOVE:
			nbd_handle_remove_device(sock,buff,leng);
			break;
		case MFSNBD_LIST:
			nbd_handle_list_devices(sock,buff,leng);
			break;
		case MFSNBD_RESIZE:
			nbd_handle_resize_device(sock,buff,leng);
			break;
	}
err:
	if (buff!=NULL) {
		free(buff);
	}
	return;
}


/* daemon main loop */


void nbd_stop_all_devices(void) {
	static bdlist *bdl,**bdlp;
	bdlp = &bdhead;
	while ((bdl=*bdlp)!=NULL) {
		nbd_stop(bdl->nbdcp);
		nbd_free(bdl->nbdcp);
		*bdlp = bdl->next;
		free(bdl);
	}
}

char* password_read(const char *filename) {
	FILE *fd;
	char *passwordbuff;
	size_t pbsize;
	int i;

	fd = fopen(filename,"r");
	if (fd==NULL) {
		fprintf(stderr,"error opening password file: %s\n",filename);
		return NULL;
	}
	passwordbuff = NULL;
	pbsize = 0;
	if (getline(&passwordbuff,&pbsize,fd)==-1) {
		fprintf(stderr,"password file (%s) is empty\n",filename);
		if (passwordbuff!=NULL) {
			free(passwordbuff);
		}
		fclose(fd);
		return NULL;
	}
	fclose(fd);
	i = strlen(passwordbuff);
	while (i>0) {
		i--;
		if (passwordbuff[i]=='\n' || passwordbuff[i]=='\r') {
			passwordbuff[i]=0;
		} else {
			break;
		}
	}
	if (i==0) {
		fprintf(stderr,"first line in password file (%s) is empty\n",filename);
		free(passwordbuff);
		return NULL;
	}
	return passwordbuff;
}

int parse_option(const char *oname,const char *ovalue) {
	if (strcmp(oname,"mfsmaster")==0) {
		if (mcfg.masterport!=NULL) {
			free(mcfg.masterhost);
		}
		mcfg.masterhost = strdup(ovalue);
	} else if (strcmp(oname,"mfsport")==0) {
		if (mcfg.masterport!=NULL) {
			free(mcfg.masterport);
		}
		mcfg.masterport = strdup(ovalue);
	} else if (strcmp(oname,"mfsbind")==0) {
		if (mcfg.masterbind!=NULL) {
			free(mcfg.masterbind);
		}
		mcfg.masterbind = strdup(ovalue);
	} else if (strcmp(oname,"mfspassword")==0) {
		if (mcfg.masterpassword!=NULL) {
			free(mcfg.masterpassword);
		}
		mcfg.masterpassword = strdup(ovalue);
	} else if (strcmp(oname,"mfssubfolder")==0) {
		if (mcfg.masterpath!=NULL) {
			free(mcfg.masterpath);
		}
		mcfg.masterpath = strdup(ovalue);
	} else if (strcmp(oname,"mfsioretries")==0) {
		mcfg.io_try_cnt = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfstimeout")==0) {
		mcfg.io_timeout = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfslogretry")==0) {
		mcfg.min_log_entry = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfswritecachesize")==0) {
		mcfg.write_cache_mb = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfsreadaheadsize")==0) {
		mcfg.read_cache_mb = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfsreadaheadleng")==0) {
		mcfg.readahead_leng = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfsreadaheadtrigger")==0) {
		mcfg.readahead_trigger = strtoul(ovalue,NULL,0);
	} else if (strcmp(oname,"mfserroronlostchunk")==0) {
		if (*ovalue) {
			printf("value %s not used in option mfserroronlostchunk\n",ovalue);
		}
		mcfg.error_on_lost_chunk = 1;
	} else if (strcmp(oname,"mfserroronnospace")==0) {
		if (*ovalue) {
			printf("value %s not used in option mfserroronnospace\n",ovalue);
		}
		mcfg.error_on_no_space = 1;
	} else if (strcmp(oname,"mfspreflabels")==0) {
		if (mcfg.preferedlabels!=NULL) {
			free(mcfg.preferedlabels);
		}
		mcfg.preferedlabels = strdup(ovalue);
	} else if (strcmp(oname,"mfsnbdtimeout")==0) {
		NbdTimeout = strtoul(ovalue,NULL,0);
	} else {
		fprintf(stderr,"unrecognized option: %s\n",oname);
		return -1;
	}
	return 0;
}

int parse_options(const char *optstring) {
	char *ostrcopy,*option,*osptr;
	char *optr;
	char *oname,*ovalue;
	char *onend,*ovend;

	ostrcopy = strdup(optstring);
	osptr = ostrcopy;

	while ((option = strsep(&osptr,","))!=NULL) {
		optr = option;
		while (*optr==' ' || *optr=='\t') {
			optr++;
		}
		oname = optr;
		while (*optr && *optr!=' ' && *optr!='\t' && *optr!='=') {
			optr++;
		}
		onend = optr;
		while (*optr==' ' || *optr=='\t') {
			optr++;
		}
		if (*optr=='=') {
			optr++;
			while (*optr==' ' || *optr=='\t') {
				optr++;
			}
			ovalue = optr;
			while (*optr && *optr!=' ' && *optr!='\t') {
				optr++;
			}
			ovend = optr;
			while (*optr==' ' || *optr=='\t') {
				optr++;
			}
		} else {
			ovalue = optr;
			ovend = optr;
		}
		if (*optr!=0) {
			printf("option malformed: %s\n",option);
			free(ostrcopy);
			return -1;
		}
		if (oname==onend) { // empty option - just ignore
			continue;
		}
		*onend = 0;
		*ovend = 0;
		if (parse_option(oname,ovalue)<0) {
			free(ostrcopy);
			return -1;
		}
	}

	free(ostrcopy);
	return 0;
}

void usage(const char *appname) {
	fprintf(stderr,"usage:\n");
	fprintf(stderr,"\tstart daemon:   %s start [ -H masterhost ] [ -P masterport ] [ -B masterbind ] [ -S masterpath ] [ -p masterpassword | -x passwordfile ] [ -l link_socket_name ] [ -i init_mappings_file ] [ -o options ]\n",appname);
	fprintf(stderr,"\tstop daemon:    %s stop [ -l link_socket_name ]\n",appname);
	fprintf(stderr,"\tadd mapping:    %s map [ -l link_socket_name ] -f mfsfile [ -d /dev/nbdX ] [ -n linkname ] [ -s bdevsize ] [ -b blocksize | -5 | -1 | -2 | -4 ] [-r] [-i]\n",appname);
	fprintf(stderr,"\tdelete mapping: %s unmap [ -l link_socket_name ] [ -f mfsfile ] [ -d /dev/nbdX ] [ -n linkname ]\n",appname);
	fprintf(stderr,"\tlist mappings:  %s list [ -l link_socket_name ] [ -t m|u|i ]\n",appname);
	fprintf(stderr,"\tchange size:    %s resize [ -l link_socket_name ] ( -f mfsfile | -d /dev/nbdX ) [ -s bdevsize ]\n",appname);
	fprintf(stderr,"\noptions:\n");
	fprintf(stderr,"\tmfswritecachesize=N      define size of write cache in MiB (default: 128)\n");
	fprintf(stderr,"\tmfsreadaheadsize=N       define size of all read ahead buffers in MiB (default: 128)\n");
	fprintf(stderr,"\tmfsreadaheadleng=N       define amount of bytes to be additionally read (default: 2097152 = 2MiB)\n");
	fprintf(stderr,"\tmfsreadaheadtrigger=N    define amount of bytes read sequentially that turns on read ahead (default: 20971520)\n");
	fprintf(stderr,"\tmfserroronlostchunk      when all known chunkservers are connected to the master and the required chunk is missing then immediately finish I/O and return an error\n");
	fprintf(stderr,"\tmfserroronnospace        when all known chunkservers are connected to the master and there is no free space then immediately finish I/O and return an error\n");
	fprintf(stderr,"\tmfsioretries=N           define number of retries before I/O error is returned (default: 30)\n");
	fprintf(stderr,"\tmfstimeout=N             define maximum timeout in seconds before I/O error is returned (default: 0 - which means no timeout)\n");
	fprintf(stderr,"\tmfslogretry=N            define minimal retry counter on which system will start log I/O messages (default: 5)\n");
	fprintf(stderr,"\tmfsmaster=HOST           define mfsmaster location (default: " DEFAULT_MASTERNAME ")\n");
	fprintf(stderr,"\tmfsport=PORT             define mfsmaster port number (default: " DEFAULT_MASTER_CLIENT_PORT ")\n");
	fprintf(stderr,"\tmfsbind=IP               define source ip address for connections (default: NOT DEFINED - chosen automatically by OS)\n");
	fprintf(stderr,"\tmfssubfolder=PATH        define subfolder to mount as root (default: /)\n");
	fprintf(stderr,"\tmfspassword=PASSWORD     authenticate to mfsmaster with given password\n");
	fprintf(stderr,"\tmfspreflabels=LABELEXPR  specify preferred labels for choosing chunkservers during I/O\n");
	fprintf(stderr,"\tmfsnbdtimeout=N          define maximum timeout in seconds before kernel gives up (default: 1800)\n");
//	exit(1);
}

int nbd_start_daemon(const char *appname,int argc,char *argv[]) {
	char *passfile;
	char *lsockname;
	char *initfname;
	int fg,ch;
	int lsock;

	int argc_back;
	char **argv_back;

	argc_back = argc;
	argv_back = argv;

	argc--;
	argv++;

	mfs_set_defaults(&mcfg);
	passfile = NULL;
	lsockname = NULL;
	initfname = NULL;
	fg = 0;
	bdhead = NULL;

	while ((ch = getopt(argc, argv, "H:P:B:S:p:x:l:i:o:Fh?")) != -1) {
		switch (ch) {
			case 'H':
				if (mcfg.masterhost!=NULL) {
					free(mcfg.masterhost);
				}
				mcfg.masterhost = strdup(optarg);
				break;
			case 'P':
				if (mcfg.masterport!=NULL) {
					free(mcfg.masterport);
				}
				mcfg.masterport = strdup(optarg);
				break;
			case 'B':
				if (mcfg.masterbind!=NULL) {
					free(mcfg.masterbind);
				}
				mcfg.masterbind = strdup(optarg);
				break;
			case 'S':
				if (mcfg.masterpath!=NULL) {
					free(mcfg.masterpath);
				}
				mcfg.masterpath = strdup(optarg);
				break;
			case 'p':
				if (mcfg.masterpassword!=NULL) {
					free(mcfg.masterpassword);
				}
				mcfg.masterpassword = strdup(optarg);
				break;
			case 'x':
				if (passfile!=NULL) {
					free(passfile);
				}
				passfile = strdup(optarg);
				break;
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				break;
			case 'i':
				if (initfname!=NULL) {
					free(initfname);
				}
				initfname = strdup(optarg);
				break;
			case 'o':
				if (parse_options(optarg)<0) {
					usage(appname);
					return 1;
				}
				break;
			case 'F':
				fg = 1;
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (passfile!=NULL) {
		if (mcfg.masterpassword!=NULL) {
			fprintf(stderr,"options '-p' and '-x' are mutually exclusive\n");
			return 1;
		}
		mcfg.masterpassword = password_read(passfile);
		if (mcfg.masterpassword==NULL) {
			return 1;
		}
	}

	if (mcfg.write_cache_mb<16) {
		fprintf(stderr,"write cache size too low (%u MiB) - increased to 16 MiB\n",mcfg.write_cache_mb);
		mcfg.write_cache_mb=16;
	}
	if (mcfg.write_cache_mb>2048) {
		fprintf(stderr,"write cache size too big (%u MiB) - decresed to 2048 MiB\n",mcfg.write_cache_mb);
		mcfg.write_cache_mb=2048;
	}
	if (mcfg.read_cache_mb<16) {
		fprintf(stderr,"read ahead size too low (%u MiB) - increased to 16 MiB\n",mcfg.read_cache_mb);
		mcfg.read_cache_mb=16;
	}
	if (mcfg.read_cache_mb>2048) {
		fprintf(stderr,"read ahead size too big (%u MiB) - decresed to 2048 MiB\n",mcfg.read_cache_mb);
		mcfg.read_cache_mb=2048;
	}
	if (mcfg.readahead_leng<0x20000) {
		fprintf(stderr,"read ahead length too low (%u B) - increased to 128 KiB\n",mcfg.readahead_leng);
		mcfg.readahead_leng=0x20000;
	}
	if (mcfg.readahead_leng>0x200000) {
		fprintf(stderr,"read ahead length too big (%u B) - decresed to 2 MiB\n",mcfg.readahead_leng);
		mcfg.readahead_leng=0x200000;
	}

	processname_init(argc_back,argv_back); // prepare everything for 'processname_set'

	if (mcfg.logident!=NULL) {
		free(mcfg.logident);
	}
	mcfg.logident = strdup("mfsblockdev");
	mcfg.logdaemon = 1;

	if (mcfg.mountpoint!=NULL) {
		free(mcfg.mountpoint);
	}
	mcfg.mountpoint = strdup("[NBD]");

	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
		mkdir(NBD_LINK_PREFIX,0777);
	}

	lsock = unixsocket();
	if (unixlisten(lsock,lsockname,5)<0) {
		if (errno==EADDRINUSE) {
			int csock;
			csock = unixsocket();
			if (unixconnect(csock,lsockname)<0) {
				if (errno==ECONNREFUSED) {
					unlink(lsockname);
				}
			} else {
				unixclose(csock);
			}
		}
		if (unixlisten(lsock,lsockname,5)<0) {
			fprintf(stderr,"error creating unix socket '%s': %s\n",lsockname,strerror(errno));
			free(lsockname);
			return 1;
		}
	}

	if (mfs_init(&mcfg,1)<0) {
		fprintf(stderr,"can't connect to master\n");
		unixclose(lsock);
		unlink(lsockname);
		return 1;
	}

	term = 0;
	if (fg==0) {
		make_daemon();
	} else {
		set_signals();
	}

	if (mfs_init(&mcfg,2)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"can't initialize MFS");
		unixclose(lsock);
		unlink(lsockname);
		free(lsockname);
		return 1;
	}

	workers_set = workers_init(150,30,0,"nbd",nbd_worker_fn);

	if (initfname!=NULL) {
		if (nbd_auto_maps(initfname)==0) {
			term = 1;
		}
	}

	if (fg==0) {
		int f;
		f = open("/dev/null", O_RDWR, 0);
		if (dup2(f,STDERR_FILENO)<0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_ERR,"dup2 error: %s",strerror(errno));
			term = 1;
		}
		close(f);
	}

	if (term==0) {
		char pname[256];
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"main loop start");
		snprintf(pname,256,"mfsbdev (daemon cmdlink:%s)",lsockname);
		pname[255] = 0;
		processname_set(pname); // removes password from 'ps'
	}

	while (term==0) {
		int csock;
		csock = unixtoaccept(lsock,100);
		if (csock>=0) {
			nbd_handle_request(csock);
			unixclose(csock);
		} else {
			if (errno!=ETIMEDOUT) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"accept returned: %s",strerror(errno));
			}
		}
	}

	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"got term signal - closing nbd devices");

	nbd_stop_all_devices();
	massert(bdhead==NULL,"structures not cleared properly");

	workers_term(workers_set);
	mfs_term();

	unixclose(lsock);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"removing socket file '%s'",lsockname);
	if (unlink(lsockname)<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't unlink socket '%s': %s",lsockname,strerror(errno));
	}
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"socket file '%s' removed",lsockname);
	free(lsockname);

	return 0;
}


/* cli dispatchers */


int nbd_stop_daemon(const char *appname,int argc,char *argv[]) {
	uint8_t buff[8];
	uint8_t *wptr;
	const uint8_t *rptr;
	char *lsockname;
	int ch;
	int csock;
	uint32_t cmd,leng;
	int res;
	int cnt;
	struct stat st;

	csock = -1;
	lsockname = NULL;
	res = 1;

	while ((ch = getopt(argc, argv, "l:?")) != -1) {
		switch (ch) {
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
	}

	csock = unixsocket();
	if (unixtoconnect(csock,lsockname,1000)<0) {
		fprintf(stderr,"can't connect to socket '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	wptr = buff;
	put32bit(&wptr,MFSNBD_STOP);
	put32bit(&wptr,0);
	if (unixtowrite(csock,buff,8,1000,1000)!=8) {
		fprintf(stderr,"unable to send data to '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	memset(buff,0,8);

	if (unixtoread(csock,buff,8,5000,5000)!=8) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	rptr = buff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);

	if (cmd!=MFSNBD_STOP) {
		fprintf(stderr,"got wrong answer from '%s': Bad Command\n",lsockname);
		goto err;
	}

	if (leng!=1) {
		fprintf(stderr,"got wrong answer from '%s': Wrong Size\n",lsockname);
		goto err;
	}

	if (unixtoread(csock,buff,1,1000,1000)!=1) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}
	unixclose(csock);
	csock = -1;

	if (buff[0]!=MFSNBD_OK) {
		fprintf(stderr,"error stopping daemon on '%s'\n",lsockname);
		goto err;
	}

	printf("daemon received STOP command\n");
	printf("waiting for daemon ...");
	fflush(stdout);
	cnt=0;
	while (stat(lsockname,&st)==0) {
		usleep(10000);
		cnt++;
		if ((cnt%100)==0) {
			printf(".");
			fflush(stdout);
		}
		if (cnt>1000) {
			printf(" giving up\n");
			goto err;
		}
	}
	printf(" ok\n");

	res = 0;
err:
	if (csock>=0) {
		unixclose(csock);
	}
	if (lsockname!=NULL) {
		free(lsockname);
	}
	return res;
}

int nbd_add_mapping(const char *appname,int argc,char *argv[]) {
	uint8_t *buff;
	uint8_t *wptr;
	const uint8_t *rptr;
	char *filename,*device,*linkname;
	uint64_t size;
	uint32_t flags;
	uint32_t bsize;
	uint32_t dsize,pleng,dleng,nleng;
	uint8_t aleng,status;
	char *answer;
	char *lsockname;
	int ch;
	int csock;
	int res;
	uint32_t cmd,leng;

	csock = -1;
	buff = NULL;
	lsockname = NULL;
	filename = NULL;
	device = NULL;
	linkname = NULL;
	answer = NULL;
	size = 0;
	bsize = 4096;
	flags = 0;
	res = 1;

	while ((ch = getopt(argc, argv, "l:f:d:n:s:b:5124ri?")) != -1) {
		switch (ch) {
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				break;
			case 'f':
				if (filename!=NULL) {
					free(filename);
				}
				filename = strdup(optarg);
				break;
			case 'd':
				if (device!=NULL) {
					free(device);
				}
				device = strdup(optarg);
				break;
			case 'n':
				if (linkname!=NULL) {
					free(linkname);
				}
				if (strlen(optarg)>NBD_LINK_PREFIX_LENG && memcmp(optarg,NBD_LINK_PREFIX,NBD_LINK_PREFIX_LENG)==0) {
					linkname = strdup(optarg+NBD_LINK_PREFIX_LENG);
				} else {
					linkname = strdup(optarg);
				}
				break;
			case 's':
				size = sizestrtod(optarg,NULL);
				break;
			case 'b':
				bsize = sizestrtod(optarg,NULL);
				break;
			case '5':
				bsize = 512;
				break;
			case '1':
				bsize = 1024;
				break;
			case '2':
				bsize = 2048;
				break;
			case '4':
				bsize = 4096;
				break;
			case 'r':
				flags |= FLAG_READONLY;
				break;
			case 'i':
				flags |= FLAG_IGNORELOCK;
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (filename==NULL) {
		fprintf(stderr,"MFS file name (option -f) not specified\n");
		goto err;
	}
	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
	}

	csock = unixsocket();
	if (unixtoconnect(csock,lsockname,1000)<0) {
		fprintf(stderr,"can't connect to socket '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	pleng = strlen(filename);
	if (pleng>65535) {
		fprintf(stderr,"MFS file name too long\n");
		goto err;
	}
	if (device!=NULL) {
		dleng = strlen(device);
		if (dleng>255) {
			fprintf(stderr,"device name too long\n");
			goto err;
		}
	} else {
		dleng = 0;
	}
	if (linkname!=NULL) {
		nleng = strlen(linkname);
		if (nleng>255) {
			fprintf(stderr,"link name too long\n");
			goto err;
		}
	} else {
		nleng = 0;
	}
	dsize = 20 + pleng + dleng + nleng;

	buff = malloc(8+dsize);
	passert(buff);
	wptr = buff;
	put32bit(&wptr,MFSNBD_ADD);
	put32bit(&wptr,dsize);

	put16bit(&wptr,pleng);
	memcpy(wptr,filename,pleng);
	wptr+=pleng;
	put8bit(&wptr,dleng);
	if (dleng>0) {
		memcpy(wptr,device,dleng);
		wptr+=dleng;
	}
	put8bit(&wptr,nleng);
	if (nleng>0) {
		memcpy(wptr,linkname,nleng);
		wptr+=nleng;
	}
	put64bit(&wptr,size);
	put32bit(&wptr,bsize);
	put32bit(&wptr,flags);

	if (unixtowrite(csock,buff,8+dsize,1000,1000)!=(ssize_t)(8+dsize)) {
		fprintf(stderr,"unable to send data to '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	memset(buff,0,8);

	if (unixtoread(csock,buff,8,5000,5000)!=8) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	rptr = buff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);

	if (cmd!=MFSNBD_ADD) {
		fprintf(stderr,"got wrong answer from '%s': Bad Command\n",lsockname);
		goto err;
	}

	if (leng<2 || leng>2+NBD_ERR_SIZE) {
		fprintf(stderr,"got wrong answer from '%s': Wrong Size\n",lsockname);
		goto err;
	}

	free(buff);
	buff = malloc(leng);
	passert(buff);

	if (unixtoread(csock,buff,leng,1000,1000)!=(ssize_t)leng) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}
	unixclose(csock);
	csock = -1;

	rptr = buff;
	status = get8bit(&rptr);
	aleng = get8bit(&rptr);
	answer = nbd_packet_to_str(rptr,aleng);

	if (answer!=NULL) {
		if (status==MFSNBD_OK) {
			printf("%s\n",answer);
		} else {
			fprintf(stderr,"%s\n",answer);
			goto err;
		}
	}

	res = 0;
err:
	if (csock>=0) {
		unixclose(csock);
	}
	if (lsockname!=NULL) {
		free(lsockname);
	}
	if (filename!=NULL) {
		free(filename);
	}
	if (device!=NULL) {
		free(device);
	}
	if (linkname!=NULL) {
		free(linkname);
	}
	if (answer!=NULL) {
		free(answer);
	}
	if (buff!=NULL) {
		free(buff);
	}
	return res;
}

int nbd_remove_mapping(const char *appname,int argc,char *argv[]) {
	uint8_t *buff;
	uint8_t *wptr;
	const uint8_t *rptr;
	char *filename,*device,*linkname;
	uint32_t dsize,pleng,dleng,nleng;
	uint8_t aleng,status;
	char *answer;
	char *lsockname;
	int ch;
	int csock;
	int res;
	uint32_t cmd,leng;

	csock = -1;
	buff = NULL;
	lsockname = NULL;
	filename = NULL;
	device = NULL;
	linkname = NULL;
	answer = NULL;
	res = 1;

	while ((ch = getopt(argc, argv, "l:f:d:n:?")) != -1) {
		switch (ch) {
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				break;
			case 'f':
				if (filename!=NULL) {
					free(filename);
				}
				filename = strdup(optarg);
				break;
			case 'd':
				if (device!=NULL) {
					free(device);
				}
				device = strdup(optarg);
				break;
			case 'n':
				if (linkname!=NULL) {
					free(linkname);
				}
				if (strlen(optarg)>NBD_LINK_PREFIX_LENG && memcmp(optarg,NBD_LINK_PREFIX,NBD_LINK_PREFIX_LENG)==0) {
					linkname = strdup(optarg+NBD_LINK_PREFIX_LENG);
				} else {
					linkname = strdup(optarg);
				}
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (filename==NULL && device==NULL && linkname==NULL) {
		fprintf(stderr,"device not specified\n");
		goto err;
	}
	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
	}

	csock = unixsocket();
	if (unixtoconnect(csock,lsockname,1000)<0) {
		fprintf(stderr,"can't connect to socket '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	if (filename!=NULL) {
		pleng = strlen(filename);
		if (pleng>65535) {
			fprintf(stderr,"MFS file name too long\n");
			goto err;
		}
	} else {
		pleng = 0;
	}
	if (device!=NULL) {
		dleng = strlen(device);
		if (dleng>255) {
			fprintf(stderr,"device name too long\n");
			goto err;
		}
	} else {
		dleng = 0;
	}
	if (linkname!=NULL) {
		nleng = strlen(linkname);
		if (nleng>255) {
			fprintf(stderr,"link name too long\n");
			goto err;
		}
	} else {
		nleng = 0;
	}
	dsize = 4 + pleng + dleng + nleng;

	buff = malloc(8+dsize);
	passert(buff);
	wptr = buff;
	put32bit(&wptr,MFSNBD_REMOVE);
	put32bit(&wptr,dsize);

	put16bit(&wptr,pleng);
	if (pleng>0) {
		memcpy(wptr,filename,pleng);
		wptr+=pleng;
	}
	put8bit(&wptr,dleng);
	if (dleng>0) {
		memcpy(wptr,device,dleng);
		wptr+=dleng;
	}
	put8bit(&wptr,nleng);
	if (nleng>0) {
		memcpy(wptr,linkname,nleng);
		wptr+=nleng;
	}

	if (unixtowrite(csock,buff,8+dsize,1000,1000)!=(ssize_t)(8+dsize)) {
		fprintf(stderr,"unable to send data to '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	memset(buff,0,8);

	if (unixtoread(csock,buff,8,5000,5000)!=8) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	rptr = buff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);

	if (cmd!=MFSNBD_REMOVE) {
		fprintf(stderr,"got wrong answer from '%s': Bad Command\n",lsockname);
		goto err;
	}

	if (leng<2 || leng>2+NBD_ERR_SIZE) {
		fprintf(stderr,"got wrong answer from '%s': Wrong Size\n",lsockname);
		goto err;
	}

	free(buff);
	buff = malloc(leng);
	passert(buff);

	if (unixtoread(csock,buff,leng,1000,1000)!=(ssize_t)leng) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}
	unixclose(csock);
	csock = -1;

	rptr = buff;
	status = get8bit(&rptr);
	aleng = get8bit(&rptr);
	answer = nbd_packet_to_str(rptr,aleng);

	if (answer!=NULL) {
		if (status==MFSNBD_OK) {
			printf("%s\n",answer);
		} else {
			fprintf(stderr,"%s\n",answer);
			goto err;
		}
	}

	res = 0;
err:
	if (csock>=0) {
		unixclose(csock);
	}
	if (lsockname!=NULL) {
		free(lsockname);
	}
	if (filename!=NULL) {
		free(filename);
	}
	if (device!=NULL) {
		free(device);
	}
	if (linkname!=NULL) {
		free(linkname);
	}
	if (answer!=NULL) {
		free(answer);
	}
	if (buff!=NULL) {
		free(buff);
	}
	return res;
}

int nbd_resize_bdev(const char *appname,int argc,char *argv[]) {
	uint8_t *buff;
	uint8_t *wptr;
	const uint8_t *rptr;
	char *filename,*device,*linkname;
	uint64_t size;
	uint32_t dsize,pleng,dleng,nleng;
	uint8_t aleng,status;
	char *answer;
	char *lsockname;
	int ch;
	int csock;
	int res;
	uint32_t cmd,leng;

	csock = -1;
	buff = NULL;
	lsockname = NULL;
	filename = NULL;
	device = NULL;
	linkname = NULL;
	answer = NULL;
	size = 0;
	res = 1;

	while ((ch = getopt(argc, argv, "l:f:d:n:s:?")) != -1) {
		switch (ch) {
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				break;
			case 'f':
				if (filename!=NULL) {
					free(filename);
				}
				filename = strdup(optarg);
				break;
			case 'd':
				if (device!=NULL) {
					free(device);
				}
				device = strdup(optarg);
				break;
			case 'n':
				if (linkname!=NULL) {
					free(linkname);
				}
				if (strlen(optarg)>NBD_LINK_PREFIX_LENG && memcmp(optarg,NBD_LINK_PREFIX,NBD_LINK_PREFIX_LENG)==0) {
					linkname = strdup(optarg+NBD_LINK_PREFIX_LENG);
				} else {
					linkname = strdup(optarg);
				}
				break;
			case 's':
				size = sizestrtod(optarg,NULL);
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (filename==NULL && device==NULL && linkname==NULL) {
		fprintf(stderr,"device not specified\n");
		goto err;
	}
	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
	}

	csock = unixsocket();
	if (unixtoconnect(csock,lsockname,1000)<0) {
		fprintf(stderr,"can't connect to socket '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	if (filename!=NULL) {
		pleng = strlen(filename);
		if (pleng>65535) {
			fprintf(stderr,"MFS file name too long\n");
			goto err;
		}
	} else {
		pleng = 0;
	}
	if (device!=NULL) {
		dleng = strlen(device);
		if (dleng>255) {
			fprintf(stderr,"device name too long\n");
			goto err;
		}
	} else {
		dleng = 0;
	}
	if (linkname!=NULL) {
		nleng = strlen(linkname);
		if (nleng>255) {
			fprintf(stderr,"link name too long\n");
			goto err;
		}
	} else {
		nleng = 0;
	}
	dsize = 12 + pleng + dleng + nleng;

	buff = malloc(8+dsize);
	passert(buff);
	wptr = buff;
	put32bit(&wptr,MFSNBD_RESIZE);
	put32bit(&wptr,dsize);

	put16bit(&wptr,pleng);
	if (pleng>0) {
		memcpy(wptr,filename,pleng);
		wptr+=pleng;
	}
	put8bit(&wptr,dleng);
	if (dleng>0) {
		memcpy(wptr,device,dleng);
		wptr+=dleng;
	}
	put8bit(&wptr,nleng);
	if (nleng>0) {
		memcpy(wptr,linkname,nleng);
		wptr+=nleng;
	}
	put64bit(&wptr,size);

	if (unixtowrite(csock,buff,8+dsize,1000,1000)!=(ssize_t)(8+dsize)) {
		fprintf(stderr,"unable to send data to '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	memset(buff,0,8);

	if (unixtoread(csock,buff,8,5000,5000)!=8) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	rptr = buff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);

	if (cmd!=MFSNBD_RESIZE) {
		fprintf(stderr,"got wrong answer from '%s': Bad Command\n",lsockname);
		goto err;
	}

	if (leng<2 || leng>2+NBD_ERR_SIZE) {
		fprintf(stderr,"got wrong answer from '%s': Wrong Size\n",lsockname);
		goto err;
	}

	free(buff);
	buff = malloc(leng);
	passert(buff);

	if (unixtoread(csock,buff,leng,1000,1000)!=(ssize_t)leng) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}
	unixclose(csock);
	csock = -1;

	rptr = buff;
	status = get8bit(&rptr);
	aleng = get8bit(&rptr);
	answer = nbd_packet_to_str(rptr,aleng);

	if (answer!=NULL) {
		if (status==MFSNBD_OK) {
			printf("%s\n",answer);
		} else {
			fprintf(stderr,"%s\n",answer);
			goto err;
		}
	}

	res = 0;
err:
	if (csock>=0) {
		unixclose(csock);
	}
	if (lsockname!=NULL) {
		free(lsockname);
	}
	if (filename!=NULL) {
		free(filename);
	}
	if (device!=NULL) {
		free(device);
	}
	if (linkname!=NULL) {
		free(linkname);
	}
	if (answer!=NULL) {
		free(answer);
	}
	if (buff!=NULL) {
		free(buff);
	}
	return res;
}

int nbd_list_mappings(const char *appname,int argc,char *argv[]) {
	uint8_t cbuff[8],*buff;
	uint8_t *wptr;
	const uint8_t *rptr;
	uint8_t dcnt;
	uint16_t pleng;
	uint8_t dleng,nleng;
	uint16_t maxpleng;
	uint8_t maxdleng,maxnleng;
	uint64_t size;
	uint64_t maxsize;
	uint32_t bsize;
	uint32_t maxbsize;
	uint32_t flags;
	uint8_t displaymode;
	char formatbuff[100];
	char *path,*device,*linkname;
	char *lsockname;
	uint8_t lsockcustom;
	int ch;
	int csock;
	uint32_t cmd,leng;
	int res;

	csock = -1;
	lsockname = NULL;
	lsockcustom = 0;
	displaymode = 0;
	buff = NULL;
	res = 1;

	while ((ch = getopt(argc, argv, "l:t:?")) != -1) {
		switch (ch) {
			case 'l':
				if (lsockname!=NULL) {
					free(lsockname);
				}
				lsockname = strdup(optarg);
				lsockcustom = 1;
				break;
			case 't':
				if (optarg[0]=='A' || optarg[0]=='a' || optarg[0]=='M' || optarg[0]=='m') {
					displaymode = 1;
				} else if (optarg[0]=='R' || optarg[0]=='r' || optarg[0]=='D' || optarg[0]=='d' || optarg[0]=='U' || optarg[0]=='u') {
					displaymode = 2;
				} else if (optarg[0]=='C' || optarg[0]=='c' || optarg[0]=='I' || optarg[0]=='i') {
					displaymode = 3;
				} else {
					fprintf(stderr,"unknown display mode: %s\n",optarg);
					usage(appname);
					return 0;
				}
				break;
			case 'h':
			default:
				usage(appname);
				return 0;
		}
	}

	if (lsockname==NULL) {
		lsockname = strdup(NBD_LINK_PREFIX "nbdsock");
	}

	csock = unixsocket();
	if (unixtoconnect(csock,lsockname,1000)<0) {
		fprintf(stderr,"can't connect to socket '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	wptr = cbuff;
	put32bit(&wptr,MFSNBD_LIST);
	put32bit(&wptr,0);
	if (unixtowrite(csock,cbuff,8,1000,1000)!=8) {
		fprintf(stderr,"unable to send data to '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	memset(cbuff,0,8);

	if (unixtoread(csock,cbuff,8,5000,5000)!=8) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	rptr = cbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);

	if (cmd!=MFSNBD_LIST) {
		fprintf(stderr,"got wrong answer from '%s': Bad Command\n",lsockname);
		goto err;
	}

	buff = malloc(leng);
	passert(buff);

	if (unixtoread(csock,buff,leng,1000,1000)!=(ssize_t)leng) {
		fprintf(stderr,"error receiving data from '%s': %s\n",lsockname,strerror(errno));
		goto err;
	}

	unixclose(csock);
	csock = -1;

	maxpleng = 0;
	maxdleng = 0;
	maxnleng = 0;
	maxsize = 0;
	maxbsize = 0;
	formatbuff[0]='\n';
	formatbuff[1]=0;
	if (displaymode==3) { // calc max path, max device, max link name
		rptr = buff;
		dcnt = get8bit(&rptr);
		while (dcnt>0) {
			pleng = get16bit(&rptr);
			rptr += pleng;
			dleng = get8bit(&rptr);
			rptr += dleng;
			nleng = get8bit(&rptr);
			rptr += nleng;
			size = get64bit(&rptr);
			bsize = get32bit(&rptr);
			rptr += 4; // ship flags
			dcnt--;
			if (pleng > maxpleng) {
				maxpleng = pleng;
			}
			if (dleng > maxdleng) {
				maxdleng = dleng;
			}
			if (nleng > maxnleng) {
				maxnleng = nleng;
			}
			if (size > maxsize) {
				maxsize = size;
			}
			if (bsize > maxbsize) {
				maxbsize = bsize;
			}
		}
		snprintf(formatbuff,100,"%"PRIu64,maxsize);
		formatbuff[99]=0;
		maxsize = strlen(formatbuff);
		snprintf(formatbuff,100,"%"PRIu32,maxbsize);
		formatbuff[99]=0;
		maxbsize = strlen(formatbuff);
		snprintf(formatbuff,100,"%%%u"PRIu64" %%%u"PRIu32" %%s,%%s\n",(unsigned int)maxsize,(unsigned int)maxbsize);
	}

	rptr = buff;
	dcnt = get8bit(&rptr);
	while (dcnt>0) {
		pleng = get16bit(&rptr);
		path = nbd_packet_to_str(rptr,pleng);
		rptr += pleng;
		dleng = get8bit(&rptr);
		device = nbd_packet_to_str(rptr,dleng);
		rptr += dleng;
		nleng = get8bit(&rptr);
		linkname = nbd_packet_to_str(rptr,nleng);
		rptr += nleng;
		size = get64bit(&rptr);
		bsize = get32bit(&rptr);
		flags = get32bit(&rptr);
		if (displaymode!=0) {
			if (displaymode==3) {
				printf("%s",path);
				while (pleng <= maxpleng) {
					putchar(' ');
					pleng++;
				}
				printf("%s",device);
				while (dleng <= maxdleng) {
					putchar(' ');
					dleng++;
				}
				printf("%s",linkname);
				while (nleng <= maxnleng) {
					putchar(' ');
					nleng++;
				}
				printf(formatbuff,size,bsize,(flags & FLAG_READONLY)?"ro":"rw",(flags & FLAG_IGNORELOCK)?"nolock":"lock");
			} else {
				if (displaymode==1) {
					printf("%s map",appname);
				} else {
					printf("%s unmap",appname);
				}
				if (lsockcustom) {
					printf(" -l '%s'",lsockname);
				}
				if (path!=NULL) {
					printf(" -f '%s'",path);
				}
				if (device!=NULL) {
					printf(" -d '%s'",device);
				}
				if (linkname!=NULL) {
					printf(" -n '%s'",linkname);
				}
				if (displaymode==1) {
					printf(" -s %"PRIu64,size);
					printf(" -b %"PRIu32,bsize);
					if (flags & FLAG_READONLY) {
						printf(" -r");
					}
					if (flags & FLAG_IGNORELOCK) {
						printf(" -i");
					}
				}
				printf("\n");
			}
		} else {
			printf("file: %s ; device: %s ; link: %s ; size: %"PRIu64" (%.3lfGiB) ; blocksize: %"PRIu32" ; rwmode: %s ; ignore_locks:%s\n",(path==NULL)?"":path,(device==NULL)?"":device,(linkname==NULL)?"":linkname,size,size/(1024.0*1024.0*1024.0),bsize,(flags & FLAG_READONLY)?"ro":"rw",(flags & FLAG_IGNORELOCK)?"yes":"no");
		}
		if (path!=NULL) {
			free(path);
		}
		if (device!=NULL) {
			free(device);
		}
		if (linkname!=NULL) {
			free(linkname);
		}
		dcnt--;
	}

	res = 0;
err:
	if (csock>=0) {
		unixclose(csock);
	}
	if (lsockname!=NULL) {
		free(lsockname);
	}
	if (buff!=NULL) {
		free(buff);
	}
	return res;
}

int main(int argc,char *argv[]) {
	char *appname;
	int res;

	appname = strdup(argv[0]);

	strerr_init();

	if (argc<2) {
		usage(appname);
		res = 1;
	} else if (strcmp(argv[1],"start")==0) {
		res = nbd_start_daemon(appname,argc,argv);
	} else if (strcmp(argv[1],"stop")==0) {
		res = nbd_stop_daemon(appname,argc-1,argv+1);
	} else if (strcmp(argv[1],"map")==0 || strcmp(argv[1],"add")==0) {
		res = nbd_add_mapping(appname,argc-1,argv+1);
	} else if (strcmp(argv[1],"unmap")==0 || strcmp(argv[1],"remove")==0 || strcmp(argv[1],"delete")==0 || strcmp(argv[1],"del")==0 || strcmp(argv[1],"rm")==0) {
		res = nbd_remove_mapping(appname,argc-1,argv+1);
	} else if (strcmp(argv[1],"resize")==0) {
		res = nbd_resize_bdev(appname,argc-1,argv+1);
	} else if (strcmp(argv[1],"list")==0) {
		res = nbd_list_mappings(appname,argc-1,argv+1);
	} else {
		usage(appname);
		res = 1;
	}
	free(appname);
	return res;
}
