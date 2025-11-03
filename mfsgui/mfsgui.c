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

#include <time.h>
#include <stddef.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"
#include "cfg.h"
#include "md5.h"
#include "main.h"
#include "clocks.h"
#include "mfslog.h"
#include "massert.h"
#include "sockets.h"
#include "mime.h"
#include "children.h"

// #define LOG_REQUESTS 1
// #define LOG_HEADERS 1

typedef struct requests_str {
	uint8_t rtype;
	uint32_t rl;
	char *request;
	uint32_t fl;
	char *fname;
	char *extra;
	const char *mimetype;
	char *mtimestr;
	char *etag;
	time_t mtime;
	uint32_t fsize;
	uint8_t *fdata;
	struct requests_str *next;
} requests;

static requests *req_head,**req_tail;

enum { METHOD_NONE,METHOD_GET,METHOD_HEAD };

enum { STATUS_NONE,STATUS_FOUND,STATUS_NOTFOUND,STATUS_FORBIDDEN,STATUS_NOTMODIFIED,STATUS_BADREQUEST,STATUS_BADMETHOD,STATUS_INTERROR };

enum { MATCH_UNKNOWN,MATCH_NO,MATCH_YES };

enum { RTYPE_NONE,RTYPE_FILE,RTYPE_CGI,RTYPE_REDIR,RTYPE_DIR };

#define HTTP_HEADERMAX 16384

#define FWD_BUFFSIZE 4096

typedef struct httphandle_str {
	int sock;
	int error;
	uint8_t *inputdata;
	uint32_t inputdataleng;
	uint8_t *outputdata;
	uint32_t outputdataleng;
	double starttime;
	char *requrl;
	char *reqargs;
	uint8_t httpver;
	uint8_t keepalive;
	uint8_t etagmatch;
	uint8_t mtimematch;
	uint8_t method;
	uint8_t status;
	requests *req;
} httphandle;

static char *RootDir = NULL;
static uint32_t RootDirLen = 0;
static char *RequestsFile = NULL;

static time_t root_dir_mtime;
static time_t requests_mtime;
static off_t requests_leng;

static uint32_t Timeout;
static char *ListenHost;
static char *ListenPort;
static uint32_t listenip; 
static uint16_t listenport;

static int lsock;
static uint32_t lsockpdescpos;

static char *OsPath = NULL;

static inline void mfscgiserv_free_request(requests *r) {
	if (r->request!=NULL) {
		free(r->request);
	}
	if (r->fname!=NULL) {
		free(r->fname);
	}
	if (r->extra!=NULL) {
		free(r->extra);
	}
	if (r->mtimestr!=NULL) {
		free(r->mtimestr);
	}
	if (r->etag!=NULL) {
		free(r->etag);
	}
	if (r->fdata!=NULL) {
		free(r->fdata);
	}
}

static inline void mfscgiserv_free_requests(void) {
	requests *rh,*nrh;

	rh = req_head;
	while (rh!=NULL) {
		nrh = rh->next;
		mfscgiserv_free_request(rh);
		free(rh);
		rh = nrh;
	}
	req_head = NULL;
	req_tail = &req_head;
}

static void mfscgiserv_generate_cache_strings(requests *r) {
	int i;
	static struct tm *structtime; 
	time_t tts;
	md5ctx ctx;
	uint8_t digest[16];

	if (r->mtimestr) {
		free(r->mtimestr);
		r->mtimestr = NULL;
	}

	if (r->etag) {
		free(r->etag);
		r->etag = NULL;
	}


	tts = r->mtime;
	structtime = gmtime(&tts);

	r->mtimestr = malloc(31);
	strftime(r->mtimestr,30,"%a, %d %b %Y %T GMT",structtime);
	r->mtimestr[30]=0;

	md5_init(&ctx);
	md5_update(&ctx,r->fdata,r->fsize);
	md5_final(digest,&ctx);

	r->etag = malloc(33);
	passert(r->etag);
	for (i=0 ; i<16 ; i++) {
		r->etag[i*2] = (digest[i]>>4)["0123456789ABCDEF"];
		r->etag[i*2+1] = (digest[i]&15)["0123456789ABCDEF"];
	}
	r->etag[32]=0;

}

static void mfscgiserv_reloaddata(requests *r,time_t mtime,off_t fsize) {
	int fd;
	uint8_t *fdata;

	fd = open(r->fname,O_RDONLY);
	if (fd<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't reload content of the file: %s",r->fname);
		return;
	}

	fdata = malloc(fsize);
	passert(fdata);
	if (read(fd,fdata,fsize)!=(ssize_t)fsize) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't reload content of the file: %s",r->fname);
		free(fdata);
		close(fd);
		return;
	}
	close(fd);

	if (r->fdata!=NULL) {
		free(r->fdata);
	}
	r->fdata = fdata;
	r->mtime = mtime;
	r->fsize = fsize;

	mfscgiserv_generate_cache_strings(r);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"guiserv: file %s has been reloaded",r->fname);
}

static int mfscgiserv_loaddata(requests *r) {
	int fd;
	struct stat st;
	fd = open(r->fname,O_RDONLY);
	if (fd<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't read content of file: %s",r->fname);
		return -1;
	}
	if (fstat(fd,&st)<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't stat file: %s",r->fname);
		close(fd);
		return -1;
	}

	r->mtime = st.st_mtime;
	r->fsize = st.st_size;
	r->fdata = malloc(r->fsize);
	passert(r->fdata);
	if (read(fd,r->fdata,r->fsize)!=(ssize_t)(r->fsize)) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't read content of the file: %s",r->fname);
		close(fd);
		return -1;
	}
	close(fd);

	mfscgiserv_generate_cache_strings(r);

	return 0;
}

static inline int mfscgiserv_isspace(char c) {
	return (c==' ' || c=='\t');
}

static inline int mfscgiserv_isnotspace(char c) {
	return (c && c!=' ' && c!='\t');
}

// lines
// ; precached files:
// f|F	REQUEST		[FILENAME	[FORCED MIME]]
// ; whole supdirectory (not precached):
// d|D	REQUEST		[DIRNAME	[FORCED MIME]]
// ; redirect to another location:
// r|R	REQUEST		[NEWURL		[EXTRA PARAMS TO ADD]]
// ; cgi script:
// c|C	REQUEST		[FILENAME	[EXTRA PARAMS TO ADD]]
//
// examples:
//
// f	/	index.html	[text/html]
// f	/acidchart.js	files/acidchart.js	text/javascript
// f	file.txt
// c	/mfs.cgi	files/mfs.cgi
// c	/metrics	files/mfs.cgi	ajax=metrics
// r	/mfs.py		/mfs.cgi
// d	/assets		assets

int mfscgiserv_parse_cfgline(char *line) {
	char *p;
	requests *req;
	char *rptr;

	int rl;
	char *request;
	int fl;
	char *filename;
	int el;
	char *extra;

	uint8_t rtype;

	switch (line[0]) {
		case 'f':
		case 'F':
			rtype = RTYPE_FILE;
			break;
		case 'c':
		case 'C':
			rtype = RTYPE_CGI;
			break;
		case 'r':
		case 'R':
			rtype = RTYPE_REDIR;
			break;
		case 'd':
		case 'D':
			rtype = RTYPE_DIR;
			break;
		case '#':
		case ';':
		case '/':
			return 0;
		default:
			return -1;
	}
	p = line+1;
	if (mfscgiserv_isnotspace(*p)) {
		return -1;
	}
	while (mfscgiserv_isspace(*p)) {
		p++;
	}

	while (*p=='/') {
		p++;
	}
	if (*p==0) {
		return -1;
	}
	rptr = p;
	rl = 0;
	while (mfscgiserv_isnotspace(*p)) {
		p++;
		rl++;
	}
	request = malloc(rl+1);
	passert(request);
	memcpy(request,rptr,rl);
	request[rl]=0;

	while (mfscgiserv_isspace(*p)) {
		p++;
	}

	if (rtype!=RTYPE_REDIR) {
		if (*p==0) { // no filename - use request as filename
			fl = rl;
		} else {
			rptr = p;
			fl = 0;
			while (mfscgiserv_isnotspace(*p)) {
				p++;
				fl++;
			}
		}

		filename = malloc(RootDirLen+fl+2);
		passert(filename);
		memcpy(filename,RootDir,RootDirLen);
		filename[RootDirLen]='/';
		memcpy(filename+RootDirLen+1,rptr,fl);
		filename[RootDirLen+fl+1]=0;
		fl += RootDirLen+1;
	} else {
		if (*p==0) {
			free(request);
			return -1;
		}

		while (*p=='/') {
			p++;
		}
		rptr = p;
		fl = 0;
		while (mfscgiserv_isnotspace(*p)) {
			p++;
			fl++;
		}

		filename = malloc(fl+1);
		passert(filename);
		memcpy(filename,rptr,fl);
		filename[fl]=0;
	}

	while (mfscgiserv_isspace(*p)) {
		p++;
	}

	if (*p==0) {
		extra = NULL;
	} else {
		rptr = p;
		el = 0;
		while (mfscgiserv_isnotspace(*p)) {
			p++;
			el++;
		}

		extra = malloc(el+1);
		passert(extra);
		memcpy(extra,rptr,el);
		extra[el]=0;
	}

	req = malloc(sizeof(requests));
	req->rtype = rtype;
	req->rl = rl;
	req->request = request;
	req->fl = fl;
	req->fname = filename;
	req->extra = extra;
	req->mtime = 0;
	req->mimetype = NULL;
	req->mtimestr = NULL;
	req->etag = NULL;
	req->fdata = NULL;
	if (rtype==RTYPE_FILE) {
		if (mfscgiserv_loaddata(req)<0) {
			mfscgiserv_free_request(req);
			return -1;
		}
		if (req->extra==NULL) {
			req->mimetype = mime_find(req->fname);
			if (req->mimetype==NULL) {
				req->mimetype = mime_find(req->request);
			}
		}
	} else if (rtype==RTYPE_CGI) {
		req->fdata = malloc(FWD_BUFFSIZE);
		req->fsize = 0;
	}
//	printf("req: %s ; fname: %s ; extra: %s ; mimetype: %s ; mtimestr: %s ; etag: %s\n",req->request,req->fname,req->extra,req->mimetype,req->mtimestr,req->etag);
	req->next = NULL;
	*req_tail = req;
	req_tail = &(req->next);
	return 0;
}

void mfscgiserv_rescan(void) {
	FILE *fd;
	char *linebuff;
	size_t lbsize;
	uint32_t s;

	fd = fopen(RequestsFile,"r");
	if (fd==NULL) {
		if (errno==ENOENT) {
			if (req_head) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: requests file (%s) not found - requests not changed",RequestsFile);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: requests file (%s) not found !!!",RequestsFile);
			}
		} else {
			if (req_head) {
				mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"guiserv: can't open mfsgui requests file (%s) - requests not changed, error",RequestsFile);
			} else {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: can't open mfsgui requests file (%s) - no requests !!!, error",RequestsFile);
			}
		}
		return;
	}

	mfscgiserv_free_requests();
	lbsize = 10000;
	linebuff = malloc(lbsize);
	while (getline(&linebuff,&lbsize,fd)!=-1) {
		s=strlen(linebuff);
		while (s>0 && (linebuff[s-1]=='\r' || linebuff[s-1]=='\n' || linebuff[s-1]=='\t' || linebuff[s-1]==' ')) {
			s--;
		}
		if (s>0) {
			linebuff[s]=0;
			if (mfscgiserv_parse_cfgline(linebuff)<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: wrong request definition: %s",linebuff);
			}
		}
	}
	fclose(fd);
	free(linebuff);
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"guiserv: requests have been reloaded");
}

void mfscgiserv_quick_rescan(void) {
	struct stat rdst;
	struct stat rqst;
	struct stat fst;
	requests *r;

	if (stat(RootDir,&rdst)>=0 && stat(RequestsFile,&rqst)>=0) {
		if (rdst.st_mtime != root_dir_mtime || rqst.st_mtime != requests_mtime || rqst.st_size != requests_leng) {
			root_dir_mtime = rdst.st_mtime;
			requests_mtime = rqst.st_mtime;
			requests_leng = rqst.st_size;
			mfscgiserv_rescan();
			return;
		}
	}
	for (r = req_head ; r!=NULL ; r=r->next) {
		if (r->rtype==RTYPE_FILE) {
			if (stat(r->fname,&fst)>=0) {
				if (r->mtime != fst.st_mtime || r->fsize != fst.st_size) {
					mfscgiserv_reloaddata(r,fst.st_mtime,fst.st_size);
				}
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: can't stat file: %s",r->fname);
			}
		}
	}
}

int mfscgiserv_expect_str(const char *sptr,const char *expect) {
	char c1,c2;
	int l;

	l = 0;
	while (*expect) {
		c1 = *sptr;
		c2 = *expect;
		if (c1==0) {
			return -1;
		}
		if (c1>='A' && c1<='Z') {
			c1 += 'a'-'A';
		}
		if (c2>='A' && c2<='Z') {
			c2 += 'a'-'A';
		}
		if (c1!=c2) {
			return -1;
		}
		sptr++;
		expect++;
		l++;
	}
	return l;
}

int mfscgiserv_check_header(const char *sptr,const char *header) {
	char c1,c2;
	int l;

	l = 0;
	while (*header) {
		c1 = *sptr;
		c2 = *header;
		if (c1==0) {
			return -1;
		}
		if (c1>='A' && c1<='Z') {
			c1 += 'a'-'A';
		}
		if (c2>='A' && c2<='Z') {
			c2 += 'a'-'A';
		}
		if (c1!=c2) {
			return -1;
		}
		sptr++;
		header++;
		l++;
	}
	if (*sptr!=':') {
		return -1;
	}
	return l+1;
}

#ifdef LOG_REQUESTS
static char* mfscgiserv_get_reqtype(requests *r) {
	if (r==NULL) {
		return "(REQUEST NOT FOUND)";
	}
	switch (r->rtype) {
		case RTYPE_FILE:
			return "FILE";
		case RTYPE_DIR:
			return "DIRECTORY";
		case RTYPE_CGI:
			return "CGI";
		case RTYPE_REDIR:
			return "REDIRECT";
	}
	return "???";
}
#endif

// interesting headers:
//	User-Agent: <UA>
//	Accept-Encoding: gzip/deflate
//	If-None-Match: "<ETAG>"
//	If-Modified-Since: <DATE>
//	Connection: close/keep-alive
void mfscgiserv_parse_hdrline(httphandle *h,uint32_t lineno,char *sptr/*,char *eptr*/) {
//	(void)eptr;
	requests *req;
	uint32_t ul;
	int i,hl;
	char *p;

	if (lineno==0) { // URL line
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"first header line: %s",sptr);
		if (h->method==METHOD_GET) {
			p = sptr+4;
		} else if (h->method==METHOD_HEAD) {
			p = sptr+5;
		} else {
			return;
		}
		if (*p!='/') {
			h->status = STATUS_BADREQUEST;
			return;
		}
		p++;
		h->requrl = p;
		ul = 0;
		while (*p && *p!=' ' && *p!='?' && *p!='&') {
			p++;
			ul++;
		}
		if (*p=='?' || *p=='&') {
			*p = 0;
			p++;
			h->reqargs = p;
			while (*p && *p!=' ') {
				p++;
			}
			*p = 0;
			p++;
		} else if (*p==' ') {
			*p = 0;
			p++;
		} else {
			h->status = STATUS_BADREQUEST;
			return;
		}

		if (strcmp(p,"HTTP/1.0")==0) {
			h->httpver = 0x10;
			h->keepalive = 0;
		} else if (strcmp(p,"HTTP/1.1")==0) {
			h->httpver = 0x11;
			h->keepalive = 1;
		}

		for (req=req_head ; req!=NULL ; req=req->next) {
			if (req->rtype==RTYPE_DIR && req->rl<ul && memcmp(req->request,h->requrl,req->rl)==0 && h->requrl[req->rl]=='/') {
				h->req = req;
				break;
			} else if (req->rl==ul && memcmp(req->request,h->requrl,ul)==0) {
				h->req = req;
				break;
			}
		}
#ifdef LOG_REQUESTS
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"guiserv (%d): requrl: %s ; reqargs: %s ; reqtype: %s ; httpver:%02X",getpid(),h->requrl,h->reqargs,mfscgiserv_get_reqtype(h->req),h->httpver);
#endif
	} else {
#ifdef LOG_HEADERS
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"guiserv (%d): received headers: %s",getpid(),sptr);
#endif
		if (h->req!=NULL && h->req->rtype==RTYPE_FILE) {
			if (h->req->etag) {
				hl = mfscgiserv_check_header(sptr,"If-None-Match");
				if (hl>0) {
					p = sptr+hl;
					while (*p && *p==' ') {
						p++;
					}
					if (*p=='"') {
						p++;
						for (i=0 ; h->etagmatch==MATCH_UNKNOWN && i<32 ; i++) {
							if (p[i]!=h->req->etag[i]) {
								h->etagmatch = MATCH_NO;
							}
						}
						if (h->etagmatch==MATCH_UNKNOWN && p[32]=='"') {
							h->etagmatch = MATCH_YES;
						}
					} else {
						h->etagmatch = MATCH_NO;
					}
//					printf("etag match: %u\n",h->etagmatch);
				}
			}
			if (h->req->mtimestr) {
				hl = mfscgiserv_check_header(sptr,"If-Modified-Since");
				if (hl>0) {
					p = sptr+hl;
					while (*p && *p==' ') {
						p++;
					}
					for (i=0 ; h->mtimematch==MATCH_UNKNOWN && i<30 ; i++) {
						if (p[i]!=h->req->mtimestr[i]) {
							h->mtimematch = MATCH_NO;
						}
					}
					if (h->mtimematch==MATCH_UNKNOWN) {
						h->mtimematch = MATCH_YES;
					}
//					printf("mtime match: %u\n",h->mtimematch);
				}
			}
		}
		hl = mfscgiserv_check_header(sptr,"Connection");
		if (hl>0) {
			p = sptr+hl;
			while (*p && *p==' ') {
				p++;
			}
			if (mfscgiserv_expect_str(p,"close")>=0) {
				h->keepalive = 0;
			}
			if (mfscgiserv_expect_str(p,"keep-alive")>=0) {
				h->keepalive = 1;
			}
		}
	}
}

void mfscgiserv_parse_headers(httphandle *h) {
	char *ptr,*found;
	uint32_t i;

	i = 0;
	ptr = (char*)h->inputdata;
	while ((found=strstr(ptr,"\r\n"))!=NULL) {
		found[0]=0;
		mfscgiserv_parse_hdrline(h,i,ptr);
//		found[0]='\r';
		ptr = found+2;
		i++;
	}
}

void mfscgiserv_printf(httphandle *h,const char *format,...) {
	int32_t maxstrleng;
	int32_t leng;
	va_list ap;

	if (h->outputdataleng+2>HTTP_HEADERMAX) {
		h->error = 1;
		return;
	}
	maxstrleng = HTTP_HEADERMAX - (h->outputdataleng + 2);
	va_start(ap,format);
	leng = vsnprintf((char*)h->outputdata+h->outputdataleng,maxstrleng,format,ap);
	va_end(ap);

#ifdef LOG_HEADERS
	mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"guiserv (%d): sent headers: %s",getpid(),(char*)h->outputdata+h->outputdataleng);
#endif
	if (h->outputdataleng + leng + 2 > HTTP_HEADERMAX) {
		h->error = 1;
		return;
	}
	h->outputdataleng += leng;
	h->outputdata[h->outputdataleng++] = '\r';
	h->outputdata[h->outputdataleng++] = '\n';
}

void mfscgiserv_read_request(httphandle *h) {
	struct pollfd pfd;
	int i;
	int searchpos;
	double ts;
	char *found;

	pfd.fd = h->sock;
	pfd.events = POLLIN;
	pfd.revents = 0;
	while (1) {
//		printf("read req / %u\n",h->inputdataleng);
		i = read(h->sock,h->inputdata+h->inputdataleng,HTTP_HEADERMAX-h->inputdataleng);
		if (i==0) {
//			printf("zero read\n");
			h->error = 1;
			return;
		}
		if (i<0 && ERRNO_ERROR) {
//			printf("error\n");
			h->error = 1;
			return;
		}
		if (pfd.revents & POLLHUP) {
//			printf("hup\n");
			h->error = 1;
			return;
		}
		if (h->inputdataleng>3) {
			searchpos = h->inputdataleng-3;
		} else {
			searchpos = 0;
		}
		if (i>0) {
			h->inputdataleng += i;
			if (h->inputdataleng>=HTTP_HEADERMAX) {
//				printf("out off buffer\n");
				h->error = 1;
				return;
			}
			h->inputdata[h->inputdataleng] = 0;
			if (h->inputdataleng>=8 && h->method==METHOD_NONE) {
				if (memcmp(h->inputdata,"GET ",4)==0) {
					h->method = METHOD_GET;
				} else if (memcmp(h->inputdata,"HEAD ",5)==0) {
					h->method = METHOD_HEAD;
				} else {
					h->status = STATUS_BADMETHOD;
//					printf("bad method\n");
					return;
				}
			}
			if ((found=strstr((char*)h->inputdata+searchpos,"\r\n\r\n"))!=NULL) {
//				printf("found end\n");
				found[2] = 0;
				return;
			}
		}
		ts = monotonic_seconds();
		if ((ts-h->starttime) >= Timeout) {
//			printf("timeout\n");
			h->error = 1;
			return;
		}
		pfd.revents = 0;
		if (poll(&pfd,1,((h->starttime+Timeout)-ts) * 1000)<0) {
			if (errno!=EINTR) {
//				printf("intr\n");
				h->error = 1;
				return;
			}
		} else {
			if (pfd.revents & POLLERR) {
//				printf("err\n");
				h->error = 1;
				return;
			}
			if ((pfd.revents & POLLIN)==0) {
//				printf("timeout/poll\n");
				h->error = 1;
				return;
			}
		}
	}
}

void mfscgiserv_write_data(httphandle *h,uint32_t skip) {
	double ts;
	uint32_t msecto;

	ts = monotonic_seconds();
	if ((ts-h->starttime) >= Timeout) {
		h->error = 1;
		return;
	}
	msecto = ((h->starttime+Timeout)-ts) * 1000;
	if (tcptowrite(h->sock,h->req->fdata+skip,h->req->fsize-skip,msecto,msecto)!=(int32_t)(h->req->fsize-skip)) {
		h->error = 1;
		return;
	}
}

void mfscgiserv_write_headers(httphandle *h) {
	double ts;
	uint32_t msecto;

	ts = monotonic_seconds();
	if ((ts-h->starttime) >= Timeout) {
		h->error = 1;
		return;
	}
	msecto = ((h->starttime+Timeout)-ts) * 1000;
	if (tcptowrite(h->sock,h->outputdata,h->outputdataleng,msecto,msecto)!=(int32_t)(h->outputdataleng)) {
		h->error = 1;
		return;
	}
}

void mfscgiserv_status(httphandle *h) {
	switch (h->status) {
		case STATUS_NONE:
			mfscgiserv_printf(h,"HTTP/1.1 200 OK");
			return;
//		case STATUS_NOCONTENT:
//			mfscgiserv_printf(h,"HTTP/1.1 204 No Content");
//			return;
		case STATUS_FOUND:
			mfscgiserv_printf(h,"HTTP/1.1 302 Found");
			return;
		case STATUS_NOTMODIFIED:
			mfscgiserv_printf(h,"HTTP/1.1 304 Not Modified");
			return;
		case STATUS_BADREQUEST:
			mfscgiserv_printf(h,"HTTP/1.1 400 Bad Request");
			return;
		case STATUS_BADMETHOD:
			mfscgiserv_printf(h,"HTTP/1.1 405 Method Not Allowed");
			return;
		case STATUS_NOTFOUND:
			mfscgiserv_printf(h,"HTTP/1.1 404 Not Found");
			return;
		case STATUS_FORBIDDEN:
			mfscgiserv_printf(h,"HTTP/1.1 403 Forbidden");
			return;
		default:
			mfscgiserv_printf(h,"HTTP/1.1 500 Internal Server Error");
			return;
	}
}

void mfscgiserv_prepare_headers(httphandle *h) {
	if (h->req==NULL) {
		h->status = STATUS_NOTFOUND;
	}
	if (h->status==STATUS_NONE) {
		if (h->etagmatch==MATCH_YES) {
			h->status = STATUS_NOTMODIFIED;
		} else if (h->etagmatch==MATCH_UNKNOWN && h->mtimematch==MATCH_YES) {
			h->status = STATUS_NOTMODIFIED;
		}
	}
	if (h->status!=STATUS_NONE) {
		h->keepalive=0;
	}

	mfscgiserv_status(h);
	mfscgiserv_printf(h,"Server: mfsgui");
	mfscgiserv_printf(h,"Connection: %s",h->keepalive?"keep-alive":"close");
	if (h->req!=NULL) {
		if (h->status==STATUS_NONE) {
			if (h->req->extra) {
				mfscgiserv_printf(h,"Content-Type: %s",h->req->extra);
			} else if (h->req->mimetype) {
				mfscgiserv_printf(h,"Content-Type: %s",h->req->mimetype);
			} else {
				mfscgiserv_printf(h,"Content-Type: text/plain");
			}
			mfscgiserv_printf(h,"Content-Length: %u",h->req->fsize);
		}
		if (h->status==STATUS_FOUND) {
			if (h->reqargs) {
				if (h->req->extra) {
					mfscgiserv_printf(h,"Location: /%s?%s&%s", h->req->fname,h->reqargs,h->req->extra);
				} else {
					mfscgiserv_printf(h,"Location: /%s?%s", h->req->fname,h->reqargs);
				}
			} else {
				if (h->req->extra) {
					mfscgiserv_printf(h,"Location: /%s?%s",h->req->fname,h->req->extra);
				} else {
					mfscgiserv_printf(h,"Location: /%s",h->req->fname);
				}
			}
		} else {
			mfscgiserv_printf(h,"Cache-Control: public,max-age=0"); // max-age=60 ???
			if (h->req->etag) {
				mfscgiserv_printf(h,"ETag: \"%s\"",h->req->etag);
			}
			if (h->req->mtimestr) {
				mfscgiserv_printf(h,"Last-Modified: %s",h->req->mtimestr);
			}
		}
	}
	mfscgiserv_printf(h,"");
}

int mfscgiserv_piperead(httphandle *h,int fd,uint32_t maxleng) {
	struct pollfd pfd;
	int i;
	double ts;

	pfd.fd = fd;
	pfd.events = POLLIN;
	pfd.revents = 0;
	h->req->fsize = 0;
	while (1) {
		i = read(fd,h->req->fdata+h->req->fsize,maxleng-h->req->fsize);
		if (i==0) {
//			printf("pipe read ret 0\n");
			return 0; // last data packet
		}
		if (i<0 && ERRNO_ERROR) {
//			printf("pipe read ret -1 with error\n");
			h->error = 1;
			return 0;
		}
		if (i>0) {
			h->req->fsize += i;
			if (h->req->fsize>=maxleng) {
				return 1; // continue
			}
		}
		ts = monotonic_seconds();
		if ((ts-h->starttime) >= Timeout) {
//			printf("pipe timeout\n");
			h->error = 1;
			return 0;
		}
		pfd.revents = 0;
		if (poll(&pfd,1,((h->starttime+Timeout)-ts) * 1000)<0) {
			if (errno!=EINTR) {
//				printf("pipe poll error\n");
				h->error = 1;
				return 0;
			}
		} else {
			if (pfd.revents & POLLERR) {
//				printf("pipe poll ret ERR\n");
				h->error = 1;
				return 0;
			}
//			if (pfd.revents & POLLHUP) {
//				printf("pipe poll ret HUP\n");
//				return 0; // last data packet
//			}
//			if ((pfd.revents & POLLIN)==0) {
//				printf("pipe poll ret 0\n");
//				h->error = 1;
//				return 0;
//			}
		}
	}
}

void mfscgiserv_handle_cgioutput(httphandle *h,int fd) {
	char *p,*sline,*eoln;
	char *status;
	uint8_t statok;
	uint8_t ctok;
	uint8_t cont;
	uint32_t skip;

	univnonblock(fd);
	cont = mfscgiserv_piperead(h,fd,FWD_BUFFSIZE-1);
	if (h->error) {
//		printf("piperead error\n");
		return;
	}
	h->req->fdata[h->req->fsize]=0;
//	i = tcptoread(fd,fwdbuff,FWD_BUFFSIZE-1,msecto,msecto);
//	printf("cgi forwarder, first block from cgi: %d,%u\n",cont,h->req->fsize);

	statok = 0;
	ctok = 0;
	p = (char*)h->req->fdata;
	while (1) {
//		printf("next line, pos: %ld\n",(uint8_t*)p-h->req->fdata);
		sline = p;
		while (*p && *p!='\n') {
			p++;
		}
		if (*p==0) {
//			printf("end of data\n");
			break;
		}
		eoln = p;
		if (eoln>sline && eoln[-1]=='\r') {
			eoln--;
		}
		*eoln = 0;
		p++;
		if (eoln==sline) {
//			printf("empty line\n");
//			p++;
//			printf("next char: %c\n",*p);
			break;
		}
		if (mfscgiserv_check_header(sline,"Status")>=0) {
//			printf("status\n");
			status = sline+7;
			while (mfscgiserv_isspace(*status)) {
				status++;
			}
			mfscgiserv_printf(h,"HTTP/1.1 %s",status);
			mfscgiserv_printf(h,"Server: mfsgui");
			mfscgiserv_printf(h,"Connection: close");
			statok = 1;
		} else {
			if (statok==0) {
				mfscgiserv_printf(h,"HTTP/1.1 200 OK");
				mfscgiserv_printf(h,"Server: mfsgui");
				mfscgiserv_printf(h,"Connection: close");
				statok = 1;
			}
			if (mfscgiserv_check_header(sline,"Content-Type")>=0) {
				ctok = 1;
			}
			mfscgiserv_printf(h,"%s",sline);
		}
	}
	if (ctok==0) {
		mfscgiserv_printf(h,"Content-Type: text/plain");
	}
	mfscgiserv_printf(h,"");

	mfscgiserv_write_headers(h);
	if (h->error) {
//		printf("headers error\n");
		return;
	}

	skip = (uint8_t*)p-h->req->fdata;

	if (skip<h->req->fsize) {
		mfscgiserv_write_data(h,skip);
	}

	if (h->error) {
//		printf("data error\n");
		return;
	}

	while (cont) {
		cont = mfscgiserv_piperead(h,fd,FWD_BUFFSIZE);
//		printf("cgihandle, cont: %u ; read: %d\n",cont,h->req->fsize);

		if (h->error) {
//			printf("piperead error\n");
			return;
		}

		if (h->req->fsize>0) {
			mfscgiserv_write_data(h,0);
		}
	
		if (h->error) {
//			printf("data error\n");
			return;
		}
	}
}

void mfscgiserv_handle_cgi(httphandle *h) {
	char *argv[] = {h->req->fname,NULL};
	char *argp[] = {
		"GATEWAY_INTERFACE=CGI/1.1",
		"SERVER_PROTOCOL=HTTP/1.1",
		"REQUEST_METHOD=GET",
		"SERVER_NAME=mfsgui",
		OsPath,
#define QSTRPOS 5
		NULL, // placement for QUERY_STRING
		NULL
	};
	char query_env[1024];
	pid_t pid;
	int fd,i;
	int cgipipe[2];

	if (pipe(cgipipe)<0) {
		h->error = 1;
		return;
	}
	pid = fork();
	if (pid==0) {
		fd = open("/dev/null", O_RDWR, 0);
		close(STDIN_FILENO);
		sassert(dup(fd)==STDIN_FILENO);
		close(STDOUT_FILENO);
		sassert(dup(cgipipe[1])==STDOUT_FILENO);
		close(STDERR_FILENO);
		sassert(dup(cgipipe[1])==STDERR_FILENO);
		for (i=3 ; i<1024 ; i++) {
			close(i);
		}
		if (h->reqargs) {
			if (h->req->extra) {
				snprintf(query_env, sizeof(query_env), "QUERY_STRING=%s&%s", h->reqargs,h->req->extra);
			} else {
				snprintf(query_env, sizeof(query_env), "QUERY_STRING=%s", h->reqargs);
			}
		} else {
			if (h->req->extra) {
				snprintf(query_env, sizeof(query_env), "QUERY_STRING=%s",h->req->extra);
			} else {
				snprintf(query_env, sizeof(query_env), "QUERY_STRING=");
			}
		}
		argp[QSTRPOS] = query_env;
		execve(h->req->fname,argv,argp);
		exit(0);
	} else if (pid<0) {
		h->error = 1;
		return;
	} else {
		close(cgipipe[1]);
		mfscgiserv_handle_cgioutput(h,cgipipe[0]);
		close(cgipipe[0]);
	}
}

void mfscgiserv_handle_dir(httphandle *h) {
	char rpath[PATH_MAX+1];
	struct stat st;
	char *fname;
	int32_t ulen;
	int fd;

	ulen = strlen(h->requrl);

	fname = malloc(h->req->fl + ulen - h->req->rl + 1);
	if (fname==NULL) {
		h->status = STATUS_INTERROR;
		return;
	}

	memcpy(fname,h->req->fname,h->req->fl);
	memcpy(fname+h->req->fl,h->requrl+h->req->rl,ulen-h->req->rl);
	fname[h->req->fl + ulen - h->req->rl] = 0;

//	printf("dir fname: %s\n",fname);
	if (realpath(fname,rpath)==NULL) {
		h->status = STATUS_NOTFOUND;
		free(fname);
		return;
	}
	free(fname);

//	printf("realpath: %s\n",rpath);
	if (strncmp(rpath,h->req->fname,h->req->fl)!=0 || rpath[h->req->fl]!='/') {
		h->status = STATUS_FORBIDDEN;
		return;
	}
	fd = open(rpath,O_RDONLY);
	if (fd<0) {
		if (errno==ENOENT) {
			h->status = STATUS_NOTFOUND;
		} else if (errno==EPERM || errno==EACCES) {
			h->status = STATUS_FORBIDDEN;
		} else {
			h->status = STATUS_INTERROR;
		}
		return;
	}
	if (fstat(fd,&st)<0) {
		h->status = STATUS_INTERROR;
		close(fd);
		return;
	}
	if ((st.st_mode & S_IFMT)!=S_IFREG) {
		h->status = STATUS_FORBIDDEN;
		close(fd);
		return;
	}

	h->req->fsize = st.st_size;
	h->req->fdata = malloc(h->req->fsize);
	if (h->req->fdata==NULL) {
		h->status = STATUS_INTERROR;
		close(fd);
		return;
	}
	if (read(fd,h->req->fdata,h->req->fsize)!=(ssize_t)(h->req->fsize)) {
		h->status = STATUS_INTERROR;
		close(fd);
		return;
	}
	close(fd);
	h->req->mimetype = mime_find(rpath);
}

void mfscgiserv_handle_httpconn(int newsock) {
	httphandle handle,*h;
//	int trno;

	h = &handle;
	h->sock = newsock;
	h->inputdata = malloc(HTTP_HEADERMAX);
	h->outputdata = malloc(HTTP_HEADERMAX);
//	trno = 0;

	do {
		h->error = 0;
		h->inputdataleng = 0;
		h->outputdataleng = 0;
		h->starttime = monotonic_seconds();
		h->requrl = NULL;
		h->reqargs = NULL;
		h->httpver = 0;
		h->keepalive = 0;
		h->etagmatch = MATCH_UNKNOWN;
		h->mtimematch = MATCH_UNKNOWN;
		h->method = METHOD_NONE;
		h->status = STATUS_NONE;
		h->req = NULL;

//		printf("handle http trno: %u\n",trno);

		mfscgiserv_read_request(h);
		if (h->error) {
			tcpclose(newsock);
			exit(1);
		}

		mfscgiserv_parse_headers(h);
		if (h->req==NULL || h->req->rtype==RTYPE_FILE) {
			mfscgiserv_prepare_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
			mfscgiserv_write_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
			if (h->req!=NULL && h->method==METHOD_GET && h->status==STATUS_NONE && h->req->rtype==RTYPE_FILE) {
				mfscgiserv_write_data(h,0);
			}
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
		} else if (h->req->rtype==RTYPE_DIR) {
			mfscgiserv_handle_dir(h);
			mfscgiserv_prepare_headers(h);
			if (h->error) {
				if (h->req->fdata!=NULL) {
					free(h->req->fdata);
				}
				tcpclose(newsock);
				exit(1);
			}
			mfscgiserv_write_headers(h);
			if (h->error) {
				if (h->req->fdata!=NULL) {
					free(h->req->fdata);
				}
				tcpclose(newsock);
				exit(1);
			}
			if (h->method==METHOD_GET && h->status==STATUS_NONE) {
				mfscgiserv_write_data(h,0);
			}
			if (h->req->fdata!=NULL) {
				free(h->req->fdata);
			}
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
		} else if (h->req->rtype==RTYPE_REDIR) {
			h->status = STATUS_FOUND;
			mfscgiserv_prepare_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
			mfscgiserv_write_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
		} else if (h->method==METHOD_HEAD) { // not supported in CGI mode
			h->status = STATUS_BADMETHOD;
			mfscgiserv_prepare_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
			mfscgiserv_write_headers(h);
			if (h->error) {
				tcpclose(newsock);
				exit(1);
			}
		} else { // CGI
			h->keepalive = 0;
			mfscgiserv_handle_cgi(h);
		}
//		trno++;
	} while (h->keepalive);
	free(h->inputdata);
	free(h->outputdata);
	tcpclose(newsock);
	exit(0);
}

void mfscgiserv_chldend(pid_t pid,int status) {
//	printf("child %d ended with status %d\n",pid,status);
	(void)status;
	children_remove(pid);
}

void mfscgiserv_handle_newconn(int newsock) {
	int r;

//	printf("new conn\n");
	r = fork();
	if (r==0) { // child
		close(lsock);
		mfscgiserv_handle_httpconn(newsock);
	} else if (r<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"guiserv: fork error");
		close(newsock);
	} else { // parent
//		printf("child %d started\n",r);
		children_add(r);
		main_chld_register(r,mfscgiserv_chldend);
		close(newsock);
	}
}

void mfscgiserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;

	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	*ndesc = pos;
}

void mfscgiserv_serve(struct pollfd *pdesc) {
	int ns;
	if (pdesc[lsockpdescpos].revents & POLLIN) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			mfscgiserv_handle_newconn(ns);
		}
	}
}

void mfscgiserv_reload_common(void) {
	char *reqfile;

	Timeout = cfg_getuint32("GUISERV_TIMEOUT",300);
	if (Timeout>65535) {
		Timeout=65535;
	} else if (Timeout<10) {
		Timeout=10;
	}

	if (RootDir!=NULL) {
		free(RootDir);
	}
	if (RequestsFile!=NULL) {
		free(RequestsFile);
	}

	RootDir = cfg_getstr("ROOT_DIR",DEFAULT_CGIDIR);
	RootDirLen = strlen(RootDir);

	reqfile = cfg_getstr("REQUESTS_FILE","requests.cfg");

	if (reqfile[0]=='/') {
		RequestsFile = reqfile;
	} else {
		int rflen;
		rflen = strlen(reqfile);
		RequestsFile = malloc(RootDirLen+rflen+2);
		passert(RequestsFile);
		memcpy(RequestsFile,RootDir,RootDirLen);
		RequestsFile[RootDirLen]='/';
		memcpy(RequestsFile+RootDirLen+1,reqfile,rflen);
		RequestsFile[RootDirLen+rflen+1]=0;
		free(reqfile);
	}

	root_dir_mtime = 0;
	requests_mtime = 0;
	requests_leng = 0;
	mfscgiserv_quick_rescan();
}

void mfscgiserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	uint32_t oldlistenip;
	uint16_t oldlistenport;
	int newlsock;

	mfscgiserv_reload_common();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	oldlistenip = listenip;
	oldlistenport = listenport;

	ListenHost = cfg_getstr("GUISERV_LISTEN_HOST","*");
	ListenPort = cfg_getstr("GUISERV_LISTEN_PORT",DEFAULT_GUI_HTTP_PORT);

	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"guiserv: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: socket address has changed, but can't be resolved (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpnumlisten(newlsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"guiserv: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"guiserv: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"guiserv: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

void mfscgiserv_term(void) {
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"guiserv: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	free(ListenHost);
	free(ListenPort);

	free(RootDir);
	free(RequestsFile);

	free(OsPath);

	mfscgiserv_free_requests();
	children_kill();
}

void mfscgiserv_prepare_path(void) {
	char *pathtmp;
	int pl;

	pathtmp = getenv("PATH");
	if (pathtmp) {
		pl = strlen(pathtmp);
	} else {
		pl = 0;
	}
	OsPath = malloc(pl+6);
	passert(OsPath);
	memcpy(OsPath,"PATH=",5);
	if (pl>0) {
		memcpy(OsPath+5,pathtmp,pl);
	}
	OsPath[pl+5]=0;
}

int mfscgiserv_init(void) {
	req_head = NULL;
	req_tail = &req_head;

	mfscgiserv_prepare_path();
	mfscgiserv_reload_common();

	ListenHost = cfg_getstr("GUISERV_LISTEN_HOST","*");
	ListenPort = cfg_getstr("GUISERV_LISTEN_PORT",DEFAULT_GUI_HTTP_PORT);

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"guiserv: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"guiserv: can't resolve %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpnumlisten(lsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"guiserv: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"guiserv: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"guiserv: listen on %s:%s",ListenHost,ListenPort);

	main_reload_register(mfscgiserv_reload);
	main_destruct_register(mfscgiserv_term);
	main_poll_register(mfscgiserv_desc,mfscgiserv_serve);
	main_time_register(1,0,mfscgiserv_quick_rescan);
	children_init();

	return 0;
}
