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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <syslog.h>
#endif
#include <stdarg.h>
#include <errno.h>
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#include "MFSCommunication.h"
#include "strerr.h"

#define LOGBUFFSIZE 2048
#define MSGBUFFSIZE 4096

static void(*mfs_log_sink)(const char *str) = NULL;
static int force_stderr = 0;
static int use_colors = 1;
static int stderr_active = 1;

// ignore all messages below this level
// this is MFS level
static int mfs_log_min_level = MFSLOG_INFO;

// elevate lower levels to this one
// this is MFS level
static int mfs_log_elevate_to = MFSLOG_NOTICE;

#ifdef WIN32
static HANDLE ehandler = NULL;
static char* lstrings[2];
#else
static int syslog_open = 0;
#endif


const char* mfs_log_priority_strings[] = {MFSLOG_STRINGS};

#define MFSLOG_PRI_MIN MFSLOG_DEBUG
#define MFSLOG_PRI_MAX MFSLOG_ERR

static const char* mfs_log_pri_to_str(int priority) {
	if (priority>=MFSLOG_PRI_MIN && priority<=MFSLOG_PRI_MAX) {
		return mfs_log_priority_strings[priority];
	}
	return "unknown";
}

int mfs_log_str_to_pri(const char *pristr) {
	char c;
	int i,j;
	int fpri;
	const char *fpristr;
	fpristr = NULL;
	fpri = -1;

	for (i=0 ; pristr[i]!=0 ; i++) {
		c = pristr[i];
		if (c>='A' && c<='Z') {
			c += 'a'-'A';
		}
		if (c>='a' && c<='z') {
			if (fpristr!=NULL) {
				if (fpristr[i]!=c) {
					return -1;
				}
			} else {
				for (j=MFSLOG_PRI_MIN ; j<=MFSLOG_PRI_MAX ; j++) {
					if (mfs_log_priority_strings[j][i]==c) {
						fpristr = mfs_log_priority_strings[j];
						fpri = j;
					}
				}
				if (fpristr==NULL) {
					return -1;
				}
			}
		} else {
			return -1;
		}
	}
	return fpri;
}

#define COLOR_DEBUG "\033[0;90m"
#define COLOR_INFO ""
#define COLOR_NOTICE "\033[1;97m"
#define COLOR_WARNING "\033[1;93m"
#define COLOR_ERROR "\033[1;31m"
#define COLOR_CLEAR "\033(B\033[m"

static int mfs_log_priority_convert(int priority) {
	int elevated_priority;
	if (priority<mfs_log_elevate_to) {
		elevated_priority = mfs_log_elevate_to;
	} else {
		elevated_priority = priority;
	}
	switch (elevated_priority) {
		case MFSLOG_DEBUG:
#ifdef WIN32
			return EVENTLOG_SUCCESS;
#else
			return LOG_DEBUG;
#endif
		case MFSLOG_INFO:
#ifdef WIN32
			return EVENTLOG_INFORMATION_TYPE;
#else
			return LOG_INFO;
#endif
		case MFSLOG_NOTICE:
#ifdef WIN32
			return EVENTLOG_INFORMATION_TYPE;
#else
			return LOG_NOTICE;
#endif
		case MFSLOG_WARNING:
#ifdef WIN32
			return EVENTLOG_WARNING_TYPE;
#else
			return LOG_WARNING;
#endif
		default: // MFSLOG_ERR
#ifdef WIN32
			return EVENTLOG_ERROR_TYPE;
#else
			return LOG_ERR;
#endif
	}
}

static const char* mfs_log_pri_to_colorstr(int priority) {
	switch (priority) {
		case MFSLOG_DEBUG:
			return COLOR_DEBUG;
		case MFSLOG_INFO:
			return COLOR_INFO;
		case MFSLOG_NOTICE:
			return COLOR_NOTICE;
		case MFSLOG_WARNING:
			return COLOR_WARNING;
		case MFSLOG_ERR:
			return COLOR_ERROR;
	}
	return "";
}

void mfs_file_log(const char *file,int line,const char *func,int bt,const char *fmt,...) {
#if defined(HAVE_EXECINFO_H) && defined(HAVE_BACKTRACE)
#define BT_BUF_SIZE 100
	int i,n;
	void *btbuf[BT_BUF_SIZE];
	char **btstr;
#endif
	va_list ap;
	static FILE *lfd = NULL;

	if (fmt==NULL) {
		if (lfd!=NULL) {
			fclose(lfd);
			lfd = NULL;
		}
		return;
	}

	if (lfd==NULL) {
		lfd = fopen("mfsdebug.txt","a");
		if (lfd==NULL) {
			return;
		}
	}
	fprintf(lfd,"%s:%d (%s):",file,line,func);
	va_start(ap,fmt);
	vfprintf(lfd,fmt,ap);
	va_end(ap);
	fprintf(lfd,"\n");
#if defined(HAVE_EXECINFO_H) && defined(HAVE_BACKTRACE)
	if (bt) {
		n = backtrace(btbuf,BT_BUF_SIZE);
#ifdef HAVE_BACKTRACE_SYMBOLS
		btstr = backtrace_symbols(btbuf,n);
#else
		btstr = NULL;
#endif

		if (btstr!=NULL) {
			for (i=1 ; i<n ; i++) { // start with 1 - do not print self
				fprintf(lfd,"\t%u: %s\n",i,btstr[i]);
			}
			free(btstr);
		} else {
			for (i=1 ; i<n ; i++) { // start with 1 - do not print self
				fprintf(lfd,"\t%u: [%p]\n",i,btbuf[i]);
			}
		}
	}
#else
	(void)bt;
#endif
}

void mfs_log(int mode,int priority,const char *fmt,...) {
	char msg[MSGBUFFSIZE];
	char p[LOGBUFFSIZE];
	int n;
	va_list ap;
	const char *_mfs_errstring;

	if (priority < mfs_log_min_level) {
		return;
	}

	if (mode & 1) {
		_mfs_errstring = strerr(errno);
	} else {
		_mfs_errstring = NULL; // silent compiler warnings
	}

	va_start(ap, fmt);
	n = vsnprintf(p, LOGBUFFSIZE, fmt, ap);
	va_end(ap);
	if (n<0) {
		return;
	}
	p[LOGBUFFSIZE-1]=0;
	if (mode & 1) {
		snprintf(msg, MSGBUFFSIZE, "%s: %s", p, _mfs_errstring);
	} else {
		snprintf(msg, MSGBUFFSIZE, "%s", p);
	}
	msg[MSGBUFFSIZE-1] = 0;
	if (mfs_log_sink!=NULL) {
		mfs_log_sink(msg);
	}
#ifdef WIN32
	if (ehandler != NULL) {
		lstrings[1] = msg;
		ReportEvent(ehandler, mfs_log_priority_convert(priority), 0, 1, NULL, 2, 0, lstrings, NULL);
	}
#else
	if (syslog_open) {
		syslog(mfs_log_priority_convert(priority),"[%s] %s",mfs_log_pri_to_str(priority),msg);
	}
#endif
	if (stderr_active==0) {
		return;
	}
#ifdef WIN32
	if ((ehandler==NULL) || force_stderr || (mode & 2)) {
#else
	if ((syslog_open==0) || force_stderr || (mode & 2)) {
#endif
		if (use_colors) {
			fprintf(stderr,"%s%s%s\n",mfs_log_pri_to_colorstr(priority),msg,COLOR_CLEAR);
		} else {
			fprintf(stderr,"%s\n",msg);
		}
	}
	return;
}

void mfs_log_set_min_level(int minlevel) {
	mfs_log_min_level = minlevel;
}

void mfs_log_set_elevate_to(int elevateto) {
	mfs_log_elevate_to = elevateto;
}

void mfs_log_set_sink_function(void(*s)(const char *str)) {
	mfs_log_sink = s;
}

void mfs_log_detach_stderr(void) {
	stderr_active = 0;
}

void mfs_log_term(void) {
#ifdef WIN32
	if (ehandler!=NULL) {
		DeregisterEventSource(ehandler);
	}
	if (lstrings[0]!=NULL) {
		free(lstrings[0]);
	}
#else
	if (syslog_open) {
		closelog();
	}
#endif
	mfs_file_log(NULL,0,NULL,0,NULL);
}

int mfs_log_init(const char *ident,int daemonflag) {
	if (ident!=NULL) {
#ifdef WIN32
		(void)daemonflag;
		ehandler = RegisterEventSource(NULL,ident);
		lstrings[0] = strdup(ident);
#else
#ifdef LOG_DAEMON
		if (daemonflag) {
			openlog(ident, LOG_PID | LOG_NDELAY, LOG_DAEMON);
		} else {
#endif
			openlog(ident, LOG_PID | LOG_NDELAY, LOG_USER);
#ifdef LOG_DAEMON
		}
#endif
		syslog_open = 1;
#endif
	}
	force_stderr = (daemonflag)?0:1;
#ifdef WIN32
	use_colors = 0;
#else
	use_colors = isatty(STDERR_FILENO)?1:0;
#endif
	stderr_active = 1;
	return 0;
}
