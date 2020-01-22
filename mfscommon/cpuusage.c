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

#if defined(GPERFTOOLS) && defined(HAVE_SETITIMER)
#undef HAVE_SETITIMER
#endif

#if defined(HAVE_SETITIMER)
#  include <sys/time.h>
#  ifndef ITIMER_REAL
#    define ITIMER_REAL 0
#  endif
#  ifndef ITIMER_VIRTUAL
#    define ITIMER_VIRTUAL 1
#  endif
#  ifndef ITIMER_PROF
#    define ITIMER_PROF 2
#  endif
#  define MAXITIMER 999
#endif

#if defined(HAVE_GETRUSAGE)
#  include <sys/types.h>
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#  ifdef HAVE_SYS_RUSAGE_H
#    include <sys/rusage.h>
#  endif
#  ifndef RUSAGE_SELF
#    define RUSAGE_SELF 0
#  endif
#endif

#if defined(HAVE_GETTIMEOFDAY)
#  include <sys/time.h>
#else
#  include <time.h>
#endif

#include <stdlib.h>
#include <inttypes.h>
// #include <syslog.h>

#if defined(HAVE_SETITIMER)
static struct itimerval it_set;
static uint32_t addsec,addusec;
#endif

#if defined(HAVE_GETRUSAGE)
static uint64_t lastautime;
static uint64_t lastastime;
#endif

static uint64_t lastget;

void cpu_init (void) {
#if defined(HAVE_SETITIMER)
	struct itimerval rc,uc,pc;
#endif
#if defined(HAVE_GETRUSAGE)
	struct rusage rus;
#endif
#if defined(HAVE_GETTIMEOFDAY)
	struct timeval tod;
#endif

#if defined(HAVE_GETTIMEOFDAY)
	gettimeofday(&tod,NULL);
	lastget = tod.tv_sec;
	lastget *= UINT64_C(1000000);
	lastget += tod.tv_usec;
#else
	lastget = time(NULL);
	lastget *= UINT64_C(1000000);
#endif

#if defined(HAVE_SETITIMER)
	addsec = 0;
	addusec = 0;
	it_set.it_interval.tv_sec = 0;
	it_set.it_interval.tv_usec = 0;
	it_set.it_value.tv_sec = MAXITIMER;
	it_set.it_value.tv_usec = 999999;
	setitimer(ITIMER_REAL,&it_set,&rc);                // real time
	setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
	setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time
#endif

#if defined(HAVE_GETRUSAGE)
        getrusage(RUSAGE_SELF,&rus);

        lastautime = (uint64_t)rus.ru_utime.tv_sec;
        lastautime *= UINT64_C(1000000);
        lastautime += (uint64_t)rus.ru_utime.tv_usec;

        lastastime = (uint64_t)rus.ru_stime.tv_sec;
        lastastime *= UINT64_C(1000000);
        lastastime += (uint64_t)rus.ru_stime.tv_usec;
#endif
}

void cpu_used (uint64_t *scpu,uint64_t *ucpu) {
#if defined(HAVE_SETITIMER)
	struct itimerval rc,uc,pc;
	uint64_t ucusec,pcusec;
#endif
#if defined(HAVE_GETRUSAGE)
	struct rusage rus;
	uint64_t autime,astime;
#endif
	uint64_t rdiff,now;
#if defined(HAVE_GETTIMEOFDAY)
	struct timeval tod;
#endif
	uint64_t systime,usertime;

#if defined(HAVE_GETTIMEOFDAY)
	gettimeofday(&tod,NULL);
	now = tod.tv_sec;
	now *= UINT64_C(1000000);
	now += tod.tv_usec;
#else
	now = time(NULL);
	now *= UINT64_C(1000000);
#endif

	if (now>lastget) {
		rdiff = now-lastget;
		lastget = now;
	} else {
		rdiff = 0;
	}

#if !(defined(HAVE_SETITIMER) || defined(HAVE_GETRUSAGE))
	systime = 0;
	usertime = 0;
#endif

//	syslog(LOG_NOTICE,"rdiff1: %"PRIu64,rdiff);
#if defined(HAVE_SETITIMER)
/* method 1 - use itimers - usually worse */
        setitimer(ITIMER_REAL,&it_set,&rc);                // real time
        setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
        setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time

        rc.it_value.tv_sec = MAXITIMER-rc.it_value.tv_sec;
        rc.it_value.tv_usec = 999999-rc.it_value.tv_usec;
        uc.it_value.tv_sec = MAXITIMER-uc.it_value.tv_sec;
        uc.it_value.tv_usec = 999999-uc.it_value.tv_usec;
        pc.it_value.tv_sec = MAXITIMER-pc.it_value.tv_sec;
        pc.it_value.tv_usec = 999999-pc.it_value.tv_usec;
	addsec += rc.it_value.tv_sec;
	addusec += rc.it_value.tv_usec;
	while (addusec>1000000) {
		addusec-=1000000;
		addsec++;
	}

	if (rc.it_value.tv_sec>=0 && rc.it_value.tv_usec>=0) {
		rdiff = (uint64_t)rc.it_value.tv_sec;
		rdiff *= UINT64_C(1000000);
		rdiff += (uint64_t)rc.it_value.tv_usec;
	}
	if (uc.it_value.tv_sec>=0 && pc.it_value.tv_sec>=0 && uc.it_value.tv_usec>=0 && pc.it_value.tv_usec>=0) {
	        ucusec = (uint64_t)uc.it_value.tv_sec;
		ucusec *= UINT64_C(1000000);
		ucusec += (uint64_t)uc.it_value.tv_usec;
	        pcusec = (uint64_t)pc.it_value.tv_sec;
		pcusec *= UINT64_C(1000000);
		pcusec += (uint64_t)pc.it_value.tv_usec;
	} else {
		ucusec = 0;
		pcusec = 0;
	}

        if (pcusec>ucusec) {
                pcusec-=ucusec;
        } else {
                pcusec=0;
        }
	usertime = ucusec;
	systime = pcusec;
#endif

//	syslog(LOG_NOTICE,"rdiff2: %"PRIu64,rdiff);
//	syslog(LOG_NOTICE,"usertime1: %"PRIu64",systime1: %"PRIu64,usertime,systime);

#if defined(HAVE_GETRUSAGE)
/* method 2 - use getrusage - usually better (if both are available, overwrites data set by itimer method) */
        getrusage(RUSAGE_SELF,&rus);

        autime = (uint64_t)rus.ru_utime.tv_sec;
        autime *= UINT64_C(1000000);
        autime += (uint64_t)rus.ru_utime.tv_usec;

        astime = (uint64_t)rus.ru_stime.tv_sec;
        astime *= UINT64_C(1000000);
        astime += (uint64_t)rus.ru_stime.tv_usec;

	if (autime>lastautime) {
		usertime = autime - lastautime;
		lastautime = autime;
	}

	if (astime>lastastime) {
		systime = astime - lastastime;
		lastastime = astime;
	}
#endif

//	syslog(LOG_NOTICE,"usertime2: %"PRIu64",systime2: %"PRIu64,usertime,systime);

	if (rdiff>0) {
		*scpu = (systime*UINT64_C(1000000000)) / rdiff;
		*ucpu = (usertime*UINT64_C(1000000000)) / rdiff;
	} else {
		*scpu = 0;
		*ucpu = 0;
	}

//	syslog(LOG_NOTICE,"scpu: %"PRIu32",ucpu: %"PRIu32,*scpu,*ucpu);
}
