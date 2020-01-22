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

/* 
   get memory usage
   special versions for FreeBSD,MacOS X and Linux (without checking in autotools)
   general 'getrusage' version for others
 */
 
#if __FreeBSD__
# include <sys/file.h>
# include <sys/sysctl.h>
# include <sys/user.h>
# include <kvm.h>
# include <sys/types.h>
# include <unistd.h>
#elif __APPLE__
# include <mach/mach_init.h>
# include <mach/task.h>
#elif __linux__
# include <fcntl.h>
# include <sys/types.h>
# include <unistd.h>
# include <stdlib.h>
#elif defined(HAVE_GETRUSAGE) && defined(HAVE_STRUCT_RUSAGE_RU_MAXRSS)
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
#  define MEMORY_USAGE 1
#endif
#include <inttypes.h>

uint8_t mem_used(uint64_t *rss,uint64_t *virt) {
#if defined(__FreeBSD__)
	static kvm_t* kd = NULL;
	
	if ( kd == NULL) {
		kd = kvm_open( NULL, "/dev/null", NULL, O_RDONLY, "kvm_open" ); // open once
	}
	if ( kd != NULL ) {
		// Use FreeBSD kvm function to get the size of resident pages (RSS).
		int pc = 0;
		struct kinfo_proc* kp;
		kp = kvm_getprocs(kd,KERN_PROC_PID,getpid(),&pc); // do not free returned struct
		if ( (kp != NULL) && (pc >= 1) ) { // in case multiple threads have the same PID
			*rss = kp->ki_rssize * getpagesize();
			*virt = kp->ki_size;
			return 1; // success
		}
	}
	*rss = 0;
	*virt = 0;
	return 0; // failed
#elif __APPLE__
	struct task_basic_info machInfo;
	mach_port_t machTask = mach_task_self();
	mach_msg_type_number_t machCount = TASK_BASIC_INFO_COUNT;
	if ( task_info( machTask, TASK_BASIC_INFO, (task_info_t)&machInfo, &machCount ) == KERN_SUCCESS ) {
		*rss = machInfo.resident_size;
		*virt = machInfo.virtual_size;
		return 1;
	}
	*rss = 0;
	*virt = 0;
	return 0; // error
#elif __linux__
	int fd = open("/proc/self/statm",O_RDONLY);
	char statbuff[1000],*p;
	int l,e;
	e = 0;
	*rss = 0;
	*virt = 0;
	if (fd>=0) {
		l = read(fd,statbuff,1000);
		if (l<1000 && l>0) {
			statbuff[l]=0;
			*virt = strtoul(statbuff,&p,10) * getpagesize();
			if (*p==' ') {
				*rss = strtoul(p+1,&p,10) * getpagesize();
				if (*p==' ' || *p=='\0') {
					e = 1;
				}
			}
		}
		close(fd);
	}
	return e;
#elif MEMORY_USAGE
	struct rusage ru;
	getrusage(RUSAGE_SELF,&ru);
	*rss = *virt = ru.ru_maxrss * UINT64_C(1024);
	return 2;
#else
	return 0;
#endif
}
