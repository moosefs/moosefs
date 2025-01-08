#ifdef __linux__
#include <sys/syscall.h>
#include <sys/resource.h>
#include <unistd.h>

#include "mfslog.h"

#if defined(SYS_ioprio_set) && defined(SYS_ioprio_get)
static inline int ioprio_set(int which, int who, int ioprio) {
	return syscall(SYS_ioprio_set, which, who, ioprio);
}

/*
static inline int ioprio_get(int which, int who) {
	return syscall(SYS_ioprio_get, which, who);
}
*/

enum {
	IOPRIO_CLASS_NONE,
	IOPRIO_CLASS_RT,
	IOPRIO_CLASS_BE,
	IOPRIO_CLASS_IDLE,
};

enum {
	IOPRIO_WHO_PROCESS = 1,
	IOPRIO_WHO_PGRP,
	IOPRIO_WHO_USER,
};

#define IOPRIO_CLASS_SHIFT	(13)
#define IOPRIO_PRIO_MASK	((1UL << IOPRIO_CLASS_SHIFT) - 1)

#define IOPRIO_PRIO_CLASS(mask)	((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)	((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data)	(((class) << IOPRIO_CLASS_SHIFT) | data)

void ionice_test(void) {
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"using linux ioprio (ionice) syscalls for I/O priority");
}

void ionice_high(void) {
	ioprio_set(IOPRIO_WHO_PROCESS,0,IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT,0));
}

void ionice_medium(void) {
	ioprio_set(IOPRIO_WHO_PROCESS,0,IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE,0));
}

void ionice_low(void) {
	setpriority(PRIO_PROCESS,0,19);
	ioprio_set(IOPRIO_WHO_PROCESS,0,IOPRIO_PRIO_VALUE(IOPRIO_CLASS_IDLE,0));
}

#else /* SYS_ioprio_set + SYS_ioprio_get */

void ionice_test(void) {
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"using linux nice for I/O priority");
}

void ionice_high(void) {
}

void ionice_medium(void) {
	int prio = getpriority(PRIO_PROCESS,0);
	setpriority(PRIO_PROCESS,0,(prio+19)/2);
}

void ionice_low(void) {
	setpriority(PRIO_PROCESS,0,19);
}

#endif /* SYS_ioprio_set + SYS_ioprio_get */

#else /* __linux__ */

// it is safe ignore this on other OS'es and live default settings

void ionice_test(void) {
}

void ionice_high(void) {
}

void ionice_medium(void) {
}

void ionice_low(void) {
}

#endif /* __linux__ */
