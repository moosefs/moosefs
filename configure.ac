#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.60)
AC_INIT([MFS], [3.0.109], [bugs@moosefs.com], [moosefs])
release=1
buildno=$(cat buildno.txt)

dnl AC_CONFIG_SRCDIR([MFSCommunication.h])
AC_CONFIG_HEADER([config.h])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIRS([m4])

versmaj=$(echo $PACKAGE_VERSION | cut -d. -f1)
versmid=$(echo $PACKAGE_VERSION | cut -d. -f2)
versmin=$(echo $PACKAGE_VERSION | cut -d. -f3)
AC_DEFINE_UNQUOTED([VERSMAJ], [$versmaj], [Major MFS version])
AC_DEFINE_UNQUOTED([VERSMID], [$versmid], [Middle MFS version])
AC_DEFINE_UNQUOTED([VERSMIN], [($versmin)*2], [Minor MFS version])
AC_DEFINE_UNQUOTED([VERSHEX], [(($versmaj)*0x10000+($versmid)*0x100+($versmin)*2)], [Full MFS version as a hex number])
#AC_DEFINE_UNQUOTED([VERSMIN], [$versmin], [Minor MFS version])
#AC_DEFINE_UNQUOTED([VERSHEX], [(($versmaj)*0x10000+($versmid)*0x100+($versmin))], [Full MFS version as a hex number])
AC_DEFINE_UNQUOTED([RELEASE], [$release], [Release number])
AC_DEFINE_UNQUOTED([BUILDNO], [$buildno], [Build number])
AC_DEFINE([VERSION2INT(maj,med,min)], [((maj)*0x10000+(med)*0x100+(((maj)>1)?((min)*2):(min)))], [Version converter from human readable format to binary format used in MFS packets])
AC_DEFINE_UNQUOTED([VERSSTR], ["${PACKAGE_VERSION}-${release}"], [MFS version - string])

# Define paths
test "$prefix" = "NONE" && prefix="$ac_default_prefix"
test "$exec_prefix" = "NONE" && exec_prefix="$prefix"
eval DATA_PATH="${localstatedir}/mfs"
eval ETC_PATH="${sysconfdir}"
eval BIN_PATH="${bindir}"
eval SBIN_PATH="${sbindir}"
# use system /var/run when using system-wide installation prefix
if test "${prefix#/usr}" != "${prefix}" -o "${prefix#/opt}" != "${prefix}"; then
	RUN_PATH="/var/run/mfs"
else
	RUN_PATH="${prefix}/var/run/mfs"
fi
AC_SUBST([DATA_PATH])
AC_SUBST([ETC_PATH])
AC_SUBST([BIN_PATH])
AC_SUBST([SBIN_PATH])
AC_SUBST([RUN_PATH])
AC_DEFINE_UNQUOTED([PREFIX], ["${prefix}"], [Installation prefix])
AC_DEFINE_UNQUOTED([ETC_PATH], ["$ETC_PATH"], [Configuration directory])
AC_DEFINE_UNQUOTED([DATA_PATH], ["$DATA_PATH"], [Data directory])
AC_DEFINE_UNQUOTED([RUN_PATH], ["$RUN_PATH"], [PID file directory])

# Checks for programs.
AM_PROG_AR
AC_PROG_CC(mycc gcc llvm-gcc clang)
AM_PROG_CC_C_O
AC_PROG_LN_S
AC_PROG_LIBTOOL

AC_ARG_ENABLE([externalcflags], [AS_HELP_STRING([--enable-externalcflags], [Build with default CFLAGS])], [ enable_externalcflags=yes ])
AC_ARG_ENABLE([gperftools], [AS_HELP_STRING([--enable-gperftools], [Build with google perftools on (debug only)])], [ enable_gperftools=yes ])
AC_ARG_ENABLE([debug], [AS_HELP_STRING([--enable-debug], [Build version without optimizations])], [ enable_debug=yes ])

if test "X$MFS_FORCE_DEBUG" = "XYES"; then
	enable_debug=yes
fi

AX_CHECK_COMPILE_FLAG([-std=c11], [ CCSTD="-std=c11" ], [
	AX_CHECK_COMPILE_FLAG([-std=gnu99], [ CCSTD="-std=gnu99" ], [
		AX_CHECK_COMPILE_FLAG([-std=c99], [ CCSTD="-std=c99" ], [
			CCSTD=""
		])
	])
])

if test "$CCSTD" = "-std=c11"; then
	ac_have_atomics=maybe
	AC_DEFINE([HAVE_C11_COMPILER], [1], [Compiler understands C11 standard])
else
	ac_have_atomics=no
fi

if test "$GCC" = "yes"; then
	if test "$enable_externalcflags" != "yes" ; then
		if test "$enable_gperftools" = "yes" -o "$enable_debug" = "yes" ; then
			CFLAGS="-O0 -fno-omit-frame-pointer -g -DMFSDEBUG $CCSTD -Wall -Wextra -Wshadow -pedantic -fwrapv"
		else
			CFLAGS="-O2 $CFLAGS -g $CCSTD -Wall -Wextra -Wshadow -pedantic -fwrapv"
		fi
	fi
	if test "$enable_gperftools" = "yes"; then
		CPPFLAGS="$CPPFLAGS -DGPERFTOOLS"
		LDFLAGS="$LDFLAGS -L/opt/local/lib -lprofiler"
	fi
fi

root_sbindir="xxx"

case $target_os in
  *linux*)
	fuse=maybe
	bdev=maybe
	CPPFLAGS="$CPPFLAGS -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -DDEFAULT_SUGID_CLEAR_MODE_EXT"
	# Check whether exec_prefix=/usr:
	eval root_sbindir="${sbindir}"
	case $exec_prefix:$prefix in
	NONE:NONE | NONE:/usr | /usr:*)
		root_sbindir="/sbin"
	esac
	;;
  *solaris*)
	# OpenSolaris has fuse available and is still identified as "solaris"
	fuse=maybe
	bdev=no
	CPPFLAGS="$CPPFLAGS -D__EXTENSIONS__ -D_POSIX_PTHREAD_SEMANTICS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *darwin*)
	fuse=maybe
	bdev=no
	# MacFUSE installs to different prefix than pkgconfig
	if test -f /usr/local/lib/pkgconfig/fuse.pc ; then
		PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
		export PKG_CONFIG_PATH
	fi
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *bsd*)
	fuse=maybe
	bdev=no
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_BSD"
	;;
  *)
	fuse=no
	bdev=no
	CPPFLAGS="$CPPFLAGS -D_GNU_SOURCE"
	;;
esac

AC_SUBST([root_sbindir])

AX_PTHREAD(, [AC_MSG_ERROR([Posix threads not found])])
dnl AC_CHECK_LIB([pthread], [pthread_create])

# Checks for libraries.

# Solaris:
AC_SEARCH_LIBS([socket], [socket])
AC_SEARCH_LIBS([getaddrinfo], [nsl])
# FreeBSD:
AC_SEARCH_LIBS([kvm_open], [kvm])
# Linux:
AC_SEARCH_LIBS([clock_gettime], [rt])

# We do not use AC_SEARCH_LIBS to avoid adding '-lm' to every binary
mathlib=no
AC_CHECK_LIB([m],[round],[ AC_CHECK_HEADERS([math.h],[mathlib=yes]) ])

if test "$mathlib" = "no"; then
	MATH_LIBS=
else
	MATH_LIBS=-lm
fi
AC_SUBST([MATH_LIBS])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statvfs.h sys/time.h syslog.h unistd.h],, [AC_MSG_ERROR([One of required headers was not found])])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_BIGENDIAN
AC_C_INLINE
AC_C_VOLATILE
AC_C_FLEXIBLE_ARRAY_MEMBER
AC_HEADER_STDBOOL
AC_TYPE_INT8_T
AC_TYPE_UINT8_T
AC_TYPE_INT16_T
AC_TYPE_UINT16_T
AC_TYPE_INT32_T
AC_TYPE_UINT32_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_CHECK_MEMBERS([struct stat.st_birthtime])
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_flags])
AC_CHECK_MEMBER([struct sockaddr_in.sin_len], [ AC_DEFINE([HAVE_SOCKADDR_SIN_LEN], [1], [Do we have sockaddr.sin_len?]) ], [], [
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
])

AC_HEADER_TIME
AC_STRUCT_TM
AC_CHECK_MEMBERS([struct tm.tm_gmtoff],,, [
#ifdef TM_IN_SYS_TIME
#  include <sys/time.h>
#else
#  include <time.h>
#endif
])

AC_SYS_LARGEFILE

# Checks for library functions.
AC_FUNC_STRERROR_R
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_FUNC_LSTAT
AC_FUNC_MEMCMP
dnl AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT

dnl AC_FUNC_MALLOC
dnl AC_FUNC_REALLOC
# rpl_malloc/rpl_realloc problem AC_FUNC_MALLOC/AC_FUNC_REALLOC !!!
AC_CHECK_FUNCS([malloc realloc reallocf])

# required functions
AC_CHECK_FUNCS([atexit bzero ftruncate getaddrinfo getpass gettimeofday memmove memset mkdir realpath poll socket strchr strdup strtol strtoul ftello fseeko],, [AC_MSG_ERROR([One of required functions was not found])])

# optional temporary file function
AC_CHECK_FUNCS([mkstemp mktemp])

# optional error conversion functions
AC_CHECK_FUNCS([strerror perror])

# optional system functions
AC_CHECK_FUNCS([dup2 mlockall getcwd setproctitle])

# check for mmap support
AC_CHECK_HEADERS([sys/mman.h], [AC_CHECK_FUNCS([mmap])])
dnl AC_CHECK_HEADERS([sys/mman.h])

# check for xattr support
AC_CHECK_HEADERS([attr/xattr.h])
AC_CHECK_HEADERS([sys/xattr.h])

# check for bsd-locks (flock) optional header
AC_CHECK_HEADERS([sys/file.h])

# check if poll is in poll.h instead of sys/poll.h
AC_CHECK_HEADERS([poll.h], [AC_CHECK_FUNCS([poll])])

# optional I/O functions
AC_CHECK_FUNCS([pread pwrite readv writev posix_fadvise])

# optional sleep function
AC_CHECK_FUNCS([nanosleep])

# optional resource usage function and headers
AC_CHECK_FUNCS([getrusage setitimer])
AC_CHECK_HEADERS([sys/rusage.h sys/resource.h])
AC_CHECK_MEMBERS([struct rusage.ru_maxrss],,, [
#include <sys/time.h>
#ifdef HAVE_GETRUSAGE
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#  ifdef HAVE_SYS_RUSAGE_H
#    include <sys/rusage.h>
#  endif
#endif
])

# optional clock related headers and functions
AC_CHECK_HEADERS([mach/mach.h mach/mach_time.h], [AC_CHECK_FUNCS([mach_absolute_time mach_timebase_info])])
AC_CHECK_FUNCS([clock_gettime])

# optional getgroups headers
AC_CHECK_HEADERS([sys/sysctl.h])

# optional out of memory killer header
AC_CHECK_HEADERS([linux/oom.h])

# glibc tuning
AC_CHECK_HEADERS([malloc.h])
AC_CHECK_FUNCS([mallopt])

# core dumps
AC_CHECK_HEADERS([sys/prctl.h], [AC_CHECK_FUNCS([prctl])])

# check for asprintf
AC_CHECK_FUNCS([asprintf])

dnl optional thread functions
dnl AC_CHECK_FUNCS([pthread_spin_lock])

dnl optional interfaces (Linux, FreeBSD)
dnl AC_CHECK_FUNCS([fopencookie funopen])

AC_LANG([C])

AC_MSG_CHECKING([for __builtin_bswap16])
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([#include <inttypes.h>],[uint16_t x = __builtin_bswap16(0x1234)])],
	[
	AC_MSG_RESULT(yes)
	AC_DEFINE([BSWAP16(x)], [__builtin_bswap16(x)], [bswap16])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE([BSWAP16(x)], [((((x)<<8)&0xFF00) | (((x)>>8)&0xFF))], [bswap16])
	])

AC_MSG_CHECKING([for __builtin_bswap32])
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([#include <inttypes.h>],[uint32_t x = __builtin_bswap32(0x12345678)])],
	[
	AC_MSG_RESULT(yes)
	AC_DEFINE([BSWAP32(x)], [__builtin_bswap32(x)], [bswap32])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE([BSWAP32(x)], [((((x)<<24)&0xFF000000) | (((x)<<8)&0xFF0000) | (((x)>>8)&0xFF00) | (((x)>>24)&0xFF))], [bswap32])
	])

AC_MSG_CHECKING([for __builtin_bswap64])
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([#include <inttypes.h>],[uint64_t x = __builtin_bswap64(0x123456789ABCDEF0)])],
	[
	AC_MSG_RESULT(yes)
	AC_DEFINE([BSWAP64(x)], [__builtin_bswap64(x)], [bswap64])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE([BSWAP64(x)], [((((x)<<56)&0xFF00000000000000) | (((x)<<40)&0xFF000000000000) | (((x)<<24)&0xFF0000000000) | (((x)<<8)&0xFF00000000) | (((x)>>8)&0xFF000000) | (((x)>>24)&0xFF0000) | (((x)>>40)&0xFF00) | (((x)>>56)&0xFF))], [bswap64])
	])


# Check for Mac OS X's broken poll 
if test "$ac_cv_func_poll" = "yes"; then 
AC_MSG_CHECKING([if poll works on devices]) 
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([[
#include <stdlib.h> 
#include <fcntl.h> 
#include <poll.h>
]],[[
int main(void) { 
	struct pollfd fds[1]; 
	int fd; 
	fd = open("/dev/null", 1); 
	fds[0].fd = fd; 
	fds[0].events = POLLIN; 
	fds[0].revents = 0; 
	if (poll(fds, 1, 0) < 0 || (fds[0].revents & POLLNVAL) != 0) { 
		exit(1); /* Does not work for devices -- fail */ 
	} 
		exit(0); 
	}
]])],[ac_have_working_poll=yes],[ac_have_working_poll=no])
if test "$ac_have_working_poll" = "no" ; then
	AC_DEFINE([BROKEN_POLL], [1],[poll does not work properly on devices])
fi
AC_MSG_RESULT($ac_have_working_poll)
fi dnl poll() was found


AC_MSG_CHECKING([for __sync_OP_and_fetch])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <sys/types.h>]],
	       [[unsigned long long int foo, bar; bar = __sync_add_and_fetch(&foo, 1); bar = __sync_sub_and_fetch(&foo, 1); bar = __sync_or_and_fetch(&foo, 1); bar = __sync_and_and_fetch(&foo, 1); bar = __sync_xor_and_fetch(&foo, 1); ]])],
	       [ac_have___sync_op_and_fetch=yes], [ac_have___sync_op_and_fetch=no])
if test "$ac_have___sync_op_and_fetch" = "yes" ; then
	AC_DEFINE([HAVE___SYNC_OP_AND_FETCH], [1], [have __sync_OP_and_fetch intrinsics])
fi
AC_MSG_RESULT($ac_have___sync_op_and_fetch)


AC_MSG_CHECKING([for __sync_fetch_and_OP])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <sys/types.h>]],
	       [[unsigned long long int foo, bar; bar = __sync_fetch_and_add(&foo, 1); bar = __sync_fetch_and_sub(&foo, 1); bar = __sync_fetch_and_or(&foo, 1); bar = __sync_fetch_and_and(&foo, 1); bar = __sync_fetch_and_xor(&foo, 1); ]])],
	       [ac_have___sync_fetch_and_op=yes], [ac_have___sync_fetch_and_op=no])
if test "$ac_have___sync_fetch_and_op" = "yes" ; then
	AC_DEFINE([HAVE___SYNC_FETCH_AND_OP], [1], [have __sync_fetch_and_OP intrinsics])
fi
AC_MSG_RESULT($ac_have___sync_fetch_and_op)

AC_MSG_CHECKING([for __sync_bool_compare_and_swap])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <sys/types.h>]],
	       [[unsigned long long int foo; int i,j; foo = 54; i = __sync_bool_compare_and_swap(&foo,54,7); j = __sync_bool_compare_and_swap(&foo,23,77); ]])],
	       [ac_have___sync_bool_compare_and_swap=yes], [ac_have___sync_bool_compare_and_swap=no])
if test "$ac_have___sync_bool_compare_and_swap" = "yes" ; then
	AC_DEFINE([HAVE___SYNC_BOOL_COMPARE_AND_SWAP], [1], [have __sync_bool_compare_and_swap intrinsic])
fi
AC_MSG_RESULT($ac_have___sync_bool_compare_and_swap)

if test "$ac_have_atomics" != "no" ; then
	AC_MSG_CHECKING([for _Atomic support])
	AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <stdatomic.h>]],
		       [[_Atomic unsigned long long int foo; atomic_init(&foo,41); atomic_fetch_add(&foo,1); ]])],
		       [ac_have_atomics=yes], [ac_have_atomics=no])
	AC_MSG_RESULT($ac_have_atomics)
fi
if test "$ac_have_atomics" = "yes" ; then
	AC_DEFINE([HAVE_ATOMICS], [1], [have support for atomic operations])
fi



saved_cflags="$CFLAGS"
CFLAGS="-Werror"
AC_MSG_CHECKING([for __attribute__((fallthrough))])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[]],[[
int main(void) {
	int a=0;
	switch (a) {
	case 1:
		a=2;
		__attribute__((fallthrough));
	case 2:
		a=1;
	}
	return a;
}
]])],[ac_have_working_attribute_fallthrough=yes],[ac_have_working_attribute_fallthrough=no])
if test "$ac_have_working_attribute_fallthrough" = "yes" ; then
	AC_DEFINE([nobreak],[__attribute__((fallthrough))],[have __attribute__((fallthrough))])
else
	AC_DEFINE([nobreak],[],[do not have __attribute__((fallthrough))])
fi
AC_MSG_RESULT($ac_have_working_attribute_fallthrough)
CFLAGS="$saved_cflags"

AC_CHECK_PROGS([KILL], [kill])
PKG_PROG_PKG_CONFIG
PKG_CHECK_MODULES([SYSTEMD], [systemd], [
	systemdunitdir="`$PKG_CONFIG --variable=systemdsystemunitdir systemd`"
], [
	systemdunitdir=no
])
AC_SUBST([systemdunitdir])

dnl this is work in progress
dnl AC_ARG_ENABLE([lightmfs], [AS_HELP_STRING([--enable-lightmfs], [Build special version])])

AC_ARG_ENABLE([all],
	[AS_HELP_STRING([--disable-all], [By default do not build anything except for manually enabled modules])])

dnl AC_ARG_ENABLE([generalmans],
dnl	[AS_HELP_STRING([--disable-general-mans], [Do not install general mans])])

AC_ARG_ENABLE([mfsmaster],
	[AS_HELP_STRING([--disable-mfsmaster], [Do not build mfsmaster])],
	[],
	[enable_mfsmaster="$enable_all"])
AC_ARG_ENABLE([mfsmetalogger],
	[AS_HELP_STRING([--disable-mfsmetalogger], [Do not build mfsmetalogger])],
	[],
	[enable_mfsmetalogger="$enable_all"])
AC_ARG_ENABLE([mfssupervisor],
	[AS_HELP_STRING([--disable-mfssupervisor], [Do not build mfssupervisor])],
	[],
	[enable_mfssupervisor="$enable_all"])
AC_ARG_ENABLE([mfschunkserver],
	[AS_HELP_STRING([--disable-mfschunkserver], [Do not build mfschunkserver])],
	[],
	[enable_mfschunkserver="$enable_all"])
AC_ARG_ENABLE([mfsmount],
	[AS_HELP_STRING([--disable-mfsmount], [Do not build mfsmount])],
	[],
	[enable_mfsmount="$enable_all"])
AC_ARG_ENABLE([mfsbdev],
	[AS_HELP_STRING([--disable-mfsbdev], [Do not build mfsbdev])],
	[],
	[enable_mfsbdev="$enable_all"])
AC_ARG_ENABLE([mfscgi],
	[AS_HELP_STRING([--disable-mfscgi], [Do not install CGI scripts])],
	[],
	[enable_mfscgi="$enable_all"])
AC_ARG_ENABLE([mfscli],
	[AS_HELP_STRING([--disable-mfscli], [Do not install CLI script])],
	[],
	[enable_mfscli="$enable_all"])
AC_ARG_ENABLE([mfscgiserv],
	[AS_HELP_STRING([--disable-mfscgiserv], [Do not install CGI server])],
	[],
	[enable_mfscgiserv="$enable_all"])
AC_ARG_ENABLE([mfsnetdump],
	[AS_HELP_STRING([--disable-mfsnetdump], [Do not build mfsnetdump])],
	[],
	[enable_mfsnetdump="$enable_all"])
AC_ARG_WITH([mfscgi-dir],
	[AS_HELP_STRING([--with-mfscgi-dir=CGIDIR], [Choose CGI directory (default=DATADIR/mfscgi)])],
	[CGIDIR="${withval}"],
	[eval eval CGIDIR="${datadir}/mfscgi"])
AC_ARG_WITH([mfscgiserv-dir],
	[AS_HELP_STRING([--with-mfscgiserv-dir=CGISERVDIR], [Choose CGI directory (default=SBINDIR)])],
	[CGISERVDIR="${withval}"],
	[eval CGISERVDIR="${sbindir}"])
AC_ARG_WITH([zlib],
	[AS_HELP_STRING([--without-zlib], [Do not use zlib for PNG compression])],
	[use_zlib="${withval}"],
	[use_zlib=yes])
AC_ARG_WITH([systemdsystemunitdir],
	[AS_HELP_STRING([--with-systemdsystemunitdir=PATH], [Path to install systemd units (no=omit)])],
	[systemdunitdir="${withval}"])

AM_CONDITIONAL([WITH_SYSTEMD], [test "$systemdunitdir" != "no"])

if test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no" -o "$enable_mfscgiserv" != "no"; then
	AC_CHECK_PROGS(PYTHON, [python3 python2 python python3.7 python3.6 python3.5 python3.4 python2.7], [na])
	if test "$PYTHON" = na; then
		PYTHON="python"
		AC_MSG_RESULT([Python binary not found !!! - using default in shebangs])
	else
		AC_MSG_RESULT([Python binary for script shebangs: $PYTHON])
	fi
fi

dnl AX_PYTHON
dnl if test "$enable_mfscgi" = "no"; then
dnl	enable_mfscgiserv=no
dnl fi
AM_CONDITIONAL([WITH_PYTHON], [test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no" -o "$enable_mfscgiserv" != "no"])
AM_CONDITIONAL([BUILD_SCRIPTS], [test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no" -o "$enable_mfscgiserv" != "no"])
AM_CONDITIONAL([INSTALL_CGI], [test "$enable_mfscgi" != "no"])
AM_CONDITIONAL([INSTALL_CLI], [test "$enable_mfscli" != "no"])
AM_CONDITIONAL([INSTALL_CGISERV], [test "$enable_mfscgiserv" != "no"])
AM_CONDITIONAL([INSTALL_MOOSEFS_MOUNT], [test "$root_sbindir" != "xxx"])
AC_SUBST([CGIDIR])
AC_SUBST([CGISERVDIR])
AC_SUBST([PYTHON])

AM_CONDITIONAL([BUILD_MASTER], [test "$enable_mfsmaster" != "no"])
AM_CONDITIONAL([BUILD_METALOGGER], [test "$enable_mfsmetalogger" != "no"])
AM_CONDITIONAL([BUILD_SUPERVISOR], [test "$enable_mfssupervisor" != "no"])
AM_CONDITIONAL([BUILD_CHUNKSERVER], [test "$enable_mfschunkserver" != "no"])


pcap=no
AC_CHECK_LIB([pcap],[pcap_lib_version],[ AC_CHECK_HEADERS([pcap.h],[pcap=yes]) ])

if test "$pcap" = "no"; then
	PCAP_LIBS=
	if test "enable_mfsnetdump" != "no"; then
		echo "****** mfsnetdump disabled ******"
		echo "* mfsnetdump needs pcap library *"
		echo "*********************************"
		enable_mfsnetdump=no
	fi
else
	PCAP_LIBS=-lpcap
	# check for new PCAP pcap_findalldevs
	AC_CHECK_LIB([pcap], [pcap_findalldevs], [AC_DEFINE([HAVE_PCAP_FINDALLDEVS],[1],[libpcap has function pcap_findalldevs])])

fi
AC_SUBST([PCAP_LIBS])

AM_CONDITIONAL([BUILD_NETDUMP], [test "$enable_mfsnetdump" != "no"])

if test "$bdev" != "no" -a "enable_mfsbdev" != "no"; then
	AC_CHECK_HEADERS([linux/nbd.h linux/fs.h],,[bdev=no])
fi

AM_CONDITIONAL([WITH_BDEV], [test "$bdev" != "no" -a "$enable_mfsbdev" != "no"])

if test "$fuse" != "no" -a "enable_mfsmount" != "no"; then
	if test -n "$PKG_CONFIG"; then
		PKG_CHECK_MODULES([FUSE], [fuse3 >= 3.2.1], [AC_DEFINE([HAVE_FUSE3],[1],[libfuse3 detected])] , [ PKG_CHECK_MODULES([FUSE], [fuse >= 2.6], , [fuse=no]) ])
	else
		echo "no pkg-config - can't check FUSE version"
		fuse=no
	fi
fi
if test "$fuse" = "no"; then
	if test "$enable_mfsmount" = "yes"; then
		if test -n "$PKG_CONFIG"; then
			AC_MSG_ERROR([mfsmount build was forced, but fuse library is too old or not installed])
		else
			AC_MSG_ERROR([mfsmount build was forced, but pkg-config not installed])
		fi
	else 
		if test "$enable_mfsmount" != "no"; then
			if test -n "$PKG_CONFIG"; then
				echo "******************************** mfsmount disabled ********************************"
				echo "* fuse library is too old or not installed - mfsmount needs version 2.6 or higher *"
				echo "***********************************************************************************"
			else
				echo "**** mfsmount disabled *****"
				echo "* pkg-config not installed *"
				echo "****************************"
			fi
		fi
	fi
fi
if test "$fuse" != "no" ; then
	case $target_os in
		freebsd6*)
		echo "*************************************************************************"
		echo "* Dynamic linker in FreeBSD 6.x doesn't support symbol versioning.      *"
		echo "* It causes calling wrong functions from libfuse and crashing mfsmount. *"
		echo "* Due to above switching libfuse from dynamic linking to static.        *"
		echo "*************************************************************************"
		FUSE_LIBS="-pthread /usr/local/lib/libfuse.a /usr/local/lib/libiconv.a"
		AC_SUBST(FUSE_LIBS)
		;;
	esac
fi

AC_CHECK_LIB([fuse], [fuse_version], [AC_DEFINE([HAVE_FUSE_VERSION],[1],[libfuse has function fuse_version])])

AM_CONDITIONAL([WITH_MOUNT], [test "$fuse" != "no" -a "$enable_mfsmount" != "no"])

AM_CONDITIONAL([BUILD_CLIENT], [test \( "$fuse" != "no" -a "$enable_mfsmount" != "no" \) -o \( "$bdev" != "no" -a "$enable_mfsbdev" != "no" \)])

AM_CONDITIONAL([CREATE_ETC_MFS], [test "$enable_mfsmaster" != "no" -o "$enable_mfsmetalogger" != "no" -o "$enable_mfschunkserver" != "no" -o "$enable_mfsmount" != "no"])
AM_CONDITIONAL([CREATE_DATA_DIR], [test "$enable_mfsmaster" != "no" -o "$enable_mfsmetalogger" != "no" -o "$enable_mfschunkserver" != "no" -o "$enable_mfscgiserv" != "no"])

if test "$use_zlib" = "yes"; then

	zlib=no
	AC_CHECK_LIB(z, zlibVersion, [ AC_CHECK_HEADERS(zlib.h,[zlib=yes]) ])

	if test "$zlib" = "no"; then
		ZLIB_LIBS=
		if test "$enable_mfsmaster" != "no" -o "$enable_mfschunkserver" != "no"; then
			AC_MSG_ERROR([zlib development library not found])
		fi
	else
		ZLIB_LIBS=-lz
	fi
	AC_SUBST([ZLIB_LIBS])
fi

DEFAULT_USER=nobody
DEFAULT_GROUP=
AC_ARG_WITH([default-user],
	[AS_HELP_STRING([--with-default-user=USER], [Choose default user to run daemons as])],
	[DEFAULT_USER="${withval}"])
AC_ARG_WITH([default-group],
	[AS_HELP_STRING([--with-default-group=GROUP], [Choose default group to run daemons as])],
	[DEFAULT_GROUP="${withval}"])
AC_DEFINE_UNQUOTED([DEFAULT_USER], ["$DEFAULT_USER"], [Default working user])
AC_DEFINE_UNQUOTED([DEFAULT_GROUP], ["$DEFAULT_GROUP"], [Default working group])
AC_SUBST([DEFAULT_USER])
AC_SUBST([DEFAULT_GROUP])

DEFAULT_PORTBASE=9400
DEFAULT_MASTERNAME=mfsmaster
AC_ARG_WITH([default-portbase],
	[AS_HELP_STRING([--with-default-portbase=NUMBER], [Default communiaction port base (default=9400, which means ports 9419,9420,etc.)])],
	[DEFAULT_PORTBASE="${withval}"])
AC_ARG_WITH([default-mastername],
	[AS_HELP_STRING([--with-default-mastername=MASTER_DNS_NAME], [Defines default DNS name pointing to all masters (default=mfsmaster)])],
	[DEFAULT_MASTERNAME="${withval}"])
DEFAULT_MASTER_CONTROL_PORT=$((DEFAULT_PORTBASE+19))
DEFAULT_MASTER_CS_PORT=$((DEFAULT_PORTBASE+20))
DEFAULT_MASTER_CLIENT_PORT=$((DEFAULT_PORTBASE+21))
DEFAULT_CS_DATA_PORT=$((DEFAULT_PORTBASE+22))
DEFAULT_CGISERV_HTTP_PORT=$((DEFAULT_PORTBASE+25))
AC_DEFINE_UNQUOTED([DEFAULT_PORTBASE], [$DEFAULT_PORTBASE], [Default port base])
AC_DEFINE_UNQUOTED([DEFAULT_MASTER_CONTROL_PORT], ["$DEFAULT_MASTER_CONTROL_PORT"], [Default master command port])
AC_DEFINE_UNQUOTED([DEFAULT_MASTER_CS_PORT], ["$DEFAULT_MASTER_CS_PORT"], [Default master chunkserver port])
AC_DEFINE_UNQUOTED([DEFAULT_MASTER_CLIENT_PORT], ["$DEFAULT_MASTER_CLIENT_PORT"], [Default master client port])
AC_DEFINE_UNQUOTED([DEFAULT_CS_DATA_PORT], ["$DEFAULT_CS_DATA_PORT"], [Default chunkserver data port])
AC_DEFINE_UNQUOTED([DEFAULT_CGISERV_HTTP_PORT], ["$DEFAULT_CGISERV_HTTP_PORT"], [Default cgiserver http port])
AC_DEFINE_UNQUOTED([DEFAULT_MASTERNAME], ["$DEFAULT_MASTERNAME"], [Default master name])
AC_SUBST([DEFAULT_PORTBASE])
AC_SUBST([DEFAULT_MASTER_CONTROL_PORT])
AC_SUBST([DEFAULT_MASTER_CS_PORT])
AC_SUBST([DEFAULT_MASTER_CLIENT_PORT])
AC_SUBST([DEFAULT_CS_DATA_PORT])
AC_SUBST([DEFAULT_CGISERV_HTTP_PORT])
AC_SUBST([DEFAULT_MASTERNAME])

if test "$enable_lightmfs" = "yes"; then
	PROTO_BASE=65536
	LIGHT_MFS=1
else
	PROTO_BASE=0
	LIGHT_MFS=0
fi
AC_SUBST([PROTO_BASE])
AC_SUBST([LIGHT_MFS])
AC_DEFINE_UNQUOTED([PROTO_BASE], [$PROTO_BASE], [Base value for protocol commands])
AC_DEFINE_UNQUOTED([LIGHT_MFS], [$LIGHT_MFS], [Build light version])

AC_SUBST([release])
AC_SUBST([buildno])

AC_CONFIG_FILES([Makefile
		mfsmanpages/Makefile
		mfschunkserver/Makefile
		mfsscripts/Makefile
		mfsdata/Makefile
		mfsmaster/Makefile
		mfsmetatools/Makefile
		mfsmetalogger/Makefile
		mfssupervisor/Makefile
		mfsclient/Makefile
		mfsnetdump/Makefile
		mfstests/Makefile
		systemd/Makefile
		mfsscripts/mfscli.py
		mfsscripts/chart.cgi
		mfsscripts/index.html
		mfsscripts/mfscgiserv.py
		mfsdata/mfschunkserver.cfg
		mfsdata/mfsmaster.cfg
		mfsdata/mfsmetalogger.cfg
		mfsdata/mfsmount.cfg
		systemd/moosefs-cgiserv.service
		systemd/moosefs-chunkserver.service
		systemd/moosefs-chunkserver@.service
		systemd/moosefs-master.service
		systemd/moosefs-master@.service
		systemd/moosefs-metalogger.service
		systemd/moosefs-metalogger@.service
])
AC_OUTPUT
echo
echo "Summary:"
echo
echo "VERSION / RELEASE / BUILDNO ......... ${PACKAGE_VERSION} / ${release} / ${buildno}"
echo
echo "CC .................................. $CC"
echo "CFLAGS .............................. $CFLAGS"
echo "CPPFLAGS ............................ $CPPFLAGS"
echo "PYTHON .............................. $PYTHON"
echo
echo "PREFIX .............................. ${prefix}"
echo "RUN_PATH ............................ $RUN_PATH"
echo "DATA_PATH ........................... $DATA_PATH"
echo "ETC_PATH ............................ $ETC_PATH"
echo "BIN_PATH ............................ $BIN_PATH"
echo "SBIN_PATH ........................... $SBIN_PATH"
echo "CGI_PATH ............................ $CGIDIR"
echo "CGISERV_PATH ........................ $CGISERVDIR"
echo
echo "DEFAULT_USER:DEFAULT_GROUP .......... ${DEFAULT_USER}:${DEFAULT_GROUP}"
echo "MASTER DNS NAME ..................... ${DEFAULT_MASTERNAME}"
echo "PORTS ............................... ${DEFAULT_MASTER_CONTROL_PORT}:${DEFAULT_MASTER_CS_PORT}:${DEFAULT_MASTER_CLIENT_PORT}:${DEFAULT_CS_DATA_PORT}:${DEFAULT_CGISERV_HTTP_PORT}"
echo
if test "$BUILD_MASTER_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_MASTER ........................ $RESULT"
if test "$BUILD_METALOGGER_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_METALOGGER .................... $RESULT"
if test "$BUILD_SUPERVISOR_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_SUPERVISOR .................... $RESULT"
if test "$BUILD_CHUNKSERVER_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_CHUNKSERVER ................... $RESULT"
if test "$BUILD_NETDUMP_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_NETDUMP ....................... $RESULT"
if test "$BUILD_SCRIPTS_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_SCRIPTS ....................... $RESULT"
if test "$INSTALL_CGI_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "    CGI ............................. $RESULT"
if test "$INSTALL_CLI_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "    CLI ............................. $RESULT"
if test "$INSTALL_CGISERV_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "    CGISERV ......................... $RESULT"
if test "$BUILD_CLIENT_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "BUILD_CLIENT ........................ $RESULT"
if test "$WITH_MOUNT_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "    MOUNT ........................... $RESULT"
if test "$WITH_BDEV_TRUE" = ""; then RESULT="yes"; else RESULT="no"; fi
echo "    BDEV ............................ $RESULT"
echo
