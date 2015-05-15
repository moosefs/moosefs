#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.60)
AC_INIT([MFS], [2.0.68], [bugs@moosefs.com], [moosefs])
release=1

dnl AC_CONFIG_SRCDIR([MFSCommunication.h])
AC_CONFIG_HEADER([config.h])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([1.14 foreign subdir-objects])
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
AC_DEFINE_UNQUOTED([VERSION2INT(maj,med,min)], [((maj)*0x10000+(med)*0x100+(((maj)>1)?((min)*2):(min)))], [Version converter from human readable format to binary format used in MFS packets])
AC_DEFINE_UNQUOTED([VERSSTR], ["${PACKAGE_VERSION}-${release}"], [MFS version - string])

# Checks for programs.
AC_PROG_CC(myclang clang llvm-gcc gcc)
dnl AC_PROG_CXX(clang++ llvm-g++ g++)
AC_PROG_LN_S
AC_PROG_LIBTOOL

AC_ARG_ENABLE([addresssanitizer], [AS_HELP_STRING([--enable-addresssanitizer], [Build with address sanitizer on (debug only)])], [ enable_addresssanitizer=yes ])
AC_ARG_ENABLE([gperftools], [AS_HELP_STRING([--enable-gperftools], [Build with google perftools on (debug only)])], [ enable_gperftools=yes ])
AC_ARG_ENABLE([debug], [AS_HELP_STRING([--enable-debug], [Build version without optimizations])], [ enable_debug=yes ])

if test "X$MFS_FORCE_DEBUG" = "XYES"; then
	enable_debug=yes
fi

if test "$GCC" = "yes"; then
	if test "$enable_addresssanitizer" = "yes" -o "$enable_gperftools" = "yes" -o "$enable_debug" = "yes" ; then
		CFLAGS="-O0 -fno-omit-frame-pointer -g -DMFSDEBUG  -std=c99 -Wall -Wextra -Wshadow -pedantic -fwrapv"
	else
		CFLAGS="-O2 $CFLAGS -std=c99 -Wall -Wextra -Wshadow -pedantic -fwrapv"
	fi
	if test "$enable_addresssanitizer" = "yes"; then
		CFLAGS="$CFLAGS -fsanitize=address"
	fi
	if test "$enable_gperftools" = "yes"; then
		CPPFLAGS="$CPPFLAGS -DGPERFTOOLS"
		LDFLAGS="$LDFLAGS -L/opt/local/lib -lprofiler"
	fi
fi

case $target_os in
  *linux*)
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -D_GNU_SOURCE -DDEFAULT_SUGID_CLEAR_MODE_EXT"
	;;
  *solaris*)
	# OpenSolaris has fuse available and is still identified as "solaris"
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -D__EXTENSIONS__ -D_POSIX_PTHREAD_SEMANTICS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *darwin*)
	fuse=maybe
	# MacFUSE installs to different prefix than pkgconfig
	if test -f /usr/local/lib/pkgconfig/fuse.pc ; then
		PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
		export PKG_CONFIG_PATH
	fi
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *bsd*)
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_BSD"
	;;
  *)
	fuse=no
	;;
esac

AX_PTHREAD(, [AC_MSG_ERROR([Posix threads not found])])
dnl AC_CHECK_LIB([pthread], [pthread_create])

# Checks for libraries.

# Solaris:
AC_SEARCH_LIBS([socket], [socket])
AC_SEARCH_LIBS([getaddrinfo], [nsl])
# FreeBSD:
AC_SEARCH_LIBS([kvm_open], [kvm])
# All:
AC_SEARCH_LIBS([round], [m])
# Linux:
AC_SEARCH_LIBS([clock_gettime], [rt])

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
AC_CHECK_MEMBER([struct sockaddr_in.sin_len], [ AC_DEFINE(HAVE_SOCKADDR_SIN_LEN, 1, [Do we have sockaddr.sin_len?]) ], [], [
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
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
dnl AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
# required functions
AC_CHECK_FUNCS([atexit bzero ftruncate getaddrinfo getpass gettimeofday memmove memset mkdir realpath poll socket strchr strdup strtol strtoul ftello fseeko],, [AC_MSG_ERROR([One of required functions was not found])])

# optional temporary file function
AC_CHECK_FUNCS([mkstemp mktemp])

# optional error conversion functions
AC_CHECK_FUNCS([strerror perror])

# optional system functions
AC_CHECK_FUNCS([dup2 mlockall getcwd])

# check for mmap support
AC_CHECK_HEADERS([sys/mman.h], [AC_CHECK_FUNCS([mmap])])
dnl AC_CHECK_HEADERS([sys/mman.h])

# optional I/O functions
AC_CHECK_FUNCS([pread pwrite readv writev])

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

# glibc tuning
AC_CHECK_HEADERS([malloc.h])
AC_CHECK_FUNCS([mallopt])

# core dumps
AC_CHECK_HEADERS([sys/prctl.h], [AC_CHECK_FUNCS([prctl])])

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
	AC_DEFINE_UNQUOTED([BSWAP16(x)], [__builtin_bswap16(x)], [bswap16])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE_UNQUOTED([BSWAP16(x)], [((((x)<<8)&0xFF00) | (((x)>>8)&0xFF))], [bswap16])
	])

AC_MSG_CHECKING([for __builtin_bswap32])
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([#include <inttypes.h>],[uint32_t x = __builtin_bswap32(0x12345678)])],
	[
	AC_MSG_RESULT(yes)
	AC_DEFINE_UNQUOTED([BSWAP32(x)], [__builtin_bswap32(x)], [bswap32])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE_UNQUOTED([BSWAP32(x)], [((((x)<<24)&0xFF000000) | (((x)<<8)&0xFF0000) | (((x)>>8)&0xFF00) | (((x)>>24)&0xFF))], [bswap32])
	])

AC_MSG_CHECKING([for __builtin_bswap64])
AC_RUN_IFELSE(
	[AC_LANG_PROGRAM([#include <inttypes.h>],[uint64_t x = __builtin_bswap64(0x123456789ABCDEF0)])],
	[
	AC_MSG_RESULT(yes)
	AC_DEFINE_UNQUOTED([BSWAP64(x)], [__builtin_bswap64(x)], [bswap64])
	],[
	AC_MSG_RESULT(no)
	AC_DEFINE_UNQUOTED([BSWAP64(x)], [((((x)<<56)&0xFF00000000000000) | (((x)<<40)&0xFF000000000000) | (((x)<<24)&0xFF0000000000) | (((x)<<8)&0xFF00000000) | (((x)>>8)&0xFF000000) | (((x)>>24)&0xFF0000) | (((x)>>40)&0xFF00) | (((x)>>56)&0xFF))], [bswap64])
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
	AC_DEFINE_UNQUOTED(BROKEN_POLL, 1,[poll does not work properly on devices])
fi
AC_MSG_RESULT($ac_have_working_poll)
fi dnl poll() was found


AC_MSG_CHECKING([for __sync_OP_and_fetch])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <sys/types.h>]],
	       [[unsigned long long int foo, bar; bar = __sync_add_and_fetch(&foo, 1); bar = __sync_sub_and_fetch(&foo, 1); bar = __sync_or_and_fetch(&foo, 1); bar = __sync_and_and_fetch(&foo, 1); bar = __sync_xor_and_fetch(&foo, 1); ]])],
	       [ac_have___sync_op_and_fetch=yes], [ac_have___sync_op_and_fetch=no])
if test "$ac_have___sync_op_and_fetch" = "yes" ; then
	AC_DEFINE_UNQUOTED(HAVE___SYNC_OP_AND_FETCH, 1, [have __sync_OP_and_fetch intrinsics])
fi
AC_MSG_RESULT($ac_have___sync_op_and_fetch)


AC_MSG_CHECKING([for __sync_fetch_and_OP])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <sys/types.h>]],
	       [[unsigned long long int foo, bar; bar = __sync_fetch_and_add(&foo, 1); bar = __sync_fetch_and_sub(&foo, 1); bar = __sync_fetch_and_or(&foo, 1); bar = __sync_fetch_and_and(&foo, 1); bar = __sync_fetch_and_xor(&foo, 1); ]])],
	       [ac_have___sync_fetch_and_op=yes], [ac_have___sync_fetch_and_op=no])
if test "$ac_have___sync_fetch_and_op" = "yes" ; then
	AC_DEFINE_UNQUOTED(HAVE___SYNC_FETCH_AND_OP, 1, [have __sync_fetch_and_OP intrinsics])
fi
AC_MSG_RESULT($ac_have___sync_fetch_and_op)


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

AC_ARG_ENABLE([generalmans],
	[AS_HELP_STRING([--disable-general-mans], [Do not install general mans])])
AC_ARG_ENABLE([mfsmaster],
	[AS_HELP_STRING([--disable-mfsmaster], [Do not build mfsmaster])])
AC_ARG_ENABLE([mfsmetalogger],
	[AS_HELP_STRING([--disable-mfsmetalogger], [Do not build mfsmetalogger])])
AC_ARG_ENABLE([mfssupervisor],
	[AS_HELP_STRING([--disable-mfssupervisor], [Do not build mfssupervisor])])
AC_ARG_ENABLE([mfschunkserver],
	[AS_HELP_STRING([--disable-mfschunkserver], [Do not build mfschunkserver])])
AC_ARG_ENABLE([mfsmount],
	[AS_HELP_STRING([--disable-mfsmount], [Do not build mfsmount])])
AC_ARG_ENABLE([mfscgi],
	[AS_HELP_STRING([--disable-mfscgi], [Do not install CGI scripts])])
AC_ARG_ENABLE([mfscli],
	[AS_HELP_STRING([--disable-mfscli], [Do not install CLI script])])
AC_ARG_ENABLE([mfscgiserv],
	[AS_HELP_STRING([--disable-mfscgiserv], [Do not install CGI server])])
AC_ARG_ENABLE([mfsnetdump],
	[AS_HELP_STRING([--disable-mfsnetdump], [Do not build mfsnetdump])])
AC_ARG_WITH([mfscgi-dir],
	[AS_HELP_STRING([--with-mfscgi-dir=CGIDIR], [Choose CGI directory (default=DATADIR/mfscgi)])],
	[CGIDIR=$withval],
	[CGIDIR=$datadir/mfscgi])
AC_ARG_WITH([mfscgiserv-dir],
	[AS_HELP_STRING([--with-mfscgiserv-dir=CGISERVDIR], [Choose CGI directory (default=SBINDIR)])],
	[CGISERVDIR=$withval],
	[CGISERVDIR=$sbindir])
AC_ARG_WITH([zlib],
	[AS_HELP_STRING([--without-zlib], [Do not use zlib for PNG compression])],
	[use_zlib=$withval],
	[use_zlib=yes])
AC_ARG_WITH([systemdsystemunitdir],
	[AS_HELP_STRING([--with-systemdsystemunitdir=PATH], [Path to install systemd units (no=omit)])],
	[systemdunitdir=$withval])

AM_CONDITIONAL([WITH_SYSTEMD], [test "$systemdunitdir" != "no"])

if test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no"; then
	AM_PATH_PYTHON([2.5],,[:])
	if test "$PYTHON" = ":"; then
		enable_mfscgi=no
		enable_mfscli=no
	fi
fi
dnl if test "$enable_mfscgi" = "no"; then
dnl	enable_mfscgiserv=no
dnl fi
AM_CONDITIONAL([WITH_PYTHON], [test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no"])
AM_CONDITIONAL([BUILD_SCRIPTS], [test "$enable_mfscgi" != "no" -o "$enable_mfscli" != "no" -o "$enable_mfscgiserv" != "no"])
AM_CONDITIONAL([INSTALL_CGI], [test "$enable_mfscgi" != "no"])
AM_CONDITIONAL([INSTALL_CLI], [test "$enable_mfscli" != "no"])
AM_CONDITIONAL([INSTALL_CGISERV], [test "$enable_mfscgiserv" != "no"])
AC_SUBST([CGIDIR])
AC_SUBST([CGISERVDIR])
AC_SUBST([PYTHON])

AM_CONDITIONAL([BUILD_MASTER], [test "$enable_mfsmaster" != "no"])
AM_CONDITIONAL([BUILD_METALOGGER], [test "$enable_mfsmetalogger" != "no"])
AM_CONDITIONAL([BUILD_SUPERVISOR], [test "$enable_mfssupervisor" != "no"])
AM_CONDITIONAL([BUILD_CHUNKSERVER], [test "$enable_mfschunkserver" != "no"])


pcap=no
AC_SEARCH_LIBS([pcap_lib_version],[pcap],[ AC_CHECK_HEADERS([pcap.h],[pcap=yes]) ])

if test "$pcap" != "yes" -a "enable_mfsnetdump" != "no"; then
	echo "****** mfsnetdump disabled ******"
	echo "* mfsnetdump needs pcap library *"
	echo "*********************************"
	enable_mfsnetdump=no
fi

AM_CONDITIONAL([BUILD_NETDUMP], [test "$enable_mfsnetdump" != "no"])

if test "$fuse" != "no" -a "enable_mfsmount" != "no"; then
	if test -n "$PKG_CONFIG"; then
		PKG_CHECK_MODULES([FUSE], [fuse >= 2.6], , [fuse=no])
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

AC_CHECK_LIB(fuse, fuse_version, [AC_DEFINE([HAVE_FUSE_VERSION],[1],[libfuse has function fuse_version])])

AM_CONDITIONAL([BUILD_MOUNT], [test "$fuse" != "no" -a "$enable_mfsmount" != "no"])

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

test "$prefix" = "NONE" && prefix=$ac_default_prefix
test "$exec_prefix" = "NONE" && exec_prefix=$prefix
eval DATA_PATH=${localstatedir}/mfs
eval ETC_PATH=${sysconfdir}
eval BIN_PATH=${bindir}
eval SBIN_PATH=${sbindir}
# use system /var/run when using system-wide installation prefix
if test "${prefix#/usr}" != "${prefix}" -o "${prefix#/opt}" != "${prefix}"; then
	RUN_PATH=/var/run/mfs
else
	RUN_PATH=${prefix}/var/run/mfs
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

DEFAULT_USER=nobody
DEFAULT_GROUP=
AC_ARG_WITH([default-user],
	[AS_HELP_STRING([--with-default-user=USER], [Choose default user to run daemons as])],
	[DEFAULT_USER=$withval])
AC_ARG_WITH([default-group],
	[AS_HELP_STRING([--with-default-group=GROUP], [Choose default group to run daemons as])],
	[DEFAULT_GROUP=$withval])
AC_DEFINE_UNQUOTED([DEFAULT_USER], ["$DEFAULT_USER"], [Default working user])
AC_DEFINE_UNQUOTED([DEFAULT_GROUP], ["$DEFAULT_GROUP"], [Default working group])
AC_SUBST([DEFAULT_USER])
AC_SUBST([DEFAULT_GROUP])

DEFAULT_PORTBASE=9400
DEFAULT_MASTERNAME=mfsmaster
AC_ARG_WITH([default-portbase],
	[AS_HELP_STRING([--with-default-portbase=NUMBER], [Default communiaction port base (default=9400, which means ports 9419,9420,etc.)])],
	[DEFAULT_PORTBASE=$withval])
AC_ARG_WITH([default-mastername],
	[AS_HELP_STRING([--with-default-mastername=MASTER_DNS_NAME], [Defines default DNS name pointing to all masters (default=mfsmaster)])],
	[DEFAULT_MASTERNAME=$withval])
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

AC_CONFIG_FILES([Makefile
		mfsmanpages/Makefile
		mfschunkserver/Makefile
		mfsscripts/Makefile
		mfsdata/Makefile
		mfsmaster/Makefile
		mfsmetadump/Makefile
		mfsmetalogger/Makefile
		mfssupervisor/Makefile
		mfsmount/Makefile
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
		systemd/moosefs-master.service
		systemd/moosefs-metalogger.service
])
AC_OUTPUT
