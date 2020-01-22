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

#if defined(_THREAD_SAFE) || defined(_REENTRANT) || defined(_USE_PTHREADS)
#  define USE_PTHREADS 1
#endif

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#ifdef USE_PTHREADS
#include <pthread.h>
#endif

#include "massert.h"

typedef struct errent {
	int num;
	const char* str;
} errent;

static errent errtab[] = {
#ifdef E2BIG
	{E2BIG,"E2BIG (Argument list too long)"},
#endif
#ifdef EACCES
	{EACCES,"EACCES (Permission denied)"},
#endif
#ifdef EADDRINUSE
	{EADDRINUSE,"EADDRINUSE (Address already in use)"},
#endif
#ifdef EADDRNOTAVAIL
	{EADDRNOTAVAIL,"EADDRNOTAVAIL (Cannot assign requested address)"},
#endif
#ifdef EADV
	{EADV,"EADV (Advertise error)"},
#endif
#ifdef EAFNOSUPPORT
	{EAFNOSUPPORT,"EAFNOSUPPORT (Address family not supported by protocol family)"},
#endif
#ifdef EAGAIN
	{EAGAIN,"EAGAIN (Resource temporarily unavailable)"},
#endif
#ifdef EALREADY
	{EALREADY,"EALREADY (Operation already in progress)"},
#endif
#ifdef EAUTH
	{EAUTH,"EAUTH (Authentication error)"},
#endif
#ifdef EBADARCH
	{EBADARCH,"EBADARCH (Bad CPU type in executable)"},
#endif
#ifdef EBADEXEC
	{EBADEXEC,"EBADEXEC (Bad executable)"},
#endif
#ifdef EBADE
	{EBADE,"EBADE (Invalid exchange)"},
#endif
#ifdef EBADFD
	{EBADFD,"EBADFD (File descriptor invalid for this operation)"},
#endif
#ifdef EBADF
	{EBADF,"EBADF (Bad file descriptor)"},
#endif
#ifdef EBADMACHO
	{EBADMACHO,"EBADMACHO (Malformed Macho file)"},
#endif
#ifdef EBADMSG
	{EBADMSG,"EBADMSG (Bad message)"},
#endif
#ifdef EBADRPC
	{EBADRPC,"EBADRPC (RPC struct is bad)"},
#endif
#ifdef EBADRQC
	{EBADRQC,"EBADRQC (Invalid request code)"},
#endif
#ifdef EBADR
	{EBADR,"EBADR (Invalid request descriptor)"},
#endif
#ifdef EBADSLT
	{EBADSLT,"EBADSLT (Invalid slot)"},
#endif
#ifdef EBFONT
	{EBFONT,"EBFONT (Bad font file format)"},
#endif
#ifdef EBUSY
	{EBUSY,"EBUSY (Device or resource busy)"},
#endif
#ifdef ECANCELED
	{ECANCELED,"ECANCELED (Operation canceled)"},
#endif
#ifdef ECHILD
	{ECHILD,"ECHILD (No child processes)"},
#endif
#ifdef ECHRNG
	{ECHRNG,"ECHRNG (Channel number out of range)"},
#endif
#ifdef ECOMM
	{ECOMM,"ECOMM (Communication error on send)"},
#endif
#ifdef ECONNABORTED
	{ECONNABORTED,"ECONNABORTED (Software caused connection abort)"},
#endif
#ifdef ECONNREFUSED
	{ECONNREFUSED,"ECONNREFUSED (Connection refused)"},
#endif
#ifdef ECONNRESET
	{ECONNRESET,"ECONNRESET (Connection reset by peer)"},
#endif
#ifdef EDEADLK
	{EDEADLK,"EDEADLK (Resource deadlock would occur)"},
#endif
#ifdef EDEADLOCK
	{EDEADLOCK,"EDEADLOCK (File locking deadlock error)"},
#endif
#ifdef EDESTADDRREQ
	{EDESTADDRREQ,"EDESTADDRREQ (Destination address required)"},
#endif
#ifdef EDEVERR
	{EDEVERR,"EDEVERR (Device error, e.g. paper out)"},
#endif
#ifdef EDOM
	{EDOM,"EDOM (Numerical argument out of domain)"},
#endif
#ifdef EDOOFUS
	{EDOOFUS,"EDOOFUS (Programming error)"},
#endif
#ifdef EDOTDOT
	{EDOTDOT,"EDOTDOT (RFS specific error)"},
#endif
#ifdef EDQUOT
	{EDQUOT,"EDQUOT (Quota exceeded)"},
#endif
#ifdef EEXIST
	{EEXIST,"EEXIST (File exists)"},
#endif
#ifdef EFAULT
	{EFAULT,"EFAULT (Bad address)"},
#endif
#ifdef EFBIG
	{EFBIG,"EFBIG (File too large)"},
#endif
#ifdef EFTYPE
	{EFTYPE,"EFTYPE (Inappropriate file type or format)"},
#endif
#ifdef EHOSTDOWN
	{EHOSTDOWN,"EHOSTDOWN (Host is down)"},
#endif
#ifdef EHOSTUNREACH
	{EHOSTUNREACH,"EHOSTUNREACH (No route to host)"},
#endif
#ifdef EIDRM
	{EIDRM,"EIDRM (Identifier removed)"},
#endif
#ifdef EILSEQ
	{EILSEQ,"EILSEQ (Illegal byte sequence)"},
#endif
#ifdef EINPROGRESS
	{EINPROGRESS,"EINPROGRESS (Operation now in progress)"},
#endif
#ifdef EINTR
	{EINTR,"EINTR (Interrupted system call)"},
#endif
#ifdef EINVAL
	{EINVAL,"EINVAL (Invalid argument)"},
#endif
#ifdef EIO
	{EIO,"EIO (Input/output error)"},
#endif
#ifdef EISCONN
	{EISCONN,"EISCONN (Transport endpoint is already connected)"},
#endif
#ifdef EISDIR
	{EISDIR,"EISDIR (Is a directory)"},
#endif
#ifdef EISNAM
	{EISNAM,"EISNAM (Is a named type file)"},
#endif
#ifdef EKEYEXPIRED
	{EKEYEXPIRED,"EKEYEXPIRED (Key has expired)"},
#endif
#ifdef EKEYREJECTED
	{EKEYREJECTED,"EKEYREJECTED (Key was rejected by service)"},
#endif
#ifdef EKEYREVOKED
	{EKEYREVOKED,"EKEYREVOKED (Key has been revoked)"},
#endif
#ifdef EL2HLT
	{EL2HLT,"EL2HLT (Level 2 halted)"},
#endif
#ifdef EL2NSYNC
	{EL2NSYNC,"EL2NSYNC (Level 2 not synchronized)"},
#endif
#ifdef EL3HLT
	{EL3HLT,"EL3HLT (Level 3 halted)"},
#endif
#ifdef EL3RST
	{EL3RST,"EL3RST (Level 3 reset)"},
#endif
#ifdef ELIBACC
	{ELIBACC,"ELIBACC (Can not access a needed shared library)"},
#endif
#ifdef ELIBBAD
	{ELIBBAD,"ELIBBAD (Accessing a corrupted shared library)"},
#endif
#ifdef ELIBEXEC
	{ELIBEXEC,"ELIBEXEC (Cannot exec a shared library directly)"},
#endif
#ifdef ELIBMAX
	{ELIBMAX,"ELIBMAX (Attempting to link in too many shared libraries)"},
#endif
#ifdef ELIBSCN
	{ELIBSCN,"ELIBSCN (.lib section in a.out corrupted)"},
#endif
#ifdef ELNRNG
	{ELNRNG,"ELNRNG (Link number out of range)"},
#endif
#ifdef ELOCKUNMAPPED
	{ELOCKUNMAPPED,"ELOCKUNMAPPED (locked lock was unmapped)"},
#endif
#ifdef ELOOP
	{ELOOP,"ELOOP (Too many levels of symbolic links)"},
#endif
#ifdef EMEDIUMTYPE
	{EMEDIUMTYPE,"EMEDIUMTYPE (Wrong medium type)"},
#endif
#ifdef EMFILE
	{EMFILE,"EMFILE (Too many open files)"},
#endif
#ifdef EMLINK
	{EMLINK,"EMLINK (Too many links)"},
#endif
#ifdef EMSGSIZE
	{EMSGSIZE,"EMSGSIZE (Message too long)"},
#endif
#ifdef EMULTIHOP
	{EMULTIHOP,"EMULTIHOP (Multihop attempted)"},
#endif
#ifdef ENAMETOOLONG
	{ENAMETOOLONG,"ENAMETOOLONG (File name too long)"},
#endif
#ifdef ENAVAIL
	{ENAVAIL,"ENAVAIL (No XENIX semaphores available)"},
#endif
#ifdef ENEEDAUTH
	{ENEEDAUTH,"ENEEDAUTH (Need authenticator)"},
#endif
#ifdef ENETDOWN
	{ENETDOWN,"ENETDOWN (Network is down)"},
#endif
#ifdef ENETRESET
	{ENETRESET,"ENETRESET (Network dropped connection because of reset)"},
#endif
#ifdef ENETUNREACH
	{ENETUNREACH,"ENETUNREACH (Network is unreachable)"},
#endif
#ifdef ENFILE
	{ENFILE,"ENFILE (Too many open files in system)"},
#endif
#ifdef ENOANO
	{ENOANO,"ENOANO (No anode)"},
#endif
#ifdef ENOATTR
	{ENOATTR,"ENOATTR (Attribute not found)"},
#endif
#ifdef ENOBUFS
	{ENOBUFS,"ENOBUFS (No buffer space available)"},
#endif
#ifdef ENOCSI
	{ENOCSI,"ENOCSI (No CSI structure available)"},
#endif
#ifdef ENODATA
	{ENODATA,"ENODATA (No data available)"},
#endif
#ifdef ENODEV
	{ENODEV,"ENODEV (Operation not supported by device or no such device)"},
#endif
#ifdef ENOENT
	{ENOENT,"ENOENT (No such file or directory)"},
#endif
#ifdef ENOEXEC
	{ENOEXEC,"ENOEXEC (Exec format error)"},
#endif
#ifdef ENOKEY
	{ENOKEY,"ENOKEY (Required key not available)"},
#endif
#ifdef ENOLCK
	{ENOLCK,"ENOLCK (No locks available)"},
#endif
#ifdef ENOLINK
	{ENOLINK,"ENOLINK (Link has been severed)"},
#endif
#ifdef ENOMEDIUM
	{ENOMEDIUM,"ENOMEDIUM (No medium found)"},
#endif
#ifdef ENOMEM
	{ENOMEM,"ENOMEM (Cannot allocate memory)"},
#endif
#ifdef ENOMSG
	{ENOMSG,"ENOMSG (No message of desired type)"},
#endif
#ifdef ENONET
	{ENONET,"ENONET (Machine is not on the network)"},
#endif
#ifdef ENOPKG
	{ENOPKG,"ENOPKG (Package not installed)"},
#endif
#ifdef ENOPOLICY
	{ENOPOLICY,"ENOPOLICY (No such policy registered)"},
#endif
#ifdef ENOPROTOOPT
	{ENOPROTOOPT,"ENOPROTOOPT (Protocol not available)"},
#endif
#ifdef ENOSPC
	{ENOSPC,"ENOSPC (No space left on device)"},
#endif
#ifdef ENOSR
	{ENOSR,"ENOSR (Out of streams resources)"},
#endif
#ifdef ENOSTR
	{ENOSTR,"ENOSTR (Device not a stream)"},
#endif
#ifdef ENOSYS
	{ENOSYS,"ENOSYS (Unsupported file system operation)"},
#endif
#ifdef ENOTACTIVE
	{ENOTACTIVE,"ENOTACTIVE (Facility is not active)"},
#endif
#ifdef ENOTBLK
	{ENOTBLK,"ENOTBLK (Block device required)"},
#endif
#ifdef ENOTCONN
	{ENOTCONN,"ENOTCONN (Transport endpoint is not connected)"},
#endif
#ifdef ENOTDIR
	{ENOTDIR,"ENOTDIR (Not a directory)"},
#endif
#ifdef ENOTEMPTY
	{ENOTEMPTY,"ENOTEMPTY (Directory not empty)"},
#endif
#ifdef ENOTNAM
	{ENOTNAM,"ENOTNAM (Not a XENIX named type file)"},
#endif
#ifdef ENOTRECOVERABLE
	{ENOTRECOVERABLE,"ENOTRECOVERABLE (State not recoverable)"},
#endif
#ifdef ENOTSOCK
	{ENOTSOCK,"ENOTSOCK (Socket operation on non-socket)"},
#endif
#ifdef ENOTSUP
	{ENOTSUP,"ENOTSUP (Operation not supported)"},
#endif
#ifdef ENOTTY
	{ENOTTY,"ENOTTY (Inappropriate ioctl for device)"},
#endif
#ifdef ENOTUNIQ
	{ENOTUNIQ,"ENOTUNIQ (Name not unique on network)"},
#endif
#ifdef ENXIO
	{ENXIO,"ENXIO (No such device or address)"},
#endif
#ifdef EOPNOTSUPP
	{EOPNOTSUPP,"EOPNOTSUPP (Operation not supported on transport endpoint)"},
#endif
#ifdef EOVERFLOW
	{EOVERFLOW,"EOVERFLOW (Value too large to be stored in data type)"},
#endif
#ifdef EOWNERDEAD
	{EOWNERDEAD,"EOWNERDEAD (Process died with the lock)"},
#endif
#ifdef EPERM
	{EPERM,"EPERM (Operation not permitted)"},
#endif
#ifdef EPFNOSUPPORT
	{EPFNOSUPPORT,"EPFNOSUPPORT (Protocol family not supported)"},
#endif
#ifdef EPIPE
	{EPIPE,"EPIPE (Broken pipe)"},
#endif
#ifdef EPROCLIM
	{EPROCLIM,"EPROCLIM (Too many processes)"},
#endif
#ifdef EPROCUNAVAIL
	{EPROCUNAVAIL,"EPROCUNAVAIL (Bad procedure for program)"},
#endif
#ifdef EPROGMISMATCH
	{EPROGMISMATCH,"EPROGMISMATCH (Program version wrong)"},
#endif
#ifdef EPROGUNAVAIL
	{EPROGUNAVAIL,"EPROGUNAVAIL (RPC prog. not avail)"},
#endif
#ifdef EPROTONOSUPPORT
	{EPROTONOSUPPORT,"EPROTONOSUPPORT (Protocol not supported)"},
#endif
#ifdef EPROTOTYPE
	{EPROTOTYPE,"EPROTOTYPE (Protocol wrong type for socket)"},
#endif
#ifdef EPROTO
	{EPROTO,"EPROTO (Protocol error)"},
#endif
#ifdef EPWROFF
	{EPWROFF,"EPWROFF (Device power is off)"},
#endif
#ifdef ERANGE
	{ERANGE,"ERANGE (Result too large)"},
#endif
#ifdef EREMCHG
	{EREMCHG,"EREMCHG (Remote address changed)"},
#endif
#ifdef EREMOTEIO
	{EREMOTEIO,"EREMOTEIO (Remote I/O error)"},
#endif
#ifdef EREMOTE
	{EREMOTE,"EREMOTE (Object is remote)"},
#endif
#ifdef ERESTART
	{ERESTART,"ERESTART (Interrupted system call should be restarted)"},
#endif
#ifdef EROFS
	{EROFS,"EROFS (Read-only file system)"},
#endif
#ifdef ERPCMISMATCH
	{ERPCMISMATCH,"ERPCMISMATCH (RPC version wrong)"},
#endif
#ifdef ESHLIBVERS
	{ESHLIBVERS,"ESHLIBVERS (Shared library version mismatch)"},
#endif
#ifdef ESHUTDOWN
	{ESHUTDOWN,"ESHUTDOWN (Cannot send after transport endpoint shutdown)"},
#endif
#ifdef ESOCKTNOSUPPORT
	{ESOCKTNOSUPPORT,"ESOCKTNOSUPPORT (Socket type not supported)"},
#endif
#ifdef ESPIPE
	{ESPIPE,"ESPIPE (Illegal seek)"},
#endif
#ifdef ESRCH
	{ESRCH,"ESRCH (No such process)"},
#endif
#ifdef ESRMNT
	{ESRMNT,"ESRMNT (Srmount error)"},
#endif
#ifdef ESTALE
	{ESTALE,"ESTALE (Stale NFS file handle)"},
#endif
#ifdef ESTRPIPE
	{ESTRPIPE,"ESTRPIPE (Streams pipe error)"},
#endif
#ifdef ETIMEDOUT
	{ETIMEDOUT,"ETIMEDOUT (Operation timed out)"},
#endif
#ifdef ETIME
	{ETIME,"ETIME (Timer expired)"},
#endif
#ifdef ETOOMANYREFS
	{ETOOMANYREFS,"ETOOMANYREFS (Too many references: cannot splice)"},
#endif
#ifdef ETXTBSY
	{ETXTBSY,"ETXTBSY (Text file busy)"},
#endif
#ifdef EUCLEAN
	{EUCLEAN,"EUCLEAN (Structure needs cleaning)"},
#endif
#ifdef EUNATCH
	{EUNATCH,"EUNATCH (Protocol driver not attached)"},
#endif
#ifdef EUSERS
	{EUSERS,"EUSERS (Too many users)"},
#endif
#ifdef EXDEV
	{EXDEV,"EXDEV (Cross-device link)"},
#endif
#ifdef EXFULL
	{EXFULL,"EXFULL (Exchange full)"},
#endif
	{0,NULL}
};

static errent *errhash = NULL;
static uint32_t errhsize = 0;

#define STRERR_BUFF_SIZE 100

#ifdef USE_PTHREADS
static pthread_key_t strerrstorage;

static void strerr_storage_free(void *ptr) {
	if (ptr!=NULL) {
		free(ptr);
	}
}

static void strerr_storage_init(void) {
	zassert(pthread_key_create(&strerrstorage,strerr_storage_free));
	zassert(pthread_setspecific(strerrstorage,NULL));
}

static void* strerr_storage_get(void) {
	uint8_t *buff;
	buff = pthread_getspecific(strerrstorage);
	if (buff==NULL) {
		buff = malloc(STRERR_BUFF_SIZE);
		passert(buff);
		zassert(pthread_setspecific(strerrstorage,buff));
	}
	return buff;
}
#else
static void* strerrstorage = NULL;

static void strerr_storage_free(void) {
	if (strerrstorage!=NULL) {
		free(strerrstorage);
	}
}

static void* strerr_storage_get(void) {
	if (strerrstorage==NULL) {
		strerrstorage = malloc(STRERR_BUFF_SIZE);
		passert(strerrstorage);
	}
	return strerrstorage;
}
#endif

void strerr_init(void) {
	uint32_t n;
	uint32_t hash,disp;

	for (n=0 ; errtab[n].str ; n++) {}

	n = (n*3)/2;

	errhsize = 1;
	while (n>0) {
		errhsize<<=1;
		n>>=1;
	}

	errhash = malloc(sizeof(errent)*errhsize);
	memset(errhash,0,sizeof(errent)*errhsize);

	for (n=0 ; errtab[n].str ; n++) {
		hash = errtab[n].num;
		disp = ((hash * 760092119) & (errhsize-1)) | 1;
		hash = ((hash * 1905886897) & (errhsize-1));
		while (errhash[hash].str!=NULL && errhash[hash].num != errtab[n].num) {
			hash+=disp;
			hash&=(errhsize-1);
		}
		if (errhash[hash].str==NULL) {
			errhash[hash] = errtab[n];
		}
	}

#ifdef USE_PTHREADS
	strerr_storage_init();
#endif
}

const char* strerr(int error) {
	uint32_t hash,disp;
	char* strbuff;

	if (error==0) {
		return "Success (errno=0)";
	}
	hash = error;
	disp = ((hash * 760092119) & (errhsize-1)) | 1;
	hash = ((hash * 1905886897) & (errhsize-1));
	while (errhash[hash].str!=NULL) {
		if (errhash[hash].num == error) {
			return errhash[hash].str;
		}
		hash+=disp;
		hash&=(errhsize-1);
	}
	strbuff = strerr_storage_get();
	snprintf(strbuff,STRERR_BUFF_SIZE,"Unknown error: %d",error);
	strbuff[STRERR_BUFF_SIZE-1] = 0;
	return strbuff;
}

void strerr_term(void) {
	free(errhash);
#ifndef USE_PTHREADS
	strerr_storage_free();
#endif
}
