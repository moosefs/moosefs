#ifndef _DLFUN_HELPERS_H_
#define _DLFUN_HELPERS_H_

#include "config.h"

#ifdef HAVE_DLFCN_H
#include <dlfcn.h>
#endif
#include <stdio.h>
#include <syslog.h>
#include <stdlib.h>

#ifndef RTLD_MAIN_ONLY
#define RTLD_MAIN_ONLY RTLD_DEFAULT
#endif

#define DLFUN_STR_AUX(x) #x

#define DECLARE_FN(rettype,fname, ...) static rettype(*loader_##fname)(__VA_ARGS__) = NULL
#define LOAD_FN(dlhandle,rettype,fname, ...) loader_##fname = (rettype (*)(__VA_ARGS__))(intptr_t)dlsym(dlhandle,DLFUN_STR_AUX(fname))
#define CHECK_FN(libname,fname) if (loader_##fname==NULL) { \
	fprintf(stderr,"can't find symbol '" DLFUN_STR_AUX(fname) "' in library: %s\n",libname); \
	return -1; \
}
#define LOAD_CHECK_FN(libname,dlhandle,rettype,fname, ...) LOAD_FN(dlhandle,rettype,fname,__VA_ARGS__); CHECK_FN(libname,fname)
#define CALL_FN(fname,...) loader_##fname(__VA_ARGS__)

#define DECLARE_MAIN_FN(rettype,fname, ...) static rettype(*main_##fname)(__VA_ARGS__) = NULL
#define LOAD_MAIN_FN(rettype,fname, ...) main_##fname = (rettype (*)(__VA_ARGS__))(intptr_t)dlsym(RTLD_MAIN_ONLY,DLFUN_STR_AUX(fname))
#define CHECK_MAIN_FN(fname) if (main_##fname==NULL) { \
	fprintf(stderr,"can't find symbol '" DLFUN_STR_AUX(fname) "' in main module\n"); \
	return -1; \
}
#define DYNCHECK_MAIN_FN(fname) if (main_##fname==NULL) { \
	syslog(LOG_ERR,"can't find symbol '" DLFUN_STR_AUX(fname) "' in main module\n"); \
	abort(); \
}
#define LOAD_CHECK_MAIN_FN(rettype,fname, ...) LOAD_MAIN_FN(rettype,fname,__VA_ARGS__); CHECK_MAIN_FN(fname)
#define HAVE_MAIN_FN(fname) ((main_##fname)!=NULL)
#define CALL_MAIN_FN(fname,...) main_##fname(__VA_ARGS__)

#endif
