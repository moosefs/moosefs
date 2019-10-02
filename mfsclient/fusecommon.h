#ifndef _FUSECOMMON_H_
#define _FUSECOMMON_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_FUSE3
#define FUSE_USE_VERSION 34
#else
#define FUSE_USE_VERSION 29
#endif

#include <fuse_lowlevel.h>

#endif
