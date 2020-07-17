sbin_PROGRAMS=mfschunkserver mfschunktool mfscsstatsdump

AM_CFLAGS=
AM_CPPFLAGS=-I$(top_srcdir)/mfscommon
AM_LDFLAGS=

mfscsstatsdump_SOURCES=\
	chartsdefs.h \
	../mfscommon/statsdump.c \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/idstr.h

mfscsstatsdump_LDFLAGS=$(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	bgjobs.c bgjobs.h \
	csserv.c csserv.h \
	mainserv.c mainserv.h \
	hddspacemgr.c hddspacemgr.h \
	masterconn.c masterconn.h \
	replicator.c replicator.h \
	chartsdata.c chartsdata.h \
	chartsdefs.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/random.c ../mfscommon/random.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/memusage.c ../mfscommon/memusage.h \
	../mfscommon/cpuusage.c ../mfscommon/cpuusage.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/slogger.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/portable.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/sizestr.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h


mfschunkserver_CFLAGS=$(PTHREAD_CFLAGS)
mfschunkserver_CPPFLAGS=$(AM_CPPFLAGS) -DMFSMAXFILES=16384 -D_USE_PTHREADS $(PTHREAD_CPPFLAGS) -DAPPNAME=mfschunkserver -DUSE_CONNCACHE
mfschunkserver_LDFLAGS=$(PTHREAD_LIBS) $(ZLIB_LIBS) $(MATH_LIBS)

mfschunktool_SOURCES= \
	mfschunktool.c \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/datapack.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

distclean-local:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
