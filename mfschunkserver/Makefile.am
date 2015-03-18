sbin_PROGRAMS=mfschunkserver mfschunktool

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DMFSMAXFILES=16384 -D_USE_PTHREADS $(PTHREAD_CPPFLAGS) -DAPPNAME=mfschunkserver -DUSE_CONNCACHE
AM_LDFLAGS=$(PTHREAD_LIBS) $(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	bgjobs.c bgjobs.h \
	csserv.c csserv.h \
	mainserv.c mainserv.h \
	hddspacemgr.c hddspacemgr.h \
	masterconn.c masterconn.h \
	replicator.c replicator.h \
	chartsdata.c chartsdata.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/random.c ../mfscommon/random.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/memusage.c ../mfscommon/memusage.h \
	../mfscommon/cpuusage.c ../mfscommon/cpuusage.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/portable.h \
	../mfscommon/MFSCommunication.h


mfschunktool_SOURCES= \
	mfschunktool.c \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfschunktool_CFLAGS=

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

mfschunkserver_CFLAGS=$(PTHREAD_CFLAGS)
