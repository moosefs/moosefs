sbin_PROGRAMS=mfsmaster

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DAPPNAME=mfsmaster
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmaster_SOURCES=\
	itree.h itree.c \
	topology.h topology.c \
	exports.h exports.c \
	bio.h bio.c \
	changelog.c changelog.h \
	chunks.c chunks.h \
	filesystem.c filesystem.h \
	xattr.c xattr.h \
	posixacl.c posixacl.h \
	openfiles.c openfiles.h \
	csdb.c csdb.h \
	sessions.c sessions.h \
	metadata.c metadata.h \
	restore.c restore.h \
	merger.c merger.h \
	missinglog.c missinglog.h \
	sharedpointer.c sharedpointer.h \
	matocsserv.c matocsserv.h \
	matoclserv.c matoclserv.h \
	matomlserv.c matomlserv.h \
	datacachemgr.c datacachemgr.h \
	chartsdata.c chartsdata.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/random.c ../mfscommon/random.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/memusage.c ../mfscommon/memusage.h \
	../mfscommon/cpuusage.c ../mfscommon/cpuusage.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/hashfn.h ../mfscommon/median.h \
	../mfscommon/buckets.h \
	../mfscommon/MFSCommunication.h


EXTRA_DIST = $(sbin_SCRIPTS)

sbin_SCRIPTS=\
	mfsmetarestore

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

mfsmaster_CFLAGS=
