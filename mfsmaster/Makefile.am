sbin_PROGRAMS = mfsmaster mfsstatsdump mfssupervisor

AM_CFLAGS =
AM_CPPFLAGS = -I$(top_srcdir)/mfscommon
AM_LDFLAGS =

mfsstatsdump_SOURCES = \
	chartsdefs.h \
	../mfscommon/statsdump.c \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/idstr.h

mfsstatsdump_CFLAGS =
mfsstatsdump_CPPFLAGS = -I$(top_srcdir)/mfscommon
mfsstatsdump_LDFLAGS = $(ZLIB_LIBS)

mfsmaster_SOURCES = \
	itree.h itree.c \
	topology.h topology.c \
	exports.h exports.c \
	bio.h bio.c \
	changelog.c changelog.h \
	chunkdelay.c chunkdelay.h \
	chunks.c chunks.h \
	filesystem.c filesystem.h \
	appendres.c appendres.h \
	xattr.c xattr.h \
	posixacl.c posixacl.h \
	flocklocks.c flocklocks.h \
	posixlocks.c posixlocks.h \
	openfiles.c openfiles.h \
	csdb.c csdb.h \
	iptosesid.c iptosesid.h \
	storageclass.c storageclass.h \
	patterns.c patterns.h \
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
	bgsaver.c bgsaver.h \
	csipmap.c csipmap.h \
	multilan.c multilan.h \
	chartsdefs.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
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
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/timeparser.c ../mfscommon/timeparser.h \
	../mfscommon/cuckoohash.c ../mfscommon/cuckoohash.h \
	../mfscommon/dictionary.c ../mfscommon/dictionary.h \
	../mfscommon/globengine.c ../mfscommon/globengine.h \
	../mfscommon/hash_begin.h ../mfscommon/hash_end.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/dlfun_helpers.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/hashfn.h \
	../mfscommon/median.h \
	../mfscommon/buckets.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/bitops.h \
	../mfscommon/glue.h \
	../mfscommon/sizestr.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h


mfsmaster_CFLAGS = $(DYNLINKER_FLAGS)
mfsmaster_CPPFLAGS = $(AM_CPPFLAGS) -DMFSMAXFILES=16384 -DAPPNAME=mfsmaster
mfsmaster_LDFLAGS = $(ZLIB_LIBS)

mfssupervisor_SOURCES = \
	mfssupervisor.c \
	../mfscommon/mastersupervisor.c ../mfscommon/mastersupervisor.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/MFSCommunication.h \
	../mfscommon/datapack.h \
	../mfscommon/idstr.h


mfssupervisor_CPPFLAGS = -I$(top_srcdir)/mfscommon -DMFSSUPERVISOR=1
mfssupervisor_CFLAGS =

EXTRA_DIST = $(sbin_SCRIPTS)

sbin_SCRIPTS = \
	mfsmetarestore

distclean-local: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
