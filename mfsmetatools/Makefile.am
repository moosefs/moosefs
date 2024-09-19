sbin_PROGRAMS = mfsmetadump mfsmetadirinfo mfsmetasearch

AM_CPPFLAGS = -I$(top_srcdir)/mfscommon

mfsmetadump_CFLAGS =
mfsmetadump_SOURCES = \
	mfsmetadump.c \
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/datapack.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

mfsmetadirinfo_CFLAGS =
mfsmetadirinfo_SOURCES = \
	mfsmetadirinfo.c \
	../mfscommon/liset64.c ../mfscommon/liset64.h \
	../mfscommon/datapack.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

mfsmetasearch_CFLAGS =
mfsmetasearch_SOURCES = \
	mfsmetasearch.c \
	searchexpr.c searchexpr.h \
	../mfscommon/liset64.c ../mfscommon/liset64.h \
	../mfscommon/datapack.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

distclean-local: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
