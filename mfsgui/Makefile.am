sbin_PROGRAMS = mfsgui
sbin_SCRIPTS = mfscgiserv

EXTRA_DIST = $(sbin_SCRIPTS)

AM_CFLAGS =
AM_CPPFLAGS = -I$(top_srcdir)/mfscommon -DAPPNAME=mfsgui
AM_LDFLAGS =

mfsgui_SOURCES = \
	init.h mime.h children.h \
	mfsgui.c mfsgui.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/timeparser.c ../mfscommon/timeparser.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/hash_begin.h ../mfscommon/hash_end.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/massert.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

distclean-local: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
