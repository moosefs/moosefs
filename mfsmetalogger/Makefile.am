sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon
AM_LDFLAGS=
AM_CFLAGS=

mfsmetalogger_SOURCES= \
	masterconn.c masterconn.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/slogger.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

mfsmetalogger_CPPFLAGS=$(AM_CPPFLAGS) -DAPPNAME=mfsmetalogger

distclean-local:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
