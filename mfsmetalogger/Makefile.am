sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DAPPNAME=mfsmetalogger
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmetalogger_SOURCES= \
	masterconn.c masterconn.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h ../mfscommon/slogger.h \
	../mfscommon/MFSCommunication.h

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

mfsmetalogger_CFLAGS=
