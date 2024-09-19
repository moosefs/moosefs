sbin_PROGRAMS = mfsmetalogger

mfsmetalogger_SOURCES = \
	masterconn.c masterconn.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/timeparser.c ../mfscommon/timeparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h


mfsmetalogger_CPPFLAGS = -I$(top_srcdir)/mfscommon -DAPPNAME=mfsmetalogger
mfsmetalogger_LDFLAGS =
mfsmetalogger_CFLAGS =


distclean-local: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
