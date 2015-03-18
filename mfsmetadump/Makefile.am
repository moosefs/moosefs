sbin_PROGRAMS=mfsmetadump

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon

mfsmetadump_SOURCES=\
	main.c \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

mfsmetadump_CFLAGS=
