sbin_PROGRAMS=mfsnetdump

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -I$(top_srcdir)/mfsnetdump -I.

mfsnetdump_SOURCES=\
	mfsnetdump.c \
	commands.h \
	../mfscommon/MFSCommunication.h

commands.h: ../mfscommon/MFSCommunication.h
	@echo "  CONV     commands"
	@cat ../mfscommon/MFSCommunication.h | grep '^#define [A-Z][A-Z]TO[A-Z][A-Z]' | grep -v MAXPACKETSIZE | awk '{ printf "{%s,\"%s\"},\n",$$2,$$2; }' > commands.h

clean:clean-am
	-rm -f commands.h

distclean:distclean-am
	-rm -f commands.h
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

mfsnetdump_CFLAGS=
