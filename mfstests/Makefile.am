TESTS = mfstest_datapack mfstest_clocks mfstest_crc32

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon

noinst_PROGRAMS = $(TESTS)

mfstest_datapack_SOURCES=\
	mfstest_datapack.c mfstest.h \
	../mfscommon/datapack.h

mfstest_datapack_CFLAGS=

mfstest_clocks_SOURCES=\
	mfstest_clocks.c mfstest.h \
	../mfscommon/clocks.h ../mfscommon/clocks.c

mfstest_clocks_CFLAGS=

mfstest_crc32_SOURCES=\
	mfstest_crc32.c mfstest.h \
	../mfscommon/crc.h ../mfscommon/crc.c \
	../mfscommon/clocks.h ../mfscommon/clocks.c

mfstest_crc32_CFLAGS=

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

