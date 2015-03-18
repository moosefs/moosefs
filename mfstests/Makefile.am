TESTS = mfstest_datapack mfstest_clocks mfstest_crc32 mfstest_delayrun

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

mfstest_delayrun_SOURCES=\
	mfstest_delayrun.c mfstest.h \
	../mfscommon/delayrun.h ../mfscommon/delayrun.c \
	../mfscommon/clocks.h ../mfscommon/clocks.c \
	../mfscommon/strerr.h ../mfscommon/strerr.c

mfstest_delayrun_LDADD=$(PTHREAD_LIBS)
mfstest_delayrun_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS
mfstest_delayrun_CPPFLAGS=$(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

