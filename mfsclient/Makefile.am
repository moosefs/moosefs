bin_PROGRAMS=mfstools
if WITH_MOUNT
bin_PROGRAMS += mfsmount
endif
if WITH_BDEV
sbin_PROGRAMS = mfsbdev
endif
lib_LTLIBRARIES=libmfsio.la
include_HEADERS=mfsio.h

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon
AM_CFLAGS=
AM_LDFLAGS=

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DAPPNAME=mfsmount -DMFSMOUNT=1
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon

mfsmount_SOURCES=\
	labelparser.c labelparser.h \
	dirattrcache.c dirattrcache.h \
	symlinkcache.c symlinkcache.h \
	negentrycache.c negentrycache.h \
	xattrcache.c xattrcache.h \
	fdcache.c fdcache.h \
	sustained_inodes.c sustained_inodes.h \
	sustained_stats.c sustained_stats.h \
	inoleng.c inoleng.h \
	getgroups.c getgroups.h \
	mfs_fuse.c mfs_fuse.h \
	mfs_meta_fuse.c mfs_meta_fuse.h \
	mastercomm.c mastercomm.h \
	extrapackets.c extrapackets.h \
	heapsorter.c heapsorter.h \
	masterproxy.c masterproxy.h \
	csorder.c csorder.h \
	chunksdatacache.c chunksdatacache.h \
	chunkrwlock.c chunkrwlock.h \
	readdata.c readdata.h \
	writedata.c writedata.h \
	csdb.c csdb.h \
	stats.c stats.h \
	oplog.c oplog.h \
	mfsmount.c mfsmount.h \
	fusecommon.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/delayrun.c ../mfscommon/delayrun.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h ../mfscommon/portable.h \
	../mfscommon/buckets_mt.h ../mfscommon/mfsalloc.h \
	../mfscommon/MFSCommunication.h

libmfsio_la_LIBADD=$(PTHREAD_LIBS)
libmfsio_la_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS -DLIBMFSIO=1
libmfsio_la_CPPFLAGS=$(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon
libmfsio_la_LDFLAGS=-version-info 1:0:0

libmfsio_la_SOURCES=\
	labelparser.c labelparser.h \
	mastercomm.c mastercomm.h \
	extrapackets.c extrapackets.h \
	heapsorter.c heapsorter.h \
	csorder.c csorder.h \
	chunksdatacache.c chunksdatacache.h \
	chunkrwlock.c chunkrwlock.h \
	readdata.c readdata.h \
	writedata.c writedata.h \
	inoleng.c inoleng.h \
	csdb.c csdb.h \
	stats.c stats.h \
	mfsio.c mfsio.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/delayrun.c ../mfscommon/delayrun.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h ../mfscommon/portable.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/MFSCommunication.h

mfsbdev_LDADD=$(PTHREAD_LIBS) libmfsio.la
mfsbdev_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS -DMFSNBD=1
mfsbdev_CPPFLAGS=$(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon
mfsbdev_SOURCES=\
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/squeue.c ../mfscommon/squeue.h \
	../mfscommon/workers.c ../mfscommon/workers.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/sizestr.h ../mfscommon/portable.h \
	nbdmain.c

mfstools_SOURCES=\
	mfstools.c \
	mfstools_master.c mfstools_master.h \
	labelparser.c labelparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/liset64.c ../mfscommon/liset64.h \
	../mfscommon/datapack.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h

mfstools_LDADD=$(MATH_LIBS)

mfstools_links=\
	mfscheckfile mfsdirinfo mfsfileinfo mfsfilerepair \
	mfsgetgoal mfsgettrashtime mfsrgetgoal mfsrgettrashtime \
	mfsrsetgoal mfsrsettrashtime mfssetgoal mfssettrashtime \
	mfscopygoal mfscopytrashtime mfsscadmin \
	mfsgetsclass mfssetsclass mfscopysclass mfsxchgsclass mfslistsclass \
	mfsgeteattr mfsseteattr mfsdeleattr mfscopyeattr \
	mfsgetquota mfssetquota mfsdelquota mfscopyquota \
	mfschkarchive mfsclrarchive mfssetarchive \
	mfsmakesnapshot mfsrmsnapshot mfsappendchunks mfsfilepaths

install-exec-hook:
	for l in $(mfstools_links) ; do \
		$(LN_S) -f mfstools $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	if test "$(root_sbindir)" != "xxx" ; then \
		$(MKDIR_P) $(DESTDIR)$(root_sbindir) ; \
		$(LN_S) -f $(bindir)/mfsmount $(DESTDIR)$(root_sbindir)/mount.moosefs ; \
	fi

distclean-local:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

