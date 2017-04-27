bin_PROGRAMS=mfsmount mfstools

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon
AM_CFLAGS=
AM_LDFLAGS=

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=29 -DAPPNAME=mfsmount
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
	main.c \
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
	../mfscommon/buckets_mt.h ../mfscommon/mfsalloc.h \
	../mfscommon/MFSCommunication.h

mfstools_SOURCES=\
	mfstools.c \
	labelparser.c labelparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/datapack.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h

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

distclean:distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

