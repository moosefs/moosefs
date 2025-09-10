bin_PROGRAMS = mfstrashtool mfstrashtime mfseattr mfsarchive mfsdiagtools mfsquota mfssnapshots mfstrashretention mfssclass mfsscadmin mfspatadmin mfsfacl
bin_SCRIPTS = mfsgetgoal mfssetgoal mfscopygoal
if WITH_MOUNT
bin_PROGRAMS += mfsmount
endif
if WITH_BDEV
sbin_PROGRAMS = mfsbdev
endif
lib_LTLIBRARIES = libmfsio.la
include_HEADERS = mfsio.h
noinst_LIBRARIES = libmfstools.a

AM_CPPFLAGS = -I$(top_srcdir)/mfscommon
AM_CFLAGS =
AM_LDFLAGS =

mfsmount_LDADD = $(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CFLAGS = $(PTHREAD_CFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DAPPNAME=mfsmount -DMFSMOUNT=1
mfsmount_CPPFLAGS = $(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon

mfsmount_SOURCES = \
	dirattrcache.c dirattrcache.h \
	dirblob_name_index.c dirblob_name_index.h \
	dirblob_node_index.c dirblob_node_index.h \
	symlinkcache.c symlinkcache.h \
	negentrycache.c negentrycache.h \
	xattrcache.c xattrcache.h \
	fdcache.c fdcache.h \
	dentry_invalidator.c dentry_invalidator.h \
	sustained_parents.c sustained_parents.h \
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
	truncate.c truncate.h \
	csdb.c csdb.h \
	stats.c stats.h \
	oplog.c oplog.h \
	mfsmount.c mfsmount.h \
	fusecommon.h \
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/delayrun.c ../mfscommon/delayrun.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/portable.h \
	../mfscommon/buckets_mt.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h


libmfsio_la_LIBADD = $(PTHREAD_LIBS)
libmfsio_la_CFLAGS = $(PTHREAD_CFLAGS) -D_USE_PTHREADS -DLIBMFSIO=1
libmfsio_la_CPPFLAGS = $(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon
libmfsio_la_LDFLAGS = -version-info 1:0:0
libmfsio_la_SOURCES = \
	mastercomm.c mastercomm.h \
	extrapackets.c extrapackets.h \
	heapsorter.c heapsorter.h \
	csorder.c csorder.h \
	chunksdatacache.c chunksdatacache.h \
	chunkrwlock.c chunkrwlock.h \
	readdata.c readdata.h \
	writedata.c writedata.h \
	truncate.c truncate.h \
	inoleng.c inoleng.h \
	csdb.c csdb.h \
	stats.c stats.h \
	mfsioint_lookupcache.c mfsioint_lookupcache.h \
	mfsioint.c mfsioint.h \
	mfsio.c mfsio.h \
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/delayrun.c ../mfscommon/delayrun.h \
	../mfscommon/pcqueue.c ../mfscommon/pcqueue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/conncache.c ../mfscommon/conncache.h \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/portable.h \
	../mfscommon/mfsalloc.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h



mfsbdev_LDADD = $(PTHREAD_LIBS) libmfsio.la
mfsbdev_CFLAGS = $(PTHREAD_CFLAGS) -D_USE_PTHREADS -DMFSNBD=1
mfsbdev_CPPFLAGS = $(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon
mfsbdev_SOURCES = \
	../mfscommon/lwthread.c ../mfscommon/lwthread.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/squeue.c ../mfscommon/squeue.h \
	../mfscommon/workers.c ../mfscommon/workers.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/datapack.h \
	../mfscommon/massert.h \
	../mfscommon/sizestr.h \
	../mfscommon/portable.h \
	../mfscommon/idstr.h \
	mfsbdev.c

mfstrashtool_SOURCES = \
	tools_packetserializer.c tools_packetserializer.h \
	tools_main.c tools_main.h \
	tools_trashadmin.c tools_trashadmin.h \
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/globengine.c ../mfscommon/globengine.h \
	../mfscommon/datapack.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h

mfstrashtool_CFLAGS = -DGLOB_TOOLS
# mfstrashtool_LDADD =

libmfstools_a_SOURCES = \
	tools_packetserializer.c tools_packetserializer.h \
	tools_common.c tools_common.h \
	../mfscommon/labelparser.c ../mfscommon/labelparser.h \
	../mfscommon/timeparser.c ../mfscommon/timeparser.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/globengine.c ../mfscommon/globengine.h \
	../mfscommon/datapack.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/idstr.h \
	../mfscommon/MFSCommunication.h
# libmfstools_a_LIBADD = 

mfstrashtime_SOURCES = \
	tools_trashtime.c libmfstools.h

mfstrashtime_CFLAGS =
mfstrashtime_LDADD = libmfstools.a
mfstrashtime_links = mfsgettrashtime mfssettrashtime mfscopytrashtime

mfstrashretention_SOURCES = \
	tools_trashretention.c libmfstools.h

mfstrashretention_CFLAGS =
mfstrashretention_LDADD = libmfstools.a
mfstrashretention_links = mfsgettrashretention mfssettrashretention mfscopytrashretention

mfseattr_SOURCES = \
	tools_eattr.c libmfstools.h

mfseattr_CFLAGS =
mfseattr_LDADD = libmfstools.a
mfseattr_links = mfsgeteattr mfsseteattr mfsdeleattr mfscopyeattr

mfsarchive_SOURCES = \
	tools_archive.c libmfstools.h

mfsarchive_CFLAGS =
mfsarchive_LDADD = libmfstools.a
mfsarchive_links = mfschkarchive mfsclrarchive mfssetarchive

mfsdiagtools_SOURCES = \
	tools_diagtools.c libmfstools.h

mfsdiagtools_CFLAGS =
mfsdiagtools_LDADD = libmfstools.a
mfsdiagtools_links = mfscheckfile mfsdirinfo mfsfileinfo mfsfilerepair mfsfilepaths

mfsquota_SOURCES = \
	tools_quota.c libmfstools.h

mfsquota_CFLAGS =
mfsquota_LDADD = libmfstools.a
mfsquota_links = mfsgetquota mfssetquota mfsdelquota mfscopyquota

mfssnapshots_SOURCES = \
	tools_snapshots.c libmfstools.h

mfssnapshots_CFLAGS =
mfssnapshots_LDADD = libmfstools.a
mfssnapshots_links = mfsmakesnapshot mfsrmsnapshot mfsappendchunks

mfssclass_SOURCES = \
	tools_sclass.c libmfstools.h

mfssclass_CFLAGS =
mfssclass_LDADD = libmfstools.a
mfssclass_links = mfsgetsclass mfssetsclass mfscopysclass mfsxchgsclass

mfsscadmin_SOURCES = \
	tools_scadmin.c libmfstools.h

mfsscadmin_CFLAGS =
mfsscadmin_LDADD = libmfstools.a
mfsscadmin_links=mfscreatesclass mfsmodifysclass mfsdeletesclass mfsclonesclass mfsrenamesclass mfslistsclass mfsimportsclass

mfspatadmin_SOURCES = \
	tools_patadmin.c libmfstools.h

mfspatadmin_CFLAGS =
mfspatadmin_LDADD = libmfstools.a
mfspatadmin_links = mfscreatepattern mfsdeletepattern mfslistpattern

mfsfacl_SOURCES = \
	tools_facl.c libmfstools.h

mfsfacl_CFLAGS =
mfsfacl_LDADD = libmfstools.a
mfsfacl_links = mfsgetfacl mfssetfacl


EXTRA_DIST = mfsgetgoal mfssetgoal mfscopygoal

install-exec-hook:
	for l in $(mfstrashtime_links) ; do \
		$(LN_S) -f mfstrashtime $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfstrashretention_links) ; do \
		$(LN_S) -f mfstrashretention $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfseattr_links) ; do \
		$(LN_S) -f mfseattr $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfsarchive_links) ; do \
		$(LN_S) -f mfsarchive $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfsdiagtools_links) ; do \
		$(LN_S) -f mfsdiagtools $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfsquota_links) ; do \
		$(LN_S) -f mfsquota $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfssnapshots_links) ; do \
		$(LN_S) -f mfssnapshots $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfssclass_links) ; do \
		$(LN_S) -f mfssclass $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfsscadmin_links) ; do \
		$(LN_S) -f mfsscadmin $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfspatadmin_links) ; do \
		$(LN_S) -f mfspatadmin $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	for l in $(mfsfacl_links) ; do \
		$(LN_S) -f mfsfacl $(DESTDIR)$(bindir)/$$l ; \
	done ; \
	if test "$(root_sbindir)" != "xxx" -a -x $(DESTDIR)$(bindir)/mfsmount; then \
		$(MKDIR_P) $(DESTDIR)$(root_sbindir) ; \
		$(LN_S) -f $(bindir)/mfsmount $(DESTDIR)$(root_sbindir)/mount.moosefs ; \
	fi

distclean-local: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile

