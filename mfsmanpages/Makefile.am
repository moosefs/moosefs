chunkserver_mans=mfschunkserver.8 mfschunkserver.cfg.5 mfshdd.cfg.5
master_mans=mfsmaster.8 mfsmetarestore.8 mfsmaster.cfg.5 mfsexports.cfg.5 mfstopology.cfg.5
cgiserv_mans=mfscgiserv.8
metalogger_mans=mfsmetalogger.8 mfsmetalogger.cfg.5
netdump_mans=mfsnetdump.8
mount_mans=\
	mfsmount.8 mfstools.1 \
	mfscheckfile.1 mfsdirinfo.1 mfsfileinfo.1 mfsfilerepair.1 \
	mfsgetgoal.1 mfsgettrashtime.1 mfsrgetgoal.1 mfsrgettrashtime.1 \
	mfsrsetgoal.1 mfsrsettrashtime.1 mfssetgoal.1 mfssettrashtime.1 \
	mfsgeteattr.1 mfsseteattr.1 mfsdeleattr.1 \
	mfsgetquota.1 mfssetquota.1 mfsdelquota.1 \
	mfsappendchunks.1 mfsmakesnapshot.1 mfsfilepaths.1

all_mans=$(chunkserver_mans) $(master_mans) $(mount_mans) $(metalogger_mans) $(cgiserv_mans) $(netdump_mans) 

EXTRA_DIST=$(all_mans)

man_MANS=

if BUILD_MASTER
man_MANS+=$(master_mans)
endif

if BUILD_CHUNKSERVER
man_MANS+=$(chunkserver_mans)
endif

if BUILD_MOUNT
man_MANS+=$(mount_mans)
endif

if BUILD_METALOGGER
man_MANS+=$(metalogger_mans)
endif

if BUILD_NETDUMP
man_MANS+=$(netdump_mans)
endif

if BUILD_SUPERVISOR
man_MANS+=$(supervisor_mans)
endif

if INSTALL_CGISERV
man_MANS+=$(cgiserv_mans)
endif

man-html: $(all_mans)
	$(MKDIR_P) html
	for f in $(all_mans) ; do \
		man2html -r $$f | tail -n +3 > html/$${f}.html ; \
	done
