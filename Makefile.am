if BUILD_MASTER
MASTERDIR=mfsmaster mfsmetatools
else
MASTERDIR=
endif

if BUILD_METALOGGER
METALOGGERDIR=mfsmetalogger
else
METALOGGERDIR=
endif

if BUILD_SUPERVISOR
SUPERVISORDIR=mfssupervisor
else
SUPERVISORDIR=
endif


if BUILD_CHUNKSERVER
CHUNKSERVERDIR=mfschunkserver
else
CHUNKSERVERDIR=
endif

if BUILD_CLIENT
CLIENTDIR=mfsclient
else
CLIENTDIR=
endif

if BUILD_SCRIPTS
SCRIPTSDIR=mfsscripts
else
SCRIPTSDIR=
endif

if BUILD_NETDUMP
NETDUMPDIR=mfsnetdump
else
NETDUMPDIR=
endif

if WITH_SYSTEMD
SYSTEMDDIR=systemd
else
SYSTEMDDIR=
endif

RELEASE = @release@
BUILDNO = @buildno@

ACLOCAL_AMFLAGS = -I m4

SUBDIRS=mfstests mfsdata mfsmanpages $(MASTERDIR) $(METALOGGERDIR) $(SUPERVISORDIR) $(CHUNKSERVERDIR) $(CLIENTDIR) $(SCRIPTSDIR) $(NETDUMPDIR) $(SYSTEMDDIR)

EXTRA_DIST=m4/ax_pthread.m4 m4/ax_check_compile_flag.m4 debian rpm macosx freebsd debian_sysv_to_systemd.sh linux_build.sh macosx_build.sh freebsd_build.sh sanitize_build.sh README.md buildno.txt

distclean-local:
	-rm -rf mfscommon/$(DEPDIR)

dist-hook:
	TODAY=`date "+%B %Y"` ; \
	for f in $(distdir)/mfsmanpages/*.[1-8] ; do \
		mv $$f tmp ; sed -e "1s#MooseFS [1-9]\{1,5\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}-[0-9]\{1,2\}#MooseFS $(VERSION)-$(RELEASE)#" -e "1s#[A-Z][a-z]* [0-9][0-9][0-9][0-9]#$$TODAY#" < tmp | grep -v MFS_PRO_MARKER > $$f ; \
	done ; \
	for f in $(distdir)/mfsdata/*.cfg* ; do \
		mv $$f tmp ; grep -v MFS_PRO_MARKER < tmp > $$f ; \
	done ; \
	mv $(distdir)/debian/control tmp ; grep -v '^#' < tmp > $(distdir)/debian/control ; \
	mv $(distdir)/rpm/mfs.spec tmp ; grep -v '^##' < tmp > $(distdir)/rpm/mfs.spec ; \
	rm -f tmp

mfsdist: dist
	mv $(distdir).tar.gz ../moosefs-dist/$(distdir)-b$(BUILDNO).tar.gz ; \
	if grep -q '(WIP)' NEWS; then \
		ln -sf $(distdir)-b$(BUILDNO).tar.gz ../moosefs-dist/$(distdir)-WIP.tar.gz ; \
		ln -sf $(distdir)-b$(BUILDNO).tar.gz ../moosefs-dist/moosefs-3.x-LATEST.tar.gz ; \
	else \
		ln -sf $(distdir)-b$(BUILDNO).tar.gz ../moosefs-dist/$(distdir).tar.gz ; \
		ln -sf $(distdir)-b$(BUILDNO).tar.gz ../moosefs-dist/$(distdir)-$(RELEASE).tar.gz ; \
		ln -sf $(distdir)-b$(BUILDNO).tar.gz ../moosefs-dist/moosefs-3.x-LATEST.tar.gz ; \
	fi
