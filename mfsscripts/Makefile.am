cgibindir = $(CGIDIR)
cgidatadir = $(CGIDIR)
cgiservdir = $(CGISERVDIR)

if INSTALL_CGI
nodist_cgibin_SCRIPTS = \
	chart.cgi

cgidata_DATA = \
	err.gif \
	index.html \
	logomini.png \
	favicon.ico \
	acidtab.js \
	mfs.css

install-exec-hook: mfscli
	if [ ! -d $(DESTDIR)$(cgibindir) ]; then \
		$(MKDIR_P) $(DESTDIR)$(cgibindir) ; \
	fi
	$(INSTALL_SCRIPT) mfscli $(DESTDIR)$(cgibindir)/mfs.cgi
endif

EXTRA_DIST = mfscli.py.in chart.cgi.in mfscgiserv.py.in index.html.in err.gif logomini.png favicon.ico acidtab.js mfs.css

if INSTALL_CLI
nodist_bin_SCRIPTS = \
	mfscli
endif

if INSTALL_CGISERV
nodist_cgiserv_SCRIPTS = \
	mfscgiserv

mfscgiserv: mfscgiserv.py
	@echo "  CONV     mfscgiserv"
	@sed -e "s#\%\%""CGIDIR""\%\%#$(CGIDIR)#" -e "s#\%\%""DATAPATH""\%\%#$(DATA_PATH)#" <mfscgiserv.py >mfscgiserv
endif

mfscli: mfscli.py
	@echo "  CONV     mfscli"
	@sed -e "s#\%\%""SBINDIR""\%\%#$(sbindir)#" -e "s# MFS_PRO_.*##" -e "s# MFS_CE_.*##" <mfscli.py >mfscli

clean-local:
	-rm -f mfscgiserv mfscli

# make python constants from MFSCommunication.h
#   ./remccoms3.sed < ../mfscommon/MFSCommunication.h | grep -v '\\$' | grep -E '^#define (ERROR|STATUS|CLTOMA|MATOCL)' | awk '{ printf "%s = %s\n",$2,$3 ; }' > const.py
