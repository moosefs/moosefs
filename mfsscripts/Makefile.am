cgidir = $(CGIDIR)

if INSTALL_CGI
nodist_cgi_SCRIPTS = chart.cgi chartdata.cgi

cgi_DATA = requests.cfg

# List of cgi subdirectories to be deployed
cgisubdirs = common views assets

install-exec-hook: mfsgui.py
	@if [ ! -d $(DESTDIR)$(cgidir) ]; then \
		$(MKDIR_P) $(DESTDIR)$(cgidir) ; \
	fi
	@for dir in $(cgisubdirs); do \
		INSTALL_DIR=$(cgidir)/$$dir; \
		$(MKDIR_P) $(DESTDIR)$$INSTALL_DIR; \
		$(INSTALL) -m 444 $$dir/* $(DESTDIR)$$INSTALL_DIR/; \
	done
	@rm -f $(DESTDIR)$(cgidir)/common/constants_ac.py.in
	$(INSTALL) mfsgui.py $(DESTDIR)$(cgidir)/mfs.cgi
	@sed -e "s|#!/usr/bin/env python3|#!/usr/bin/env @PYTHON@|" < $(DESTDIR)$(cgidir)/mfs.cgi > shebang_me.tmp && mv shebang_me.tmp $(DESTDIR)$(cgidir)/mfs.cgi
	@chmod a+x $(DESTDIR)$(cgidir)/mfs.cgi
endif

EXTRA_DIST = mfscli.py mfsgui.py chart.cgi.in chartdata.cgi.in \
	requests.cfg \
	common views assets

if INSTALL_CLI
nodist_bin_SCRIPTS = mfscli
endif

mfscli: mfscli.py common/*.py
	@echo "  CONV     mfscli"
# Create a single mfscli file with all the code in one place:
#  Read the mfscli.py file and replace the "from folder.filename import *" statements marked with comment #MFS_INLINE_IMPORT
#  with the content of the corresponding files
# NOTE: inline import is allowed only for the NON-INTENDED 'from ... import *' statements
#   Check if the line matches the pattern "from folder.filename import * # MFS_INLINE_IMPORT"
#   Extract the filename from the matched pattern and replace dots with slashes to treat it as a path and append .py extension
#   Append the content of the file if the corresponding file exists
#   Or If the file does not exist, put the original line and echo a warning
#   If the line does not match the pattern, put the original line 
	@sh -c 'input_file="mfscli.py"; out_file="mfscli"; \
	: > "$$out_file"; \
	while IFS= read -r line; do \
		case "$$line" in \
			from\ *\ import\ *\#\ MFS_INLINE_IMPORT) \
				module=$$(printf "%s" "$$line" | sed -E "s/from (common\\.[a-z_]+).*/\\1/"); \
				if test -n "$$module"; then \
					file_path=$$(printf "%s.py" "$$(echo "$$module" | tr "." "/")"); \
					if [ -f "$$file_path" ]; then \
						echo "### $$line - inlined below:" >> "$$out_file"; \
						echo "### Imported inline from $$file_path:" >> "$$out_file"; \
						cat "$$file_path" >> "$$out_file"; \
						echo "" >> "$$out_file"; echo "### End of inline import from $$file_path" >> "$$out_file"; echo "" >> "$$out_file"; \
					else \
						printf "%s\n" "$$line" >> "$$out_file"; \
						echo "  Warning: File $$file_path does not exist, keeping the original import line"; \
					fi; \
				else \
					printf "%s\n" "$$line" >> "$$out_file"; \
				fi ;; \
			*) printf "%s\n" "$$line" >> "$$out_file" ;; \
		esac; \
	done < "$$input_file"'
	@sed -e "s|#!/usr/bin/env python3|#!/usr/bin/env @PYTHON@|" < mfscli > shebang_me.tmp && mv shebang_me.tmp mfscli
	@chmod a+x mfscli

clean-local:
	-rm -f mfscli

# make python constants from MFSCommunication.h
#   ./remccoms3.sed < ../mfscommon/MFSCommunication.h | grep -v '\\$' | grep -E '^#define (ERROR|STATUS|CLTOMA|MATOCL)' | awk '{ printf "%s = %s\n",$2,$3 ; }' > const.py
