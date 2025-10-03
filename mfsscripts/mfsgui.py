#!/usr/bin/env python3

# Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
# 
# This file is part of MooseFS.
# 
# MooseFS is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 2 (only).
# 
# MooseFS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with MooseFS; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
# or visit http://www.gnu.org/licenses/gpl-2.0.html

import sys

if sys.version_info[0]<3 or (sys.version_info[0]==3 and sys.version_info[1]<4):
	print("Content-Type: text/html; charset=UTF-8\r\n\r")
	print("""<!DOCTYPE html>""")
	print("""<html lang="en">""")
	print("""<head>""")
	print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
	print("""<title>MooseFS</title>""")
	print("""</head>""")
	print("""<body>""")
	print("""<h2>MooseFS GUI: Unsupported python version, minimum required version is 3.4</h2>""")
	print("""</body>""")
	print("""</html>""")
	sys.exit(1)

from common.constants_ac import * # MFS_INLINE_IMPORT
from common.constants import *    # MFS_INLINE_IMPORT
from common.errors import *       # MFS_INLINE_IMPORT
from common.utils import *        # MFS_INLINE_IMPORT
from common.conn import *         # MFS_INLINE_IMPORT
from common.models import *       # MFS_INLINE_IMPORT
from common.cluster import *      # MFS_INLINE_IMPORT
from common.dataprovider import * # MFS_INLINE_IMPORT
from common.organization import * # MFS_INLINE_IMPORT
from common.utilsgui import *
import common.validator as validator

ajax_request = AJAX_NONE #is it ajax request?
readonly = False     #if readonly - don't render CGI links for commands
selectable = True    #if not selectable - don't render drop-downs and other selectors (to switch views with page reload)
donotresolve = 0     #resolve or not various ip addresses
instancename = "My MooseFS"

masterhost = DEFAULT_MASTERNAME
masterport = DEFAULT_MASTER_CLIENT_PORT



# set default output encoding to utf-8 (our html page encoding)
if sys.version_info[1]<7:
	import codecs
	sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
else:
	sys.stdout.reconfigure(encoding='utf-8')

fields = MFSFieldStorage()
fields.initFromURL()		

if fields.getstr("ajax")=="container":
	ajax_request = AJAX_CONTAINER
elif fields.getstr("ajax")=="metrics":
	ajax_request = AJAX_METRICS

if fields.getvalue("ajax")!=None:
	fields.pop("ajax") #prevent from including ajax URL param any further

try:
	if "masterhost" in fields:
		masterhost = fields.getstr("masterhost")
		if type(masterhost) is list:
			masterhost = ";".join(masterhost)
		masterhost = masterhost.replace('"','').replace('<','').replace('>','').replace("'",'').replace('&','').replace('%','')
except Exception:
	pass
masterport = int(fields.getint("masterport",DEFAULT_MASTER_CLIENT_PORT))
mastername = fields.getstr("mastername", "")

# connect to masters and find who is the leader etc
cl = Cluster(masterhost, masterport, fields.getstr("leaderip"))
if (cl.leaderfound()):
	fields.append("leaderip",cl.master().host)


html_title = "MooseFS"
if len(mastername)>0: html_title += " (%s)" % htmlentities(mastername)

errmsg = None
if cl.master()==None:
	errmsg = """Can't connect to the MooseFS Master server (%s)""" % (masterhost)
if (cl.leaderfound() or cl.electfound() or cl.usurperfound() or cl.followerfound()):
	if cl.master().version_unknown():
		errmsg = """Can't detect the MooseFS Master server version (%s)""" % (masterhost)
	elif cl.master().version_less_than(3,0,0):
		errmsg = """This version of MooseFS Master server (%s) is not supported (pre 3.0.0)""" % (masterhost)
if errmsg:
	print("Content-Type: text/html; charset=UTF-8\r\n\r")
	print("""<!DOCTYPE html>""")
	print("""<html lang="en">""")
	print("""<head>""")
	print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
	print("""<title>MooseFS GUI %s</title>""" % (htmlentities(mastername)))
	# leave this script a the beginning to prevent screen blinking when using dark mode
	print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
		if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
		//--><!]]></script>""")		
	print("""<link rel="stylesheet" href="assets/mfs.css" type="text/css" />""")
	print("""</head>""")
	print("""<body>""")
	print("""<h1 class="center">%s</h1>""" % htmlentities(errmsg))
	print("""</body>""")
	print("""</html>""")
	sys.exit(1)

# initialize global data provider
dataprovider = DataProvider(cl, donotresolve)
# initialize global validator
vld = validator.Validator(dataprovider)

# prior to rendering anything process commands if any
import views.commands
views.commands.process_commands(cl,fields,html_title)

# organization of menus, sections and subsections
org = Organization(dataprovider,guimode=True, ajax_request=ajax_request)

# decode URL parameters
if "readonly" in fields: #readonly=1 - don't show any command links (on/off maintenance, remove etc.)
	if fields.getvalue("readonly")=="1":
		readonly = True
if "selectable" in fields: #selectable=0 - don't show any selectors requiring page refresh (knobs, drop-downs etc.)
	if fields.getvalue("selectable")=="0":
		selectable = False
if "sections" in fields: #list of "-" separated sections to show
	sectionstr = fields.getvalue("sections")
	if type(sectionstr) is list:
		sectionstr = "-".join(sectionstr)
	sectionset = set(sectionstr.split("-"))
else:
	sectionset = set((org.get_default_section(),)) # by default:show the 1st section if no section is defined in url
if "subsections" in fields: #list of "-" separated subsections to show
	subsectionstr = fields.getvalue("subsections")
	if type(subsectionstr) is list:
		subsectionstr = "-".join(subsectionstr)
	subsectionset = set(subsectionstr.split("-"))
else:
	subsectionset = set([])

org.set_sections(sectionset,subsectionset)

#######################################################
###                 RENDER OUTPUT                   ###
#######################################################

# Print Prometheus metrics and exit
if ajax_request == AJAX_METRICS: 
	import views.metrics as metrics
	metrics.print_render(fields, masterhost, masterport)
	exit(0)

# print header
print("Content-Type: text/html; charset=UTF-8\r\n\r")

# print html header and menu
if not ajax_request:
	import views.header
	print_out(views.header.render(dataprovider, fields, vld, html_title, org, sectionset))
	print("""<div id="container">""")
	print("""<div id="container-ajax">""")
	if cl.leaderfound()==0:
		# deal with missing leader in the cluster
		import views.missingleader
		print_out(views.missingleader.render(dataprovider, fields, vld))



#######################################################
###                 RENDER SECTIONS                 ###
#######################################################

# Status section
if org.shall_render("ST"):
	try:
		import views.graph
		print_out(views.graph.render(dataprovider, fields, vld))
	except Exception:
		print_exception()	

# Info section

#Cluster summary subsection
if org.shall_render("IG"):
	try:
		ci = dataprovider.get_clusterinfo()
		import views.clusterinfo
		print_out(views.clusterinfo.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

#Metadata servers subsection
if org.shall_render("IM"):
	try:		
		# update master servers delay times prior to getting the list of master servers
		highest_saved_metaversion, highest_metaversion_checksum = cl.update_masterservers_delays()
		import views.metaservers
		print_out(views.metaservers.render(dataprovider,fields,vld))
	except Exception:
		print_exception()


# Chunks matrix
if org.shall_render("IC"):
	try:
		import views.matrix 
		print_out(views.matrix.render(dataprovider, fields, vld, selectable))
	except Exception:
		print_exception()

# Filesystem self-check loop (GUI only)
if org.shall_render("FL"):
	try:
		import views.healthselfcheck
		print_out(views.healthselfcheck.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# Chunk house-keeping loop (GUI only)
if org.shall_render("CL"):
	try:
		import views.chunksloop
		print_out(views.chunksloop.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# memory usage table
if org.shall_render("MU"):
	try:
		# mu.memlabels,abrlabels,mu.totalused,mu.totalallocated,memusage = dataprovider.get_memory_usage()
		mu = dataprovider.get_memory_usage()
		import views.memory
		print_out(views.memory.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# Chunkservers section
if org.shall_render("CS"):
	try:
		import views.chunkservers 
		print_out(views.chunkservers.render(dataprovider, fields, vld, readonly, selectable))
	except Exception:
		print_exception()

# Disks section
if org.shall_render("HD"):
	try:
		import views.disks
		print_out(views.disks.render(dataprovider, fields, vld, readonly, selectable))
	except TimeoutError:
		print_error("Timeout connecting chunk servers. Hint: check if chunk servers are available from this server (the one hosting GUI/CLI).")
	except Exception:
		print_exception()

# Exports section
if org.shall_render("EX"):
	try:
		import views.exports
		print_out(views.exports.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

if org.shall_render("MD"):
	html_menu_subsections(org, fields, "MD")

# Mounts - parameters section
if org.shall_render("MS"):
	try:
		import views.mountsparameters
		print_out(views.mountsparameters.render(dataprovider, fields, vld, readonly))
	except Exception:
		print_exception()

# Mounts - operations section
if org.shall_render("MO"):
		try:
			import views.mountsoperations
			print_out(views.mountsoperations.render(dataprovider, fields, vld))
		except Exception:
			print_exception()

# Open files section
if org.shall_render("OF"):
	try:
		import views.openfiles
		print_out(views.openfiles.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

if org.shall_render("RP"):
	html_menu_subsections(org, fields, "RP")
	
# Storage classes section
if org.shall_render("SC"):
	try:
		import views.sclasses
		print_out(views.sclasses.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# Override patterns section
if org.shall_render("PA"):
	try:
		import views.opatterns
		print_out(views.opatterns.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# Quotas
if org.shall_render("QU"):
	try:
		import views.quotas
		print_out(views.quotas.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

if not ajax_request:
	print("""</div><!-- end of container-ajax -->""")
	# content from now on (i.e., charts) is not subject of the full DOM ajax update

# Master and Chunkservers charts
if org.shall_render("XC"):
	html_menu_subsections(org, fields, "XC")

# Include servers charts scripts
if (org.shall_render("MC") or org.shall_render("CC")):
	print("""<script src="assets/acidchart.js" type="text/javascript"></script>""")

# Master servers charts
# charts are ajax-updated individually, without resending the page content
if org.shall_render("MC"):
	out = []
	try:
		import views.chartsmc
		print_out(views.chartsmc.render(dataprovider, fields, vld))
	except Exception:
		print_exception()

# Chunkservers charts
# charts are ajax-updated individually, without resending the page content
if org.shall_render("CC"):
		out = []
		try:
			import views.chartscc
			print_out(views.chartscc.render(dataprovider, fields, vld))
		except Exception:
			print_exception()


#######################################################
###                 RENDER FOOTER                   ###
#######################################################

if not ajax_request:
	print("""</div><!-- end of container -->""")
	import views.footer
	print_out(views.footer.render(dataprovider, fields, vld))
