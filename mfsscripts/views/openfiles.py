from common.utils import *
from common.utilsgui import *
from common.models import *

def render(dp, fields, vld):
	OFsessionid = fields.getint("OFsessionid", 0)
	OForder = fields.getint("OForder", 0)
	OFrev = fields.getint("OFrev", 0)

	ALinode = fields.getint("ALinode", 0)
	ALorder = fields.getint("ALorder", 0)
	ALrev = fields.getint("ALrev", 0)

	sessionsdata = {}
	for ses in dp.get_sessions():
		if ses.sessionid>0 and ses.sessionid < 0x80000000:
			sessionsdata[ses.sessionid]=(ses.host,ses.info,ses.openfiles)
	out = []
	class_no_table=""
	if OFsessionid==0:
		class_no_table="no_table"
	out.append("""<form action="#"><div class="tab_title %s">Open files for client: """ % class_no_table)
	out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&OFsessionid='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"OFsessionid":""}))
	if OFsessionid==0:
		out.append("""<option value="0" selected="selected"> select session</option>""")
	sessions = list(sessionsdata.keys())
	sessions.sort()
	for sessionid in sessions:
		host,info,openfiles = sessionsdata[sessionid]
		if OFsessionid==sessionid:
			out.append("""<option value="%s" selected="selected">%s: %s:%s (open files: ~%u)</option>""" % (sessionid,sessionid,host,info,openfiles))
		else:
			out.append("""<option value="%s">%s: %s:%s (open files: ~%u)</option>""" % (sessionid,sessionid,host,info,openfiles))
	out.append("""</select><span class="arrow"></span></div></div></form>""")

	if OFsessionid!=0:
		inodes = set()
		out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsopenfiles">""")
		out.append("""	<tr>""")
		out.append("""		<th class="acid_tab_enumerate">#</th>""")
		out.append("""		<th>Session&nbsp;id</th>""")
		out.append("""		<th>Host</th>""")
		out.append("""		<th>IP</th>""")
		out.append("""		<th>Mount&nbsp;point</th>""")
		out.append("""		<th>Inode</th>""")
		out.append("""		<th>Paths</th>""")
		out.append("""	</tr>""")
				
		for of in dp.get_openfiles(OFsessionid, OForder, OFrev):
			inodes.add(of.inode)
			for path in of.paths:
				out.append("""	<tr>""")
				out.append("""		<td align="right"></td>""")
				out.append("""		<td align="center">%u</td>""" % of.sessionid)
				out.append("""		<td align="left">%s</td>""" % of.host)
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (of.sortipnum,of.ipnum))
				out.append("""		<td align="left">%s</td>""" % htmlentities(of.info))
				out.append("""		<td align="center">%u</td>""" % of.inode)
				out.append("""		<td align="left">%s</td>""" % htmlentities(path))
				out.append("""	</tr>""")
		out.append("""</table>""")
	
		# Acquired locks for inode
		if ALinode not in inodes:
				ALinode = 0
		if len(inodes)>0:
			class_no_table = ""
			if ALinode == 0:
				class_no_table = "no_table"
			out.append("""<form action="#"><div class="tab_title %s">Acquired locks for inode: """ % class_no_table)
			out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&ALinode='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"ALinode":""}))
			if ALinode==0:
				out.append("""<option value="0" selected="selected"> select inode</option>""")
			inodeslist = list(inodes)
			inodeslist.sort()
			for inode in inodeslist:
				if ALinode==inode:
					out.append("""<option value="%u" selected="selected">%u</option>""" % (inode,inode))
				else:
					out.append("""<option value="%u">%u</option>""" % (inode,inode))
			out.append("""</select><span class="arrow"></span></div></div></form>""")
			if ALinode!=0:
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_acquiredlocks">""")
				out.append("""	<tr>""")
				out.append("""		<th class="acid_tab_enumerate">#</th>""")
				out.append("""		<th>Session&nbsp;id</th>""")
				out.append("""		<th>Host</th>""")
				out.append("""		<th>IP</th>""")
				out.append("""		<th>Mount&nbsp;point</th>""")
				out.append("""		<th>Lock type</th>""")
				out.append("""		<th>Owner id</th>""")
				out.append("""		<th>Pid</th>""")
				out.append("""		<th>Start</th>""")
				out.append("""		<th>End</th>""")
				out.append("""		<th>R/W</th>""")
				out.append("""	</tr>""")

				for al in dp.get_acquiredlocks(ALinode, ALorder, ALrev):
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="center">%u</td>""" % al.sessionid)
					out.append("""		<td align="left">%s</td>""" % al.host)
					out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (al.sortipnum,al.ipnum))
					out.append("""		<td align="left">%s</td>""" % htmlentities(al.info))
					out.append("""		<td align="center">%s</td>""" % al.locktype)
					out.append("""		<td align="right">%u</td>""" % al.owner)
					if al.pid==0 and al.start==0 and al.end==0:
						out.append("""		<td align="right">-1</td>""")
						out.append("""		<td align="right">0</td>""")
						out.append("""		<td align="right">EOF</td>""")
					else:
						out.append("""		<td align="right">%u</td>""" % al.pid)
						out.append("""		<td align="right">%u</td>""" % al.start)
						if al.end > 0x7FFFFFFFFFFFFFFF:
							out.append("""		<td align="right">EOF</td>""")
						else:
							out.append("""		<td align="right">%u</td>""" % al.end)
					out.append("""		<td align="center">%s</td>""" % ("Read (shared)" if al.ctype==MFS_LOCK_TYPE_SHARED else "Write (exclusive)" if al.ctype==MFS_LOCK_TYPE_EXCLUSIVE else "Unknown"))
					out.append("""	</tr>""")
				out.append("""</table>""")

	return out