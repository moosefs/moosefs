from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld, readonly):
	MSorder = fields.getint("MSorder", 0)
	MSrev = fields.getint("MSrev", 0)

	out = []
	out.append("""<div class="tab_title">Active mounts (parameters)</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmounts">""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
	out.append("""		<th rowspan="2" class="wrap">Session id</th>""")
	out.append("""		<th rowspan="2">Host</th>""")
	out.append("""		<th rowspan="2">IP</th>""")
	out.append("""		<th rowspan="2" class="wrap">Mount point</th>""")
	out.append("""		<th rowspan="2" class="wrap">Open files</th>""")
	out.append("""		<th rowspan="2" class="wrap">Number of connections</th>""")
	out.append("""		<th rowspan="2">Version</th>""")
	out.append("""		<th rowspan="2">Root dir</th>""")
	out.append("""		<th rowspan="2">RO/RW</th>""")
	out.append("""		<th rowspan="2" class="wrap">Restricted IP</th>""")
	out.append("""		<th rowspan="2" class="wrap">Ignore gid</th>""")
	out.append("""		<th rowspan="2">Admin</th>""")
	out.append("""		<th colspan="2">Map&nbsp;root</th>""")
	out.append("""		<th colspan="2">Map&nbsp;users</th>""")
	out.append("""		<th rowspan="2">Allowed&nbsp;sclasses</th>""")
	out.append("""		<th colspan="2">Trashretention&nbsp;limits</th>""")
	if dp.master().has_feature(FEATURE_EXPORT_UMASK):
		out.append("""		<th rowspan="2" class="wrap">Global umask</th>""")
	if dp.master().has_feature(FEATURE_EXPORT_DISABLES):
		out.append("""		<th rowspan="2" class="wrap">Disables mask</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th>uid</th>""")
	out.append("""		<th>gid</th>""")
	out.append("""		<th>uid</th>""")
	out.append("""		<th>gid</th>""")
	out.append("""		<th>min</th>""")
	out.append("""		<th>max</th>""")
	out.append("""	</tr>""")

	sessions,dsessions = dp.get_sessions_by_state(MSorder, MSrev)

	# Show active mounts
	for ses in sessions:
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="center">%s</td>""" % ses.get_sessionstr())
		out.append("""		<td align="left">%s</td>""" % ses.host)
		out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (ses.sortip,ses.strip))
		out.append("""		<td align="left">%s</td>""" % htmlentities(ses.info))
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(ses.openfiles))
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(ses.nsocks))
		issues = vld.check_mount_version(ses)
		out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (ses.sortver,issues.span(ses.strver.replace("PRO","<small>PRO</small>"))))
		out.append("""		<td align="left">%s</td>""" % (".&nbsp;(META)" if ses.meta else htmlentities(ses.path)))
		out.append("""		<td align="center">%s</td>""" % ("ro" if ses.sesflags&1 else "rw"))
		out.append("""		<td align="center">%s</td>""" % ("no" if ses.sesflags&2 else "yes"))
		out.append("""		<td align="center">%s</td>""" % ("-" if ses.meta else "yes" if ses.sesflags&4 else "no"))
		out.append("""		<td align="center">%s</td>""" % ("-" if ses.meta else "yes" if ses.sesflags&8 else "no"))
		if ses.meta:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		else:
			out.append("""		<td align="right">%u</td>""" % ses.rootuid)
			out.append("""		<td align="right">%u</td>""" % ses.rootgid)
		if ses.meta or (ses.sesflags&16)==0:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		else:
			out.append("""		<td align="right">%u</td>""" % ses.mapalluid)
			out.append("""		<td align="right">%u</td>""" % ses.mapallgid)
		out.append("""		<td align="center"><span class="sortkey">%u</span>%s</td>""" % (ses.get_sclassgroups_sort(),ses.get_sclassgroups_str()))
		if ses.mintrashretention!=None and ses.maxtrashretention!=None:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (ses.mintrashretention,timeduration_to_fullstr(ses.mintrashretention),timeduration_to_shortstr(ses.mintrashretention)))
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (ses.maxtrashretention,timeduration_to_fullstr(ses.maxtrashretention),timeduration_to_shortstr(ses.maxtrashretention)))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if dp.master().has_feature(FEATURE_EXPORT_UMASK):
			if ses.umaskval==None:
				out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
			else:
				out.append("""		<td align="center">%03o</td>""" % ses.umaskval)
		if dp.master().has_feature(FEATURE_EXPORT_DISABLES):
			out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%08X</a></td>""" % (ses.disables,disablesmask_to_string(ses.disables),ses.disables))
		out.append("""	</tr>""")
	out.append("""</table>""")
	
	# Show inactive mounts
	if len(dsessions)>0:
		out.append("""<div class="tab_title" data-tt="mnt_inactive_list">Inactive mounts (parameters)</div>""")
		out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmounts">""")
		out.append("""	<tr>""")
		out.append("""		<th class="acid_tab_enumerate">#</th>""")
		out.append("""		<th>Session&nbsp;id</th>""")
		out.append("""		<th>Host</th>""")
		out.append("""		<th>IP</th>""")
		out.append("""		<th>Mount&nbsp;point</th>""")
		out.append("""		<th>Open files</th>""")
		out.append("""		<th>Expires</th>""")
		if (not readonly):
			out.append("""		<th>cmd</th>""")
		out.append("""	</tr>""")
		
		issues = vld.issue('mnt_inactive')
		for ses in dsessions:
			out.append("""	<tr>""")
			out.append("""		<td align="right"></td>""")
			out.append("""		<td align="center">%s</td>""" % issues.span(str(ses.sessionid)))
			out.append("""		<td align="left">%s</td>""" % ses.host)
			out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (ses.sortip,ses.strip))
			out.append("""		<td align="left">%s</td>""" % ses.info)
			out.append("""		<td align="center">%u</td>""" % ses.openfiles)
			out.append("""		<td align="center">%s s</td>""" % decimal_number_html(ses.expire))
			if (not readonly):
				out.append("""		<td align="center"><a href="%s">click to remove</a></td>""" % fields.createhtmllink({"MSremove":("%u" % (ses.sessionid))}))
			out.append("""	</tr>""")
		out.append("""</table>""")

	return out