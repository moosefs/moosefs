from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	EXorder = fields.getint("EXorder", 0)
	EXrev = fields.getint("EXrev", 0)

	out = []
	out.append("""<div class="tab_title">Exports</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsexports">""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
	out.append("""		<th colspan="2">IP&nbsp;range</th>""")
	out.append("""		<th rowspan="2">Path</th>""")
	out.append("""		<th rowspan="2">Minimum<br/>version</th>""")
	out.append("""		<th rowspan="2">Alldirs</th>""")
	out.append("""		<th rowspan="2">Password</th>""")
	out.append("""		<th rowspan="2">RO/RW</th>""")
	out.append("""		<th rowspan="2">Restricted&nbsp;IP</th>""")
	out.append("""		<th rowspan="2">Ignore<br/>gid</th>""")
	out.append("""		<th rowspan="2">Admin</th>""")
	out.append("""		<th colspan="2">Map&nbsp;root</th>""")
	out.append("""		<th colspan="2">Map&nbsp;users</th>""")
	out.append("""		<th rowspan="2">Allowed<br/>sclasses</th>""")
	out.append("""		<th colspan="2">Trashretention&nbsp;limit</th>""")
	if dp.master().has_feature(FEATURE_EXPORT_UMASK):
		out.append("""		<th rowspan="2">Global<br/>umask</th>""")
	if dp.master().has_feature(FEATURE_EXPORT_DISABLES):
		out.append("""		<th rowspan="2">Disables<br/>mask</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th style="min-width:80px;">from</th>""")
	out.append("""		<th style="min-width:80px;">to</th>""")
	out.append("""		<th style="min-width:40px;">uid</th>""")
	out.append("""		<th style="min-width:40px;">gid</th>""")
	out.append("""		<th style="min-width:40px;">uid</th>""")
	out.append("""		<th style="min-width:40px;">gid</th>""")
	out.append("""		<th style="min-width:40px;">min</th>""")
	out.append("""		<th style="min-width:40px;">max</th>""")
	out.append("""	</tr>""")

	for ee in dp.get_exports(EXorder,EXrev):
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (ee.sortipfrom,ee.stripfrom))
		out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (ee.sortipto,ee.stripto))
		out.append("""		<td align="left">%s</td>""" % (".&nbsp;(META)" if ee.meta else htmlentities(ee.path)))
		out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (ee.sortver,ee.strver))
		out.append("""		<td align="center">%s</td>""" % ("-" if ee.meta else "yes" if ee.is_alldirs() else "no"))
		out.append("""		<td align="center">%s</td>""" % ("yes" if ee.is_password() else "no"))
		out.append("""		<td align="center">%s</td>""" % ("ro" if ee.is_readonly() else "rw"))
		out.append("""		<td align="center">%s</td>""" % ("no" if ee.is_unrestricted() else "yes"))
		out.append("""		<td align="center">%s</td>""" % ("-" if ee.meta else "yes" if ee.ignore_gid() else "no"))
		out.append("""		<td align="center">%s</td>""" % ("-" if ee.meta else "yes" if ee.is_admin() else "no"))
		if ee.meta:
			out.append("""		<td align="right"><span class="sortkey">-1</span>-</td>""")
			out.append("""		<td align="right"><span class="sortkey">-1</span>-</td>""")
		else:
			out.append("""		<td align="right">%u</td>""" % ee.rootuid)
			out.append("""		<td align="right">%u</td>""" % ee.rootgid)
		if ee.meta or (ee.map_user())==0:
			out.append("""		<td align="center"><span class="sortkey">-1</span>&nbsp;</td>""")
			out.append("""		<td align="center"><span class="sortkey">-1</span>&nbsp;</td>""")
		else:
			out.append("""		<td align="right">%u</td>""" % ee.mapalluid)
			out.append("""		<td align="right">%u</td>""" % ee.mapallgid)
		out.append("""		<td align="center"><span class="sortkey">%u</span>%s</td>""" % (ee.sclassgroups_sort,ee.sclassgroups_str))
		if ee.mintrashretention!=None and ee.maxtrashretention!=None:
			out.append("""		<td align="right"><span class="sortkey">%u</span><span data-tt-info="%s">%s</span></td>""" % (ee.mintrashretention,timeduration_to_fullstr(ee.mintrashretention),timeduration_to_shortstr(ee.mintrashretention)))
			out.append("""		<td align="right"><span class="sortkey">%u</span><span data-tt-info="%s">%s</span></td>""" % (ee.maxtrashretention,timeduration_to_fullstr(ee.maxtrashretention),timeduration_to_shortstr(ee.maxtrashretention)))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1</span>&nbsp;</td>""")
			out.append("""		<td align="center"><span class="sortkey">-1</span>&nbsp;</td>""")
		if dp.master().has_feature(FEATURE_EXPORT_UMASK):
			if ee.umaskval==None:
				out.append("""		<td align="center"><span class="sortkey">-1</span>-</td>""")
			else:
				out.append("""		<td align="center">%03o</td>""" % ee.umaskval)
		if dp.master().has_feature(FEATURE_EXPORT_DISABLES):
			if ee.disables!=0:
				helpmsg = "Disabled operations:<br/>%s" % disablesmask_to_string(ee.disables)
			else:
				helpmsg = "All operations enabled"
			out.append("""		<td align="center"><span class="sortkey">%u</span><span data-tt-info="%s">%08X</span></td>""" % (ee.disables,helpmsg,ee.disables))
		out.append("""	</tr>""")

	out.append("""</table>""")
	return out