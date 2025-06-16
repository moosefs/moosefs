from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	MOorder = fields.getint("MOorder", 0)
	MOrev = fields.getint("MOrev", 0)
	MOdata = fields.getint("MOdata", 0)

	out = []
	out.append("""<div class="tab_title">Active mounts (operations)</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsops" id="mfsops">""")
	out.append("""	<tr>""")
	out.append("""		<th colspan="4" class="knob-cell">""")
	options=[(-90,"Last hour",None,"acid_tab.switchdisplay('mfsops','opshour_vis',0);"),
			(90, "Current hour",None,"acid_tab.switchdisplay('mfsops','opshour_vis',1);")]
	out.append(html_knob_selector("opshour_vis",9,(210,22),(100,11),options))

	out.append("""		</th>""")
	out.append("""		<th colspan="%u" style="vertical-align: middle;">""" % (1+dp.stats_to_show))
	out.append("""			<span class="opshour_vis0">Last hour operations</span>""")
	out.append("""			<span class="opshour_vis1">Current hour operations</span>""")
	out.append("""		</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="1" class="acid_tab_enumerate">#</th>""")
	out.append("""		<th rowspan="1">Host</th>""")
	out.append("""		<th rowspan="1">IP</th>""")
	out.append("""		<th rowspan="1">Mount&nbsp;point</th>""")
	out.append("""		<th class="acid_tab_level_1">statfs</th>""")
	out.append("""		<th class="acid_tab_level_1">getattr</th>""")
	out.append("""		<th class="acid_tab_level_1">setattr</th>""")
	out.append("""		<th class="acid_tab_level_1">lookup</th>""")
	out.append("""		<th class="acid_tab_level_1">mkdir</th>""")
	out.append("""		<th class="acid_tab_level_1">rmdir</th>""")
	out.append("""		<th class="acid_tab_level_1">symlink</th>""")
	out.append("""		<th class="acid_tab_level_1">readlink</th>""")
	out.append("""		<th class="acid_tab_level_1">mknod</th>""")
	out.append("""		<th class="acid_tab_level_1">unlink</th>""")
	out.append("""		<th class="acid_tab_level_1">rename</th>""")
	out.append("""		<th class="acid_tab_level_1">link</th>""")
	out.append("""		<th class="acid_tab_level_1">readdir</th>""")
	out.append("""		<th class="acid_tab_level_1">open</th>""")
	out.append("""		<th class="acid_tab_level_1">rchunk</th>""")
	out.append("""		<th class="acid_tab_level_1">wchunk</th>""")
	if (dp.stats_to_show>16):
		out.append("""		<th class="acid_tab_level_1">read</th>""")
		out.append("""		<th class="acid_tab_level_1">write</th>""")
		out.append("""		<th class="acid_tab_level_1">fsync</th>""")
		out.append("""		<th class="acid_tab_level_1">snapshot</th>""")
		out.append("""		<th class="acid_tab_level_1">truncate</th>""")
		out.append("""		<th class="acid_tab_level_1">getxattr</th>""")
		out.append("""		<th class="acid_tab_level_1">setxattr</th>""")
		out.append("""		<th class="acid_tab_level_1">getfacl</th>""")
		out.append("""		<th class="acid_tab_level_1">setfacl</th>""")
		out.append("""		<th class="acid_tab_level_1">create</th>""")
		out.append("""		<th class="acid_tab_level_1">lock</th>""")
		out.append("""		<th class="acid_tab_level_1">meta</th>""")
	out.append("""		<th class="acid_tab_level_1">total</th>""")
	out.append("""	</tr>""")

	for ses in dp.get_sessions_order_by_mo(MOorder,MOrev,MOdata):
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="left">%s</td>""" % ses.host)
		out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (ses.sortip,ses.strip))
		out.append("""		<td align="left">%s</td>""" % htmlentities(ses.info))
		for st in range(dp.stats_to_show):
			out.append("""		<td align="right">""")
			out.append("""			<span class="opshour_vis0"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (ses.stats_c[st],ses.stats_l[st],decimal_number_html(ses.stats_l[st])))
			out.append("""			<span class="opshour_vis1"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (ses.stats_c[st],ses.stats_l[st],decimal_number_html(ses.stats_c[st])))
			out.append("""		</td>""")
		out.append("""		<td align="right">""")
		out.append("""			<span class="opshour_vis0"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (sum(ses.stats_c),sum(ses.stats_l),decimal_number_html(sum(ses.stats_l))))
		out.append("""			<span class="opshour_vis1"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (sum(ses.stats_c),sum(ses.stats_l),decimal_number_html(sum(ses.stats_c))))
		out.append("""		</td>""")
		out.append("""	</tr>""")

	out.append("""</table>""")
	return out