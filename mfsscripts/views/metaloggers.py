from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	MBorder = fields.getint("MBorder", 0)
	MBrev = fields.getint("MBrev", 0)

	out = []
	# out.append("""<div class="tab_title">Metadata backup loggers</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmbl">""")
	out.append("""	<tr>""")
	out.append("""		<th class="acid_tab_enumerate">#</th>""")
	out.append("""		<th>IP</th>""")
	out.append("""		<th>Version</th>""")
	out.append("""		<th style="border-right:0;">Metalogger host</th>""")
	out.append("""		<th></th>""")
	out.append("""	</tr>""")

	for ml in dp.get_metaloggers(MBorder, MBrev):
		issues = vld.check_ml_version(ml)
		out.append("""	<tr>""")
		# out.append("""		<td align="right"></td><td align="left">%s</td><td align="center"><span class="sortkey">%s </span>%s</td><td align="center"><span class="sortkey">%s </span>%s</td>""" % (ml.host,ml.sortip,ml.strip,ml.sortver,issues.span(ml.strver.replace("PRO","<small>PRO</small>"))))
		out.append("""    <td align="right"></td>""")
		out.append("""		<td align="center" style="width:6%%;"><span class="sortkey">%s </span>%s</td>""" % (ml.sortip, ml.strip))
		out.append("""		<td align="center" style="width:6%%;"><span class="sortkey">%s </span>%s</td>""" % (ml.sortver, issues.span(ml.strver.replace("PRO","<small>PRO</small>"))))
		out.append("""		<td align="center" style="width:16%%;border-right:0;">%s</td>""" % ml.host) 
		out.append("""		<td></td>""") 
		out.append("""	</tr>""")

	out.append("""</table>""")

	return out