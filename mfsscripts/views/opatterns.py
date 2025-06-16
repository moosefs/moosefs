from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	PAorder = fields.getint("PAorder", 0)
	PArev = fields.getint("PArev", 0)
	
	out = []
	out.append("""<div class="tab_title">Override patterns</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfspatterns">""")
	out.append("""	<tr>""")
	out.append("""		<th class="acid_tab_enumerate">#</th>""")
	out.append("""		<th>Pattern (GLOB)</th>""")
	out.append("""		<th>euid</th>""")
	out.append("""		<th>egid</th>""")
	out.append("""		<th>Priority</th>""")
	out.append("""		<th>Storage&nbsp;class</th>""")
	out.append("""		<th>Trash&nbsp;retention</th>""")
	out.append("""		<th>Extra&nbsp;attributes</th>""")
	out.append("""	</tr>""")

	for op in dp.get_opatterns(PAorder, PArev):
		if op.sclassname==None:
			op.sclassname = "-"
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="center">%s</td>""" % htmlentities(op.globname))
		out.append("""		<td align="center">%s</td>""" % op.euidstr)
		out.append("""		<td align="center">%s</td>""" % op.egidstr)
		out.append("""		<td align="center">%u</td>""" % op.priority)
		out.append("""		<td align="center">%s</td>""" % htmlentities(op.sclassname))
		if op.trashretention==None:
			out.append("""		<td align="center"><span class="sortkey">0 </span>-</td>""")
		else:
			out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (op.trashretention,hours_to_str(op.trashretention),timeduration_to_shortstr(op.trashretention*3600)))
		out.append("""		<td align="center"><span class="sortkey">%u </span>%s</td>""" % (op.seteattr|op.clreattr,op.eattrstr))
		out.append("""	</tr>""")

	out.append("""</table>""")
	return out