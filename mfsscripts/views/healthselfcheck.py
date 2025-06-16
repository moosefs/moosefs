from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	hsc = dp.get_health_selfcheck()
	mchunks = dp.get_missing_chunks(None)
	show_mfiles = len(mchunks)>0 
	out = []
	issues = vld.check_health_selfcheck(hsc)
	out.append("""<div class="tab_title">Filesystem self-check %s </div>""" % issues.icon())
	if show_mfiles:
		out.append("""<table class="FR panel no-hover">""")
		out.append(""" <tr><td>""")
	out.append("""<table class="FR no-hover">""")
	
	out.append("""	<tr>""")
	out.append("""		<th colspan="2">Self-check loop</th>""")
	out.append("""		<th colspan="5">Files (health)</th>""")
	out.append("""		<th colspan="3">Chunks (health)</th>""")
	out.append("""	</tr>""")

	out.append("""	<tr>""")
	out.append("""		<th style="min-width: 100px;">start time</th>""")
	out.append("""		<th style="min-width: 100px;">end time</th>""")
	out.append("""		<th style="min-width: 100px;">checked</th>""")
	out.append("""		<th style="min-width: 100px;">missing</th>""")
	out.append("""		<th style="min-width: 100px;">undergoal</th>""")
	out.append("""		<th style="min-width: 100px;">missing in trash</th>""")
	out.append("""		<th style="min-width: 100px;">missing sustained</th>""")
	out.append("""		<th style="min-width: 100px;">checked</th>""")
	out.append("""		<th style="min-width: 100px;">missing</th>""")
	out.append("""		<th style="min-width: 100px;">undergoal</th>""")
	out.append("""	</tr>""")
	if hsc.loopstart>0:
		out.append("""	<tr>""")
		out.append("""		<td align="center">%s</td>""" % (datetime_to_str(hsc.loopstart),))
		out.append("""		<td align="center">%s</td>""" % (datetime_to_str(hsc.loopend),))
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(hsc.files))
		out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if hsc.mfiles==0 else "MISSING", decimal_number_html(hsc.mfiles)))
		out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if hsc.ugfiles==0 else "UNDERGOAL", decimal_number_html(hsc.ugfiles)))
		out.append("""		<td align="center">%s</td>""" % decimal_number_na_html(hsc.mtfiles))
		out.append("""		<td align="center">%s</td>""" % decimal_number_na_html(hsc.msfiles))
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(hsc.chunks))
		out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if hsc.mchunks==0 else "MISSING", decimal_number_html(hsc.mchunks)))
		out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if hsc.ugchunks==0 else "UNDERGOAL", decimal_number_html(hsc.ugchunks)))
		out.append("""	</tr>""")
		if hsc.msgbuffleng>0:
			if hsc.msgbuffleng==FSTEST_INFO_STR_LIMIT:
				out.append("""	<tr><th colspan="10">Important messages (truncated, there is more...):</th></tr>""")
			else:
				out.append("""	<tr><th colspan="10">Important messages:</th></tr>""")
			out.append("""	<tr>""")
			out.append("""		<td colspan="10" align="left"><pre>%s</pre></td>""" % (htmlentities(hsc.datastr)))
			out.append("""	</tr>""")
	else:
		out.append("""	<tr>""")
		out.append("""		<td colspan="10" align="center">No data, self-check loop not finished yet</td>""" )
		out.append("""	</tr>""")
	out.append("""</table>""")

	if show_mfiles:
		import views.missingfiles
		out.append("\n".join(views.missingfiles.render(dp, fields, vld)))
		out.append(""" </td></tr>""")
		out.append("""</table>""")
	
	return out