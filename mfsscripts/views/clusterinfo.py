from common.constants import *
from common.utils import *
from common.utilsgui import *
from common.models import *

def render(dp, fields, vld):
	ci = dp.get_clusterinfo()
	out = []
	out.append("""<div class="tab_title">Cluster summary</div>""")
	out.append("""<table class="FR no-hover">""")

	out.append("""	<tr>""")
	out.append("""		<th colspan="3">Cluster space</th>""")
	out.append("""		<th colspan="2">Trash</th>""")
	out.append("""		<th colspan="2">Sustained</th>""")
	out.append("""		<th colspan="3">File system objects</th>""")
	out.append("""		<th rowspan="2">Chunks<br/>total</th>""")
	if ci.metainfomode:
		out.append("""		<th rowspan="2"><a style="cursor:default" title="chunk is in EC format when redundancy level in EC format is higher than number of full copies">EC chunks</a></th>""")
		out.append("""		<th rowspan="2"><a style="cursor:default" title="storage needed to maintain defined redundancy level divided by raw data size">Disks overhead<br/>due to redundancy</a></th>""")
		out.append("""		<th rowspan="2"><a style="cursor:default" title="assumes that full and EC chunks have the same average length and that we save extra copies using redundancy level from EC definition">Space saved<br/>by EC</a></th>""")
	else:
		out.append("""		<th colspan="2">Chunk copies</th>""")
	out.append("""	</tr>""")

	out.append("""	<tr>""")
	out.append("""		<th style="width:70px;">total</th>""")
	out.append("""		<th style="width:70px;">available</th>""")

	out.append("""		<th style="width:140px;">% used</th>""")
	out.append("""		<th style="min-width:50px;">space</th>""")
	out.append("""		<th style="min-width:50px;">files</th>""")
	out.append("""		<th style="min-width:50px;">space</th>""")
	out.append("""		<th style="min-width:50px;">files</th>""")
	out.append("""		<th style="min-width:50px;">all</th>""")
	out.append("""		<th style="min-width:50px;">directories</th>""")
	out.append("""		<th style="min-width:50px;">files</th>""")
	if not ci.metainfomode:
		out.append("""		<th><span data-tt-help="Chunks from all disks: both 'regular' and 'marked for removal'">all</span></th>""")
		out.append("""		<th><span data-tt-help="Only chunks from 'regular' hdd space (excluded 'marked for removal')">regular</span></th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<td align="center"><span data-tt-info="%s B">%s</span></td>""" % (decimal_number(ci.totalspace),humanize_number(ci.totalspace,"&nbsp;")))
	out.append("""		<td align="center"><span data-tt-info="%s B">%s</span></td>""" % (decimal_number(ci.availspace),humanize_number(ci.availspace,"&nbsp;")))
	usedpercent=100*(ci.totalspace-ci.availspace)/ci.totalspace if ci.totalspace!=0 else 0
	out.append("""		<td align="center"><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGVALUE"><span>%.1f</span></div></div></td>""" % (100.0-usedpercent, usedpercent))
	out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(ci.trspace),humanize_number(ci.trspace,"&nbsp;")))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.trfiles)) 
	out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(ci.respace),humanize_number(ci.respace,"&nbsp;")))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.refiles))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.nodes))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.dirs))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.files))
	out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.chunks))
	if ci.metainfomode:
		if ci.ecchunkspercent!=None:
			out.append("""		<td align="center"><a style="cursor:default" title="full chunks: %u ; EC8 chunks: %u ; EC4 chunks: %u">%.1f&#8239;%%</a></td>""" % (ci.copychunks,ci.ec8chunks,ci.ec4chunks,ci.ecchunkspercent))
		else:
			out.append("""		<td align="center"><a style="cursor:default" title="full chunks: %u ; EC8 chunks: %u ; EC4 chunks: %u">-</a></td>""" % (ci.copychunks,ci.ec8chunks,ci.ec4chunks))
		if ci.dataredundancyratio!=None:
			out.append("""		<td align="center"><a style="cursor:default" title="full copies: %u ; EC8 parts: %u ; EC4 parts: %u ; chunks: %u">+ %.1f&#8239;%%</a></td>""" % (ci.chunkcopies,ci.chunkec8parts,ci.chunkec4parts,(ci.copychunks+ci.ec8chunks+ci.ec4chunks),(ci.dataredundancyratio-1)*100))
		else:
			out.append("""		<td align="center"><a style="cursor:default" title="full copies: %u ; EC8 parts: %u ; EC4 parts: %u ; chunks: %u">-</a></td>""" % (ci.chunkcopies,ci.chunkec8parts,ci.chunkec4parts,(ci.copychunks+ci.ec8chunks+ci.ec4chunks)))
		if ci.savedbyec!=None:
			out.append("""		<td align="center">%s</td>""" % (("about "+humanize_number(ci.savedbyec,"&nbsp;")) if ci.savedbyec>0 else "not yet"))
		else:
			out.append("""		<td align="center">-</td>""")
	else:
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.allcopies))
		out.append("""		<td align="center">%s</td>""" % decimal_number_html(ci.regularcopies))
	out.append("""	</tr>""")
	out.append("""</table>""")
	return out