from common.models import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	out = []
	ci = dp.get_chunktestinfo()
	if type(ci)==ChunkTestInfo72:
		out.append("""<div class="tab_title">Chunks housekeeping operation</div>""")
		out.append("""<table class="FR no-hover">""")
		out.append("""	<tr>""")
		out.append("""		<th colspan="2">Loop time</th>""")
		out.append("""		<th colspan="4">Deletions</th>""")
		out.append("""		<th colspan="3">Replications</th>""")
		out.append("""		<th colspan="2">Locked</th>""")
		out.append("""	</tr>""")
		out.append("""	<tr>""")
		out.append("""		<th>start</th>""")
		out.append("""		<th>end</th>""")
		out.append("""		<th>invalid</th>""")
		out.append("""		<th>unused</th>""")
		out.append("""		<th>disk clean</th>""")
		out.append("""		<th>over goal</th>""")
		out.append("""		<th>under goal</th>""")
		out.append("""		<th>wrong labels</th>""")
		out.append("""		<th>rebalance</th>""")
		out.append("""		<th>unused</th>""")
		out.append("""		<th>used</th>""")
		out.append("""	</tr>""")
		if ci.loopstart>0:
			out.append("""	<tr>""")
			out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(ci.loopstart)),))
			out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(ci.loopend)),))
			out.append("""		<td align="right">%u/%u</td>""" % (ci.del_invalid,ci.del_invalid+ci.ndel_invalid))
			out.append("""		<td align="right">%u/%u</td>""" % (ci.del_unused,ci.del_unused+ci.ndel_unused))
			out.append("""		<td align="right">%u/%u</td>""" % (ci.del_dclean,ci.del_dclean+ci.ndel_dclean))
			out.append("""		<td align="right">%u/%u</td>""" % (ci.del_ogoal,ci.del_ogoal+ci.ndel_ogoal))
			out.append("""		<td align="right">%u/%u</td>""" % (ci.rep_ugoal,ci.rep_ugoal+ci.nrep_ugoal))
			out.append("""		<td align="right">%u/%u/%u</td>""" % (ci.rep_wlab,ci.labels_dont_match,ci.rep_wlab+ci.nrep_wlab+ci.labels_dont_match))
			out.append("""		<td align="right">%u</td>""" % ci.rebalance)
			out.append("""		<td align="right">%u</td>""" % ci.locked_unused)
			out.append("""		<td align="right">%u</td>""" % ci.locked_used)
			out.append("""	</tr>""")
		else:
			out.append("""	<tr>""")
			out.append("""		<td colspan="11" align="center">No data, self-check loop not finished yet</td>""")
			out.append("""	</tr>""")
		out.append("""</table>""")

	elif type(ci)==ChunkTestInfo96:
		if ci.loopstart>0:
			out.append("""<div class="tab_title">Chunks housekeeping operations</div>""")
			out.append("""<table class="FR panel no-hover">""")
			out.append(""" <tr><td>""")
			tdata=[["Loop start", datetime_to_str(ci.loopstart)], 
					["Loop end", datetime_to_str(ci.loopend)], 
					["Locked unused chunks",decimal_number_html(ci.locked_unused)],
					["Locked chunks", decimal_number_html(ci.locked_used)],
					["Fixed chunks", decimal_number_html(ci.fixed)],
					["Forced keep mode", decimal_number_html(ci.forcekeep)]]
			out.append(html_table_vertical("Housekeeping loop statistics", tdata))
			out.append(""" </td><td>""")
			tdata=[["Invalid", decimal_number_html(ci.delete_invalid)], 
					["Removed", decimal_number_html(ci.delete_no_longer_needed)], 
					["Wrong version", decimal_number_html(ci.delete_wrong_version)],
					["Excess", decimal_number_html(ci.delete_excess_copy)],
					["Marked for removal", decimal_number_html(ci.delete_diskclean_copy)]]
			out.append(html_table_vertical("Chunk copies - deletions", tdata))

			tdata=[["Needed", decimal_number_html(ci.replicate_needed_copy)], 
					["Wrong labels", decimal_number_html(ci.replicate_wronglabels_copy)]]
			out.append(html_table_vertical("Chunks copies - replications", tdata))	
			out.append(""" </td><td>""")
			tdata=[["Duplicated", decimal_number_html(ci.delete_duplicated_ecpart)], 
					["Excess", decimal_number_html(ci.delete_excess_ecpart)], 
					["Marked for removal", decimal_number_html(ci.delete_diskclean_ecpart)]]
			out.append(html_table_vertical("EC parts - deletions", tdata))

			tdata=[["Duplicated server", decimal_number_html(ci.replicate_dupserver_ecpart)], 
					["Needed", decimal_number_html(ci.replicate_needed_ecpart)], 
					["Wrong labels", decimal_number_html(ci.replicate_wronglabels_ecpart)],
					["Recovered", decimal_number_html(ci.recover_ecpart)],
					["Calculated checksums", decimal_number_html(ci.calculate_ecchksum)]]
			out.append(html_table_vertical("EC parts - replications", tdata))
			out.append(""" </td><td>""")
			tdata=[["Split: copies &rarr; EC parts", decimal_number_html(ci.split_copy_into_ecparts)], 
					["Join: EC parts &rarr; copies", decimal_number_html(ci.join_ecparts_into_copy)]]
			out.append(html_table_vertical("Copies &harr; EC parts", tdata))	

			tdata=[["Rebalance", decimal_number_html(ci.replicate_rebalance)]]
			out.append(html_table_vertical("Replications", tdata))	

			out.append(""" </td></tr>""")
			out.append("""</table>""")						
		else:
			out.append("""<div class="tab_title">Chunks housekeeping operations</div>""")
			out.append("""<table class="FR">""")
			out.append("""	<tr>""")
			out.append("""		<td align="center">No data, self-check loop not finished yet</td>""")
			out.append("""	</tr>""")
			out.append("""</table>""")
	return out
