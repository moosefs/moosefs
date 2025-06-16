from common.constants import *
from common.utilsgui import *

def render(dp, fields, vld):
	DEFAULT_MFLIMIT = 10
	MForder = fields.getint("MForder", 0)
	MFrev = fields.getint("MFrev", 0)
	MFlimit = fields.getint("MFlimit", DEFAULT_MFLIMIT)
	# if MFlimit!=0 and MFlimit<DEFAULT_MFLIMIT:
	# 	MFlimit = DEFAULT_MFLIMIT

	mchunks = dp.get_missing_chunks(MForder, MFrev)
	# mchunks = mchunks * 60
	# mchunks = mchunks[:5]
	mccnt = len(mchunks)
	if mccnt==0:
		return

	out = []
	# out.append("""<div class="tab_title">Missing files %s</div>""" % vld.issue('data_missing_files').icon())
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_missingfiles">""")
	show_all = MFlimit==0 or MFlimit>=mccnt
	pagination = None

	if mccnt<DEFAULT_MFLIMIT:
		subtitle = ""
	else:
		if show_all:
			pagination = html_refresh_a('less', 'MFlimit', DEFAULT_MFLIMIT)
			subtitle = """- %u of %u entries - %s""" % (mccnt,mccnt,pagination)
		else:
			pagination = html_refresh_a('10 more', 'MFlimit', MFlimit+DEFAULT_MFLIMIT)
			if mccnt-MFlimit>100:
				pagination = "%s - %s" % (pagination, html_refresh_a('100 more', 'MFlimit', MFlimit+100))
			pagination = "%s - %s" % (pagination, html_refresh_a('all', 'MFlimit', '0'))
			if MFlimit>DEFAULT_MFLIMIT:
				pagination = "%s - %s" % (pagination, html_refresh_a('less', 'MFlimit', DEFAULT_MFLIMIT))
			subtitle = """- %u of %u entries - %s""" % (MFlimit,mccnt,pagination)

	out.append("""	<tr><th colspan="6">Missing files %s (gathered by the previous filesystem self-check loop) %s</th></tr>""" % (vld.issue('data_missing_files').icon(),subtitle))
	out.append("""	<tr>""")
	out.append("""		<th class="acid_tab_enumerate">#</th>""")
	out.append("""		<th>Paths</th>""")
	out.append("""		<th>Inode</th>""")
	out.append("""		<th>Index</th>""")
	out.append("""		<th>Chunk&nbsp;id</th>""")
	out.append("""		<th>Type&nbsp;of&nbsp;missing&nbsp;chunk</th>""")
	out.append("""	</tr>""")

	missingcount = 0
	for mc in mchunks:
		info = vld.get_missing_chunk_info(mc)
		if mccnt>0:
			if len(mc.paths)==0:
				if missingcount<MFlimit or MFlimit==0:
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="left"> * unknown path * (deleted file)</td>""")
					out.append("""		<td align="center">%u</td>""" % mc.inode)
					out.append("""		<td align="center">%u</td>""" % mc.indx)
					out.append("""		<td align="center" class="monospace">%s</td>""" % mc.get_chunkid_str())
					out.append("""		<td align="center">%s</td>""" % info.span_noicon(mc.get_mtype_str()))
					out.append("""	</tr>""")
				missingcount += 1
			else:
				for path in mc.paths:
					if missingcount<MFlimit or MFlimit==0:
						out.append("""	<tr>""")
						out.append("""		<td align="right"></td>""")
						out.append("""		<td align="left">%s</td>""" % path)
						out.append("""		<td align="center">%u</td>""" % mc.inode)
						out.append("""		<td align="center">%u</td>""" % mc.indx)
						out.append("""		<td align="center" class="monospace">%s</td>""" % mc.get_chunkid_str())
						out.append("""		<td align="center">%s</td>""" % info.span_noicon(mc.get_mtype_str()))
						out.append("""	</tr>""")
					missingcount += 1

	if pagination:
		txt = "all" if (MFlimit==0 or MFlimit>mccnt) else "%s of %s" % (MFlimit,mccnt)
		out.append("""<tfoot><tr><td colspan="6">Showing %s entries - %s</td></tr></tfoot>""" % (txt,pagination))
	out.append("""</table>""")
	return out