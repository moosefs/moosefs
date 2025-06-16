from common.constants import *
from common.utils import *
from common.utilsgui import *
from common.models import *

def render(dp, fields, vld):
	SCorder = fields.getint("SCorder", 0)
	SCrev = fields.getint("SCrev", 0)
	show_counters = fields.getint("SCcounters", 0) # show inodes and chunk counters columns?

	usingEC = dp.master().version_at_least(4,5,0)
	show_arch_min_size = dp.master().version_at_least(4,34,0)
	show_labelmode_overrides = dp.master().has_feature(FEATURE_LABELMODE_OVERRIDES) # v.4.53.0
	show_export_group_and_priority = dp.master().has_feature(FEATURE_SCLASSGROUPS) # v.4.57.0
	out = []
	out.append("""<div class="tab_title">Storage classes""")
	out.append("""</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfssc" id="mfssc">""")
	if usingEC:
		out.append("""	<tr>""")
		colspan = 6 if show_export_group_and_priority else 4
		out.append("""		<th rowspan="2" colspan="%u" class="knob-cell">""" % (colspan,))
		options=[(-57,"Hide","counters","document.location.href='%s&SCcounters=0'" % fields.createrawlink({"SCcounters":""})),
				(57, "Show","counters","document.location.href='%s&SCcounters=1'" % fields.createrawlink({"SCcounters":""}))]
		selected = 1 if show_counters else 0
		out.append(html_knob_selector("sccnt_vis",9,(150,28),(80,14),options,True,selected))
		out.append("""		</th>""")
		if show_counters:
			out.append("""		<th colspan="2" rowspan="2" class="acid_tab_skip sccnt_vis1">Inodes<br/>count</th>""")
		out.append("""		<th colspan="%d" rowspan="1" class="acid_tab_skip sccnt_vis1">States transitions</th>""" % (3 + show_arch_min_size))
		out.append("""		<th rowspan="4">Default<br/>class<br/>match<br/>mode</th>""")
		if show_counters:
			out.append("""		<th rowspan="1" colspan="%u" class="acid_tab_skip">States definitions and counters</th>""" % (12 + show_labelmode_overrides))
		else:
			out.append("""		<th rowspan="1" colspan="%u" class="acid_tab_skip">States definitions</th>""" % (6 + show_labelmode_overrides))
		out.append("""	</tr>""")

		out.append("""	<tr>""")
		out.append("""		<th colspan="%d" class="acid_tab_skip">KEEP &harr; ARCHIVE</th>""" % (2 + show_arch_min_size))
		out.append("""		<th class="acid_tab_skip">&rarr; TRASH</th>""")
		if show_labelmode_overrides:
			out.append("""		<th rowspan="3" class="acid_tab_skip">State<br/>match<br/>mode</th>""")
		out.append("""		<th rowspan="3" class="acid_tab_skip">State</th>""")
		out.append("""		<th rowspan="2" colspan="3" class="acid_tab_skip">Redundancy</th>""")
		out.append("""		<th rowspan="2" colspan="2" class="acid_tab_skip">Storage</th>""")
		if show_counters:
			out.append("""		<th rowspan="1" colspan="6" class="acid_tab_skip">Chunks count</th>""")
		out.append("""	</tr>""")

		out.append("""	<tr>""")
		out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
		out.append("""		<th rowspan="2">Id</th>""")
		out.append("""		<th rowspan="2" class="acid_tab_alpha">Name</th>""")
		if show_export_group_and_priority:
			out.append("""		<th rowspan="2">Join<br/>priority</th>""")
			out.append("""		<th rowspan="2">Export<br/>group</th>""")
		out.append("""		<th rowspan="2">Admin<br/>only</th>""")
		if show_counters:
			out.append("""		<th rowspan="2">files</th>""")
			out.append("""		<th rowspan="2">dirs</th>""")
		out.append("""		<th rowspan="2">mode</th>""")
		out.append("""		<th rowspan="2" style="min-width:32px;">delay</th>""")
		if show_arch_min_size:
			out.append("""		<th rowspan="2" style="min-width:32px;">min<br/>size</th>""")
		out.append("""		<th rowspan="2">min trash<br/>retention</th>""") #min trash retension
		if show_counters:
			out.append("""		<th colspan="2" class="acid_tab_skip">deficient</th>""")
			out.append("""		<th colspan="2" class="acid_tab_skip">stable</th>""")
			out.append("""		<th colspan="2" class="acid_tab_skip">over goal</th>""")
		out.append("""	</tr>""")

		out.append("""	<tr>""")
		out.append("""		<th class="acid_tab_skip">level</th>""")
		out.append("""		<th class="acid_tab_skip">format</th>""")
		out.append("""		<th class="acid_tab_skip">achievable</th>""")
		out.append("""		<th class="acid_tab_skip">labels expressions</th>""")
		out.append("""		<th class="acid_tab_skip">distribution</th>""")
		if show_counters:
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">copy</th>""")
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">copy</th>""")
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">copy</th>""")
			out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
		out.append("""	</tr>""")
	else:
		show_counters = 1 # always show counters for older versions
		out.append("""	<tr>""")
		out.append("""		<th rowspan="3" class="acid_tab_enumerate">#</th>""")
		out.append("""		<th rowspan="2">Id</th>""")
		out.append("""		<th rowspan="2" classname="acid_tab_alpha">Name</th>""")
		out.append("""		<th rowspan="2">Admin only</th>""")
		out.append("""		<th colspan="2" class="acid_tab_skip">Inodes count</th>""")
		out.append("""		<th rowspan="2">Archive mode</th>""")
		out.append("""		<th rowspan="2">Archive delay</th>""")
		out.append("""		<th rowspan="2">Min trashretention</th>""")
		out.append("""		<th rowspan="2">Labels mode</th>""")
		out.append("""		<th colspan="5" class="acid_tab_skip">State definitions</th>""")
		out.append("""		<th colspan="5" class="acid_tab_skip">Chunks count</th>""")
		out.append("""	</tr>""")
		out.append("""	<tr>""")
		out.append("""		<th>files</th>""")
		out.append("""		<th>dirs</th>""")
		out.append("""		<th class="acid_tab_skip">state</th>""")
		out.append("""		<th class="acid_tab_skip">format</th>""")
		out.append("""		<th class="acid_tab_skip">achievable</th>""")
		out.append("""		<th class="acid_tab_skip">labels expressions</th>""")
		out.append("""		<th class="acid_tab_skip">distribution</th>""")
		out.append("""		<th class="acid_tab_skip">deficient</th>""")
		out.append("""		<th class="acid_tab_skip">stable</th>""")
		out.append("""		<th class="acid_tab_skip">over goal</th>""")
		out.append("""	</tr>""")
	
	for sc in dp.get_sclasses(SCorder, SCrev):
		out.append("""	<tr>""")
		rowcnt = len(sc.states)
		out.append("""		<td rowspan="%u" align="right"></td>""" % (rowcnt,))
		out.append("""		<td rowspan="%u" align="right">%u</td>""" % (rowcnt,sc.sclassid))
		# name
		sclassname=htmlentities(sc.sclassname)
		if sc.sclassdesc!=None and sc.sclassdesc!="":
			desc = htmlentities(sc.sclassdesc)
			out.append("""		<td rowspan="%u" class="word-wrap" style="min-width:5em;max-width:15em;white-space:normal;"><span class="center bold">%s</span><br/><span class="em9" style="text-overflow: ellipsis;">%s</span></td>""" % (rowcnt,sclassname,desc))
		else:
			out.append("""		<td rowspan="%u" class="center bold">%s</td>""" % (rowcnt,sclassname))
		# priority and export group
		if show_export_group_and_priority:
			out.append("""		<td rowspan="%u" align="center">%u</td>""" % (rowcnt,sc.priority))
			out.append("""		<td rowspan="%u" align="center">%u</td>""" % (rowcnt,sc.export_group))
		# admin only
		out.append("""		<td rowspan="%u" align="center">%s</td>""" % (rowcnt,sc.get_admin_only_str().capitalize()))
		# counters
		if show_counters:
			out.append("""		<td rowspan="%u" align="right">%s</td>""" % (rowcnt,decimal_number_html(sc.files)))
			out.append("""		<td rowspan="%u" align="right">%s</td>""" % (rowcnt,decimal_number_html(sc.dirs)))
		# mode
		out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span>%s</td>""" % (rowcnt,sc.arch_mode,mode2html(sc)))
		# delay		
		info = "sc_arch_delay0" if sc.arch_delay==0 else ""
		out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span><span data-tt="%s">%s</span></td>""" % (rowcnt,sc.arch_delay,info,archdelay2html(sc)))
		# min size
		if show_arch_min_size:
			info = ("%s B" % decimal_number(sc.arch_min_size)) if sc.arch_min_size>0 else "sc_arch_min_size0"
			out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span><span data-tt="%s">%s</span>""" % (rowcnt,sc.arch_min_size,info,humanize_number(sc.arch_min_size,nnbsp)))
		# min trash retention
		issues = vld.check_sc_mintrashretention(sc)
		out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span>%s</td>""" % (rowcnt,sc.min_trashretention,issues.span_noicon(mintrashretention2html(sc))))
		# labels match mode
		overrided = False
		clrcls = ""
		border = ""
		if show_labelmode_overrides:
			for st in sc.states:
				if st.defined and st.labelsmodeover>=0 and st.labelsmodeover<=2:
					overrided = True
					break
			if overrided: clrcls = "GRAY"
			border = """ style="border-right: none;" """
		out.append("""		<td rowspan="%u" align="center" %s><span class="%s">%s</span></td>""" % (rowcnt,border,clrcls,scfitmode2html(sc)))
		
		# states: CREATE, KEEP, ARCHIVE, TRASH
		newrow = 0
		group_states = sc.isKeepOnly() # group states if only KEEP is defined (this is often the case)
		for line,st in enumerate(sc.states):
			show_line = not group_states or (group_states and line==0)
			rowspan = 4 if group_states else 1
			if newrow:
				out.append("""	</tr>""")
				out.append("""	<tr>""")
			else:
				newrow=1
			colorclass = "" if st.defined else "GRAY"
			wcolorclass = "WARNING" if st.defined or group_states else "GRAY"
			ecolorclass = "ERROR" if st.defined or group_states else "GRAY"
			if show_line:
				if show_labelmode_overrides:
					clrcls = "" if overrided else "GRAY"
					arrow = "&rarr;" if (not (st.labelsmodeover>=0 and st.labelsmodeover<=2 and st.defined)) else "&nbsp;"
					out.append("""		<td rowspan="%u" align="left" style="border-left: none;"><span style="margin-left:-5px;" class="GRAY monospace">%s</span>&#8239;<span class="%s">%s</span></td>""" % (rowspan,arrow,clrcls,stfitmode2html(st,sc)))
			# state name
			out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (colorclass,st.fullname))

			# level 
			if show_line:
				colorclass = "" if group_states else colorclass
				if usingEC and st.ec_level!=None and st.ec_level>0:
					out.append("""		<td rowspan="%u" align="center"><span class="%s bold">%s</span></td>""" % (rowspan,colorclass,st.ec_chksum_parts))
				else:
					copies = len(st.labellist)
					if usingEC:
						out.append("""		<td rowspan="%u" align="center"><span class="%s bold">%s</span></td>""" % (rowspan,colorclass,copies-1))
				# format 
				out.append("""		<td rowspan="%u" align="left"><span class="%s">&nbsp;%s</span></td>""" % (rowspan,colorclass,format2html(usingEC,st)))

				# achievable
				if st.canbefulfilled==3:
					out.append("""		<td rowspan="%u" align="center"><span class="%s">Yes</span></td>""" % (rowspan,colorclass))
				elif st.canbefulfilled==2:
					out.append("""		<td rowspan="%u" align="center"><span class="%s" data-tt="sc_achievable_overloaded">Overloaded</span></td>""" % (rowspan,wcolorclass))
				elif st.canbefulfilled==1:
					out.append("""		<td rowspan="%u" align="center"><span class="%s" data-tt="sc_achievable_nospace">No space</span></td>""" % (rowspan,wcolorclass))
				elif st.canbefulfilled==4:
					out.append("""		<td rowspan="%u" align="center"><span class="%s" data-tt="sc_achievable_keeponly">EC on hold</span></td>""" % (rowspan,wcolorclass))
				else:
					out.append("""		<td rowspan="%u" align="center"><span class="%s" data-tt="sc_achievable_no">No</span></td>""" % (rowspan,ecolorclass))
				
				# labels
				(tooltip, txt) = labels2html(usingEC,sc,st)
				out.append("""		<td rowspan="%u" align="center"><span data-tt-info="%s">%s</span></td>""" % (rowspan, tooltip, txt))

				# distribution
				distribution = uniqmask_to_str(st.uniqmask)
				distribution = "random" if distribution=='-' else distribution
				out.append("""		<td rowspan="%u" align="center"><span class="%s">%s</span></td>""" % (rowspan,colorclass,distribution))

			# chunk counters
			if show_counters:
				if usingEC:
					if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.counters[3]!=None and st.counters[4]!=None and st.counters[5]!=None and st.defined:
						if st.ec_level!=None and st.ec_level>0:
							# EC state
							out.append("""		<td align="right"><span class="WARNING" data-tt="sc_deficient_chunks_ec_as_copy">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[0])) if st.counters[0]>0 else "&nbsp;"))
							out.append("""		<td align="right"><span class="WARNING" data-tt="sc_deficient_chunks_same_format">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[1])) if st.counters[1]>0 else "&nbsp;"))
							out.append("""		<td align="right"><span class="WRONGFORMAT" data-tt="sc_ec_as_copy">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[2])) if st.counters[2]>0 else "&nbsp;"))
							out.append("""		<td align="right"><span class="OK">%s</span></td>""" % decimal_number_html(st.counters[3]))
						else:
							# copies state
							out.append("""		<td align="right"><span class="WARNING" data-tt="sc_deficient_chunks_same_format">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[0])) if st.counters[0]>0 else "&nbsp;"))
							out.append("""		<td align="right"><span class="WARNING" data-tt="sc_deficient_chunks_copy_as_ec">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[1])) if st.counters[1]>0 else "&nbsp;"))
							out.append("""		<td align="right"><span class="OK">%s</span></td>""" % decimal_number_html(st.counters[2]))
							out.append("""		<td align="right"><span class="WRONGFORMAT" data-tt="sc_copy_as_ec">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[3])) if st.counters[3]>0 else "&nbsp;"))
						out.append("""		<td align="right"><span class="NOTICE">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[4])) if st.counters[4]>0 else "&nbsp;"))
						out.append("""		<td align="right"><span class="NOTICE">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[5])) if st.counters[5]>0 else "&nbsp;"))
					else:
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
				else:
					if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.defined:
						out.append("""		<td align="right"><span class="WARNING">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[0])) if st.counters[0]>0 else "&nbsp;"))
						out.append("""		<td align="right"><span class="OK">%s</span></td>""" % decimal_number_html(st.counters[1]))
						out.append("""		<td align="right"><span class="NOTICE">%s</span></td>""" % (("%s" % decimal_number_html(st.counters[2])) if st.counters[2]>0 else "&nbsp;"))
					else:
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")
						out.append("""		<td align="center"></td>""")

		out.append("""	</tr>""")
	out.append("""</table>""")

	return out

def format2html(usingEC,st):
	if usingEC and st.ec_level!=None and st.ec_level>0:
		return "EC&#8239;%u&#8239;+%u</span></td>" % (st.ec_data_parts,st.ec_chksum_parts)
	else:
		copies = len(st.labellist)
		copies_txt = "copies" if copies>1 else "copy"
		return "%u %s" % (copies,copies_txt)


# get mode cell content
def mode2html(sc):
	arch_mode_list = []
	if sc.arch_mode&SCLASS_ARCH_MODE_CHUNK:
		mode='Per chunk<br/><span style="display: block; margin-top: -4px;">&rarr;</span>'
		arch_mode_list.append("chunk's<br/>mtime")
	elif sc.arch_mode&SCLASS_ARCH_MODE_FAST:
		mode='Fast<br/><span style="display: block; margin-top: -4px;">&rarr;</span>'
	else:
		if sc.arch_mode&SCLASS_ARCH_MODE_REVERSIBLE:
			mode='Reversible<br/><span style="display: block; margin-top: -4px;">&harr;</span>'
		else:
			mode='Oneway<br/><span style="display: block; margin: -4px 0 4px 0;">&rarr;</span>'
		align = "&nbsp;" if sc.arch_mode&SCLASS_ARCH_MODE_MTIME else ""
		if sc.arch_mode&SCLASS_ARCH_MODE_CTIME:
			arch_mode_list.append(align+"ctime")
		if sc.arch_mode&SCLASS_ARCH_MODE_MTIME:
			arch_mode_list.append("mtime")
		if sc.arch_mode&SCLASS_ARCH_MODE_ATIME:
			arch_mode_list.append(align+"atime")
	arch_mode_str = "<br/>".join(arch_mode_list)

	if len(arch_mode_str)>0:
		return mode+arch_mode_str
	else:
		return mode

def scfitmode2html(sc):
	return "Loose" if sc.labels_mode==0 else "Standard" if sc.labels_mode==1 else "Strict"

def stfitmode2html(st,sc):
	return "Loose" if st.labelsmodeover==0 else "Standard" if st.labelsmodeover==1 else "Strict" if st.labelsmodeover==2 else scfitmode2html(sc)


# get delay cell content
def archdelay2html(sc):
	if (sc.arch_delay>0 and (sc.arch_mode&SCLASS_ARCH_MODE_FAST)==0):
			return hours2html(sc.arch_delay)
	if sc.arch_mode&SCLASS_ARCH_MODE_FAST!=0:
		return ""
	return "0"

def mintrashretention2html(sc):
	if not sc.defined_trash and sc.min_trashretention==0:
		return 'n/a'
	if sc.min_trashretention>0:
		return hours2html(sc.min_trashretention)
	return "0"

# get labels cell class, tooltip and content
def labels2html(usingEC, sc, st):
	def label2str(label, literals=['any server', ' and ', ' or ', ' not']):
		if sc.labels_ver==4:
			return labelexpr_to_str(label, literals)
		else:
			return labelmasks_to_str(label)
	
	if sc.isKeepOnly():
		title = "Labels expressions (all states)"
	else:
		title = "%s labels expressions" % st.fullname
	tooltip = "<table class='labels-table'>"
	lead = "Class " if len(sc.sclassname)<5 else ""
	tooltip += "<tr><th colspan='3'>%s<span class='NOTICE em11'>%s</span></th></tr>" % (lead,sc.sclassname)
	tooltip += "<tr><th colspan='3'>%s<hr></th></tr>" % title
	tooltip += "<tr><th class='em11 NOTICE'>%s</th><th>Labels<br/>expression</th><th>Available<br/>servers</th></tr>" % format2html(usingEC,st)
	labelsarr = []
	i = 1
	odd = True
	not_enough_servers = False
	only_one_server = False
	if st.isEC():
		for i in range(2):
			if len(st.labellist)>i:
				_,matchingservers,labelexpr = st.labellist[i]
			elif len(st.labellist)>0:
				_,matchingservers,labelexpr = st.labellist[0]
			else:
				matchingservers = 0
				labelexpr = []
			if matchingservers==0:
				not_enough_servers = True
			srvavailclr = ""
			tooltip += "<tr class='%s'>" % ("odd" if i==0 else "even")
			if i==0:
				tooltip += "<td class='left' style='padding-left:10px; min-width:90px;'>%s data parts</td>" % str(st.ec_data_parts)
			else:
				tooltip += "<td class='left' style='padding-left:10px; min-width:90px;'>%s parity part%s</td>" % (str(st.ec_chksum_parts), "s" if st.ec_chksum_parts>1 else "")
			tooltip += "<td>%s</td>" % htmlentities(label2str(labelexpr))
			srvmatchclr = "ERROR" if matchingservers==0 else "WARNING" if matchingservers==1 else srvavailclr
			tooltip += "<td class='right' style='padding-right:20px;'><span class='%s'>%s</span> <span class='%s'>of %s</span></td>" % (srvmatchclr, str(matchingservers), srvavailclr, str(sc.availableservers))
			tooltip += "</tr>"

	else:
		for _,matchingservers,labelexpr in st.labellist:
			if matchingservers==0:
				not_enough_servers = True
			elif matchingservers==1:
				only_one_server = True
			if odd:
				tooltip += "<tr class='odd'>"
			else:
				tooltip += "<tr class='even'>"
			srvavailclr = "ERROR" if i>sc.availableservers else ""
			tooltip += "<td class='right %s' style='padding-right:10px;'>%s<span class='em8'>%s</span> copy</td>" % (srvavailclr, str(i), ordinal_suffix(i))
			tooltip += "<td>%s</td>" % htmlentities(label2str(labelexpr))
			srvmatchclr = "ERROR" if matchingservers==0 else "WARNING" if matchingservers==1 else srvavailclr
			tooltip += "<td class='right' style='padding-right:20px;'><span class='%s'>%s</span> <span class='%s'>of %s</span></td>" % (srvmatchclr, str(matchingservers), srvavailclr, str(sc.availableservers))
			tooltip += "</tr>"
			i+=1
			odd = not odd
	tooltip += "<tr><td colspan='3'>"
	if st.isEC() and st.ec_data_parts+st.ec_chksum_parts>sc.availableservers:
		tooltip += "<hr><b>Warning</b>: There are more data and parity parts (%s+%s=%s) than available servers (%s)." % (st.ec_data_parts, st.ec_chksum_parts, st.ec_data_parts+st.ec_chksum_parts, sc.availableservers)
		not_enough_servers = False # do not show this warning any more
	if not st.isEC() and len(st.labellist)>sc.availableservers:
		tooltip += "<hr><b>Warning:</b> There are more expected copies (labels expressions) than available servers."
	if not_enough_servers:
		tooltip += "<hr><b>Warning:</b> There are not enough servers to fulfill the labels expression for this state."
	elif only_one_server:
		tooltip += "<hr><b>Warning:</b> There is only single server that matches some labels expressions for this state."
	if sc.isKeepOnly():
		tooltip += "<hr>This storage class has only definition for the KEEP state. All other states (CREATE, ARCHIVE and TRASH) inherit the labels expression from the KEEP state."
	elif not st.defined:
		tooltip += "<hr>This state (%s) is not explicitly defined in the storage class, it inherits the labels expression from the KEEP state." % st.fullname
	tooltip += "</td></tr>"
	tooltip += "</table>"

	i = 1
	foldedlist = labellist_fold(st.labellist)
	for _,matchingservers,label in foldedlist:
		
		if st.defined or sc.isKeepOnly():
			cls_expr = ""
			if int(matchingservers)==0:
				clz = "ERROR"
			elif int(matchingservers)==1:
				clz = "WARNING"
			else:
				clz = "GRAY"
		else:
			clz = "GRAY"	
			cls_expr = "GRAY"
		labelstr = label2str(label, literals=['*', '', '|', '~'])
		if labelstr=="*":
			labelstr = "Any server"
		else:
			labelstr = """<span class="monospace">%s</span>""" % htmlentities(labelstr)
		if (len(foldedlist)>1):
			if st.isEC():
				txt = "data:" if i==1 else "parity:"
			else:
				txt = "%s%s:" % (i,ordinal_suffix(i))
		else:
			txt = "all:"
		labelstr = """<span class="bold em9 %s">%s&#8239;</span><span class="%s">%s</span>""" % (clz, txt, cls_expr, labelstr)
		labelsarr.append(labelstr)
		i+=1
	
	txt = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;".join(labelsarr)
	return (tooltip, txt)