from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld, readonly, selectable):
	CSorder = fields.getint("CSorder", 0)
	CSrev = fields.getint("CSrev", 0)
	CScsid = fields.getstr("CScsid", "")
	CSlabel = fields.getstr("CSlabel", "ANY").upper()

	servers,dservers = dp.get_chunkservers_by_state(CSorder,CSrev)

	usedsum = 0
	totalsum = 0
	for cs in servers:
		if cs.total>0:
			usedsum+=cs.used
			totalsum+=cs.total
	if totalsum>0:
		avgpercent = (usedsum*100.0)/totalsum
	else:
		avgpercent = 0

	# sometimes cgi shows only a single server
	if (CScsid!="None" and CScsid!=""): 
		flt = lambda cs: ("%s:%s" % (cs.strip,cs.port)==CScsid)
		servers = list(filter(flt, servers))
		dservers = list(filter(flt, dservers))

	out = []
	if len(servers)>0:
		labels_dict = {}
		for cs in servers:
			for label in cs.labelstr.split(","):
				if label!="-":
					if label in labels_dict:
						labels_dict[label] += 1
					else:
						labels_dict[label] = 1
		if selectable and len(servers)>1 and len(labels_dict)>0:
			out.append("""<form action="#"><div class="tab_title">Chunkservers, label: """ )
			out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&CSlabel='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"CSlabel":""}))
			entrystr = []
			entrydesc = {}
			entrystr.append("ANY")
			srv_count = len(servers)
			entrydesc["ANY"] =  "Any (%u server%s)" % (srv_count, "s" if srv_count>1 else "")
			labels_dict = {key: labels_dict[key] for key in sorted(labels_dict)}
			select_any = True
			for label in labels_dict:
				srv_count = labels_dict[label]
				label = label.upper()		
				entrystr.append(label)
				entrydesc[label] = "%s (%u server%s)" % (label, srv_count, "s" if srv_count>1 else "")
				if label==CSlabel:
					select_any = False
			if select_any:
				CSlabel = "ANY" # wrong label given in url, force selecting "ANY" label
			for estr in entrystr:
				if estr==CSlabel:
					out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
				else:
					out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
			out.append("""</select><span class="arrow"></span></div>""")
			out.append("""</div></form>""")
		else:
			if (len(servers)==1):
				out.append("""<div class="tab_title">Chunkserver details</div>""")
			else:
				out.append("""<div class="tab_title">Chunkservers</div>""")
		out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfscs">""")
		out.append("""	<tr>""")
		out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
		out.append("""		<th rowspan="2">Id</th>""")
		out.append("""		<th rowspan="2">Host</th>""")
		out.append("""		<th rowspan="2">IP</th>""")
		out.append("""		<th rowspan="2">Port</th>""")
		out.append("""		<th rowspan="2">Labels</th>""")
		out.append("""		<th rowspan="2">Version</th>""")
		out.append("""		<th rowspan="2">Queue</th>""")
		out.append("""		<th rowspan="2">Queue state</th>""")
		out.append("""		<th rowspan="2">Maintenance</th>""")
		out.append("""		<th colspan="4">'Regular' hdd space</th>""")
		if dp.master().version_at_least(3,0,38):
			out.append("""		<th colspan="5">'Marked for removal' hdd space</th>""")
		else:
			out.append("""		<th colspan="4">'Marked for removal' hdd space</th>""")
		out.append("""	</tr>""")
		out.append("""	<tr>""")
		out.append("""		<th>chunks</th>""")
		out.append("""		<th>used</th>""")
		out.append("""		<th>total</th>""")
		out.append("""		<th class="progbar">% used</th>""")
		if dp.master().version_at_least(3,0,38):
			out.append("""		<th>status</th>""")
		out.append("""		<th>chunks</th>""")
		out.append("""		<th>used</th>""")
		out.append("""		<th>total</th>""")
		out.append("""		<th class="progbar">% used</th>""")
		out.append("""	</tr>""")

	if (CSlabel!="ANY" and CSlabel!=""): 
		flt = lambda cs: (CSlabel in cs.labelstr)
		servers = list(filter(flt, servers))
		dservers = list(filter(flt, dservers))

	# iterate all connected servers
	for cs in servers:
		if cs.strip!=cs.stroip:
			cs.strip = "%s&#8239;&rightarrow;&#8239;%s" % (cs.stroip,cs.strip)
		if (cs.is_maintenance_off()):
			mmchecked = ""
			mmstr = "OFF"
			mmurl = fields.createhtmllink({"CSmaintenanceon":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = ''
		elif (cs.is_maintenance_on()):
			mmchecked = "checked"
			mmstr = "ON"
			mmurl = fields.createhtmllink({"CSmaintenanceoff":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = html_icon('icon-wrench','cs_maintain')
		else:
			mmchecked = "checked"
			mmstr = "TMP"
			mmurl = fields.createhtmllink({"CSmaintenanceoff":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = html_icon('icon-wrench','cs_maintain_tmp')
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="center">%u</td>""" % cs.csid)
		# hostname
		out.append("""		<td align="left"><span class="text-icon">%s%s</span></td>""" % (cs.host, mmicon))
		out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (cs.sortip,cs.strip))
		out.append("""		<td align="center">%u</td>""" % cs.port)
		# labels
		out.append("""		<td align="left" class="monospace">%s</td>""" % cs.labelstr)
		issues = vld.check_cs_version(cs)
		out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (cs.sortver,'',issues.span(cs.strver.replace("PRO","<small>PRO</small>"))))
		# queue 
		out.append("""		<td align="right">%u</td>""" % (cs.queue))
		if cs.queue_state==CS_LOAD_FAST_REBALANCE:
			out.append("""		<td align="center"><a style="cursor:default" title="%s" href="%s">%s</a></td>""" % (cs.queue_state_info,fields.createhtmllink({"CSbacktowork":("%s:%u" % (cs.strip,cs.port))}),cs.queue_state_str))
		else:
			issues = vld.check_cs_queue_state(cs)
			info = cs.queue_state_info if cs.queue_state!=CS_LOAD_NORMAL else ""
			out.append("""		<td align="center"><span data-tt-info="%s">%s</span></td>""" % (info, issues.span(cs.queue_state_str)))
		# maintenance
		if dp.cluster.leaderfound():
			if readonly:
				out.append("""		<td class="center">%s</td>""" % mmstr)
			else:
				out.append("""		<td class="center">""")
				out.append("""			<span><label class="switch width34" for="maintenance-checkbox-%s"><input type="checkbox" id="maintenance-checkbox-%s" onchange="window.location.href='%s'" %s/><span class="slider round checked_blue"><span class="slider-text">%s</span></span></label></span>""" % (cs.hostkey, cs.hostkey, mmurl, mmchecked,mmstr))
				out.append("""		</td>""")
		else:
			out.append("""		<td class="center">n/a</td>""")
		# chunks
		out.append("""		<td align="right">%s</td>""" % (decimal_number_html(cs.chunks)))
		issues = vld.check_cs_hdds(cs)
		out.append("""		<td align="right"><span class="sortkey">%u </span><span data-tt-info="%s B">%s</span></td>""" % (cs.used, decimal_number(cs.used), issues.span(humanize_number(cs.used, "&nbsp;"))))
		out.append("""		<td align="right"><span class="sortkey">%u </span><span data-tt-info="%s B">%s</span></td>""" % (cs.total,decimal_number(cs.total),humanize_number(cs.total,"&nbsp;")))
		if cs.total>0:
			usedpercent = (cs.used*100.0)/cs.total
			if avgpercent-usedpercent>=0.1:
				diffstr = "%.1f pp. lower than all chunkservers' average (%.1f%%)" % (avgpercent-usedpercent, avgpercent)
			elif usedpercent-avgpercent>=0.1:
				diffstr = "%.1f pp. higher than all chunkservers' average (%.1f%%)" % (usedpercent-avgpercent, avgpercent)
			else:
				diffstr = "equal to chunkservers' average"
			out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGAVG" style="width:%.2f%%"></div><div class="PROGVALUE"><span data-tt-info="%s">%.1f</span></div></div></td>""" % (usedpercent,100.0-usedpercent,avgpercent,diffstr,usedpercent))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span></span></div></div></td>""")
		if dp.master().version_at_least(3,0,38):

			if cs.tdchunks==0 or dp.cluster.leaderfound()==0:
				out.append("""		<td align="center">-</td>""")
			else:
				issues = vld.check_cs_mfr_status(cs)
				clz = "OK" if issues.len()==0 else "MFRREADY" if cs.mfrstatus==MFRSTATUS_READY else ""
				out.append("""		<td>%s</td>""" % issues.span(cs.get_mfr_status_str(),clz))
		out.append("""		<td align="right">%s</td><td align="right"><span class="sortkey">%u </span><span data-tt-info="%s B">%s</span></td><td align="right"><span class="sortkey">%u </span><span data-tt-info="%s B">%s</span></td>""" % (decimal_number_html(cs.tdchunks),cs.tdused,decimal_number(cs.tdused),humanize_number(cs.tdused,"&nbsp;"),cs.tdtotal,decimal_number(cs.tdtotal),humanize_number(cs.tdtotal,"&nbsp;")))
		if (cs.tdtotal>0):
			usedpercent = (cs.tdused*100.0)/cs.tdtotal
			out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGVALUE"><span>%.1f</span></div></div></td>""" % (usedpercent,100.0-usedpercent,usedpercent))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span></span></div></div></td>""")
		out.append("""	</tr>""")
	out.append("""</table>""")

	if len(dservers)>0:
		if (len(dservers)==1):
			out.append("""<div class="tab_title">Disconnected chunkserver details</div>""")
		else:
			out.append("""<div class="tab_title">Disconnected chunkservers</div>""")
		out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsdiscs">""")
		out.append("""	<tr>""")
		out.append("""		<th class="acid_tab_enumerate">#</th>""")
		out.append("""		<th>Host</th>""")
		out.append("""		<th>IP</th>""")
		out.append("""		<th>Port</th>""")
		out.append("""		<th>Id</th>""")
		out.append("""		<th>Maintenance</th>""")
		if (not readonly):
			if dp.cluster.leaderfound() and dp.cluster.deputyfound()==0:
				out.append("""		<th class="acid_tab_skip">Remove</th>""")
			else:
				out.append("""		<th class="acid_tab_skip">Temporarily remove</th>""")
		out.append("""	</tr>""")

	# iterate all disconnected servers
	for cs in dservers:
		if cs.strip!=cs.stroip:
			cs.strip = "%s &rightarrow; %s" % (cs.stroip,cs.strip)
		out.append("""	<tr>""")
		if dp.cluster.leaderfound()==0:
			mmicon = ''
			cl = "DISCONNECTED"
		elif cs.is_maintenance_off():
			mmchecked = ""
			mmstr = "OFF"
			mmurl = fields.createhtmllink({"CSmaintenanceon":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = html_icon('icon-error','cs_unreachable')
			cl = "DISCONNECTED"
		elif cs.is_maintenance_on():
			mmchecked = "checked"
			mmstr = "ON"
			mmurl = fields.createhtmllink({"CSmaintenanceoff":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = html_icon('icon-warning','cs_unreachable_maintain')+html_icon('icon-wrench','cs_unreachable_maintain')
			cl = "MAINTAINED"
		else:
			if cs.maintenanceto==0xFFFFFFFF:
				mmstr = "ON" #should not be here
			else:
				mmstr = """%u:%02u""" % (cs.maintenanceto // 60, cs.maintenanceto % 60)
			mmchecked = "checked"
			mmurl = fields.createhtmllink({"CSmaintenanceoff":("%s:%u" % (cs.stroip,cs.port))})
			mmicon = html_icon('icon-warning','cs_maintain_tmp')+html_icon('icon-wrench','cs_maintain_tmp')+html_icon('icon-stopwatch','cs_maintain_tmp', 1, 'icon-blue')
			cl = "TMPMAINTAINED"
		out.append("""		<td align="right"></td><td align="left"><span class="%s text-icon">%s%s</span></td>""" % (cl,cs.host,mmicon))
		out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (cs.sortip,cl,cs.strip))
		out.append("""		<td align="center"><span class="%s">%u</span></td>""" % (cl,cs.port))
		out.append("""		<td align="right"><span class="%s">%u</span></td>""" % (cl,cs.csid))
		if dp.cluster.leaderfound(): #maintenance
			if readonly:
				out.append("""		<td class="center">%s</td>""" % mmstr)
			else:
				out.append("""		<td class="center">""")
				out.append("""			<span><label class="switch width38" for="maintenance-checkbox-%s"><input type="checkbox" id="maintenance-checkbox-%s" onchange="window.location.href='%s'" %s/><span class="slider round checked_blue"><span class="slider-text countdown">%s</span></span></label></span>""" % (cs.hostkey, cs.hostkey, mmurl, mmchecked, mmstr))
				out.append("""		</td>""")
		else:
			out.append("""		<td align="center"><span class="%s">not available</td>""" % cl)
		if (not readonly): #remove cmd
			if dp.cluster.leaderfound() and dp.cluster.deputyfound()==0:
				out.append("""		<td align="center"><a class="VISIBLELINK" href="%s">click to remove</a></td>""" % (fields.createhtmllink({"CSremove":("%s:%u" % (cs.stroip,cs.port))})))
			elif dp.master().version_at_least(3,0,67):
				out.append("""		<td align="center"><a class="VISIBLELINK" href="%s">click to temporarily remove</a></td>""" % (fields.createhtmllink({"CStmpremove":("%s:%u" % (cs.stroip,cs.port))})))
			else:
				out.append("""		<td align="center">not available</td>""")

		out.append("""	</tr>""")
	out.append("""</table>""")
	
	return out
