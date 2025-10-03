import time
from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld, readonly, selectable):
	HDperiod = fields.getint("HDperiod", 0)
	HDtime = fields.getint("HDtime", 0)
	HDorder = fields.getint("HDorder", 0)
	HDrev = fields.getint("HDrev", 0)
	HDdata = fields.getstr("HDdata", "")

	(hdds, scanhdds) = dp.get_hdds(HDdata, HDperiod, HDtime, HDorder, HDrev)
	out = []
	class_no_table = ""
	if len(hdds)==0 and len(scanhdds)==0:
		class_no_table = "no_table"
	if selectable:
		out.append("""<form action="#"><div class="tab_title %s">Disks, select: """ % class_no_table )
		out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&HDdata='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"HDdata":""}))
		entrystr = []
		entrydesc = {}
		entrystr.append("ALL")
		entrydesc["ALL"] = "All disks"
		entrystr.append("ERR")
		entrydesc["ERR"] = "Disks with errors only"
		entrystr.append("NOK")
		entrydesc["NOK"] = "Disks with status other than ok"
		servers=dp.get_chunkservers()
		for cs in servers:
			hostx = resolve(cs.strip,0,UNRESOLVED)
			if hostx==UNRESOLVED:
				host = ""
			else:
				host = " / "+hostx
			entrystr.append(cs.hostkey)
			entrydesc[cs.hostkey] = "Server: %s%s" % (cs.hostkey,host)
		if HDdata not in entrystr:
			out.append("""<option value="" selected="selected">Pick option...</option>""")
		for estr in entrystr:
			if estr==HDdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""</select><span class="arrow"></span></div>""")
		out.append("""</div></form>""")
	else:
		if len(hdds)>0 or len(scanhdds)>0:
			out.append("""<div class="tab_title %s">Disks</div>""")

	if len(hdds)==0 and len(scanhdds)==0:
		return out
	
	# prepare headers
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfshdd" id="mfshdd">""")
	out.append("""	<tr>""")
	out.append("""		<th colspan="5" rowspan="2" class="knob-cell">""")

	options=[(-50, "IP address", None, "acid_tab.switchdisplay('mfshdd','hddaddrname_vis',0);"),
			(-130,"Server name", None,"acid_tab.switchdisplay('mfshdd','hddaddrname_vis',1);")]
	out.append(html_knob_selector("hddaddrname_vis",12,(130,41),(110,20),options))

	options=[(-50,"Last minute", None, "acid_tab.switchdisplay('mfshdd','hddperiod_vis',0);"),
			(50,"Last hour", None, "javascript:acid_tab.switchdisplay('mfshdd','hddperiod_vis',1);"),
			(130,"Last day", None, "javascript:acid_tab.switchdisplay('mfshdd','hddperiod_vis',2);")]
	out.append(html_knob_selector("hddperiod_vis",12,(210,41),(110,20),options))

	options=[(50, "Max time", None,"javascript:acid_tab.switchdisplay('mfshdd','hddtime_vis',0);"),
			(130,"Avg time", None,"javascript:acid_tab.switchdisplay('mfshdd','hddtime_vis',1)")]
	out.append(html_knob_selector("hddtime_vis",12,(110,41),(20,20),options))

	out.append("""		</th>""")
	out.append("""		<th colspan="8">""")
	out.append("""			<span class="hddperiod_vis0">I/O stats - last minute</span>""")
	out.append("""			<span class="hddperiod_vis1">I/O stats - last hour</span>""")
	out.append("""			<span class="hddperiod_vis2">I/O stats - last day</span>""")
	out.append("""		</th>""")
	out.append("""		<th colspan="3" rowspan="2">Capacity</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th colspan="2"><span data-tt-help="Average data transfer speed">transfer</span></th>""")
	out.append("""		<th colspan="3">""")
	out.append("""			<span class="hddtime_vis0" data-tt-help="Maximum time of read or write one chunk block (up to 64kB)">max operation time</span>""")
	out.append("""			<span class="hddtime_vis1" data-tt-help="Average time of read or write one chunk block (up to 64kB)">average operation time</span>""")
	out.append("""		</th>""")
	out.append("""		<th colspan="3"><span data-tt-help="Number of chunk block operations: reads, writes and fsyncs respectively">number of ops</span></th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th class="acid_tab_enumerate">#</th>""")
	out.append("""		<th class="acid_tab_level_1"><span class="hddaddrname_vis0">IP</span><span class="hddaddrname_vis1">server name</span> and path</th>""")
	out.append("""		<th>chunks</th>""")
	out.append("""		<th>last error</th>""")
	out.append("""		<th>status</th>""")
	out.append("""		<th class="acid_tab_level_1">read</th>""")
	out.append("""		<th class="acid_tab_level_1">write</th>""")
	out.append("""		<th class="acid_tab_level_2">read</th>""")
	out.append("""		<th class="acid_tab_level_2">write</th>""")
	out.append("""		<th class="acid_tab_level_2">fsync</th>""")
	out.append("""		<th class="acid_tab_level_1">read</th>""")
	out.append("""		<th class="acid_tab_level_1">write</th>""")
	out.append("""		<th class="acid_tab_level_1">fsync</th>""")
	out.append("""		<th>used</th>""")
	out.append("""		<th>total</th>""")
	out.append("""		<th class="progbar">% used</th>""")
	out.append("""	</tr>""")

	# calculate some stats
	usedsum = {}
	totalsum = {}
	hostavg = {}
	for _,hdd in hdds+scanhdds:
		if hdd.hostkey not in usedsum:
			usedsum[hdd.hostkey]=0
			totalsum[hdd.hostkey]=0
			hostavg[hdd.hostkey]=0
		if hdd.flags&CS_HDD_SCANNING==0 and hdd.total and hdd.total>0:
			usedsum[hdd.hostkey]+=hdd.used
			totalsum[hdd.hostkey]+=hdd.total
			if totalsum[hdd.hostkey]>0:
				hostavg[hdd.hostkey] = (usedsum[hdd.hostkey] * 100.0) / totalsum[hdd.hostkey]

	for sf,hdd in hdds+scanhdds:
		if not hdd.is_valid():
			lerror= '-'
		elif not hdd.has_errors():
			lerror = 'no errors'
		else:
			errtimetuple = time.localtime(hdd.errtime)
			errtimelong = time.strftime("%Y-%m-%d %H:%M:%S",errtimetuple)
			errtimeshort = time.strftime("%Y-%m-%d %H:%M",errtimetuple)
			if readonly:
				lerror = '<span data-tt-warning="Error at %s <br/>on chunk #%016X">%s</span>' % (errtimelong,hdd.errchunkid,errtimeshort)
			else:
				if hdd.flags&CS_HDD_DAMAGED:
					lerror = """<span data-tt-warning="Error occurred at %s<br/>on chunk #%016X<br/>You can't dismiss it because this disk<br/>is marked as damaged.">%s</span>""" % (errtimelong,hdd.errchunkid,errtimeshort)
				else:
					found_cs = list(filter(lambda cs: cs.hostkey == hdd.hostkey, dp.get_chunkservers(None)))
					cs = found_cs[0] if len(found_cs)==1 else None
					if cs and cs.version>=(4,33,0):
						lerror = """<a href="%s" data-tt-warning="Click, to dismiss error that occurred<br/>at %s <br/>on chunk #%016X">%s</a>""" % (fields.createhtmllink({"CSclearerrors":hdd.clearerrorarg}),errtimelong,hdd.errchunkid,errtimeshort)
					else:
						lerror = """<span data-tt-warning="Error occurred at %s<br/>on chunk #%016X<br/>You can't dismiss it because this chunkserver<br/>is too old (min. v.4.33 is required).<br/>It is recommended to upgrade chunkserver.">%s</span>""" % (errtimelong,hdd.errchunkid,errtimeshort)

		if hdd.is_valid():
			chunkscnttxt=decimal_number_html(hdd.chunkscnt)
			usedtxt = humanize_number(hdd.used,"&nbsp;")
			totaltxt = humanize_number(hdd.total,"&nbsp;")
		else:
			chunkscnttxt = '-'
			usedtxt = '-'
			totaltxt = '-'
		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="left"><span class="hddaddrname_vis0"><span class="sortkey">%s</span>%s</span><span class="hddaddrname_vis1">%s</span></td>""" % (htmlentities(hdd.sortippath),htmlentities(hdd.ippath).replace(":/", ": /"),htmlentities(hdd.hostpath).replace(":/", ": /")))
		issues = vld.check_hdd_status(hdd)
		clz = "OK" if issues.len()==0 else "MFRREADY" if hdd.mfrstatus==MFRSTATUS_READY else ""
		out.append("""		<td align="right">%s</td><td><span class="sortkey">%u</span>%s</td><td>%s</td>""" % (chunkscnttxt,hdd.errtime,lerror,issues.span(hdd.get_status_str(),clz)))
		validdata = [1,1,1]
		for i in range(3):
			if hdd.rbw[i]==0 and hdd.wbw[i]==0 and hdd.usecreadmax[i]==0 and hdd.usecwritemax[i]==0 and hdd.usecfsyncmax[i]==0 and hdd.rops[i]==0 and hdd.wops[i]==0:
				validdata[i] = 0
		# rbw
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				<span class="sortkey">%u </span><span data-tt-info="%s B/s">%s/s</span>""" % (hdd.rbw[i],decimal_number(hdd.rbw[i]),humanize_number(hdd.rbw[i],"&nbsp;")))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# wbw
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				<span class="sortkey">%u </span><span data-tt-info="%s B/s">%s/s</span>""" % (hdd.wbw[i],decimal_number(hdd.wbw[i]),humanize_number(hdd.wbw[i],"&nbsp;")))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# readtime
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecreadmax[i]))
				out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecreadavg[i]))
			else:
				out.append("""				<span><span class="sortkey">-1 </span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# writetime
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecwritemax[i]))
				out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecwriteavg[i]))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# fsynctime
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecfsyncmax[i]))
				out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecfsyncavg[i]))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# rops
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				if hdd.rops[i]>0:
					bsize = hdd.rbytes[i]/hdd.rops[i]
				else:
					bsize = 0
				out.append("""				<span data-tt-info="average block size: %u B">%s</span>""" % (bsize,decimal_number_html(hdd.rops[i])))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# wops
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				if hdd.wops[i]>0:
					bsize = hdd.wbytes[i]/hdd.wops[i]
				else:
					bsize = 0
				out.append("""				<span data-tt-info="average block size: %u B">%s</span>""" % (bsize,decimal_number_html(hdd.wops[i])))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		# fsyncops
		out.append("""		<td align="right">""")
		for i in range(3):
			out.append("""			<span class="hddperiod_vis%u">""" % i)
			if validdata[i]:
				out.append("""				%s""" % decimal_number_html(hdd.fsyncops[i]))
			else:
				out.append("""				<span><span class="sortkey">-1</span>&nbsp;</span>""")
			out.append("""			</span>""")
		out.append("""		</td>""")
		if hdd.flags&CS_HDD_SCANNING:
			out.append("""		<td colspan="3" align="right"><span class="sortkey">0 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.0f%%;"></div><div class="PROGVALUE"><span>%.0f%% scanned</span></div></div></td>""" % (100.0-hdd.used,hdd.used))
		else:
			issues = vld.check_hdd_usage(hdd)
			out.append("""		<td align="right"><span class="sortkey">%u</span><span data-tt-info="%s B">%s</span></td><td align="right"><span class="sortkey">%u </span><span data-tt-info="%s B">%s</spana></td>""" % (hdd.used,decimal_number(hdd.used),issues.span(usedtxt),hdd.total,decimal_number(hdd.total),totaltxt))
			if hdd.total>0:
				usedpercent = (hdd.used*100.0)/hdd.total
				avgpercent = hostavg[hdd.hostkey]
				if totalsum[hdd.hostkey]!=hdd.total: 
					if avgpercent-usedpercent>=0.1:
						diffstr = "%.1f pp. lower than '%s' chunkserver's average (%.1f%%)" % (avgpercent-usedpercent, hdd.hoststr, avgpercent)
					elif usedpercent-avgpercent>=0.1:
						diffstr = "%.1f pp. higher than '%s' chunkserver's average (%.1f%%)" % (usedpercent-avgpercent, hdd.hoststr, avgpercent)
					else:
						diffstr = "equal to '%s' chunkserver's average" % hdd.hoststr
				else:
					diffstr = ''
				out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGAVG" style="width:%.2f%%"></div><div class="PROGVALUE"><span><span data-tt-info="%s">%.1f</span></span></div></div></td>""" % (usedpercent,100.0-usedpercent,avgpercent,diffstr,usedpercent))
			else: #skip for single disk chunkserver
				out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span>-</span></div></div></td>""")
		out.append("""	</tr>""")				
	out.append("""</table>""")

	return out