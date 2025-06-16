from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	CCdata = fields.getstr("CCdata", "")

	out = []

	hostlist = []
	for cs in dp.get_chunkservers(): # default sort by ip/port
		if (cs.flags&1)==0:
			hostlist.append((cs.ip,cs.port,cs.version))
	icharts = (
		(100,'cpu','cpu usage (percent)','<b>cpu usage</b>, sys: BOXGR2A user: BOXGR2B','sys: BOXGR2A user: BOXGR2B'),
		(107,'memory','memory usage (if available) rss + virt','<b>memory usage</b> (if available), rss: BOXGR2A + virt: BOXGR2B','rss: BOXGR2A + virt: BOXGR2B'),
		(109,'space','raw disk space usage (used/total)','<b>raw disk space usage</b>, used: BOXGR2A total: BOXGR2B','used: BOXGR2A total: BOXGR2B'),
		(110,'chunks','number of stored chunks (ec8/ec4/copy)','<b>number of stored chunks</b>, ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C','ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C'),
		(111,'hddcnt','number of folders (hard drives) with data','<b>number of folders (hard drives) with data</b>, damaged: BOXGR3A marked for removal: BOXGR3B working: BOXGR3C','damaged: BOXGR3A marked for removal: BOXGR3B working: BOXGR3C'),
		(47,'udiff','usage difference between the most and least used disk','',''),
		(28,'load','max number operations waiting in queue','',''),
		(101,'datain','traffic from clients and other chunkservers (bits/s - main server + replicator)','<b>traffic from clients and other chunkservers</b> (bits/s), main server: BOXGR2A + replicator: BOXGR2B','main server: BOXGR2A + replicator: BOXGR2B'),
		(102,'dataout','traffic to clients and other chunkservers (bits/s - main server + replicator)','<b>traffic to clients and other chunkservers</b> (bits/s), main server: BOXGR2A + replicator: BOXGR2B','main server: BOXGR2A + replicator: BOXGR2B'),
		(103,'bytesr','bytes read - data/other (bytes/s)','<b>bytes read</b> (bytes/s), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
		(104,'bytesw','bytes written - data/other (bytes/s)','<b>bytes written</b> (bytes/s), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
		(2,'masterin','traffic from master (bits/s)','',''),
		(3,'masterout','traffic to master (bits/s)','',''),
		(105,'hddopr','number of low-level read operations (per minute)','<b>number of low-level read operations</b> (per minute), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
		(106,'hddopw','number of low-level write operations (per minute)','<b>number of low-level write operations</b> (per minute), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
		(16,'hlopr','number of high-level read operations (per minute)','',''),
		(17,'hlopw','number of high-level write operations (per minute)','',''),
		(18,'rtime','time of data read operations','',''),
		(19,'wtime','time of data write operations','',''),
		(20,'repl','number of chunk replications (per minute)','',''),
		(21,'create','number of chunk creations (per minute)','',''),
		(22,'delete','number of chunk deletions (per minute)','',''),
		(33,'change','number of chunk internal operations duplicate, truncate, etc. (per minute)','',''),
		(108,'move','number of chunk internal rebalances per minute (high speed + low speed)','<b>number of chunk internal rebalances</b> (per minute), high speed: BOXGR2A + low speed: BOXGR2B','high speed: BOXGR2A + low speed: BOXGR2B')
	)

	charts = []
	for id,oname,desc,fdesc,sdesc in icharts:
		if fdesc=='':
			fdesc = '<b>' + desc.replace(' (', '</b> (')
			if not '</b>' in fdesc:
				fdesc = fdesc + '</b>'
		fdeschtml = fdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
		sdeschtml = sdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
		charts.append((id,oname,desc,fdeschtml,sdeschtml))
	servers = []
	entrystr = []
	entrydesc = {}
	if len(hostlist)>0:
		for id,oname,desc,fdesc,sdesc in charts:
			name = oname.replace(":","")
			entrystr.append(name)
			entrydesc[name] = desc
		for ip,port,version in hostlist:
			if version>=(2,0,15):
				chmode = 10 if version>=(4,31,0) else 12
				strip = "%u.%u.%u.%u" % ip
				name = "%s:%u" % (strip,port)
				namearg = "%s:%u" % (name,chmode)
				hostx = resolve(strip,0,UNRESOLVED)
				if hostx==UNRESOLVED:
					host = ""
				else:
					host = " / "+hostx
				entrystr.append(namearg)
				entrydesc[namearg] = "Server: %s%s" % (name,host)
				servers.append((strip,port,"cs_"+name.replace(".","_").replace(":","_"),"Server: <b>%s%s</b>" % (name,host),chmode))

	cchtmp = CCdata.split(":")
	if len(cchtmp)==2:
		cchtmp = (cchtmp[0],cchtmp[1],0)
	if len(cchtmp)==3:
		cshost = cchtmp[0]
		csport = cchtmp[1]
		csmode = int(cchtmp[2])
		if csmode<10:
			csmode = 1
		else:
			csmode -= 10

		out.append("""<div class="tab_title">Chunkservers charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th class="knob-cell chart-range">""")
		out.append(html_knob_selector_chart_range('cs_main'))
		out.append("""		</th>""")
		out.append("""		<th class="chart-select">""")
		out.append("""			<div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="cs_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
		if CCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==CCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		out.append("""		</th>""")
		out.append("""	</tr>""")
		if CCdata in entrystr:
			for id,name,desc,fdesc,sdesc in charts:
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="cs_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="cs_%s_c" class="CHARTJSC">""" % name)
				out.append("""					<span class="chartcaptionjs">%s</span>""" % fdesc)
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="cs_%s_l" class="CHARTJSL"></div>""" % name)
				out.append("""			<div id="cs_%s_r" class="CHARTJSR"></div>""" % name)
				out.append("""		</td>""")
				out.append("""	</tr>""")
		out.append("""</table></form>""")

		if CCdata in entrystr:
			out.append("""<div class="tab_title">Chunkserver charts (comparison)</div>""")
			out.append("""<form action="#"><table class="FR">""")
			for i in range(2):
				out.append("""	<tr>""")
				if i==0:
					out.append("""		<th class="knob-cell chart-cmp-range">""")
					out.append(html_knob_selector_chart_range('cs_cmp')) 
				else:
					out.append("""		<th style="text-align: right;vertical-align:middle;border:none;">""")
					out.append("""versus:""")
				out.append("""		</th>""")
				out.append("""		<th class="chart-cmp-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" id="cschart%u_select" name="cschart%u" size="1" onchange="cs_change_type(%u,this.options[this.selectedIndex].value)">""" % (i,i,i))
				no = 0
				for id,name,desc,fdesc,sdesc in charts:
					out.append("""				<option value="%u">%s</option>""" % (no,desc))
					no += 1
				out.append("""			</select><span class="arrow"></span></div>""")
				out.append("""		</th>""")
				out.append("""	</tr>""")
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="cs_%u_p" colspan="2" style="height:124px; border-top: none;" valign="middle">""" % i)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="cs_%u_c" class="CHARTJSC">""" % i)
				out.append("""					<span id="cs_%u_d" class="chartcaptionjs">%s</span>""" % (i,charts[0][3]))
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="cs_%u_l" class="CHARTJSL"></div>""" % i)
				out.append("""			<div id="cs_%u_r" class="CHARTJSR"></div>""" % i)
				out.append("""		</td>""")
				out.append("""	</tr>""")
			out.append("""</table></form>""")

			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var cs_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts ])))
			out.append("""	var cs_inames = [%s];""" % ",".join(map(repr,[ x[1] for x in charts ])))
			out.append("""	var cs_idesc = [%s];""" % ",".join(map(repr,[ x[3] for x in charts ])))
			out.append("""	var cs_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
			out.append("""	var cs_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
			out.append("""	var cs_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
			out.append("""	var cs_host = "%s";""" % cshost)
			out.append("""	var cs_port = "%s";""" % csport)
			out.append("""	var cs_mode = %u;""" % csmode)
			out.append("""	var cs_valid = 1;""")
			out.append("""	var cs_base_href = "%s";""" % fields.createrawlink({"CCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		else:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var cs_valid = 0;""")
			out.append("""	var cs_base_href = "%s";""" % fields.createrawlink({"CCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
function cs_create_charts(show_loading=true) {
var i;
cs_charttab = [];
if (cs_valid) {
for (i=0 ; i<cs_inames.length ; i++) {
	cs_charttab.push(new AcidChartWrapper("cs_"+cs_inames[i]+"_","cs_main",cs_host,cs_port,cs_mode,cs_vids[i],show_loading));
}
if (cs_chartcmp[0]) document.getElementById('cschart0_select').value = cs_vids.indexOf(cs_chartcmp[0].id);
if (cs_chartcmp[1]) document.getElementById('cschart1_select').value = cs_vids.indexOf(cs_chartcmp[1].id);
cs_chartcmp[0] = new AcidChartWrapper("cs_0_","cs_cmp",cs_host,cs_port,cs_mode,(cs_chartcmp[0]) ? cs_chartcmp[0].id : cs_vids[0],show_loading);
cs_chartcmp[1] = new AcidChartWrapper("cs_1_","cs_cmp",cs_host,cs_port,cs_mode,(cs_chartcmp[1]) ? cs_chartcmp[1].id : cs_vids[0],show_loading);
}
}

// main charts
function cs_change_data(indx, ccdata) {
var sindx,i;
if (cs_valid) {
sindx = indx - cs_inames.length;
if (sindx >= 0 && sindx < cs_modes.length) {
	for (i=0 ; i<cs_vids.length ; i++) {
		cs_charttab[i].set_host_port_id(cs_hosts[sindx],cs_ports[sindx],cs_modes[sindx],cs_vids[i]);
	}
}
}
document.location.replace(cs_base_href + "&CCdata=" + ccdata);		   
}
// compare charts
function cs_change_type(chartid,indx) {
var descel;
if (cs_valid) {
cs_chartcmp[chartid].set_id(cs_vids[indx]);
descel = document.getElementById("cs_"+chartid+"_d");
descel.innerHTML = cs_idesc[indx];
}
}
cs_charttab = [];
cs_chartcmp = [];
cs_create_charts();
//--><!]]>
</script>""")
	elif len(cchtmp)==1 and len(CCdata)>0:
		chid = 0
		sdescadd = ''
		for id,name,desc,fdesc,sdesc in charts:
			if name==CCdata:
				chid = id
				if sdesc!='':
					sdescadd = ', '+sdesc
		if chid==0:
			try:
				chid = int(CCdata)
			except Exception:
				pass
		if chid<=0 or chid>=1000:
			CCdata = ""

		out.append("""<div class="tab_title">Chunkservers charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th class="knob-cell chart-range">""")
		out.append(html_knob_selector_chart_range('cs_main'))
		out.append("""		</th>""")
		out.append("""		<th class="chart-select">""")
		out.append("""			<div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="cs_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
		if CCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==CCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		if CCdata in entrystr:
			out.append("""		<label class="switch" for="cs_commonscale" id="cs_commonscale-slide" style="margin-left:10px;"><input type="checkbox" id="cs_commonscale" onchange="AcidChartSetCommonScale('cs_main',this.checked)"/><span class="slider round"></span><span class="text">Use common Y-scale</span></label>""")				
		out.append("""		</th>""")
		out.append("""	</tr>""")
		if CCdata in entrystr:
			for cshost,csport,name,desc,chmode in servers:
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="cs_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="cs_%s_c" class="CHARTJSC">""" % name)
				out.append("""					<span id="cs_%s_d" class="chartcaptionjs">%s%s</span>""" % (name,desc,sdescadd))
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="cs_%s_l" class="CHARTJSL"></div>""" % name)
				out.append("""			<div id="cs_%s_r" class="CHARTJSR"></div>""" % name)
				out.append("""		</td>""")
				out.append("""	</tr>""")
		out.append("""</table></form>""")

		if CCdata in entrystr:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var cs_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts ])))
			out.append("""	var cs_idesc = [%s];""" % ",".join(map(repr,[ x[4] for x in charts ])))
			out.append("""	var cs_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
			out.append("""	var cs_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
			out.append("""	var cs_inames = [%s];""" % ",".join(map(repr,[ x[2] for x in servers ])))
			out.append("""	var cs_sdesc = [%s];""" % ",".join(map(repr,[ x[3] for x in servers ])))
			out.append("""	var cs_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
			out.append("""	var cs_chid = %u;""" % chid)
			out.append("""	var cs_valid = 1;""")
			out.append("""	var cs_base_href = "%s";""" % fields.createrawlink({"CCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		else:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var cs_valid = 0;""")
			out.append("""	var cs_base_href = "%s";""" % fields.createrawlink({"CCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")

		out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
function cs_create_charts(show_loading=true) {
var i;
cs_charttab = [];
if (cs_valid) {
for (i=0 ; i<cs_inames.length ; i++) {
	cs_charttab.push(new AcidChartWrapper("cs_"+cs_inames[i]+"_","cs_main",cs_hosts[i],cs_ports[i],cs_modes[i],cs_chid,show_loading));
}
}
}
// main charts
function cs_change_data(indx, ccdata) {
var i,chartid;
var descel;
if (cs_valid && indx >= 0 && indx < cs_vids.length) {
for (i=0 ; i<cs_inames.length ; i++) {
	chartid = cs_inames[i];
	cs_charttab[i].set_id(cs_vids[indx]);
	descel = document.getElementById("cs_"+chartid+"_d");
	if (cs_idesc[indx]!="") {
		descel.innerHTML = cs_sdesc[i] + " " + cs_idesc[indx];
	} else {
		descel.innerHTML = cs_sdesc[i];
	}
}
}
document.location.replace(cs_base_href + "&CCdata=" + ccdata);
}
cs_charttab = [];
cs_create_charts();
//--><!]]>
</script>""")
	else:
		out.append("""<div class="tab_title">Chunkservers charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th>""")
		out.append("""			Select: <div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="document.location.href='%s&CCdata='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"CCdata":""}))
		if CCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==CCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		out.append("""		</th>""")
		out.append("""	</tr>""")
		out.append("""</table></form>""")

	return out