from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	out = []
	icharts = (
		(100,0,'cpu','cpu usage (percent)','<b>cpu usage</b>, sys: BOXGR2A user: BOXGR2B','sys: BOXGR2A user: BOXGR2B'),
		(101,0,'memory','memory usage (if available) rss + virt)','<b>memory usage</b> (if available), rss: BOXGR2A + virt: BOXGR2B','rss: BOXGR2A + virt: BOXGR2B'),
		(102,0,'space','raw disk space usage (used/total)','<b>raw disk space usage</b>, used: BOXGR2A total: BOXGR2B','used: BOXGR2A total: BOXGR2B'),
		(108,0,'objects','number of meta objects (others/files)','<b>number of meta objects</b>, others: BOXGR2A files: BOXGR2B','others: BOXGR2A files: BOXGR2B'),
		(109,0,'chunks','number of chunks (ec8/ec4/copy)','<b>number of chunks</b>, ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C','ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C'),
		(110,0,'regunder','number of regular chunks in danger','<b>number of regular chunks</b>, endangered: BOXGR2A undergoal: BOXGR2B','endangered: BOXGR2A undergoal: BOXGR2B'),
		(111,0,'allunder','number of all chunks in danger','<b>number of all chunks</b>, endangered: BOXGR2A undergoal: BOXGR2B ','endangered: BOXGR2A undergoal: BOXGR2B'),
		(112,0,'cservers','number of chunk servers','<b>number of chunk servers</b>, disconnected: BOXGR3A disconnected in maintenance: BOXGR3B working: BOXGR3C','disconnected: BOXGR3A disconnected in maintenance: BOXGR3B working: BOXGR3C'),
		(67,0,'udiff','space usage difference','<b>difference in space usage between the most and the least used chunk server</b>',''),
		(63,0,'delay','master max delay in seconds','<b>master max delay in seconds</b>',''),
		(103,0,'dels','chunk deletions per minute','<b>chunk deletions</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
		(104,0,'repl','chunk replications per minute','<b>chunk replications</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
		(105,0,'creat','chunk creations per minute','<b>chunk creations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
		(106,0,'change','chunk internal operations per minute','<b>chunk internal operations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
		(107,0,'split','chunk local split operations per minute','<b>chunk local split operations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
		(68,0,'mountbytrcvd','traffic from cluster, data only (bytes per second)','',''),
		(69,0,'mountbytsent','traffic to cluster, data only (bytes per second)','',''),
		(49,0,'bread','traffic from cluster, data+overhead (bytes per second)','',''),
		(50,0,'bwrite','traffic to cluster, data+overhead (bytes per second)','',''),
		(21,0,'prcvd','packets received (per second)','',''),
		(22,0,'psent','packets sent (per second)','',''),
		(23,0,'brcvd','bits received (per second)','',''),
		(24,0,'bsent','bits sent (per second)','',''),
		(4,1,'statfs','statfs operations (per minute)','',''),
		(5,1,'getattr','getattr operations (per minute)','',''),
		(6,1,'setattr','setattr operations (per minute)','',''),
		(7,1,'lookup','lookup operations (per minute)','',''),
		(8,1,'mkdir','mkdir operations (per minute)','',''),
		(9,1,'rmdir','rmdir operations (per minute)','',''),
		(10,1,'symlink','symlink operations (per minute)','',''),
		(11,1,'readlink','readlink operations (per minute)','',''),
		(12,1,'mknod','mknod operations (per minute)','',''),
		(13,1,'unlink','unlink operations (per minute)','',''),
		(14,1,'rename','rename operations (per minute)','',''),
		(15,1,'link','link operations (per minute)','',''),
		(16,1,'readdir','readdir operations (per minute)','',''),
		(17,1,'open','open operations (per minute)','',''),
		(51,1,'read','read operations (per minute)','',''),
		(52,1,'write','write operations (per minute)','',''),
		(53,1,'fsync','fsync operations (per minute)','',''),
		(56,1,'truncate','truncate operations (per minute)','',''),
		(61,1,'create','file create operations (per minute)','',''),
		(54,1,'lock','file lock operations (per minute)','',''),
		(55,1,'snapshot','snapshot operations (per minute)','',''),
		(57,1,'getxattr','getxattr operations (per minute)','',''),
		(58,1,'setxattr','setxattr operations (per minute)','',''),
		(59,1,'getfacl','getfacl operations (per minute)','',''),
		(60,1,'setfacl','setfacl operations (per minute)','',''),
		(62,1,'meta','all meta data operations (per minute)','<b>all meta operations</b> - sclass, trashretention, eattr, etc. (per minute)','')
	)

	MCdata = fields.getstr("MCdata", "")

	charts = ([],[])
	for id,sheet,oname,desc,fdesc,sdesc in icharts:
		if not dp.master().is_pro() and ( id==63 ):
			continue
		if fdesc=='':
			fdesc = '<b>' + desc.replace(' (per', '</b> (per').replace(' (bytes per', '</b> (bytes per')
			if not '</b>' in fdesc:
				fdesc = fdesc + '</b>'
		fdeschtml = fdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
		sdeschtml = sdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
		charts[sheet].append((id,oname,desc,fdeschtml,sdeschtml))
	if MCdata=="" and dp.cluster.leaderfound():
		MCdata="%s:%u:%u" % (dp.master().host,dp.master().port,10 if dp.master().version_at_least(4,31,0) else 11)
	servers = []
	entrystr = []
	entrydesc = {}
	mservers = dp.get_masterservers(0) # sort by ip/port
	if len(mservers)>0:
		for sheet in [0,1]:
			for id,oname,desc,fdesc,sdesc in charts[sheet]:
				name = oname.replace(":","")
				entrystr.append(name)
				entrydesc[name] = desc
		for ms in mservers:
			if ms.version>=(2,0,15):
				chmode = 10 if ms.version>=(4,31,0) else 11
				name = "%s:%u" % (ms.strip,ms.port)
				namearg_res = "%s:%u" % (name,chmode)
				namearg_ops = "%s:%u" % (name,chmode+10)
				hostx = resolve(ms.strip,0,UNRESOLVED)
				if hostx==UNRESOLVED:
					host = "Server"
				else:
					host = hostx
				entrystr.append(namearg_res)
				entrystr.append(namearg_ops)
				entrydesc[namearg_res] = "%s: %s - resources %s" % (host, name," (leader)" if (dp.cluster.leaderfound() and ms.strip==dp.master().host) else "")
				entrydesc[namearg_ops] = "%s: %s - operations %s" % (host,name," (leader)" if (dp.cluster.leaderfound() and ms.strip==dp.master().host) else "")
				servers.append((ms.strip,ms.port,"ma_"+name.replace(".","_").replace(":","_"),"Server: <b>%s</b>" % (name),chmode))

	mchtmp = MCdata.split(":")
	if len(mchtmp)==2:
		mchtmp = (mchtmp[0],mchtmp[1],0)
	if len(mchtmp)==3:
		mahost = mchtmp[0]
		maport = mchtmp[1]
		mamode = int(mchtmp[2])
		if mamode>=20:
			mamode -= 20
			masheet = 1
		elif mamode>=10:
			mamode -= 10
			masheet = 0
		else:
			mamode = 1
			masheet = 0

		out.append("""<div class="tab_title">Master server charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th class="knob-cell chart-range">""")
		out.append(html_knob_selector_chart_range('ma_main'))
		out.append("""		</th>""")
		out.append("""		<th class="chart-select">""")
		out.append("""			<div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="ma_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
		if MCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==MCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		out.append("""		</th>""")
		out.append("""	</tr>""")

		if MCdata in entrystr:
			for id,name,desc,fdesc,sdesc in charts[masheet]:
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="ma_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="ma_%s_c" class="CHARTJSC">""" % name)
				out.append("""					<span class="chartcaptionjs">%s</span>""" % fdesc)
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="ma_%s_l" class="CHARTJSL"></div>""" % name)
				out.append("""			<div id="ma_%s_r" class="CHARTJSR"></div>""" % name)
				out.append("""		</td>""")
				out.append("""	</tr>""")
		out.append("""</table></form>""")

		if MCdata in entrystr:
			out.append("""<div class="tab_title">Master server charts (comparison)</div>""")
			out.append("""<form action="#"><table class="FR">""")
			for i in range(2):
				out.append("""	<tr>""")
				if i==0:
					out.append("""		<th class="knob-cell chart-cmp-range">""")
					out.append(html_knob_selector_chart_range('ma_cmp')) 
				else:
					out.append("""		<th style="text-align: right;vertical-align:middle;border:none;">""")
					out.append("""versus:""")
				out.append("""		</th>""")
				out.append("""		<th class="chart-cmp-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" id="machart%u_select" name="machart%u" size="1" onchange="ma_change_type(%u,this.options[this.selectedIndex].value)">""" % (i,i,i))
				no = 0
				for id,name,desc,fdesc,sdesc in charts[0]:
					out.append("""				<option value="%u">%s</option>""" % (no,desc))
					no += 1
				for id,name,desc,fdesc,sdesc in charts[1]:
					out.append("""				<option value="%u">%s</option>""" % (no,desc))
					no += 1
				out.append("""			</select><span class="arrow"></span></div>""")	
				out.append("""		</th>""")
				out.append("""	</tr>""")
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="ma_%u_p" colspan="2" style="height: 124px; border-top: none;" valign="middle">""" % i)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="ma_%u_c" class="CHARTJSC">""" % i)
				out.append("""					<span id="ma_%u_d" class="chartcaptionjs">%s</span>""" % (i,charts[0][0][3]))
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="ma_%u_l" class="CHARTJSL"></div>""" % i)
				out.append("""			<div id="ma_%u_r" class="CHARTJSR"></div>""" % i)
				out.append("""		</td>""")
				out.append("""	</tr>""")
			out.append("""</table></form>""")

			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var ma_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts[masheet] ])))
			out.append("""	var ma_inames = [%s];""" % ",".join(map(repr,[ x[1] for x in charts[masheet] ])))
			out.append("""	var ma_idesc = [%s];""" % ",".join(map(repr,[ x[3] for x in charts[masheet] ])))
			out.append("""	var ma_vids_cmp = [%s];""" % ",".join(map(repr,[ x[0] for x in (charts[0]+charts[1]) ])))
			out.append("""	var ma_idesc_cmp = [%s];""" % ",".join(map(repr,[ x[3] for x in (charts[0]+charts[1]) ])))
			out.append("""	var ma_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
			out.append("""	var ma_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
			out.append("""	var ma_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
			out.append("""	var ma_host = "%s";""" % mahost)
			out.append("""	var ma_port = "%s";""" % maport)
			out.append("""	var ma_mode = %u;""" % mamode)
			out.append("""	var ma_valid = 1;""")
			out.append("""	var ma_base_href = "%s";""" % fields.createrawlink({"MCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		else:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var ma_valid = 0;""")
			out.append("""	var ma_base_href = "%s";""" % fields.createrawlink({"MCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
//#1 - comparison charts
function ma_create_charts(show_loading=true) {
ma_charttab = [];
var i;
if (ma_valid) {
for (i=0 ; i<ma_inames.length ; i++) {
	ma_charttab.push(new AcidChartWrapper("ma_"+ma_inames[i]+"_","ma_main",ma_host,ma_port,ma_mode,ma_vids[i],show_loading));
}
if (ma_chartcmp[0]) document.getElementById('machart0_select').value = ma_vids_cmp.indexOf(ma_chartcmp[0].id);
if (ma_chartcmp[1]) document.getElementById('machart1_select').value = ma_vids_cmp.indexOf(ma_chartcmp[1].id);
ma_chartcmp[0] = new AcidChartWrapper("ma_0_","ma_cmp",ma_host,ma_port,ma_mode,(ma_chartcmp[0]) ? ma_chartcmp[0].id : ma_vids_cmp[0],show_loading);
ma_chartcmp[1] = new AcidChartWrapper("ma_1_","ma_cmp",ma_host,ma_port,ma_mode,(ma_chartcmp[1]) ? ma_chartcmp[1].id : ma_vids_cmp[0],show_loading);
}
}

// main charts data update
function ma_change_data(indx, mcdata) {
var sindx,i;
if (ma_valid) {
sindx = indx - ma_inames.length;
if (sindx >= 0 && sindx < ma_modes.length) {
	for (i=0 ; i<ma_vids.length ; i++) {
		ma_charttab[i].set_host_port_id(ma_hosts[sindx],ma_ports[sindx],ma_modes[sindx],ma_vids[i]);
	}
}
}
rotateKnob("ma_main", -135);
document.location.replace(ma_base_href + "&MCdata=" + mcdata);
}

// compare charts data update
function ma_change_type(chartid, indx) {
var descel;
if (ma_valid) {
ma_chartcmp[chartid].set_id(ma_vids_cmp[indx]);
descel = document.getElementById("ma_"+chartid+"_d");
descel.innerHTML = ma_idesc_cmp[indx];
}
}

// charts initialization
ma_charttab = [];
ma_chartcmp = [];
ma_create_charts();
//--><!]]>
</script>""")

	elif len(mchtmp)==1 and len(MCdata)>0:
		chid = 0
		sdescadd = ''
		for chlist in charts:
			for id,name,desc,fdesc,sdesc in chlist:
				if name==MCdata:
					chid = id
					if sdesc!='':
						sdescadd = ', '+sdesc
		if chid==0:
			try:
				chid = int(MCdata)
			except Exception:
				pass
		if chid<=0 or chid>=1000:
			MCdata = ""

		out.append("""<div class="tab_title">Master server charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th class="knob-cell chart-range">""")
		out.append(html_knob_selector_chart_range('ma_main'))
		out.append("""		</th>""")
		out.append("""		<th class="chart-select">""")
		out.append("""			<div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="ma_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
		if MCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==MCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		if MCdata in entrystr:
			out.append("""		<label class="switch" for="ma_commonscale" id="ma_commonscale-slide" style="margin-left:10px;"><input type="checkbox" id="ma_commonscale"  onchange="AcidChartSetCommonScale('ma_main',this.checked)"/><span class="slider round"></span><span class="text">Use common Y-scale</span></label>""")				
		out.append("""		</th>""")
		out.append("""	</tr>""")
		if MCdata in entrystr:
			for mahost,maport,name,desc,chmode in servers:
				out.append("""	<tr class="C2 CHART">""")
				out.append("""		<td id="ma_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
				out.append("""			<div class="CHARTJSW">""")
				out.append("""				<div id="ma_%s_c" class="CHARTJSC">""" % name)
				out.append("""					<span id="ma_%s_d" class="chartcaptionjs">%s%s</span>""" % (name,desc,sdescadd))
				out.append("""				</div>""")
				out.append("""			</div>""")
				out.append("""			<div id="ma_%s_l" class="CHARTJSL"></div>""" % name)
				out.append("""			<div id="ma_%s_r" class="CHARTJSR"></div>""" % name)
				out.append("""		</td>""")
				out.append("""	</tr>""")
		out.append("""</table></form>""")

		if MCdata in entrystr:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var ma_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in (charts[0]+charts[1]) ])))
			out.append("""	var ma_idesc = [%s];""" % ",".join(map(repr,[ x[4] for x in (charts[0]+charts[1]) ])))
			out.append("""	var ma_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
			out.append("""	var ma_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
			out.append("""	var ma_inames = [%s];""" % ",".join(map(repr,[ x[2] for x in servers ])))
			out.append("""	var ma_sdesc = [%s];""" % ",".join(map(repr,[ x[3] for x in servers ])))
			out.append("""	var ma_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
			out.append("""	var ma_chid = %u;""" % chid)
			out.append("""	var ma_valid = 1;""")
			out.append("""	var ma_base_href = "%s";""" % fields.createrawlink({"MCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")
		else:
			out.append("""<script type="text/javascript">""")
			out.append("""<!--//--><![CDATA[//><!--""")
			out.append("""	var ma_valid = 0;""")
			out.append("""	var ma_base_href = "%s";""" % fields.createrawlink({"MCdata":""}))
			out.append("""//--><!]]>""")
			out.append("""</script>""")

		out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
//#2 - no comparison charts
function ma_create_charts(show_loading=true) {
var i;
ma_charttab = [];
if (ma_valid) {
for (i=0 ; i<ma_inames.length ; i++) {
	ma_charttab.push(new AcidChartWrapper("ma_"+ma_inames[i]+"_","ma_main",ma_hosts[i],ma_ports[i],ma_modes[i],ma_chid,show_loading));
}
}
}

// main charts data update
function ma_change_data(indx, mcdata) {
var i,chartid;
var descel;
if (ma_valid && indx >= 0 && indx < ma_vids.length) {
for (i=0 ; i<ma_inames.length ; i++) {
	chartid = ma_inames[i];
	ma_charttab[i].set_id(ma_vids[indx]);
	descel = document.getElementById("ma_"+chartid+"_d");
	if (ma_idesc[indx]!="") {
		descel.innerHTML = ma_sdesc[i] + " " + ma_idesc[indx];
	} else {
		descel.innerHTML = ma_sdesc[i];
	}
}
}
document.location.replace(ma_base_href + "&MCdata=" + mcdata);
}

// charts initialization
ma_charttab = [];
ma_create_charts();
//--><!]]>
</script>""")
	else:
		out.append("""<div class="tab_title">Master server charts</div>""")
		out.append("""<form action="#"><table class="FR">""")
		out.append("""	<tr>""")
		out.append("""		<th>""")
		out.append("""			Select: <div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="document.location.href='%s&MCdata='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"MCdata":""}))
		if MCdata not in entrystr:
			out.append("""				<option value="" selected="selected"> data type or server</option>""")
		for estr in entrystr:
			if estr==MCdata:
				out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
			else:
				out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
		out.append("""			</select><span class="arrow"></span></div>""")
		out.append("""		</th>""")
		out.append("""	</tr>""")
		out.append("""</table></form>""")

	return out