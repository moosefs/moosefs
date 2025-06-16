from common.constants import *
from common.utils import *
from common.utilsgui import *
import views.metaloggers

def render(dp, fields, vld):
	IMorder = fields.getint("IMorder", 0)
	IMrev = fields.getint("IMrev", 0)
	MBorder = fields.getint("MBorder", 0)
	MBrev = fields.getint("MBrev", 0)

	# update master servers delay times prior to getting the list of master servers
	highest_saved_metaversion, highest_metaversion_checksum = dp.cluster.update_masterservers_delays()

	mservers = dp.get_masterservers(IMorder, IMrev)
	mloggers = dp.get_metaloggers(MBorder, MBrev)

	out = []
	issues = vld.check_cluster_masters(mservers, len(dp.get_metaloggers()))
	if len(mloggers)>0 and len(mservers)>0:
		if len(mservers)==1:
			out.append("""<div class="tab_title">Metadata server (Master) and Metaloggers%s</div>""" % issues.span())
		else:
			out.append("""<div class="tab_title">Metadata servers (masters) and Metaloggers%s</div>""" % issues.span())
		out.append("""<table class="FR panel no-hover">""")
		out.append(""" <tr><td>""")		
	elif len(mservers)==1:
		out.append("""<div class="tab_title">Metadata server (Master)%s</div>""" % issues.span())
	elif len(mservers)>1:
		out.append("""<div class="tab_title">Metadata servers (masters)%s</div>""" % issues.span())
	elif len(mloggers)>0:
		out.append("""<div class="tab_title">Metadata loggers%s</div>""" % issues.span())
	else:
		out.append("""<div class="tab_title">Missing metadata servers%s</div>""" % issues.span())

	out.append("""<table class="acid_tab acid_tab_storageid_mfsmasters">""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
	out.append("""		<th rowspan="2">IP</th>""")
	out.append("""		<th rowspan="2">Version</th>""")
	out.append("""		<th rowspan="2">State</th>""")
	out.append("""		<th rowspan="2">Local time</th>""")
	out.append("""		<th colspan="3">Current metadata</th>""")
	out.append("""		<th colspan="2">Resources</th>""")
	out.append("""		<th colspan="5">Last metadata save</th>""")
	out.append("""		<th rowspan="2" data-tt="info_masters_exports_checksum">Exports<br/>checksum</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th>id</th>""")
	out.append("""		<th>version</th>""")
	out.append("""		<th>delay</th>""")
	out.append("""		<th>RAM</th>""")
	out.append("""		<th>CPU (sys&#8239;+&#8239;usr)</th>""")
	out.append("""		<th>time</th>""")
	out.append("""		<th>duration</th>""")
	out.append("""		<th>status</th>""")
	out.append("""		<th>version</th>""")
	out.append("""		<th>checksum</th>""")
	out.append("""	</tr>""")

	if len(mservers)==0:
		out.append("""	<tr><td colspan="10">Metadata (Master) servers not found! Check your DNS</td></tr>""")


	for ms in mservers:
		out.append("""	<tr>""")
		issues = vld.check_follower_version(ms)
		out.append("""		<td align="right"></td><td align="center" style="width:6%%;"><span class="sortkey">%s </span>%s</td><td align="center" style="width:6%%;"><span class="sortkey">%s</span>%s</td>""" % (ms.sortip,ms.strip,ms.sortver,issues.span(ms.strver.replace(" PRO", "<small> PRO</small>"))))
		issues = vld.check_ms_state(ms)
		if issues.any():
			out.append("""		<td align="center">%s</td>""" % issues.span(ms.statestr))
		else:
			out.append("""		<td align="center"><span class="STATECOLOR%u">%s</span></td>""" % (ms.statecolor, ms.statestr))
		if ms.usectime==None or ms.usectime==0:
			out.append("""		<td align="center">-</td>""")
		else:
			issues = vld.check_follower_clock(ms.secdelta)
			out.append("""		<td align="center">%s%s</td>""" % (datetime_to_str(ms.usectime//1000000), issues.span()))
		if ms.metaid!=None:
			issues = vld.check_follower_metaid(ms)
			out.append("""		<td align="center">%016X%s</td>""" % (ms.metaid, issues.span()))
		else:
			out.append("""		<td align="center">-</td>""")
		metaversion_str=decimal_number_html(ms.metaversion) if ms.metaversion>0 else '-'
		out.append("""		<td align="right">%s</td>""" % (metaversion_str))
		if ms.metadelay==None:
			out.append("""		<td align="right">-</td>""")
		else:
			issues = vld.check_follower_metadelay(ms.metadelay)
			out.append("""		<td align="right">%.0f&#8239;s%s</td>""" % (ms.metadelay, issues.span()))
		if ms.memusage>0:
			out.append("""		<td align="right"><span data-tt-info="%s&#8239;B">%s</span></td>""" % (decimal_number(ms.memusage),humanize_number(ms.memusage,"&nbsp;")))
		else:
			msg = vld.issue('ms_no_ram_info').span('n/a') if ms.is_active() else '-'
			out.append("""		<td align="center">%s</td>""" % msg)
		if ms.syscpu>0 or ms.usercpu>0:
			out.append("""		<td align="center"><span data-tt-info="%.3f%% (sys:%.3f%%, usr:%.3f%%)">%.1f%%&nbsp;(%.1f&#8239;+&#8239;%.1f)</span></td>""" % (ms.syscpu+ms.usercpu,ms.syscpu,ms.usercpu,ms.syscpu+ms.usercpu,ms.syscpu,ms.usercpu))
		else:
			msg = vld.issue('ms_no_cpu_info').span('n/a') if ms.is_active() else '-'
			out.append("""		<td align="center">%s</td>""" % msg)
		if ms.lastsuccessfulstore>0:
			issues = vld.check_ms_metadata_save_older(ms, highest_saved_metaversion)
			out.append("""		<td align="center">%s</td>""" % issues.span(datetime_to_str(ms.lastsuccessfulstore)))
			issues = vld.check_last_save_duration(ms)
			out.append("""		<td align="center"><span data-tt-info="%s">%s</span></td>""" % (timeduration_to_fullstr(ms.lastsaveseconds),issues.span(timeduration_to_shortstr(ms.lastsaveseconds,"&#8239;"))))
		else:
			out.append("""		<td align="center">-</td><td align="center">-</td>""")
		if ms.lastsuccessfulstore>0 or ms.lastsavestatus>0:
			issues = vld.check_ms_last_store_status(ms)
			txt='Background save' if ms.lastsavestatus==LASTSTORE_META_STORED_BG else "Got from other" if ms.lastsavestatus==LASTSTORE_DOWNLOADED else 'Foreground save' if ms.lastsavestatus==LASTSTORE_META_STORED_FG else "CRC background save" if ms.lastsavestatus==LASTSTORE_CRC_STORED_BG else "Unknown: %u" % ms.lastsavestatus
			out.append("""		<td align="center"><span class="%s" data-tt="%s">%s%s</span></td>""" % (issues.css_class(), issues.data_help(), txt, issues.icon()))
		else:
			out.append("""		<td align="center">-</td>""")
		if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==LASTSTORE_META_STORED_BG or ms.lastsavestatus>=LASTSTORE_META_STORED_FG) and ms.lastsavemetaversion!=None:
			issues = vld.check_ms_metadata_save_older(ms, highest_saved_metaversion)
			out.append("""		<td align="right"><span class="%s" data-tt="%s">%s</span></td>""" % (issues.css_class(),issues.data_help(), decimal_number_html(ms.lastsavemetaversion)))
		else:
			out.append("""		<td align="center">-</td>""")
		if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==LASTSTORE_META_STORED_BG or ms.lastsavestatus>=LASTSTORE_META_STORED_FG) and ms.lastsavemetaversion!=None and ms.lastsavemetachecksum!=None:
			issues = vld.check_ms_checksum_mismatch(ms, highest_saved_metaversion, highest_metaversion_checksum)
			out.append("""		<td align="center"><span class="%s" data-tt="%s">%08X %s</span></td>""" % (issues.css_class(),issues.data_help(),ms.lastsavemetachecksum,issues.icon()))
		else:
			out.append("""		<td align="center">-</td>""")
		if ms.exportschecksum!=None:
			issues = vld.check_follower_checksum(ms)
			out.append("""		<td align="center">%s</td>""" % issues.span(("%016X" % ms.exportschecksum)[:8]))
		else:
			out.append("""		<td align="center">-</td>""")
		out.append("""	</tr>""")	

	out.append("""</table>""")

	if len(mloggers)>0:
		out_mloggers = views.metaloggers.render(dp, fields, vld)
		out.extend(out_mloggers)

	if len(mloggers)>0 and len(mservers)>0:
		out.append("""</table>""") #panel table


	return out