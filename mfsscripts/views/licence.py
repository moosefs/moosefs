import time

from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	out = []
	lic = dp.get_licence()
	if lic:
		colnames = lic.licextrainfo.keys()
		if lic.licver==0:
			columns = 8
		else:
			columns = 7 + len(colnames)
		out.append("""<div class="tab_title">Licence info</div>""")
		out.append("""<table class="FR no-hover">""")
		out.append("""	<tr>""")
		if lic.licver>=1:
			out.append("""		<th>ID</th>""")
		out.append("""		<th>Issuer</th>""")
		if lic.licver==0:
			out.append("""		<th>User</th>""")
		else:
			out.append("""		<th>Client</th>""")
			for colname in colnames:
				out.append("""		<th>%s</th>""" % htmlentities(colname))
		out.append("""		<th>Type</th>""")
		out.append("""		<th>Max version</th>""")
		out.append("""		<th>Expires</th>""")
		if lic.licver==0:
			out.append("""		<th>Max chunkserver size</th>""")
			out.append("""		<th>Max number of chunkservers</th>""")
			out.append("""		<th>Quota</th>""")
		else:
			out.append("""		<th>Utilization (raw data)</th>""")
		out.append("""	</tr>""")
		out.append("""	<tr>""")

		if lic.licver>=1:
			out.append("""		<td>%s</td>""" % htmlentities(lic.licid))
		out.append("""		<td>%s</td>""" % htmlentities(lic.licissuer))
		out.append("""		<td>%s</td>""" % htmlentities(lic.licuser))
		if lic.licver>=1:
			for colname in colnames:
				out.append("""		<td>%s</td>""" % htmlentities(lic.licextrainfo[colname]))
		out.append("""		<td>%s</td>""" % htmlentities(lic.lictypestr))
		issues = vld.check_licence_version(lic, dp.master().version)
		out.append("""		<td>%s</td>""" % issues.span(lic.get_max_version_str()))
		timeissues = vld.check_licence_time(lic)
		if lic.is_time_unlimited():
			out.append("""		<td>never</td>""")
		else:
			if lic.licleft<0:
				expires_msg = " (expired)"
			else:
				expires_msg = """ <span data-tt-info="%s">%s</span>""" % (timeduration_to_fullstr(lic.licleft),timeduration_to_shortstr(lic.licleft))
			out.append("""		<td>%s</td>""" % timeissues.span(time.asctime(time.localtime(lic.licmaxtime))+expires_msg ))
		
		sizeissues = vld.check_licence_size(lic)
		if lic.licver==0:
			if lic.is_cs_size_unlimited():
				out.append("""		<td>unlimited</td>""")
			else:
				out.append("""		<td><span data-tt-info="%s B">%s</span></td>""" % (decimal_number(lic.csmaxsize),humanize_number(lic.csmaxsize,"&nbsp;")))
			if lic.is_cs_cnt_unlimited():
				out.append("""		<td>unlimited</td>""")
			else:
				out.append("""		<td>%u</td>""" % lic.csmaxcnt)
			if lic.quota:
				out.append("""		<td>YES</td>""")
			else:
				out.append("""		<td>NO</td>""")
		else:
			if lic.is_size_unlimited():
				out.append("""		<td>unlimited</td>""")
			else:
				size_msg = sizeissues.span("%s of %s (%.2f%%)" % (humanize_number(lic.currentsize,"&nbsp;"),humanize_number(lic.licmaxsize,"&nbsp;"),(100.0 * lic.currentsize / lic.licmaxsize)) )
				out.append("""		<td><span class="OK" data-tt-info="used: %s B, available: %s B">%s</span></td>""" % (decimal_number(lic.currentsize),decimal_number(lic.licmaxsize),size_msg))
		out.append("""	</tr>""")
		if lic.licver>=1:
			if len(lic.addinfo)>0:
				out.append("""	<tr><td colspan="%u">%s</td></tr>""" % (columns,htmlentities(lic.addinfo)))
			if sizeissues.contains('lic_exhausted'):
				out.append("""	<tr><td colspan="%u"><span class="ERROR">You have reached the raw space limit of your licence. This eventually results in your system becoming read-only. In order to expand your licence, please contact our team at <a href="mailto:contact@moosefs.com">contact@moosefs.com</a></span></td></tr>""" % columns)
			elif sizeissues.contains('lic_exhausting') or sizeissues.contains('lic_almost_exhausted'):
				out.append("""	<tr><td colspan="%u"><span class="WARNING">You are reaching the raw space limit of your licence. In order to expand it, please contact our team at <a href="mailto:contact@moosefs.com">contact@moosefs.com</a></span></td></tr>""" % columns)
			if timeissues.contains('lic_expiring'):
				out.append("""	<tr><td colspan="%u"><span class="WARNING">Your licence is about to expire. In order to extend it, please contact our team at <a href="mailto:contact@moosefs.com">contact@moosefs.com</a></span></td></tr>""" % columns)
			elif timeissues.contains('lic_expired'):
				out.append("""	<tr><td colspan="%u"><span class="ERROR">Your licence has expired. In order to extend it, please contact our team at <a href="mailto:contact@moosefs.com">contact@moosefs.com</a></span></td></tr>""" % columns)
		out.append("""</table>""")
	else:
		out.append("""<div class="tab_title ERROR">Oops!</div>""")
		out.append("""<table class="FR MESSAGE">""")
		out.append("""	<tr><td align="left"><span class="ERROR">Unrecognized answer from Master server</span></td></tr>""")
		out.append("""</table>""")

	return out