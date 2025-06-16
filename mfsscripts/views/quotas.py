from common.utils import *
from common.utilsgui import *
from common.models import *

def render(dp, fields, vld):
	QUorder = fields.getint("QUorder", 0)
	QUrev = fields.getint("QUrev", 0)

	out = []
	out.append("""<div class="tab_title">Active quotas</div>""")
	out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsquota">""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="3" class="acid_tab_enumerate">#</th>""")
	out.append("""		<th rowspan="3">Path</th>""")
	out.append("""	<th colspan="6">Soft&nbsp;quota</th>""")
	out.append("""	<th colspan="4">Hard&nbsp;quota</th>""")
	out.append("""	<th colspan="12">Current&nbsp;values</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th rowspan="2" class="wrap">grace period</th>""")
	out.append("""		<th rowspan="2" class="wrap">time to expire</th>""")
	out.append("""		<th rowspan="2">inodes</th>""")
	out.append("""		<th rowspan="2">length</th>""")
	out.append("""		<th rowspan="2">size</th>""")
	out.append("""		<th rowspan="2" class="wrap">real size</th>""")
	out.append("""		<th rowspan="2">inodes</th>""")
	out.append("""		<th rowspan="2">length</th>""")
	out.append("""		<th rowspan="2">size</th>""")
	out.append("""		<th rowspan="2" class="wrap">real size</th>""")
	out.append("""		<th colspan="3">inodes</th>""")
	out.append("""		<th colspan="3">length</th>""")
	out.append("""		<th colspan="3">size</th>""")
	out.append("""		<th colspan="3">real&nbsp;size</th>""")
	out.append("""	</tr>""")
	out.append("""	<tr>""")
	out.append("""		<th>value</th>""")
	out.append("""		<th>% soft</th>""")
	out.append("""		<th>% hard</th>""")
	out.append("""		<th>value</th>""")
	out.append("""		<th>% soft</th>""")
	out.append("""		<th>% hard</th>""")
	out.append("""		<th>value</th>""")
	out.append("""		<th>% soft</th>""")
	out.append("""		<th>% hard</th>""")
	out.append("""		<th>value</th>""")
	out.append("""		<th>% soft</th>""")
	out.append("""		<th>% hard</th>""")
	out.append("""	</tr>""")


	maxperc,quotas = dp.get_quotas(QUorder, QUrev)
	for qu in quotas:
		graceperiod_default = 0
		if dp.master().has_feature(FEATURE_DEFAULT_GRACEPERIOD):
			if qu.exceeded & 2:
				qu.exceeded &= 1
				graceperiod_default = 1

		out.append("""	<tr>""")
		out.append("""		<td align="right"></td>""")
		out.append("""		<td align="left">%s</td>""" % htmlentities(qu.path))
		if qu.graceperiod>0:
			out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s%s</a></td>""" % (qu.graceperiod,timeduration_to_fullstr(qu.graceperiod),timeduration_to_shortstr(qu.graceperiod)," (default)" if graceperiod_default else ""))
		else:
			out.append("""		<td align="center"><span class="sortkey">0 </span>default</td>""")
		if qu.timetoblock<0xFFFFFFFF:
			if qu.timetoblock>0:
				out.append("""		<td align="center"><span class="SEXCEEDED"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></span></td>""" % (qu.timetoblock,timeduration_to_fullstr(qu.timetoblock),timeduration_to_shortstr(qu.timetoblock)))
			else:
				out.append("""		<td align="center"><span class="EXCEEDED"><span class="sortkey">0 </span>expired</span></td>""")
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&1:
			out.append("""		<td align="right"><span>%s</span></td>""" % decimal_number_html(qu.sinodes))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&2:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.slength,decimal_number(qu.slength),humanize_number(qu.slength,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&4:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.ssize,decimal_number(qu.ssize),humanize_number(qu.ssize,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&8:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.srealsize,decimal_number(qu.srealsize),humanize_number(qu.srealsize,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&16:
			out.append("""		<td align="right"><span>%s</span></td>""" % decimal_number_html(qu.hinodes))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&32:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.hlength,decimal_number(qu.hlength),humanize_number(qu.hlength,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&64:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.hsize,decimal_number(qu.hsize),humanize_number(qu.hsize,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&128:
			out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (qu.hrealsize,decimal_number(qu.hrealsize),humanize_number(qu.hrealsize,"&nbsp;")))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		out.append("""		<td align="right">%s</td>""" % decimal_number_html(qu.cinodes))
		if qu.qflags&1:
			if qu.sinodes>0:
				if qu.sinodes>=qu.cinodes:
					clz="NOTEXCEEDED"
				elif qu.timetoblock>0:
					clz="SEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.cinodes)/qu.sinodes))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&16:
			if qu.hinodes>0:
				if qu.hinodes>qu.cinodes:
					clz="NOTEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.cinodes)/qu.hinodes))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (qu.clength,decimal_number(qu.clength),humanize_number(qu.clength,"&nbsp;")))
		if qu.qflags&2:
			if qu.slength>0:
				if qu.slength>=qu.clength:
					clz="NOTEXCEEDED"
				elif qu.timetoblock>0:
					clz="SEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.clength)/qu.slength))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&32:
			if qu.hlength>0:
				if qu.hlength>qu.clength:
					clz="NOTEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.clength)/qu.hlength))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (qu.csize,decimal_number(qu.csize),humanize_number(qu.csize,"&nbsp;")))
		if qu.qflags&4:
			if qu.ssize>0:
				if qu.ssize>=qu.csize:
					clz="NOTEXCEEDED"
				elif qu.timetoblock>0:
					clz="SEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.csize)/qu.ssize))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&64:
			if qu.hsize>0:
				if qu.hsize>qu.csize:
					clz="NOTEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.csize)/qu.hsize))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (qu.crealsize,decimal_number(qu.crealsize),humanize_number(qu.crealsize,"&nbsp;")))
		if qu.qflags&8:
			if qu.srealsize>0:
				if qu.srealsize>=qu.crealsize:
					clz="NOTEXCEEDED"
				elif qu.timetoblock>0:
					clz="SEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.crealsize)/qu.srealsize))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		if qu.qflags&128:
			if qu.hrealsize>0:
				if qu.hrealsize>qu.crealsize:
					clz="NOTEXCEEDED"
				else:
					clz="EXCEEDED"
				out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (clz,(100.0*qu.crealsize)/qu.hrealsize))
			else:
				out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
		else:
			out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
		out.append("""	</tr>""")

	out.append("""</table>""")
	return out