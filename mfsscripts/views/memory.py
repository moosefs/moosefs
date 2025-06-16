from common.utilsgui import *

def render(dp, fields, vld):
	# mu.memlabels,mu.abrlabels,mu.totalused,mu.totalallocated,mu.memusage = dp.get_memory_usage()
	mu = dp.get_memory_usage()
	out = []
	out.append("""<div class="tab_title">Memory usage details</div>""")
	out.append("""<table class="FR">""")
	out.append("""	<tr><th></th>""")
	for label in mu.memlabels:
		out.append("""		<th style="min-width: 70px;">%s</th>""" % label)
	out.append("""	<th style="min-width: 70px;">total</th></tr>""")
	out.append("""	<tr><th align="center">Used</th>""")
	for i in range(len(mu.memlabels)):
		out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(mu.memusage[1+i*2]),humanize_number(mu.memusage[1+i*2],"&nbsp;")))
	out.append("""	<td align="center"><a style="cursor:default" title="%s B">%s</a></td></tr>""" % (decimal_number(mu.totalused),humanize_number(mu.totalused,"&nbsp;")))
	out.append("""	<tr><th align="center">Allocated</th>""")
	for i in range(len(mu.memlabels)):
		out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(mu.memusage[i*2]),humanize_number(mu.memusage[i*2],"&nbsp;")))
	out.append("""	<td align="center"><a style="cursor:default" title="%s B">%s</a></td></tr>""" % (decimal_number(mu.totalallocated),humanize_number(mu.totalallocated,"&nbsp;")))
	out.append("""	<tr><th align="center">Utilization</th>""")
	for i in range(len(mu.memlabels)):
		if mu.memusage[i*2]:
			percent = "%.2f %%" % (100.0 * mu.memusage[1+i*2] / mu.memusage[i*2])
		else:
			percent = "-"
		out.append("""		<td align="center">%s</td>""" % percent)
	if mu.totalallocated:
		percent = "%.2f %%" % (100.0 * mu.totalused / mu.totalallocated)
	else:
		percent = "-"
	out.append("""	<td align="center">%s</td></tr>""" % percent)
	if mu.totalallocated>0:
		out.append("""	<tr><th rowspan="2" align="center">Distribution</th>""")
		for i in range(len(mu.memlabels)):
			tpercent = "%.2f %%" % (100.0 * mu.memusage[i*2] / mu.totalallocated)
			out.append("""		<td align="center">%s</td>""" % tpercent)
		out.append("""	<td>-</td></tr>""")
		out.append("""  <tr>""")
		out.append("""		<td colspan="%d" class="NOPADDING">""" % (len(mu.memlabels)+1))
		out.append("""			<table width="100%" style="border:0px;" id="bar"><tr>""")
		memdistribution = []
		other = 0.0
		for i,(label,abr) in enumerate(zip(mu.memlabels,mu.abrlabels)):
			tpercent = (100.0 * mu.memusage[i*2] / mu.totalallocated)
			if tpercent>5.0:
				memdistribution.append((tpercent,label,abr))
			else:
				other+=tpercent
		memdistribution.sort()
		memdistribution.reverse()
		if other>0:
			memdistribution.append((other,None,None))
		clz = "FIRST"
		labels = []
		tooltips = []
		for i,(percent,label,abr) in enumerate(memdistribution):
			if label:
				if percent>7.0:
					out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,clz,label,percent,label))
				elif percent>3.0:
					out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,clz,label,percent,abr)) 
				else:
					out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,clz,label,percent,"#")) 
				labels.append(label)
				tooltips.append("%s (%.2f %%)" % (label,percent))
			else:
				out.append("""				<td style="width:%.2f%%;" class="MEMDISTOTHER MEMDIST%s">others</td>""" % (percent,clz))
				labels.append("others")
				tooltips.append("other memory segments (%.2f %%)" % (percent))
			clz = "MID"
		out.append("""			</tr></table>""")
		out.append("""<script type="text/javascript">""")
		out.append("""<!--//--><![CDATA[//><!--""")
		out.append("""	var bar_labels = [%s];""" % ",".join(map(repr,labels)))
		out.append("""	var bar_tooltips = [%s];""" % ",".join(map(repr,tooltips)))
		out.append("""//--><!]]>""")
		out.append("""</script>""")
		out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
function bar_refresh() {
var b = document.getElementById("bar");
var i,j,x;
if (b) {
var x = b.getElementsByTagName("td");
for (i=0 ; i<x.length ; i++) {
x[i].innerHTML = "";
}
for (i=0 ; i<x.length ; i++) {
var width = x[i].clientWidth;
var label = bar_labels[i];
var tooltip = bar_tooltips[i];
x[i].innerHTML = "<a title='" + tooltip + "'>" + label + "</a>";
if (width<x[i].clientWidth) {
	x[i].innerHTML = "<a title='" + tooltip + "'>&#8230;</a>";
	if (width<x[i].clientWidth) {
		x[i].innerHTML = "<a title='" + tooltip + "'>&#8226;</a>";
		if (width<x[i].clientWidth) {
			x[i].innerHTML = "<a title='" + tooltip + "'>.</a>";
			if (width<x[i].clientWidth) {
				x[i].innerHTML = "";
			}
		}
	} else {
		for (j=1 ; j<bar_labels[i].length-1 ; j++) {
			x[i].innerHTML = "<a title='" + tooltip + "'>"+label.substring(0,j) + "&#8230;</a>";
			if (width<x[i].clientWidth) {
				break;
			}
		}
		x[i].innerHTML = "<a title='" + tooltip + "'>" + label.substring(0,j-1) + "&#8230;</a>";
	}
}
}
}
}

function bar_add_event(obj,type,fn) {
if (obj.addEventListener) {
obj.addEventListener(type, fn, false);
} else if (obj.attachEvent) {
obj.attachEvent('on'+type, fn);
}
}

//bar_add_event(window,"load",bar_refresh); - comment due to flickering on load (for short bars <10%)
bar_add_event(window,"resize",bar_refresh);
//--><!]]>
</script>""")
		out.append("""		</td>""")
		out.append("""	</tr>""")
	out.append("""</table>""")
	return out