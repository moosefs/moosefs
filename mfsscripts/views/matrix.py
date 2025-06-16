from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld, selectable):	
	ICsclassid = fields.getint("ICsclassid", -1)
	(matrix,progressstatus)=dp.get_matrix(ICsclassid)
	# Get sums of missing, undergoal and endangered  (for all sclasses)
	(mx_summary,_)=dp.get_matrix_summary()
	issues_sum = vld.check_mx_summary(mx_summary)

	if len(matrix)==2 or len(matrix)==6 or len(matrix)==8:
		out = []
		out.append("""<form action="#">""")
		out.append("""<div class="tab_title">Chunk matrix table %s</div>""" % issues_sum.icon())
		out.append("""<table class="acid_tab acid_tab_storageid_mfsmatrix" id="mfsmatrix">""")
		out.append("""	<tr><th colspan="14">""") 
		out.append("""<div style="display: flex; align-items: center;">""")
		options=[(-45, "Regular chunks only", "'marked for removal' excluded","acid_tab.switchdisplay('mfsmatrix','matrixar_vis',1);"),
				(45, "All chunks","'marked for removal' included","acid_tab.switchdisplay('mfsmatrix','matrixar_vis',0);")]
		out.append(html_knob_selector("matrixar_vis",12,(380,42),(190,20),options))
		if len(matrix)==6:
			options=[(-45, "Copies and EC chunks", None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',0);"),
					(45, "Copies only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',1);"),
					(135, "EC chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',2);")]
			out.append(html_knob_selector("matrixec_vis",12,(340,42),(170,20),options))
		elif len(matrix)==8:
			options=[(-45,"Copies and EC chunks", None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',0);"),
					(45,"EC8 chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',2);"),
					(135,"EC4 chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',3);"),
					(-135,"Copies only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',1);")]
			out.append(html_knob_selector("matrixec_vis",12,(350,42),(175,20),options))

		
		if dp.master().has_feature(FEATURE_SCLASS_IN_MATRIX):
			sclass_dict = dp.get_used_sclasses_names()
		else:
			sclass_dict = {}
		if len(sclass_dict)>1 and selectable:
			sclass_used=[]
			name_too_long=False
			for sclassid in sclass_dict:
				if ICsclassid!=sclassid:
					if sclass_dict[sclassid].has_chunks:
						sclass_used.append((sclassid,sclass_dict[sclassid].name))
						if len(sclass_dict[sclassid].name)>17:
							name_too_long=True
				else:
					sclass_used.append((sclassid,sclass_dict[sclassid].name))
					if len(sclass_dict[sclassid].name)>17:
						name_too_long=True					
			sclass_count=len(sclass_used)
			if sclass_count<=5 and not name_too_long: #just a few (max 5+1=6) storage classes, use a knob 
				angles=[[-45],[-45,45],[-45,45,135],[-135,-45,45,135],[-135,-45,45,90,135],[-135,-90,-45,45,90,135]]
				options=[(angles[sclass_count][0],"All storage classes", None,"document.location.href='%s&ICsclassid=-1'" % fields.createrawlink({"ICsclassid":""}))]
				selected = 0 #default selection
				i=1
				for (sclassid, name) in sclass_used:
					options.append((angles[sclass_count][i],name, None,"document.location.href='%s&ICsclassid=%u'" % (fields.createrawlink({"ICsclassid":""}),sclassid)))
					selected = i if ICsclassid==sclassid else selected
					i+=1
				out.append(html_knob_selector("sclass_knob",12,(340,43),(170,21),options,True,selected))
			else: #too many classes to display as a knob, use a drop-down instead 
				out.append("""	<label for="storage_class_select">Storage class </label><div class="select-fl"><select id="storage_class_select" name="storage_class" onchange="document.location.href='%s&ICsclassid='+this.options[this.selectedIndex].value">""" % fields.createrawlink({"ICsclassid":""}))
				if ICsclassid>=0:
					out.append("""		<option value="-1">all storage classes</option>""")
				else:
					out.append("""		<option value="-1" selected="selected">all storage classes</option>""")
				for sclassid in sclass_dict:
					if ICsclassid!=sclassid:
						if sclass_dict[sclassid].has_chunks:
							out.append("""		<option value="%u">%s</option>""" % (sclassid,sclass_dict[sclassid].name))
					else:
						out.append("""		<option value="%u" selected="selected">%s</option>""" % (sclassid,sclass_dict[sclassid].name))
				if ICsclassid>=0 and ICsclassid not in sclass_dict:
					out.append("""		<option value="%u" selected="selected">nonexistent class number %u</option>""" % (ICsclassid,ICsclassid))
				out.append("""	</select><span class="arrow"></span></div>""")
		out.append("""</div>""")
		out.append("""	</th></tr>""")
		issues = vld.check_mx_progress(progressstatus)
		if issues.any():
			progressstr = "disconnections" if (progressstatus==1) else "connections" if (progressstatus==2) else "connections and disconnections"
			msg = "Counters may not be valid - %s in progress" % progressstr
			out.append("""	<tr><th colspan="14">%s</th></tr>""" % issues.span(msg))
		out.append("""	<tr>""")
		out.append("""		<th rowspan="2" colspan="2" class="acid_tab_skip">""")
		out.append("""		</th>""")
		out.append("""		<th colspan="12" class="acid_tab_skip">""")
		line = []
		line.append("""<span class="matrixec_vis0">Actual Redundancy Level</span>""")
		line.append("""<span class="matrixec_vis1">Actual number of full copies</span>""")
		line.append("""<span class="matrixec_vis2">Actual number of EC8 parts</span>""")
		if len(matrix)>=8:
			line.append("""<span class="matrixec_vis3">Actual number of EC4 parts</span>""")
		line.append("""<span class="matrixar_vis0">, 'marked for removal' included</span>""")
		line.append("""<span class="matrixar_vis1">, 'marked for removal' excluded</span>""")	
		line.append("""<span class="matrixec_vis0">&nbsp;<small>('RL = n' means EC: 8&#x202F;+n or 4&#x202F;+n&#x202F;parts, GOAL: 1&#x202F;+n&#x202F;copies)</small></span>""")
		out.append("".join(line))
		out.append("""		</th>""")
		out.append("""	</tr>""")
		out.append("""	<tr>""")
		for rl in range(11):
			out.append("""		<th class="acid_tab_skip" style="min-width:50px;">""")
			out.append("""			<span class="matrixec_vis0">%s</span>""" % ("missing" if (rl==0) else "RL&#x202F;>=&#x202F;9" if (rl==10) else ("RL&#x202F;=&#x202F;%u" % (rl-1))))
			out.append("""			<span class="matrixec_vis1">%s</span>""" % ("missing" if (rl==0) else ("1&#x202F;+%u&#x202F;copies" % (rl-1))))
			out.append("""			<span class="matrixec_vis2">%s</span>""" % ("missing" if (rl==0) else ("8&#x202F;+%u&#x202F;parts" % (rl-1))))
			if len(matrix)>=8:
				out.append("""			<span class="matrixec_vis3">%s</span>""" % ("missing" if (rl==0) else ("4&#x202F;+%u&#x202F;parts" % (rl-1))))
			out.append("""		</th>""")
		out.append("""		<th class="acid_tab_skip" style="min-width:50px;">all</th>""")
		out.append("""	</tr>""")

	classsum = []
	sumlist = []
	for i in range(len(matrix)):
		classsum.append(7*[0])
		sumlist.append(11*[0])
	left_col_once=1

	for goal in range(11):
		out.append("""	<tr>""")
		if left_col_once:
			vertical_title = """<svg viewbox="0 0 16 220" width="16" height="220" xmlns="http://www.w3.org/2000/svg"><text transform="rotate(-90)" style="font-family: arial, verdana, sans-serif; font-size: 13px; text-anchor: middle;" x="-110" y="12">PLACEHOLDER</text></svg>"""
			out.append("""<th rowspan="12" style="min-width: 16px;" class="acid_tab_skip">""")
			out.append("""			<div class="matrixec_vis0">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected Redundancy Level (RL)"))
			out.append("""			<div class="matrixec_vis1">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of copies"))
			out.append("""			<div class="matrixec_vis2">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of parts"))
			if len(matrix)>=8:
				out.append("""			<div class="matrixec_vis3">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of parts"))
			out.append("""</th>""")
			left_col_once=0
		if goal==0:
			out.append("""		<th align="center" class="acid_tab_skip">deleted</th>""")
		else:
			out.append("""		<th align="center" class="acid_tab_skip" style="min-width:50px;">""")
			out.append("""			<span class="matrixec_vis0">RL&#x202F;=&#x202F;%u</span>""" % (goal-1))
			out.append("""			<span class="matrixec_vis1">%s</span>""" % ("-" if (goal>9) else ("1&#x202F;+%u&#x202F;copies" % (goal-1))))
			out.append("""			<span class="matrixec_vis2">%s</span>""" % ("-" if (goal==1) else ("8&#x202F;+%u&#x202F;parts" % (goal-1))))
			if len(matrix)>=8:
				out.append("""			<span class="matrixec_vis3">%s</span>""" % ("-" if (goal==1) else ("4&#x202F;+%u&#x202F;parts" % (goal-1))))
			out.append("""		</th>""")
		# columns
		for actual in range(11):
			(col,clz)=redundancy2colclass(goal,actual)
			for i in range(len(matrix)):
				classsum[i][col]+=matrix[i][goal][actual]
			out.append("""		<td align="right" class="acid_tab_skip">""")
			if len(matrix)==8:
				out.append("""			<span class="matrixar_vis0">""")
				if matrix[0][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(matrix[0][goal][actual])))
				if matrix[2][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(matrix[2][goal][actual])))
				if matrix[4][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(matrix[4][goal][actual])))
				if matrix[6][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis3">%s</span>""" % (clz,decimal_number_html(matrix[6][goal][actual])))
				out.append("""			</span>""")
				out.append("""			<span class="matrixar_vis1">""")
				if matrix[1][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(matrix[1][goal][actual])))
				if matrix[3][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(matrix[3][goal][actual])))
				if matrix[5][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(matrix[5][goal][actual])))
				if matrix[7][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis3">%s</span>""" % (clz,decimal_number_html(matrix[7][goal][actual])))
				out.append("""			</span>""")
			elif len(matrix)==6:
				out.append("""			<span class="matrixar_vis0">""")
				if matrix[0][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(matrix[0][goal][actual])))
				if matrix[2][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(matrix[2][goal][actual])))
				if matrix[4][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(matrix[4][goal][actual])))
				out.append("""			</span>""")
				out.append("""			<span class="matrixar_vis1">""")
				if matrix[1][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(matrix[1][goal][actual])))
				if matrix[3][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(matrix[3][goal][actual])))
				if matrix[5][goal][actual]>0:
					out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(matrix[5][goal][actual])))
				out.append("""			</span>""")
			else:
				if matrix[0][goal][actual]>0:
					out.append("""			<span class="%s matrixar_vis0">%s</span>""" % (clz,decimal_number_html(matrix[0][goal][actual])))
				if matrix[1][goal][actual]>0:
					out.append("""			<span class="%s matrixar_vis1">%s</span>""" % (clz,decimal_number_html(matrix[1][goal][actual])))
			out.append("""		</td>""")

		if goal==0:
			clz="IGNORE"
		else:
			clz=""
		out.append("""		<td align="right" class="acid_tab_skip">""")
		if len(matrix)==8:
			out.append("""			<span class="matrixar_vis0">""")
			out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(sum(matrix[0][goal]))))
			out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(sum(matrix[2][goal]))))
			out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(sum(matrix[4][goal]))))
			out.append("""				<span class="%s matrixec_vis3">%s</span>""" % (clz,decimal_number_html(sum(matrix[6][goal]))))
			out.append("""			</span>""")
			out.append("""			<span class="matrixar_vis1">""")
			out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(sum(matrix[1][goal]))))
			out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(sum(matrix[3][goal]))))
			out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(sum(matrix[5][goal]))))
			out.append("""				<span class="%s matrixec_vis3">%s</span>""" % (clz,decimal_number_html(sum(matrix[7][goal]))))
			out.append("""			</span>""")
		elif len(matrix)==6:
			out.append("""			<span class="matrixar_vis0">""")
			out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(sum(matrix[0][goal]))))
			out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(sum(matrix[2][goal]))))
			out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(sum(matrix[4][goal]))))
			out.append("""			</span>""")
			out.append("""			<span class="matrixar_vis1">""")
			out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (clz,decimal_number_html(sum(matrix[1][goal]))))
			out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (clz,decimal_number_html(sum(matrix[3][goal]))))
			out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (clz,decimal_number_html(sum(matrix[5][goal]))))
			out.append("""			</span>""")
		else:
			out.append("""			<span class="%s matrixar_vis0">%s</span>""" % (clz,decimal_number_html(sum(matrix[0][goal]))))
			out.append("""			<span class="%s matrixar_vis1">%s</span>""" % (clz,decimal_number_html(sum(matrix[1][goal]))))
		out.append("""		</td>""")
		out.append("""	</tr>""")

		if goal>0:
			for i in range(len(matrix)):
				sumlist[i] = [ a + b for (a,b) in zip(sumlist[i],matrix[i][goal])]

	out.append("""	<tr>""")
	out.append("""		<th align="center" class="acid_tab_skip">all 1+</th>""")
	for actual in range(11):
		out.append("""		<td align="right" class="acid_tab_skip">""")
		if len(matrix)==8:
			out.append("""			<span class="matrixar_vis0">""")
			out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[0][actual]))
			out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[2][actual]))
			out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[4][actual]))
			out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sumlist[6][actual]))
			out.append("""			</span>""")
			out.append("""			<span class="matrixar_vis1">""")
			out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[1][actual]))
			out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[3][actual]))
			out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[5][actual]))
			out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sumlist[7][actual]))
			out.append("""			</span>""")
		elif len(matrix)==6:
			out.append("""			<span class="matrixar_vis0">""")
			out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[0][actual]))
			out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[2][actual]))
			out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[4][actual]))
			out.append("""			</span>""")
			out.append("""			<span class="matrixar_vis1">""")
			out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[1][actual]))
			out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[3][actual]))
			out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[5][actual]))
			out.append("""			</span>""")
		else:
			out.append("""			<span class="matrixar_vis0">%s</span>""" % decimal_number_html(sumlist[0][actual]))
			out.append("""			<span class="matrixar_vis1">%s</span>""" % decimal_number_html(sumlist[1][actual]))
		out.append("""		</td>""")
	out.append("""		<td align="right" class="acid_tab_skip">""")
	if len(matrix)==8:
		out.append("""			<span class="matrixar_vis0">""")
		out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
		out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[2])))
		out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[4])))
		out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sum(sumlist[6])))
		out.append("""			</span>""")
		out.append("""			<span class="matrixar_vis1">""")
		out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[1])))
		out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[3])))
		out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[5])))
		out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sum(sumlist[7])))
		out.append("""			</span>""")
	elif len(matrix)==6:
		out.append("""			<span class="matrixar_vis0">""")
		out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
		out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[2])))
		out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[4])))
		out.append("""			</span>""")
		out.append("""			<span class="matrixar_vis1">""")
		out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[1])))
		out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[3])))
		out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[5])))
		out.append("""			</span>""")
	else:
		out.append("""			<span class="matrixar_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
		out.append("""			<span class="matrixar_vis1">%s</span>""" % decimal_number_html(sum(sumlist[1])))
	out.append("""		</td>""")
	out.append("""	</tr>""")
	out.append("""	<tr><th align="center" class="acid_tab_skip"></th><td colspan="13" class="acid_tab_skip" style="padding-left:80px;padding-right:80px;">""")
	if len(matrix)==8:
		out.append("""		<span class="matrixar_vis0">""")
		out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[2][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[4][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis3">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[6][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""		</span>""")
		out.append("""		<span class="matrixar_vis1">""")
		out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[3][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[5][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis3">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[7][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""		</span>""")
	elif len(matrix)==6:
		out.append("""		<span class="matrixar_vis0">""")
		out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[2][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[4][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""		</span>""")
		out.append("""		<span class="matrixar_vis1">""")
		out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[3][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[5][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""		</span>""")
	else:
		out.append("""		<span class="matrixar_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
		out.append("""		<span class="matrixar_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
	out.append("""	</td></tr>""")
	out.append("""</table></form>""")
	return out