import sys
from common.constants import *
from common.utils import *
from common.utilsgui import *

def render(dp, fields, vld):
	out = []
	out.append("""<div id="footer">""")
	if dp.master()!=None:
		instancename=""
		if dp.master().has_feature(FEATURE_INSTANCE_NAME):
			data,length = dp.master().command(CLTOMA_INSTANCE_NAME,MATOCL_INSTANCE_NAME)
			if length>0:
				pos = 0
				err = 0
				instancename,pos,err = get_string_from_packet(data,pos,err)
		if instancename=="":
			instancename="(no name)"
	else:
		instancename="(unknown)"
	# Instance name
	out.append("""<div class="footer-left" data-tt="gui_instance_name"><span class="lg sm">Instance: </span><b>%s</b></div>""" % instancename)	
	
	# Auto refresh switch
	out.append("""<div class="footer-left" style="padding-left: 14px;" data-tt="gui_refresh_switch">""")
	out.append(""" <label class="switch" for="auto-refresh" id="auto-refresh-slide"><input type="checkbox" id="auto-refresh" />""")
	out.append(""" <span class="slider round"><span class="slider-text"></span></span></label>""")
	out.append("""</div>""")

	# Refresh button
	out.append("""<div class="lg sm md footer-left" style="padding-left: 3px; padding-right: 6px;">""")
	out.append(""" <span id="refresh-button" data-tt="gui_refresh_manual">&nbsp;Refresh:</span>&nbsp;<span id="refresh-timestamp" data-tt="gui_refresh_timestamp">%s</span>""" % datetime_to_str(time.time()))
	out.append("""</div>""")

	# GUI and python version
	out.append("""<div class="lg sm md footer-right text-icon">""")
	prover=""
	guiver = VERSION
	ci = dp.get_clusterinfo()
	if ci:
		issues=vld.check_gui_ver(ci.strver, guiver)
		data_help = issues.data_help()
		icon = issues.icon()
		text_class = issues.css_class()
	else:
		data_help=""
		icon=""
		text_class=""
	out.append("""<span class="lg sm">GUI&nbsp;</span><span style="padding-bottom:0;">%s</span><span class="%s" data-tt="%s">v.%s%s</span><span class="lg sm">&nbsp;&nbsp;Python v.%u.%u</span>""" % (icon, text_class, data_help, VERSION, prover, sys.version_info[0], sys.version_info[1]))
	out.append("""</div>""")

	# Dark mode switch
	out.append("""<div class="footer-right" id="theme-toggle" data-tt="gui_dark_mode"><svg height="14px" width="14px"><use xlink:href="#icon-light"/></svg></div>""")
	
	# Tooltip on/off switch
	out.append("""<div class="footer-right" data-tt="gui_tt_onoff">""")
	out.append(""" <label class="switch" for="tt-onoff" id="tt-onoff-slide"><input checked type="checkbox" id="tt-onoff"/>""")
	out.append(""" <span class="slider round"><span class="slider-text" style="font-size: 11.5px;">?</span></span></label>""")
	out.append("""</div>""")

	out.append("""</div>""")
	out.append("""  <script type="text/javascript" src="assets/acidtab.js"></script>""")
	out.append("""  <script type="text/javascript" src="assets/guiscripts.js"></script>""")
	out.append("""</body>""")
	out.append("""</html>""")

	return out