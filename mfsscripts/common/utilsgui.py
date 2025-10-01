import os
import math
import traceback

nnbsp = "&#8239;" # narrow non-breaking space

from common.utils import *

class MFSFieldStorage:
	def __init__(self):
		self.data = {}
	def __repr__(self):
		return repr(self.data)
	def __str__(self):
		return str(self.data)
	def __contains__(self,key):
		return key in self.data
	def __iter__(self):
		return iter(self.data.keys())
	def initFromURL(self):
		for k,v in xurllib.parse_qsl(os.environ["QUERY_STRING"]):
			if k in self.data:
				if type(self.data[k])==str:
					x = self.data[k]
					self.data[k] = [x]
				self.data[k].append(v)
			else:
				self.data[k] = v
	def getvalue(self,key,default=None):
		if key in self.data:
			if type(self.data[key]) is list and len(self.data[key])>=1:
				return self.data[key][0]
			return self.data[key]
		return default
	def getint(self,key,default=None):
		try:
			return int(self.getvalue(key,default))
		except Exception:
			return default
	def getstr(self,key,default=None):
		return str(self.getvalue(key,default))
	def pop(self,key):
		return self.data.pop(key)
	def append(self, key, value):
		self.data[key]=value

	def createhtmllink(self, update):
		fields = self
		c = []
		for k in fields:
			if k not in update:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						c.append("%s=%s" % (k,urlescape(el)))
				elif type(f) is str:
					c.append("%s=%s" % (k,urlescape(f)))
		for k,v in update.items():
			if v!="":
				c.append("%s=%s" % (k,urlescape(v)))
		return "mfs.cgi?%s" % ("&amp;".join(c))

	def createrawlink(self, update):
		fields = self
		c = []
		for k in fields:
			if k not in update:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						c.append("%s=%s" % (k,urlescape(el)))
				elif type(f) is str:
					c.append("%s=%s" % (k,urlescape(f)))
		for k,v in update.items():
			if v!="":
				c.append("%s=%s" % (k,urlescape(v)))
		return ("mfs.cgi?%s" % ("&".join(c))).replace('"','').replace("'","")

	def createorderlink(self, prefix,columnid):
		fields = self
		ordername = "%sorder" % prefix
		revname = "%srev" % prefix
		orderval = fields.getint(ordername,0)
		revval = fields.getint(revname,0)
		return self.createhtmllink({revname:"1"}) if orderval==columnid and revval==0 else self.createhtmllink({ordername:str(columnid),revname:"0"})

	def createinputs(self, ignorefields):
		fields = self
		for k in fields:
			if k not in ignorefields:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						yield """<input type="hidden" name="%s" value="%s">""" % (k,htmlentities(el))
				elif type(f) is str:
					yield """<input type="hidden" name="%s" value="%s">""" % (k,htmlentities(f))
		return
	

try:
	import urllib.parse as xurllib
except ImportError:
	import urllib as xurllib

def htmlentities(str):
	try:
		return str.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace("'",'&apos;').replace('"','&quot;')
	except:
		return ""
	
def urlescape(str):
	return xurllib.quote_plus(str)



# Generates a simple vertical table with a title row (if provided) and individual td styling (if provided)
def html_table_vertical(title, tdata, tablecls=""):
	out=[]
	out.append("""<table class="FR vertical no-hover %s">""" % tablecls)
	if title:
		out.append(""" <thead><tr><th colspan="2">%s</th></tr></thead>""" % title)
	out.append(""" <tbody>""")
	for row in tdata:
		out.append("""		<tr><th>%s</th>""" % str(row[0]))
		out.append("""		<td class="%s">%s</td></tr>""" % ("center" if len(row)<3 else row[2], str(row[1])))
	out.append(""" </tbody>""")
	out.append("""</table>""")
	return "\n".join(out)

# Generates a SVG-based round knob for selecting (from 2 to 6) different options
# id - string knob unique identifier
# r - circle radius
# wh - tuple (width, height) of svg picture
# cxy - tuple (cx,cy) center of knob circle 
# opts - a list of options, each option is a tuple: (degrees (0=north), option_title, option_subtitle, onclick_function)
# store - flag if knob position should be stored in the browser session storage
# selected - selected option index
def html_knob_selector(id, r, wh, cxy, opts, store=True, selected=None):
	w=wh[0]
	h=wh[1]
	cx=cxy[0]
	cy=cxy[1]	
	rx = r+4  #x-offset from circle center to line break
	tx = 12   #x-text offset from line break
	store_cls = "" if store else "dont-store"

	degrees_arr = []
	fun_arr = []
	out =[]

	# prepare onclick-s for all options
	for i in range(len(opts)): #display titles, subtitles
		opt=opts[i]
		degrees=opt[0]
		name=opt[1]
		subtitle=opt[2]
		if len(name)<4:
			name= name+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" if degrees>0 else "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + name
		anchor = "start" if degrees>0 else "end"
		x = cx+rx+tx if degrees>0 else cx-rx-tx #text x-pos
		y = cy + math.tan((90+abs(degrees)) * math.pi / 180)*rx + 3 #text y-pos
		y = max(y,10)
		#setup a function (w/timeout) to call on a one of knob's title click
		out.append("""<g onclick="rotateKnob('%s',%u);setTimeout(() => {%s;}, 300);"><text class="option-name" style="text-anchor: %s;" x="%f" y="%f">%s</text>""" % (id,degrees,opt[3],anchor,x,y,name))
		if subtitle!=None: #display option's subtitle if there is any
			out.append("""<text class="option-sub-name" style="text-anchor: %s;" x="%f" y="%f">%s</text>""" % (anchor,x,y+12,subtitle))
		out.append("""</g>""")
		degrees_arr.append(str(degrees))
		#setup a function (w/timeout) to call on a knob circle click (auto rotate) 
		fun_arr.append("setTimeout(() => {%s;}, 300);" % opt[3].replace("'", '&quot;')) 
	
	# knob's circle onclick rotation
	knob_onclick = """rotateNextKnob('""" +id+ """',["""
	knob_onclick += ",".join(degrees_arr)
	knob_onclick += """],[' """ 
	knob_onclick += "', '".join(fun_arr)
	knob_onclick += """ '])"""
	out.append("""<g transform="translate(%f,%f)" onclick="%s">""" % (cx, cy, knob_onclick))

	#draw lines
	for i in range(len(opts)):
		opt=opts[i]
		degrees=opt[0]
		anchor = "start" if degrees>0 else "end"
		x = rx if degrees>0 else -rx #line break x-pos
		x1 = x+8 if degrees>0 else x-8
		y = math.tan((90+abs(degrees)) * math.pi / 180)*rx #line break y-pos
		out.append("""<polyline class="line" points="0 0 %f %f %f %f"/>""" % (x,y,x1,y))		

	out.append("""<circle cx="0" cy="0" r="%f" class="circle-background" /><circle cx="0" cy="0" r="%f" class="circle-foreground" />""" % (r,r))
	if selected==None:
		initial_rot = 'data-initial-rotation="%u"' % opts[0][0]
	else:
		initial_rot = 'data-selected-rotation="%u"' % opts[selected][0]
	out.append("""<g id="%s" class="knob-arrow %s" style="transform: rotate(var(--%s-knob-rotation));" %s><polygon points="0 2 2 1 2 -%f 0 -%f -2 -%f -2 1" /></g>""" % (id,store_cls,id,initial_rot,r+1,r+3,r+1))
	out.append("""</g></svg>""")
	out.insert(0,"""<svg viewbox="0 0 %f %f" width="%f" height="%f" class="knob-selector" xmlns="http://www.w3.org/2000/svg">""" % (w,h,w,h))
	return "\n".join(out)

#Helper for generating server chart's time-selector knob
def html_knob_selector_chart_range(id):
	options=[(-135,"2 days",  None,"AcidChartSetRange('%s',0)" % id),
			(-45,"2 weeks", None,"AcidChartSetRange('%s',1)" % id),
			(45,  "3 months",None,"AcidChartSetRange('%s',2)" % id),
			(135, "10 years",None,"AcidChartSetRange('%s',3)" % id)]
	return html_knob_selector(id,9,(170,34),(85,16),options,False)

# Generates a full span+svg tag with an icon
def html_icon(id, help='', scale=1, clz=''):
	if help:
		return '<svg class="icon" height="12px" width="12px" data-tt="%s"><use class="%s" transform="scale(%f)" xlink:href="#%s"/></svg>' % (help,clz,scale,id)
	else:
		return '<svg class="icon" height="12px" width="12px"><use class="%s" transform="scale(%f)" xlink:href="#%s"/></svg>' % (clz,scale,id)

def html_refresh_a(content, param=None, new_param_value=None):
	if param and new_param_value:
		return """<a class="VISIBLELINK" href="javascript:void(0);" onclick="refreshWithNewParam('%s','%s');">%s</a>""" % (param, new_param_value, content)
	else:
		return """<a class="VISIBLELINK" href="javascript:void(0);" onclick="refreshWithNewParam();">%s</a>""" % (content)

# Prints provided 'out' table with lines of text
def print_out(out):
	print("\n".join(out))

def hours2html(hours):
	return hours_to_str(hours, nnbsp)


# common auxiliary functions
def html_menu_subsections(org, fields, section):
	print("""<div class="submenu">""")
	for k in org.menu_subitems(section):
		name=org.sections_defs[k][0]
		if k in org.subsectionset:
			active = 'active'
			if len(org.subsectionset)<=1:
				hrefname=None
				hreficon=None
			else:
				hrefname=fields.createhtmllink({"subsections":k})
				hreficon=fields.createhtmllink({"subsections":"-".join(org.subsectionset-set([k]))})
		else:
			active = ''
			hrefname=fields.createhtmllink({"subsections":k})
			hreficon=fields.createhtmllink({"subsections":"-".join(org.subsectionset|set([k]))})
		for responsive in ["xs md", "sm lg"]:
			if hrefname:
				print("""<div class="submenu-item %s %s"><a href="%s">%s</a><a href="%s"><svg class="thumbtack %s"><use xlink:href="#icon-thumbtack"/></svg></a></div>""" % (responsive, active,hrefname,name,hreficon,active))
			else:
				print("""<div class="submenu-item %s %s">%s<svg class="thumbtack %s"><use xlink:href="#icon-thumbtack"/></svg></div>""" % (responsive, active,name,active))
	print("""</div>""")

def print_error(msg):
	out = []
	out.append("""<div class="tab_title ERROR">Oops!</div>""")
	out.append("""<table class="FR MESSAGE">""")
	out.append("""	<tr><td align="left"><span class="ERROR">An error has occurred:</span> %s</td></tr>""" % msg)
	out.append("""</table>""")
	print("\n".join(out))

def print_exception():
	# exc_type, exc_value, exc_traceback = sys.exc_info()
	try:
		print("""<div class="tab_title ERROR">Oops!</div>""")
		print("""<table class="FR MESSAGE">""")
		print("""<tr><td align="left"><span class="ERROR">An error has occurred. Check your MooseFS configuration and network connections. </span><br/>If you decide to seek support because of this error, please include the following traceback:""")
		print("""<pre>""")
		print(traceback.format_exc().strip())
		print("""</pre></td></tr>""")
		print("""</table>""")
	except Exception:
		print(traceback.format_exc().strip())
