import math

from common.constants import *
from common.utils import *
from common.utilsgui import *
from common.models import *

# Capacity etc. Warning/Error thresholds
trsh_wrn = TRESHOLD_WARNING
trsh_err = TRESHOLD_ERROR

padding = 0       # graph padding (all borders)
tile_padding = 10 # tile padding  

ms_top = 10            # master servers colum top position
ms_left = tile_padding # master servers column left position
ms_width = 260         # master server icon width
ms_height = 50         # master server icon height
ms_margin_top = 30     # master servers vertical spacing 

mls_tile_margin_top = 20 # metaloggers colum top margin
mls_left = tile_padding # metaloggers column left position
mls_width = ms_width    # metaloggers icon width
mls_height = 30         # metaloggers icon height
mls_margin_top = 30     # metaloggers vertical spacing

cs_top = ms_top    # chunk servers column top position
cs_left_1st = 490  # chunk servers 1st column left position
cs_left_2nd = 800  # chunk servers 2nd column left position
cs_width = ms_width     # chunk server icon width
cs_height = 50     # chunk server icon height
cs_margin_top = 40 # chunk servers vertical spacing 

net_backbone_hmargin = 12
net_backbone_x = ms_width+130

green =  'fill: var(--ok-clr);'
orange = 'fill: var(--warning-clr);'
red =    'fill: var(--error-clr);'
bold =   'font-weight: bold;'
left =   'text-anchor:start;'
middle = 'text-anchor:middle;'
right =  'text-anchor:end;'
em08 =  'font-size:0.8em;'
em09 =  'font-size:0.9em;'
em11 =  'font-size:1.1em;'
em12 =  'font-size:1.2em;'
em14 =  'font-size:1.4em;'

# def defs():
  # out = []
  # out.append('<defs>')
  # out.append('</defs>')
  # return "\n".join(out)

# def styles():
#   out = []
#   out.append('<style>')
#   out.append('</style>')
#   return "\n".join(out)

# Returns x.y.z version number (without "PRO" suffix)
def version_number(strver):
  if strver.find(' PRO') > 0:
    return strver[:-4]
  else:
    return strver

def humanize(number, thousand=1000, suff=''):
  if number==None: return 'n/a'
  number*=100
  scale=0
  while number>=99950:
    number = number//thousand
    scale+=1
  if number<995 and scale>0:
    b = (number+5)//10
    nstr = "%u.%u" % divmod(b,10)
  else:
    b = (number+50)//100
    if number>0 and b==0:
      nstr = "<&#8239;1" # number is small but not zero
    else:
      nstr = "%u" % b
  if scale>0:
    if (suff=='iB' or suff=='iB/s'):
      slist="-kMGTPEZY"
    else:
      slist=['','k','M',' Bn']
    return (nstr,"%s%s" % (slist[scale],suff))
  else:
    suff = 'B' if suff=='iB' else suff
    suff = 'B/s' if suff=='iB/s' else suff
    return (nstr,suff)

def humanize_str(number, thousand=1000, suff=''):
  if number==None: return 'n/a'
  (str, suff) = humanize(number, thousand, suff)
  return str+'&#8239;'+suff

def humanize_bytes(number, suff='iB'):
  if number==None: return 'n/a'
  return humanize(number, 1024, suff)

def humanize_bytes_str(number, suff='iB'):
  if number==None: return 'n/a'
  (str, suff) = humanize_bytes(number, suff)
  return str+'&#8239;'+suff

# Translates given percent (0..100) number into approximate text
def percent2approx(percent):
  if percent==0.0:
    return '0'
  if percent<0.1:
    return '<1&permil;'
  if percent<1:
    return '<1%'
  if percent<99:
    return '%.0f%%' % percent
  if percent>=100:
    return 'all'
  return '>%.0f%%' % percent

# Translates given tag to x,y by adding the "g transform" tag
def translate(tag, x, y, onclick=''):
  if onclick:
    return '<g transform="matrix(1,0,0,1,%s,%s)" style="cursor: pointer;" onclick="%s">\n %s\n</g>' % (x, y, onclick, tag)
  else:
    return '<g transform="matrix(1,0,0,1,%s,%s)">\n %s\n</g>' % (x, y, tag)

# Generate text with class clz at position x, y
def text(msg, x, y, clz, maxLength=0, style=''):
  if maxLength>0:
    if len(msg) > maxLength:
      if msg[maxLength-2] == '.':
        msg = (msg[:maxLength-2])
      else:
        msg = (msg[:maxLength-1])
      msg += '...' #'…'
  return '<text class="%s" style="%s" x="%s" y="%s">%s</text>' % (clz, style, x, y, msg)

# Generates arc at x, y (left-upper corner of a surrounding box) with given radius starting and ending at given angles: <path fill="none" stroke="#446688" stroke-width="3" d="M 184.64101615137756 170 A 40 40 0 1 0 115.35898384862246 170"></path>
def arc(x, y, radius, startAngle, endAngle, strokeWidth=8, clz=''):
  def  polarToCartesian(x, y, radius, angleInDegrees):
    angleInRadians = (angleInDegrees-90) * 0.0174532 #math.pi / 180.0
    return (x + (radius * math.cos(angleInRadians)),y + (radius * math.sin(angleInRadians)))
  x=x+radius+(strokeWidth/2.0)
  y=y+radius+(strokeWidth/2.0)
  (startx, starty) = polarToCartesian(x, y, radius, endAngle)
  (endx, endy) = polarToCartesian(x, y, radius, startAngle)
  largeArcFlag = '0' if endAngle - startAngle <= 180 else '1';
  points = 'M %f %f A %f %f 0 %s 0 %f %f' % (startx, starty, radius, radius, largeArcFlag, endx, endy)
  return'<path class="%s" style="stroke-width: %fpx" d="%s"/>' % (clz,strokeWidth,points)

# Generate arc gauge with name, green, orange or red depending on val threshold (75%/90%) 
def arc_gauge(x, y, name, label, val, label_style='', r=12, arcTo=120, strokeWidth=8): 
  out = []
  if name:
    out.append(text(name, x+r+strokeWidth/2.0, y, 'arc-gauge-name', 0, label_style))
  out.append(arc(x, y+strokeWidth/2.0, r, -arcTo, arcTo, strokeWidth, 'arc-gauge-bckgrd'))
  val = min(val, 1.0)
  val = max(val, 0.0)
  clz = 'arc-gauge-green' if val < trsh_wrn else ('arc-gauge-orange' if val < trsh_err else 'arc-gauge-red')
  endAngle = val*2.0*arcTo - arcTo
  endAngle = max(endAngle, -arcTo+1)
  out.append(arc(x, y+strokeWidth/2.0, r, -arcTo, endAngle, strokeWidth, clz))
  if label:
    if (type(label) is tuple):
      out.append(text(label[0], x+r+strokeWidth/2, y+r+strokeWidth+2, 'val-label-value',0,middle+em11))
      out.append(text(label[1], x+r+strokeWidth/2, y+r+strokeWidth+12, 'val-label-units',0,middle+em08))
    else:
      out.append(text(label, x, y+13, 'arc-gauge-label'))
  return "\n".join(out)

# # Generate horizontal linear % gauge with name, green, orange or red depending on val threshold (75%/90%) 
# def hgauge(name, val, x, y):
#   out = []
#   out.append(text(name, x, y, 'label'))
#   h = 5
#   w = ms_width/3
#   out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="1" ry="1" class="gauge-bckgrd"/>' % (x+3, y-h, w, h))
#   val = min(val, 1)
#   val = max(val, 0)
#   clz = 'gauge-green' if val < trsh_wrn else ('gauge-orange' if val < trsh_err else 'gauge-red')
#   w = val*w
#   w = max(w, 0.5)
#   out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="1" ry="1" class="%s"/>' % (x+3, y-h, w, h, clz))
#   return "\n".join(out)

# Generates svg text memory gauge
def svg_mem_gauge(name, val, x, y):
  out=[]
  out.append(text(name, x, y, 'val-label-name'))
  (hval, units)=humanize_bytes(val)
  out.append(text(hval, x, y+15, 'val-label-value'))
  out.append(text(units, x, y+28, 'val-label-units'))
  return "\n".join(out)

# Generates div text memory gauge
def html_mem_gauge(name, val, color_class=''):
  (hval, units)=humanize_bytes(val)
  out=[]
  out.append('<table class="gauge">')
  if name:
    out.append('<tr><td class="name">%s</td></tr>' % name)
  out.append('<tr><td class="txt-value %s">%s</td></tr>' % (color_class,hval))
  out.append('<tr><td class="units %s">%s</td></tr>' % (color_class, units))
  out.append('</table>')
  return "\n".join(out)

def svg_tag(w, h, svg_body):
  return '<svg viewbox="0 0 %u %u" width="%u" height="%u" xmlns="http://www.w3.org/2000/svg">%s</svg>' % (w, h, w, h, svg_body)

# Generates a full span+svg tag with an icon
def html_icon(id, scale=1.0):
  return '<span class="icon"><svg height="%fpx" width="%fpx"><use transform="scale(%f)" xlink:href="#%s"/></svg></span>' % (12*scale, 12*scale, scale, id)

##############################################
# Generate cluster state html
def html_cluster_state(info):
  out = []

  out.append('<div class="info-cards">')
  if 'totalspace' in info and 'availspace' in info and info['totalspace']!=0:
    # Total and available size
    out.append('<div class="card pointer" onclick="showGraphInfo(\'IG\')" data-tt="st_card_cluster_space">')
    out.append('<table class="info-table">')
    out.append('<tr class="low"><td class="bold">Total space</td><td>Free</td></tr>')
    out.append('<tr><td>%s</td>' % svg_tag(2*20+8,2*20+8, arc_gauge(0,0, '', humanize_bytes(info['totalspace']), float(info['totalspace']-info['availspace'])/float(info['totalspace']), bold, 20, 120, 8))) 
    out.append('<td>%s</td></tr>' % html_mem_gauge('', info['availspace']))
    out.append('</table>')
    out.append('</div>') #card Total and available size
    out.append('<div class="card-divider"></div>')

  #Health
  severity=0
  if 'chunks_progress' in info and info['chunks_progress']==0:
    if 'chunks_total' in info and info['chunks_total']>0 and 'chunks_missing' in info and info['chunks_missing']>0:
      severity=2
      data_health='<td class="label-value"><span class="error-txt">%s missing</span></td></tr>' % percent2approx(100.0*info['chunks_missing']/float(info['chunks_total']))
    elif 'chunks_total' in info and info['chunks_total']>0 and 'chunks_undergoal' in info and 'chunks_endangered' in info and (info['chunks_undergoal']+info['chunks_endangered'])>0:
      severity=1
      data_health='<td class="label-value"><span class="warning-txt">%s in danger</span></td></tr>' % percent2approx(100.0*(info['chunks_undergoal']+info['chunks_endangered'])/float(info['chunks_total']))
    else:
      data_health='<td class="label-value">Normal</td></tr>'
  else:
    data_health='<td class="label-value"><span class="warning-txt">checking…</span></td></tr>'
  if 'servers_warnings' in info and len(info['servers_warnings'])>0:
    severity = 1 if severity < 2 else severity
    servers_health='<td class="label-value"><span class="warning-txt">%s abnormal</span></td></tr>' % len(info['servers_warnings'])
  else:
    servers_health='<td class="label-value">Normal</td></tr>'
  if severity==2:
    icon=html_icon('icon-error', 1.2)
  elif severity==1:
    icon=html_icon('icon-warning', 1.2)
  else:
    icon=html_icon('icon-ok-circle', 1.2)
  out.append('<div class="card">')
  out.append('<table class="info-table">')
  out.append('<tr><td colspan="2" class="card-title"><div class="text-icon" data-tt="st_card_health">Health %s</div></td></tr>' % icon)
  if 'leaderfound' in info and info['leaderfound']==1:
    out.append('<tr class="pointer" onclick="showGraphInfo(\'IC\',\'ICsclassid=-1\')"><td class="label-name">Data:</td>')
    out.append(data_health)
  else:
    out.append('<tr class="pointer" onclick="showGraphInfo(\'IG\')"><td class="label-name">Leader:</td>')
    out.append('<td class="label-value"><span class="error-txt">missing</span></td></tr>')
  out.append('<tr><td class="label-name">Servers:</td>')
  out.append(servers_health)
  out.append('</table>')
  out.append('</div>') #card health
  out.append('<div class="card-divider"></div>')

  #Usage
  out.append('<div class="card">')
  out.append('<table class="info-table">')
  out.append('<tr><td colspan="4" class="card-title" data-tt="st_card_usage">Usage</td></tr>') 
  out.append('<tr class="pointer"">')
  out.append('<td class="label-name" onclick="showGraphInfo(\'IG\')">Files:</td>')
  if 'files' in info and 'trfiles' in info:
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\')">%s</td>' % humanize_str(info['files']-info['trfiles']))
  else:
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\')">Unknown</td>')
  out.append('<td class="label-name" onclick="showGraphInfo(\'MS\')">Mounts:</td>')
  if 'mounts' in info:
    out.append('<td class="label-value" onclick="showGraphInfo(\'MS\')">%s</td>' % humanize_str(info['mounts']))
  else:
    out.append('<td class="label-value" onclick="showGraphInfo(\'MS\')">Unknown</td>')
  out.append('</tr>')
  out.append('<tr class="pointer"">')
  out.append('<td class="label-name" onclick="showGraphInfo( \'IG\')">Folders:</td>')
  if 'dirs' in info:
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\')">%s</td>' % humanize_str(info['dirs']))
  else:
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\'")>Unknown</td>')
  out.append('<td class="label-name" onclick="showGraphInfo( \'IG\')">Trash size:</td>')
  if 'trspace' in info :
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\')">%s</td>' % humanize_bytes_str(info['trspace']))
  else:
    out.append('<td class="label-value" onclick="showGraphInfo( \'IG\')">Unknown</td>')
  out.append('</tr>')
  out.append('</table>')
  out.append('</div>') #card usage

  out.append('<div class="card-divider"></div>')

  #Performance
  out.append('<div class="card" data-tt="st_card_performance">')
  out.append('<table class="info-table pointer" onclick="showGraphInfo(\'MO\')">')
  out.append('<tr><td colspan="4" class="card-title">Performance</td></tr>') 
  out.append('<tr>')
  if 'bread_ext' in info and info['bread_ext']!=None:
    out.append('<td class="label-name"">Outbound:</td>')
    out.append('<td class="label-value">%s</td>' % humanize_bytes_str(info['bread_ext'],'iB/s'))
  else:
    out.append('<td></td>')
    out.append('<td></td>')
  out.append('<td class="label-name">R/W ops:</td>')
  if 'rops' in info and 'wops' in info and info['rops']!=None and info['wops']!=None:
    out.append('<td class="label-value">%s/s</td>' % humanize_str(info['rops']+info['wops']))
  else:
    out.append('<td class="label-value">n/a</td>')
  out.append('</tr>')
  out.append('<tr>')
  if 'bwrite_ext' in info and info['bwrite_ext']!=None:
    out.append('<td class="label-name">Inbound:</td>')
    out.append('<td class="label-value">%s</td>' % humanize_bytes_str(info['bwrite_ext'],'iB/s'))
  else:
    out.append('<td></td>')
    out.append('<td></td>')
  out.append('<td class="label-name">All ops:</td>')
  if 'ops' in info and info['ops']!=None:
    out.append('<td class="label-value">%s/s</td>' % humanize_str(info['ops']))
  else:
    out.append('<td class="label-value">n/a</td>')
  out.append('</tr>')
  out.append('</table>')
  out.append('</div>') #card performance


  #Licence (if applicable)
  if 'licmaxsize' in info and 'currentsize' in info and info['licmaxsize']!=0  and info['currentsize']!=None and info['licmaxsize']!=None:
    licleft=info['licmaxsize']-info['currentsize']
    out.append('<div class="card-divider"></div>')
    out.append('<div class="card pointer" onclick="showGraphInfo(\'LI\')" data-tt="st_card_licence">')
    out.append('<table class="info-table">')
    if licleft < 0:
      liclefttxt = 'Over'
      licleftclass = 'ERROR'
    else:
      liclefttxt = 'Left'
      licleftclass = ''
    out.append('<tr class="low"><td class="bold">Licence</td><td>%s</td></tr>' % liclefttxt)
    out.append('<tr><td>%s</td>' % svg_tag(2*20+8,2*20+8,arc_gauge(0,0, '', humanize_bytes(info['licmaxsize']), (float(info['currentsize'])/float(info['licmaxsize'])), bold, 20, 120, 8))) 
    out.append('<td>%s</td></tr>' % html_mem_gauge('', abs(licleft), licleftclass))
    out.append('</table>')
    out.append('</div>') #card Licence

  # out.append('<div class="cards-underline"></div>')  
  out.append('</div> <!-- info-cards -->') #info-cards

  return "\n".join(out)

##############################################
# Generate master server icon 
def mserver(srv, net_offset_y, leader_strver):
  #TODO: add metadata in-sync / out-of-sync warnings
  live = 'live' in srv and srv['live']==1
  led_cls = 'led-normal'
  srv_tt = []
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (ms_width, ms_height))
  out.append(text(srv['name'], 21, 20, "srv-name", 17))
  if not live:
    led_cls = 'led-error'
    srv_tt.append('follower_unreachable')
    out.append(text('UNREACHABLE', 7, 35, 'state-error'))
  if live:
    if 'strver' in srv and srv['strver'] != '0.0.0':
      ver_cls = "text-version"
      if leader_strver!='' and leader_strver!=srv['strver']:
        ver_cls += "-mismatch"  
        led_cls = 'led-warning'
      out.append('<line class="srv-line-ver" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (ms_width-17, 0, ms_width-17, ms_height))
      out.append('<g transform="translate(%f,%f) rotate(90)">' % (ms_width-13, ms_height/2))
      out.append(text(version_number(srv['strver']), 0, 0, ver_cls))
      out.append('</g>')
    if 'statestr' in srv:
      if len(srv['statestr'])>15:
        words=srv['statestr'].split(' ')
        line1=" ".join(words[:2])
        line2=" ".join(words[2:])
        out.append(text(line1, 7, 34, 'srv-state-small'))
        out.append(text(line2, 7, 46, 'srv-state-small'))
      else:
        out.append(text(srv['statestr'], 7, 40, 'srv-state'))
      if not srv['statestr'] in ['LEADER', 'FOLLOWER', 'MASTER', '-']:
        led_cls = 'led-warning'
    if 'cpuload' in srv:
      # out.append(hgauge('CPU', srv['cpuload'], 28, 42))
      x=cs_width-100
      out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 7, x, ms_height-7))
      out.append(arc_gauge(x+5, 14, 'CPU', '', srv['cpuload']))
      if srv['cpuload'] > trsh_wrn:
        led_cls = 'led-warning'
    if 'memory' in srv:
      x=ms_width-58
      out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 7, x, ms_height-7))
      out.append(text('RAM', x+20, 14, 'val-label-name'))
      (memval, memunits)=humanize_bytes(srv['memory'])
      out.append(text(memval, x+20, 29, 'val-label-value'))
      out.append(text(memunits, x+20, 42, 'val-label-units'))
  if 'ip' in srv and 'port' in srv:
    out.append(text(srv['ip'], ms_width+5, net_offset_y-3, 'ip-address-ms'))
    out.append(text(':'+str(srv['port']), ms_width+5, net_offset_y+12, 'ip-port-ms'))
  out.append('<circle cx="12" cy="14" r="5" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (ms_width, ms_height))
  if srv_tt:
    # wrap with <g> to display tooltip over entire rect area
    out.append('</g>')
    out.insert(0, '<g data-tt="%s">' % ",".join(srv_tt))
  return "\n".join(out)

##############################################
# Generate metalogger server icon
def mlserver(srv, net_offset_y, leader_strver):
  led_cls = 'led-normal'
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (mls_width, mls_height))
  out.append(text(srv['name'], 21, 20, "srv-name", 24))
  if 'strver' in srv and srv['strver'] != '0.0.0':
    ver_cls = "text-version"
    if leader_strver!='' and leader_strver!=srv['strver']:
      ver_cls += "-mismatch"  
      led_cls = 'led-warning'
    out.append('<line class="srv-line-ver" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (mls_width-40, 0, mls_width-40, mls_height))
    out.append(text(version_number(srv['strver']), mls_width-20, mls_height/2+5, ver_cls))
  if 'ip' in srv :
    out.append(text(srv['ip'], ms_width+5, net_offset_y-3, 'ip-address-ms'))
  out.append('<circle cx="12" cy="14" r="5" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (mls_width, mls_height))
  return "\n".join(out)

##############################################
# Generate chunk server icon
def cserver(srv, net_offset_y, leader_strver):
  live = 'live' in srv and srv['live']==1
  maintenance = 'maintenance' in srv and srv['maintenance']==1
  led_cls = 'led-normal'
  srv_tt = []
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (ms_width, ms_height))
  out.append(text(srv['name'], 21, 20, "srv-name", 12))
  if not live:
    if maintenance:
      led_cls = 'led-maintenance'
      srv_tt.append('cs_unreachable_maintain')
      out.append(text('MAINTENANCE', 7, 35, "state-maintenance"))
      out.append(text('UNREACHABLE', 94, 35, "state-error"))
    else:
      led_cls = 'led-error'
      srv_tt.append('cs_unreachable')
      out.append(text('UNREACHABLE', 7, 35, "state-error"))
  
  if live:
    if maintenance:
      led_cls = 'led-maintenance'
      srv_tt.append('cs_maintain')
      out.append(text('MAINTENANCE', 7, 35, "state-maintenance"))
    if 'strver' in srv and srv['strver'] != '0.0.0':
      ver_cls = "text-version"
      if leader_strver!='' and leader_strver!=srv['strver']:
        ver_cls += "-mismatch"  
        led_cls = 'led-warning'
      out.append('<line class="srv-line-ver" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (cs_width-17, 0, cs_width-17, cs_height))
      out.append('<g transform="translate(%f,%f) rotate(90)">' % (cs_width-13, cs_height/2))
      out.append(text(version_number(srv['strver']), 0, 0, ver_cls))
      out.append('</g>')
    if not maintenance and 'queue_state_msg' in srv:
      out.append(text('Queue:', 7, 33, 'label',0,left))
      if srv['queue_state_str'] == 'Overloaded': #FIXME: use queue_state and a numeric constant
        state_cls = 'state-error'
        led_cls = 'led-warning'
      else:
        state_cls = 'state-normal'
      out.append(text(srv['queue_state_msg'], 7, 44, state_cls))

    if 'hdd_reg_total' in srv and 'hdd_reg_used' in srv:
      x=cs_width-132
      out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 7, x, cs_height-7))
      if srv['hdd_reg_total']!=0:
        out.append(arc_gauge(x+6, 14, 'HDD', '', float(srv['hdd_reg_used'])/float(srv['hdd_reg_total']), bold))
        if srv['hdd_reg_used']/srv['hdd_reg_total'] > trsh_wrn:
          led_cls = 'led-warning'
      else:
        out.append(text('HDD', x+22, 14, 'arc-gauge-name', 0, bold+middle))
      x+=36
      # out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 18, x, cs_height-7))
      out.append(text('Free', x+20, 14, 'val-label-name'))
      (hddfree, hddunits)=humanize_bytes(srv['hdd_reg_total']-srv['hdd_reg_used'])
      out.append(text(hddfree, x+20, 29, 'val-label-value'))
      out.append(text(hddunits, x+20, 42, 'val-label-units'))
      x+=36
      out.append(text('Health', x+20, 14, 'val-label-name'))
      if 'hdds_status' in srv:
        if srv['hdds_status'].startswith('error'):
          srv_tt.append('hdd_damage');
          out.append('<use xlink:href="#icon-error" x="%f" y="19" style="%s"/>' % (x+13, red))
          out.append(text('Errors', x+20, 42, 'value',0,middle+red+em08))
          led_cls = 'led-warning'
        elif srv['hdds_status'].startswith('warning'):
          srv_tt.append('hdd_error');
          out.append('<use xlink:href="#icon-warning" x="%f" y="19" style="%s"/>' % (x+13, orange))
          out.append(text('Warnings', x+20, 42, 'value',0,middle+orange+em08))
          led_cls = 'led-warning'
        elif srv['hdds_status'].startswith('ok'):
          out.append('<g transform="translate(%f,21)"><use xlink:href="#icon-ok-circle" transform="scale(1.5)"/></g>' % (x+11))
        else:
          out.append(text('unknown', x+20, 42, 'value',0,middle))
      else:
        out.append(text('unknown', x+20, 42, 'value',0,middle))

  if 'ip' in srv and 'port' in srv:
    out.append(text(srv['ip'], -5, net_offset_y-3, 'ip-address-cs'))
    out.append(text(":"+str(srv['port']), -5, net_offset_y+12, 'ip-port-cs'))

  out.append('<circle cx="12" cy="14" r="5" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (ms_width, ms_height))
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (ms_width, ms_height))
  if srv_tt:
    # wrap with <g> to display tooltip over entire rect area
    out.append('</g>')
    out.insert(0, '<g data-tt="%s">' % ",".join(srv_tt))
  return "\n".join(out)

##############################################
# Generate SVG picture of a cluster
##############################################
def svg_mfs_graph( masterservers, metaloggers, chunkservers, leader_strver):
  svg_h = 0
  svg_w = net_backbone_x

  out = []  
  # out.append(defs())
  # out.append(styles())

  # Print master servers
  net_offset_y = 0.6 * ms_height
  x = ms_left
  y = ms_top
  net_start_y = y
  net_end_y = 0

  if len(masterservers) > 0:
    # master servers tile
    tile_width=(net_backbone_x-net_backbone_hmargin)-(x-tile_padding)
    tile_height=2*tile_padding+(len(masterservers)-1)*(ms_height+ms_margin_top)+ms_height+22
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x-tile_padding, y-tile_padding, tile_width, tile_height))
    out.append(text('Master servers', x-tile_padding+tile_width-8, y-tile_padding+tile_height-9, 'tile-title-right'))
    
    for ms in masterservers:
      out.append('<line class="network-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x+ms_width, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(mserver(ms,net_offset_y,leader_strver), x, y, "showGraphInfo('IM')"))
      net_end_y = y+net_offset_y
      y+=ms_height+ms_margin_top
    y=ms_top+tile_height
    svg_h=max(svg_h, y+ms_height)

  # Print metaloggers
  if len(metaloggers) > 0:
    net_offset_y = 0.7 * mls_height
    x=mls_left
    y=y+mls_tile_margin_top
    # metaloggers tile
    tile_width=(net_backbone_x-net_backbone_hmargin)-(x-tile_padding)
    tile_height=2*tile_padding+(len(metaloggers)-1)*(mls_height+mls_margin_top)+mls_height+22
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x-tile_padding, y-tile_padding, tile_width, tile_height))
    out.append(text('Metaloggers', x-tile_padding+tile_width-8, y-tile_padding+tile_height-9, 'tile-title-right'))
    svg_h=max(svg_h, y-tile_padding+tile_height)

    for mls in metaloggers:
      out.append('<line class="network-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x+ms_width, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(mlserver(mls,net_offset_y,leader_strver), x, y, "showGraphInfo('IM')"))
      net_end_y = y+net_offset_y
      y+=mls_height+mls_margin_top
    y-=mls_margin_top
  
  # Print chunk servers
  net_offset_y = 0.6 * cs_height
  odd = True
  cy_1st = cs_top
  cy_2nd = cs_top+cs_margin_top
  net_start_y = min(cy_1st,net_start_y)
  if len(chunkservers) > 0:
    # chunk servers tile
    x=net_backbone_x+net_backbone_hmargin
    y=cy_1st-tile_padding

    tile_width=cs_left_2nd+cs_width+tile_padding-(net_backbone_x+net_backbone_hmargin)
    tile_height=tile_padding+(math.ceil(len(chunkservers)/2)-1)*(cs_height+cs_margin_top)+cs_height+tile_padding+24
    if len(chunkservers) % 2 == 0:
      tile_height+=(cy_2nd-cy_1st)
    
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x, y, tile_width, tile_height))
    out.append(text('Chunkservers', x+8, y+tile_height-9, 'tile-title-left'))
    svg_h=max(svg_h, y+tile_height)
    svg_w=max(svg_w, x+tile_width)
    for cs in chunkservers:
      if odd:
        x = cs_left_1st
        y = cy_1st
        cy_1st += cs_height+cs_margin_top
      else:
        x = cs_left_2nd
        y = cy_2nd
        cy_2nd += cs_height+cs_margin_top
      net_end_y = max(net_end_y,y+net_offset_y)
      out.append('<line class="network-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(cserver(cs,net_offset_y,leader_strver), x, y, "showGraphInfo('CS-HD', 'CScsid=%s&HDdata=%s')" % (cs['id'],cs['id'])))
      odd = not odd
    cy_1st-=cs_margin_top
    cy_2nd-=cs_margin_top
  svg_h=max(svg_h, cy_1st)
  svg_h=max(svg_h, cy_2nd)
  
  net_start_y += net_offset_y
  # net_end_y += net_offset_y

  # Print network backbone line
  out.append('<line class="network-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (net_backbone_x, net_start_y, net_backbone_x, net_end_y))

  # Adjust overall height, width and padding
  out.insert(0, '<g transform="matrix(1,0,0,1,%s,%s)">' % (padding, padding))
  svg_h+=2*padding
  svg_w+=2*padding
  out.insert(0, '<svg height="%d" width="%d">' % (svg_h, svg_w))

  out.append('</g>')
  out.append('</svg>')
  return "\n".join(out)



###################################################
### Prepare data for rendering and order render ###
###################################################

def render(dp, fields, vld):
  servers_warnings = []

  ci=dp.get_clusterinfo()
  
  # Prepare master servers info
  masterservers = []
  for ms in dp.get_masterservers(0): # sort by ip/port
    server = {
      "name": dp.cluster.masterhost,
      "ip": ms.strip,
      "port": ms.port,
      "strver": ms.strver,
      "statestr": ms.statestr,
      "cpuload": (ms.syscpu+ms.usercpu)/100.0,
      "memory": ms.memusage,
      "live": 1 if ms.is_active() else 0
    }
    if server['live']!=1 or ci.strver!=ms.strver or (not ms.statestr in ['LEADER', 'FOLLOWER', 'MASTER', '-']) or server['cpuload'] > trsh_wrn:
      servers_warnings.append(ms.strip)
    masterservers.append(server)

  metaloggers = []
  for ml in dp.get_metaloggers(2): # sort by ip/port
    server = {
      "name": ml.host,
      "ip": ml.strip,
      "strver": ml.strver,
    }
    if ci.strver!=ml.strver:
      servers_warnings.append(ml.strip)
    metaloggers.append(server)
  
  chunkservers = []
  (hdds_all, scanhdds)=dp.get_hdds("ALL")
  for cs in dp.get_chunkservers(): # default sort by ip/port
    (hdds_status, hdds)=dp.cs_hdds_status(cs, hdds_all+scanhdds)
    if cs.is_maintenance_off() or not dp.cluster.leaderfound():
      maintenance = 0
    else:
      maintenance = 1
    srv = {
      "id": "%s:%s" % (cs.strip,cs.port),
      "name": cs.host,
      "strver": cs.strver,
      "ip": cs.strip,
      "port": cs.port,
      "live": 1 if cs.is_connected() else 0,     # if (cs.flags&1)!=0: <- dead server
      "maintenance": maintenance, # 1 - server in maintenance mode
      "queue": cs.queue,
      "queue_state": cs.queue_state,
      "queue_state_str": cs.queue_state_str,
      "queue_state_msg": cs.queue_state_msg,
      "flags": cs.flags,
      "hdd_reg_total": cs.total,
      "hdd_reg_used": cs.used,
      "hdd_rem_total": cs.tdtotal,
      "hdd_rem_used": cs.tdused,
      "hdds_status": hdds_status,
      "hdds": None #unused: hdds
      }
    chunkservers.append(srv)

    if srv['live']!=1 or srv['maintenance']!=0 or ci.strver!=cs.strver or srv['queue_state'] == CS_LOAD_OVERLOADED or (srv['hdd_reg_total']!=0 and srv['hdd_reg_used']/srv['hdd_reg_total'] > trsh_wrn) or hdds_status!='ok':
      servers_warnings.append(cs.strip)
  
  # Prepare general cluster info 
  # Gather IOPS and W/R IOPS 
  sessions=dp.get_sessions(None) # don't sort
  rops_c_total = 0
  rops_l_total = 0
  wops_c_total = 0
  wops_l_total = 0
  ops_c_total = 0
  ops_l_total = 0
  for ses in sessions: #summing both last and current hour operations
    ops_c_total += sum(ses.stats_c)
    ops_l_total += sum(ses.stats_l)
    if (dp.stats_to_show>16):
      rops_c_total=ses.stats_c[16] # 17: read current hour
      wops_c_total=ses.stats_c[17] # 18: write current hour
      rops_l_total=ses.stats_l[16] # 17: read last hour
      wops_l_total=ses.stats_l[17] # 18: write last hour
  #if more than half of the minute passed (of current hour), calculate ops per second based on current hour stats
  if seconds_since_beginning_of_hour()>30.0: 
    ops = float(ops_c_total)/float(seconds_since_beginning_of_hour())
    rops = float(rops_c_total)/float(seconds_since_beginning_of_hour())
    wops = float(wops_c_total)/float(seconds_since_beginning_of_hour())
  else: # take last (previous) hour stats
    ops = float(ops_l_total)/3600.0
    rops = float(rops_l_total)/3600.0
    wops = float(wops_l_total)/3600.0
  if (dp.stats_to_show<=16):
    wops = None # this master doesn't support W/R stats
    ops = None

  # Gather throughput stats
  bread_ext = None
  bwrite_ext = None
  if dp.cluster.master().version_at_least(4,57,0): # mountbytrcvd, mountbytsent available from v.4.57.0
    # Get 60 minutes average of given (single row) master server chart data
    def get_mschart_60min_avg(no):
      raw = 0
      chrange = 0   # 1 minute ticks
      mccount = 60  # take 60 minutes (or less if 60 minutes not available)
      _,base,datadict = get_charts_multi_data(dp.cluster.master(), no*10+chrange, mccount)
      if datadict!=None and len(datadict)>0:
        val = 0
        count = 0
        ch1data,_,_,_,mul,div = datadict[0] # ch1data,ch2data,ch3data,ts,mul,div
        mul,div = adjust_muldiv(mul,div,base)
        chdata = charts_convert_data(ch1data,mul,div,raw)
        for chval in chdata:
          if chval!=None:
            val += chval
            count += 1
        if count>0:
          return val / count
        else:
          return None
        
    bread_ext = get_mschart_60min_avg(68) # 68 mountbytrcvd
    bwrite_ext = get_mschart_60min_avg(69) # 69 mountbytsent


  licence=dp.get_licence()
  licmaxsize = licence.licmaxsize if licence != None else None
  currentsize = licence.currentsize if licence != None else None
  # Get sums of missing, undergoal and endangered  (for all sclasses)
  (mx_summary,progressstatus)=dp.get_matrix_summary()
  clusterinfo = {
    "leaderfound" : 1 if dp.cluster.leaderfound() else 0,
    "strver": ci.strver,
    "totalspace": ci.totalspace,
    "availspace": ci.availspace,
    "freespace": ci.freespace,
    "dirs": ci.dirs,
    "files": ci.files,
    "trfiles": ci.trfiles,
    "trspace": ci.trspace,
    "chunks_progress": progressstatus,
    "chunks_total": mx_summary[MX_COL_TOTAL],
    "chunks_missing": mx_summary[MX_COL_MISSING],
    "chunks_undergoal": mx_summary[MX_COL_UNDERGOAL],
    "chunks_endangered": mx_summary[MX_COL_ENDANGERED],
    "servers_warnings": servers_warnings,
    "mounts": len(sessions),
    "ops": ops,	  #all (metadata and read and write) operations per second
    "rops": rops, #read operations per second
    "wops": wops, #write operations per second
    "bread_ext": bread_ext,   #traffic read by all CS - only from clients (ext)
    "bwrite_ext": bwrite_ext, #traffic write to all CS - only from clients (ext)
    "licmaxsize": licmaxsize, 
    "currentsize": currentsize
  }

  out = []
  out.append('<div id="mfsgraph-tile">')
  out.append(html_cluster_state(clusterinfo))
  out.append('<div id="mfsgraph">')
  out.append(svg_mfs_graph(masterservers, metaloggers, chunkservers, ci.strver))
  out.append('</div><!-- mfsgraph -->')
  out.append('<div id="mfsgraph-info"></div>')
  out.append('</div>')
  return out