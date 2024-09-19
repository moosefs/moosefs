import math

padding = 2      # graph padding (all borders)
tile_padding = 20 # tile padding  

state_height = 70 # cluster state tile height

ms_top = 105      # master servers colum top position
ms_left = tile_padding     # master servers column left position
ms_width = 240  # master server icon width
ms_height = 50  # master server icon height
ms_v_spacing = 80 # master servers vertical spacing 

mls_padding_top = 110  # metaloggers colum top padding
mls_left = tile_padding     # metaloggers column left positionz
mls_width = 240  # metaloggers icon width
mls_height = 30  # metaloggers icon height
mls_v_spacing = 60 # metaloggers vertical spacing

cs_top = ms_top   # chunk servers column top position
cs_left_1st = 560 # chunk servers 1st column left position
cs_left_2nd = 860 # chunk servers 2nd column left position
cs_width = 240    # chunk server icon width
cs_height = 50    # chunk server icon height
cs_v_spacing = 90 # chunk servers vertical spacing 

net_backbone_x = ms_width+150

# Tresholds
trsh_wrn = 0.8
trsh_err = 0.95

green =  'fill: var(--ok-clr);'
orange = 'fill: var(--warning-clr);'
red =    'fill: var(--error-clr);'
bold =   'font-weight: bold;'
left =   'text-anchor:start;'
middle = 'text-anchor:middle;'
right =  'text-anchor:end;'
rem12 =  'font-size:1.2rem;'
rem14 =  'font-size:1.4rem;'

servers_nominal = True 

def defs():
  out = []
  out.append('<defs>')
  out.append('<g id="circle-check" transform="scale(0.038)"><path d="M243.8 339.8C232.9 350.7 215.1 350.7 204.2 339.8L140.2 275.8C129.3 264.9 129.3 247.1 140.2 236.2C151.1 225.3 168.9 225.3 179.8 236.2L224 280.4L332.2 172.2C343.1 161.3 360.9 161.3 371.8 172.2C382.7 183.1 382.7 200.9 371.8 211.8L243.8 339.8zM512 256C512 397.4 397.4 512 256 512C114.6 512 0 397.4 0 256C0 114.6 114.6 0 256 0C397.4 0 512 114.6 512 256zM256 48C141.1 48 48 141.1 48 256C48 370.9 141.1 464 256 464C370.9 464 464 370.9 464 256C464 141.1 370.9 48 256 48z"></path></g>')  
  out.append('<g id="triangle-exclamation" transform="scale(0.03)"><path d="M506.3 417l-213.3-364C284.8 39 270.4 32 256 32C241.6 32 227.2 39 218.1 53l-213.2 364C-10.59 444.9 9.851 480 42.74 480h426.6C502.1 480 522.6 445 506.3 417zM52.58 432L255.1 84.8L459.4 432H52.58zM256 337.1c-17.36 0-31.44 14.08-31.44 31.44c0 17.36 14.11 31.44 31.48 31.44s31.4-14.08 31.4-31.44C287.4 351.2 273.4 337.1 256 337.1zM232 184v96C232 293.3 242.8 304 256 304s24-10.75 24-24v-96C280 170.8 269.3 160 256 160S232 170.8 232 184z"></path></g>')  
  #out.append('<g id="octagon-exclamation-light" transform="scale(0.03)"><path d="M256 127.1C269.3 127.1 280 138.7 280 151.1V263.1C280 277.3 269.3 287.1 256 287.1C242.7 287.1 232 277.3 232 263.1V151.1C232 138.7 242.7 127.1 256 127.1V127.1zM288 351.1C288 369.7 273.7 383.1 256 383.1C238.3 383.1 224 369.7 224 351.1C224 334.3 238.3 319.1 256 319.1C273.7 319.1 288 334.3 288 351.1zM.0669 191.5C.0669 172.4 7.652 154.1 21.16 140.6L140.6 21.15C154.1 7.648 172.4 .0625 191.5 .0625H320.5C339.6 .0625 357.9 7.648 371.4 21.15L490.8 140.6C504.3 154.1 511.9 172.4 511.9 191.5V320.5C511.9 339.6 504.3 357.9 490.8 371.4L371.4 490.8C357.9 504.3 339.6 511.9 320.5 511.9H191.5C172.4 511.9 154.1 504.3 140.6 490.8L21.15 371.4C7.652 357.9 .0666 339.6 .0666 320.5L.0669 191.5zM55.1 174.5C50.6 179 48.07 185.2 48.07 191.5V320.5C48.07 326.8 50.6 332.9 55.1 337.4L174.5 456.9C179 461.4 185.2 463.9 191.5 463.9H320.5C326.8 463.9 332.1 461.4 337.5 456.9L456.9 337.4C461.4 332.9 463.9 326.8 463.9 320.5V191.5C463.9 185.2 461.4 179 456.9 174.5L337.5 55.09C332.1 50.59 326.8 48.06 320.5 48.06H191.5C185.2 48.06 179 50.59 174.5 55.09L55.1 174.5zM21.15 371.4L55.1 337.4z"></path></g>')  
  out.append('<g id="octagon-exclamation" transform="scale(0.03)"><path d="M140.6 21.15C154.1 7.648 172.4 .0625 191.5 .0625H320.5C339.6 .0625 357.9 7.648 371.4 21.15L490.8 140.6C504.3 154.1 511.9 172.4 511.9 191.5V320.5C511.9 339.6 504.3 357.9 490.8 371.4L371.4 490.8C357.9 504.3 339.6 511.9 320.5 511.9H191.5C172.4 511.9 154.1 504.3 140.6 490.8L21.15 371.4C7.652 357.9 .0666 339.6 .0666 320.5V191.5C.0666 172.4 7.652 154.1 21.15 140.6L140.6 21.15zM232 151.1V263.1C232 277.3 242.7 287.1 256 287.1C269.3 287.1 280 277.3 280 263.1V151.1C280 138.7 269.3 127.1 256 127.1C242.7 127.1 232 138.7 232 151.1V151.1zM256 319.1C238.3 319.1 224 334.3 224 351.1C224 369.7 238.3 383.1 256 383.1C273.7 383.1 288 369.7 288 351.1C288 334.3 273.7 319.1 256 319.1z"></path></g>')  
  out.append('</defs>')
  return "\n".join(out)

def styles():
  out = []
  out.append('<style>')
  out.append('.tile-title-right {fill: #aaa; font-family: Arial, sans-serif; font-size: 18px; font-weight: bold; white-space: pre;text-anchor:end;}') # server name
  out.append('.tile-title-left {fill: #aaa; font-family: Arial, sans-serif; font-size: 18px; font-weight: bold; white-space: pre;}') # server name
  out.append('.tile-bckgrnd {fill:#d9deda77;}') #servers group backround rect
  out.append('.srv-rect-bckgrnd {fill: #acbeb155;}') # server outer rectangle 
  out.append('.srv-rect {fill: none; stroke: #222; stroke-width: 2px;}') # server outer rectangle 
  out.append('.srv-name {fill: #111; font-family: Arial, sans-serif; font-size: 16px; white-space: pre;}') # server name
  out.append('.srv-state {fill: #111; font-family: Arial, sans-serif; font-size: 14px; white-space: pre;}') # server state
  out.append('.srv-state-small {fill: #111; font-family: Arial, sans-serif; font-size: 12px; white-space: pre;}') # server state
  out.append('.srv-line {stroke: #888; stroke-width: 1px;vector-effect: non-scaling-stroke;}') # vertical line dividing gauges
  out.append('.srv-line-ver {stroke: #888; stroke-width: 1px;vector-effect: non-scaling-stroke;}') # vertical line dividing version
  out.append('.ip-address-cs {fill: #555; font-family: Arial, sans-serif; font-size: 13px; font-weight: bold; white-space: pre;text-anchor:end;}') # cs ip address
  out.append('.ip-port-cs {fill: #555; font-family: Arial, sans-serif; font-size: 13px; white-space: pre;text-anchor:end;}') # cs ip address
  out.append('.ip-address-ms {fill: #555; font-family: Arial, sans-serif; font-size: 13px; font-weight: bold; white-space: pre;}') # ms ip address
  out.append('.ip-port-ms {fill: #555; font-family: Arial, sans-serif; font-size: 13px; white-space: pre;;}') # ms ip address
  out.append('.led-normal {fill: #03c123;}')       # live server, everything nominal, circle "led" green
  out.append('.led-warning {fill: #d69f00;}')      # warnings for server, circle "led" orange
  out.append('.led-error {fill: #c10303;}')        # dead server, circle "led" red
  out.append('.led-maintenance {fill: #465cfb;}')  # cs maintenance mode, circle "led" blue
  out.append('.label {fill: #555; font-family: Arial, sans-serif; font-size: 11px; white-space: pre;text-anchor:end;}') # linear gauge name
  out.append('.label-bold {fill: #555; font-family: Arial, sans-serif; font-size: 11px; white-space: pre; font-weight: bold; text-anchor:end;}') # linear gauge name
  out.append('.label-version {fill: #555; font-family: Arial, sans-serif; font-size: 11px; white-space: pre;text-anchor:middle; }') # linear gauge name
  out.append('.label-version-mismatch {fill: #c10303; font-family: Arial, sans-serif; font-size: 11px; font-weight: bold; white-space: pre; text-anchor:middle;}') # linear gauge name
  out.append('.value {fill: #222; font-family: Arial, sans-serif; font-size: 11px; white-space: pre;}') # linear gauge name
  out.append('.state-normal {fill: #129f32; font-family: Arial, sans-serif; font-size: 11px; font-weight: bold; white-space: pre;}') # server error (dead) msg
  out.append('.state-warning {fill: #d69f00; font-family: Arial, sans-serif; font-size: 11px; font-weight: bold; white-space: pre;}') # server warning msg
  out.append('.state-error {fill: #c10303; font-family: Arial, sans-serif; font-size: 11px; font-weight: bold; text-transform: uppercase; white-space: pre;}') # server error (dead) msg
  out.append('.state-maintenance {fill: #465cfb; font-family: Arial, sans-serif; font-size: 10px; white-space: pre;}') # cs maintenance msg
  out.append('.gauge-bckgrd {fill: #c3c3c3;}')  # linear gauge background
  out.append('.gauge-green  {fill: #129f32;}')  # linear gauge green (ok)
  out.append('.gauge-orange {fill: #d69f00;}')  # linear gauge orange (warning)
  out.append('.gauge-red    {fill: #c10303;}')  # linear gauge red (critical)
  # out.append('.arc-gauge-name {fill: #555; font-family: Arial, sans-serif; font-size: 11px; white-space: pre;text-anchor:middle;}') # arc gauge name
  # out.append('.arc-gauge-bckgrd {fill: none; stroke-linecap: round;stroke: #c3c3c3;}')  
  # out.append('.arc-gauge-green  {fill: none; stroke-linecap: round;stroke: #129f32;}')    
  # out.append('.arc-gauge-orange {fill: none; stroke-linecap: round;stroke: #d69f00;}') 
  # out.append('.arc-gauge-red    {fill: none; stroke-linecap: round;stroke: #c10303;}') 
  # out.append('.arc-gauge-label {fill: #222; font-family: Arial, sans-serif; font-size: 13px; white-space: pre;text-anchor:middle;}')  # arc gauge label
  out.append('.val-label-name {fill: #555; font-family: Arial, sans-serif; font-size: 11px; white-space: pre;text-anchor:middle;}')  # txt gauge name
  out.append('.val-label-value {fill: #222; font-family: Arial, sans-serif; font-size: 16px; white-space: pre;text-anchor:middle;}') # txt gauge value
  out.append('.val-label-units {fill: #222; font-family: Arial, sans-serif; font-size: 13px; white-space: pre;text-anchor:middle;}') # txt gauge unit
  out.append('.net-line {fill: none; stroke: #666; stroke-width: .5px;}') # network line
  out.append('</style>')
  return "\n".join(out)

# Returns x.y.z version number (without "PRO" suffix)
def version_number(strver):
  if strver.find(' PRO') > 0:
    return strver[:-4]
  else:
    return strver

def humanize(number, thousand=1000, suff=''):
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
    nstr = "%u" % b
  if scale>0:
    if (suff=='iB'):
      slist="-kMGTPEZY"
    else:
      slist=['','k','M',' Bn']
    return (nstr,"%s%s" % (slist[scale],suff))
  else:
    suff = 'B' if suff=='iB' else suff
    return (nstr,suff)

def humanize_str(number, thousand=1000, suff=''):
  (str, suff) = humanize(number, thousand, suff)
  return str+suff

def humanize_bytes(number, suff='iB'):
  return humanize(number, 1024, suff)

def humanize_bytes_str(number, suff='iB'):
  (str, suff) = humanize_bytes(number, suff)
  return str+' '+suff

# Translate given tag to x,y by adding the "g transform" tag
def translate(tag, x, y):
  return '<g transform="matrix(1,0,0,1,%s,%s)">\n %s\n</g>' % (x, y, tag)

# Generate text with class cls at position x, y
def text(msg, x, y, cls, maxLength=0, style=''):
  if maxLength>0:
    msg = (msg[:maxLength-1] + '…') if len(msg) > maxLength else msg
  return '<text class="%s" style="%s" x="%s" y="%s">%s</text>' % (cls, style, x, y, msg)

# Generates arc at x, y (left-upper corner of a surrounding box) with given radius starting and ending at given angles: <path fill="none" stroke="#446688" stroke-width="3" d="M 184.64101615137756 170 A 40 40 0 1 0 115.35898384862246 170"></path>
def arc(x: float, y: float, radius: float, startAngle: float, endAngle: float, strokeWidth:int =8, cls: str=''):
  def  polarToCartesian(x, y, radius, angleInDegrees):
    angleInRadians = (angleInDegrees-90) * 0.0174532 #math.pi / 180.0
    return (x + (radius * math.cos(angleInRadians)),y + (radius * math.sin(angleInRadians)))
  x=x+radius+(strokeWidth/2.0)
  y=y+radius+(strokeWidth/2.0)
  (startx, starty) = polarToCartesian(x, y, radius, endAngle)
  (endx, endy) = polarToCartesian(x, y, radius, startAngle)
  largeArcFlag = '0' if endAngle - startAngle <= 180 else '1';
  points = 'M %f %f A %f %f 0 %s 0 %f %f' % (startx, starty, radius, radius, largeArcFlag, endx, endy)
  return'<path class="%s" style="stroke-width: %fpx" d="%s"/>' % (cls,strokeWidth,points)

# Generate arc gauge with name, green, orange or red depending on val treshold (75%/90%) 
def arc_gauge(x: float, y: float, name: str, label: str, val: float, label_cls: str='', r: float=12, arcTo: float=120, strokeWidth: float=8): 
  out = []
  if name:
    out.append(text(name, x+r+strokeWidth/2, y, 'arc-gauge-name',0,label_cls))
  out.append(arc(x, y+strokeWidth/2, r, -arcTo, arcTo, strokeWidth, 'arc-gauge-bckgrd'))
  val = min(val, 1)
  val = max(val, 0)
  cls = 'arc-gauge-green' if val < trsh_wrn else ('arc-gauge-orange' if val < trsh_err else 'arc-gauge-red')
  endAngle = val*2*arcTo - arcTo
  endAngle = max(endAngle, -arcTo+1)
  out.append(arc(x, y+strokeWidth/2, r, -arcTo, endAngle, strokeWidth, cls))
  if label:
    out.append(text(label, x, y+13, 'arc-gauge-label'))
  return "\n".join(out)

# Generate horizontal linear % gauge with name, green, orange or red depending on val treshold (75%/90%) 
def hgauge(name: str, val: float, x:float, y: float):
  out = []
  out.append(text(name, x, y, 'label'))
  h = 5
  w = ms_width/3
  out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="1" ry="1" class="gauge-bckgrd"/>' % (x+3, y-h, w, h))
  val = min(val, 1)
  val = max(val, 0)
  cls = 'gauge-green' if val < trsh_wrn else ('gauge-orange' if val < trsh_err else 'gauge-red')
  w = val*w
  w = max(w, 0.5)
  out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="1" ry="1" class="%s"/>' % (x+3, y-h, w, h, cls))
  return "\n".join(out)

# Generates svg text memory gauge
def svg_mem_gauge(name: str, val: float, x: float, y: float):
  out=[]
  out.append(text(name, x, y, 'val-label-name'))
  (hval, units)=humanize_bytes(val)
  out.append(text(hval, x, y+15, 'val-label-value'))
  out.append(text(units, x, y+28, 'val-label-units'))
  return "\n".join(out)

# Generates div text memory gauge
def html_mem_gauge(name: str, val: float):
  (hval, units)=humanize_bytes(val)
  out=[]
  out.append('<table class="gauge">')
  out.append('<tr><td class="name">%s</td></tr>' % name)
  out.append('<tr><td class="txt-value">%s</td></tr>' % hval)
  out.append('<tr><td class="units">%s</td></tr>' % units)
  out.append('</table>')
  return "\n".join(out)

def svg_tag(w, h, svgbody):
  return '<svg viewbox="0 0 %u %u" width="%u" height="%u" xmlns="http://www.w3.org/2000/svg">%s</svg>' % (w, h, w, h, svgbody)

##############################################
# Generate cluster state html
def html_cluster_state(info):
  # print(info)
  global servers_nominal
  out = []
  out.append('<div class="cards">')
  if 'totalspace' in info and 'availspace' in info and info['totalspace']!=0:
    out.append('<div class="card">')
    out.append('<div class="card-header">Space</div>')
    out.append('<div class="card-body">')
    out.append('<table><tr><td>')
    out.append('<table class="gauge">')
    out.append('<tr><td class="name">Used</td></tr>')
    out.append('<tr><td class="value">%s</td></tr>' % svg_tag(2*18+10,2*18+10,arc_gauge(0,0, '', '', (info['totalspace']-info['availspace'])/info['totalspace'], bold, 18, 120, 10)))
    out.append('</table>')
    out.append('</td><td>')
    out.append(html_mem_gauge('Free', info['availspace']))
    out.append('</td></tr></table>')
    out.append('</div>') #card-body
    out.append('</div>') #card
  
  out.append('<div class="card">')
  out.append('<div class="card-header">Data</div>')
  out.append('<div class="card-body">')
  out.append('<table>')
  if 'chunks-total' in info and info['chunks-total']>0 and 'chunks-missing' in info and info['chunks-missing']>0:
    out.append('<tr><td class="label-name error-txt">Missing:</td>')
    out.append('<td class="label-value"><span class="error-txt">%.1f%%</span> (%u chunks)</td></tr>' % (100*info['chunks-missing']/info['chunks-total'], info['chunks-missing']))
  elif 'chunks-total' in info and info['chunks-total']>0 and 'chunks-undergoal' in info and 'chunks-endangered' in info and (info['chunks-undergoal']+info['chunks-endangered'])>0:
    out.append('<tr><td class="label-name warning-txt">In danger:</td>')
    out.append('<td class="label-value"><span class="warning-txt">%.1f%%</span> (%u chunks)</td></tr>' % (100*(info['chunks-undergoal']+info['chunks-endangered'])/info['chunks-total'], info['chunks-undergoal']+info['chunks-endangered']))
  else:
    out.append('<tr><td class="label-name">Health:</td>')
    out.append('<td class="label-value">Normal</td></tr>')
  if 'files' in info and 'trfiles' in info:
    out.append('<tr><td class="label-name">Files:</td>')
    out.append('<td class="label-value">%s</td></tr>' % humanize_str(info['files']-info['trfiles']))
  if 'dirs' in info:
    out.append('<tr><td class="label-name">Folders:</td>')
    out.append('<td class="label-value">%s</td></tr>' % humanize_str(info['dirs']))

  out.append('</table>')
  out.append('</div>') #card-body
  out.append('</div>') #card

  # out.append('<div style="clear: right;" >asdf</div>')


  # if 'files' in info and 'trfiles' in info:  
  #   out.append(text('Files:', x, 0, 'label'))
  #   out.append(text(humanize_str(info['files']-info['trfiles']), x+2, 0, 'value'))
  # if 'dirs' in info:  
  #   out.append(text('Folders:', x, 15, 'label'))
  #   out.append(text(humanize_str(info['dirs']), x+2, 15, 'value'))
  # if 'trspace' in info:
  #   out.append(text('Trash size:', x, 30, 'label'))
  #   out.append(text(humanize_bytes_str(info['trspace']), x+2, 30, 'value'))
  # out.append(vline(x+56))

  # out.append(text('Health', x+24, 0, 'label',0,bold))
  # out.append(text('Data:', x, 15, 'label'))
  # if 'chunks-missing' in info and info['chunks-missing']>0:
  #   out.append(text('missings', x+2, 15, 'value',0,bold+red))
  # elif 'chunks-undergoal' in info and info['chunks-undergoal']>0:
  #   out.append(text('undergoals', x+2, 15, 'value',0, bold+orange))
  # else:
  #   out.append(text('healthy', x+2, 15, 'value'))

  # out.append(text('Servers:', x, 30, 'label'))
  # if servers_nominal:
  #   out.append(text('healthy', x+2, 30, 'value',0, bold+green))
  # else:
  #   out.append(text('warnings', x+2, 30, 'value',0, bold+orange))

  # out.append(text('Usage', x+12, 0, 'label-bold'))
  # out.append(text('Mounts:', x, 15, 'label'))
  # if 'mounts' in info :
  #   out.append(text(humanize_str(info['mounts']), x+3, 15, 'value',0))
  # else:
  #   out.append(text('unknown', x+3, 15, 'value'))
  # out.append(text('Operations:', x, 30, 'label'))
  # if 'operations_c_h' in info :
  #   out.append(text(humanize_str(info['operations_c_h']/3600)+'/sec', x+3, 30, 'value',0))
  # else:
  #   out.append(text('unknown', x+3, 30, 'value'))
  # out.append(vline(x+58))

  # if 'licmaxsize' in info and 'currentsize' in info and info['licmaxsize']!=0:
  #   out.append(arc_gauge('Licence', '', (info['currentsize'])/info['licmaxsize'], 120, x, 18, bold))
  #   out.append(svg_mem_gauge('Left', info['licmaxsize']-info['currentsize'], x+50, 0))
  
  out.append('</div>') #cards
  return "\n".join(out)

##############################################
# Generate cluster state tile
def cluster_state(info):
  def vline(x):
    return '<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, -10, x, 33)
  global servers_nominal
  out = []
  x=29.5
  # y=0
  # state_width = (cs_left_2nd+ms_width+tile_padding)-x  # cluster state tile width
  state_width = 550
  # print(clusterinfo)
  out.append('<g transform="translate(0, 20) scale(1.3)">')
  out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x-30, -15, state_width, state_height/1.3))
  
  if 'totalspace' in info and 'availspace' in info and info['totalspace']!=0:
    out.append(arc_gauge(x-10, 0, 'Total space', '', (info['totalspace']-info['availspace'])/info['totalspace'], bold))
    out.append(svg_mem_gauge('Free', info['availspace'], x+50, 0))
  out.append(vline(x+68))

  x+=128
  if 'files' in info and 'trfiles' in info:  
    out.append(text('Files:', x, 0, 'label'))
    out.append(text(humanize_str(info['files']-info['trfiles']), x+2, 0, 'value'))
  if 'dirs' in info:  
    out.append(text('Folders:', x, 15, 'label'))
    out.append(text(humanize_str(info['dirs']), x+2, 15, 'value'))
  if 'trspace' in info:
    out.append(text('Trash size:', x, 30, 'label'))
    out.append(text(humanize_bytes_str(info['trspace']), x+2, 30, 'value'))
  out.append(vline(x+56))

  x+=105
  out.append(text('Health', x+24, 0, 'label',0,bold))
  out.append(text('Data:', x, 15, 'label')) 
  if 'chunks-missing' in info and info['chunks-missing']>0:
    out.append(text('missings', x+2, 15, 'value',0,bold+red))
  elif 'chunks-endangered' in info and info['chunks-endangered']>0:
    out.append(text('in danger', x+2, 15, 'value',0, bold+orange))
  elif 'chunks-undergoal' in info and info['chunks-undergoal']>0:
    out.append(text('undergoals', x+2, 15, 'value',0, bold+orange))
  else:
    out.append(text('healthy', x+2, 15, 'value'))

  out.append(text('Servers:', x, 30, 'label'))
  if servers_nominal:
    out.append(text('healthy', x+2, 30, 'value',0, bold+green))
  else:
    out.append(text('warnings', x+2, 30, 'value',0, bold+orange))

  x+=128
  out.append(vline(x-62))
  out.append(text('Usage', x+12, 0, 'label-bold'))
  out.append(text('Mounts:', x, 15, 'label'))
  if 'mounts' in info :
    out.append(text(humanize_str(info['mounts']), x+3, 15, 'value',0))
  else:
    out.append(text('unknown', x+3, 15, 'value'))
  out.append(text('Operations:', x, 30, 'label'))
  if 'operations_c_h' in info :
    out.append(text(humanize_str(info['operations_c_h']/3600)+'/sec', x+3, 30, 'value',0))
  else:
    out.append(text('unknown', x+3, 30, 'value'))
  out.append(vline(x+58))

  x+=90
  if 'licmaxsize' in info and 'currentsize' in info and info['licmaxsize']!=0 and info['licmaxsize']!=None:
    out.append(arc_gauge(x-10, 0, 'Licence', '', (info['currentsize'])/info['licmaxsize'], bold))
    out.append(svg_mem_gauge('Left', info['licmaxsize']-info['currentsize'], x+50, 0))
  
  out.append('</g>')
  return "\n".join(out)

##############################################
# Generate master server icon
def mserver(srv, net_offset_y, strverldr):
  #TODO: add metadata in-sync / out-of-sync warnings
  global servers_nominal
  live = 'live' in srv and srv['live']==1
  led_cls = 'led-normal'
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (ms_width, ms_height))
  out.append(text(srv['name'], 25, 20, "srv-name", 18))
  if not live:
    servers_nominal = False
    led_cls = 'led-error'
    out.append(text('UNREACHABLE', 7, 31, 'state-error'))
  if live:
    if 'strver' in srv and srv['strver'] != '0.0.0':
      ver_cls = "label-version"
      if strverldr!='' and strverldr!=srv['strver']:
        ver_cls += "-mismatch"  
        servers_nominal = False
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
        servers_nominal = False
        led_cls = 'led-warning'
    if 'cpuload' in srv:
      # out.append(hgauge('CPU', srv['cpuload'], 28, 42))
      x=cs_width-100
      out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 7, x, ms_height-7))
      out.append(arc_gauge(x+5, 14, 'CPU', '', srv['cpuload']))
      if srv['cpuload'] > trsh_wrn:
        servers_nominal = False
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
  out.append('<circle cx="14" cy="14" r="7" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (ms_width, ms_height))
  return "\n".join(out)

##############################################
# Generate metalogger server icon
def mlserver(srv, net_offset_y, strverldr):
  global servers_nominal
  live = 'live' in srv and srv['live']==1
  led_cls = 'led-normal'
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (mls_width, mls_height))
  out.append(text(srv['name'], 25, 20, "srv-name", 18))
  if 'strver' in srv and srv['strver'] != '0.0.0':
    ver_cls = "label-version"
    if strverldr!='' and strverldr!=srv['strver']:
      servers_nominal = False
      ver_cls += "-mismatch"  
      led_cls = 'led-warning'
    out.append('<line class="srv-line-ver" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (mls_width-40, 0, mls_width-40, mls_height))
    out.append(text(version_number(srv['strver']), mls_width-20, mls_height/2+5, ver_cls))
  if 'ip' in srv :
    out.append(text(srv['ip'], ms_width+5, net_offset_y-3, 'ip-address-ms'))
  out.append('<circle cx="14" cy="14" r="7" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (mls_width, mls_height))
  return "\n".join(out)

##############################################
# Generate chunk server icon
def cserver(srv, net_offset_y, strverldr):
  global servers_nominal
  live = 'live' in srv and srv['live']==1
  maintenance = 'maintenance' in srv and srv['maintenance']==1
  led_cls = 'led-normal'
  out = []
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect-bckgrnd"/>' % (ms_width, ms_height))
  out.append(text(srv['name'], 25, 20, "srv-name", 8))
  if not live:
    servers_nominal = False
    led_cls = 'led-error'
    out.append(text('UNREACHABLE', 7, 31, "state-error"))
  elif maintenance:
    led_cls = 'led-maintenance' 
    out.append(text('MAINTENANCE', 7, 31, "state-maintenance"))
  if live:
    if 'strver' in srv and srv['strver'] != '0.0.0':
      ver_cls = "label-version"
      if strverldr!='' and strverldr!=srv['strver']:
        servers_nominal = False
        ver_cls += "-mismatch"  
        led_cls = 'led-warning'
      out.append('<line class="srv-line-ver" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (cs_width-17, 0, cs_width-17, cs_height))
      out.append('<g transform="translate(%f,%f) rotate(90)">' % (cs_width-13, cs_height/2))
      out.append(text(version_number(srv['strver']), 0, 0, ver_cls))
      out.append('</g>')
    if 'cpuload' in srv:
      out.append(hgauge('CPU', srv['cpuload'], 28, 42))
    if not maintenance and 'load_state_msg' in srv:
      out.append(text('Queue:', 7, 34, 'label',0,left))
      if srv['load_state'] == 'OVLD':
        servers_nominal = False
        state_cls = 'state-error'
        led_cls = 'led-warning'
      else:
        state_cls = 'state-normal'
      out.append(text(srv['load_state_msg'], 7, 44, state_cls))

    if 'hdd_reg_total' in srv and 'hdd_reg_used' in srv:
      x=cs_width-145
      out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 7, x, cs_height-7))
      if srv['hdd_reg_total']!=0:
        out.append(arc_gauge(x+5, 14, 'HDD', '', srv['hdd_reg_used']/srv['hdd_reg_total']))
        if srv['hdd_reg_used']/srv['hdd_reg_total'] > trsh_wrn:
          servers_nominal = False
          led_cls = 'led-warning'
      x=cs_width-108
      # out.append('<line class="srv-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, 18, x, cs_height-7))
      out.append(text('Free', x+20, 14, 'val-label-name'))
      (hddfree, hddunits)=humanize_bytes(srv['hdd_reg_total']-srv['hdd_reg_used'])
      out.append(text(hddfree, x+20, 29, 'val-label-value'))
      out.append(text(hddunits, x+20, 42, 'val-label-units'))
      x=cs_width-65
      out.append(text('Health', x+20, 14, 'val-label-name'))
      if 'hdds_status' in srv:
        if srv['hdds_status'].startswith('error'):
          out.append('<use xlink:href="#octagon-exclamation" x="%f" y="17" style="%s"/>' % (x+13, red))
          out.append(text('Errors', x+20, 42, 'value',0,middle+red))
          servers_nominal = False
          led_cls = 'led-warning'
        elif srv['hdds_status'].startswith('warning'):
          out.append('<use xlink:href="#triangle-exclamation" x="%f" y="17" style="%s"/>' % (x+13, orange))
          out.append(text('Warnings', x+20, 42, 'value',0,middle))
          servers_nominal = False
          led_cls = 'led-warning'
        elif srv['hdds_status'].startswith('ok'):
          out.append('<use xlink:href="#circle-check" x="%f" y="21" style="%s"/>' % (x+11, green))
        else:
          out.append(text('unknown', x+20, 42, 'value',0,middle))
      else:
        out.append(text('unknown', x+20, 42, 'value',0,middle))

  if 'ip' in srv and 'port' in srv:
    out.append(text(srv['ip'], -5, net_offset_y-3, 'ip-address-cs'))
    out.append(text(":"+str(srv['port']), -5, net_offset_y+12, 'ip-port-cs'))

  out.append('<circle cx="14" cy="14" r="7" class="%s"/>' % led_cls)
  out.append('<rect width="%d" height="%d" rx="4" ry="4" class="srv-rect"/>' % (ms_width, ms_height))
  return "\n".join(out)

##############################################
# Generate SVG picture of a cluster
##############################################
def svg(clusterinfo, masterservers, metaloggers, chunkservers):
  global servers_nominal
  svg_h = 0
  svg_w = net_backbone_x

  out = []  
  out.append(defs())
  out.append(styles())

  #Background
  # out.append('<rect width="100%" height="100%" fill="orange"/>')
  
  # This is the cluster reference version number (leader's version number)
  strverldr = clusterinfo['strver'] if 'strver' in clusterinfo else ''

  # Print master servers
  net_offset_y = 0.6 * ms_height
  x = ms_left
  y = ms_top
  net_start_y = y
  net_end_y = 0

  if len(masterservers) > 0:
    # master servers tile
    tile_width=(net_backbone_x-15)-(x-tile_padding)
    tile_height=2*tile_padding+(len(masterservers)-1)*ms_v_spacing+ms_height+10
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x-tile_padding, y-tile_padding, tile_width, tile_height))
    out.append(text('Master servers', x-tile_padding+tile_width-8, y-tile_padding+tile_height-8, 'tile-title-right'))
    
    for ms in masterservers:
      out.append('<line class="net-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x+ms_width, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(mserver(ms,net_offset_y,strverldr), x, y))
      net_end_y = y+net_offset_y
      y+=ms_v_spacing
    y-=ms_v_spacing
    svg_h=max(svg_h, y+ms_height)

  # Print metaloggers
  if len(metaloggers) > 0:
    net_offset_y = 0.7 * mls_height
    x=mls_left
    y=y+mls_padding_top
    # metaloggers tile
    tile_width=(net_backbone_x-15)-(x-tile_padding)
    tile_height=2*tile_padding+(len(metaloggers)-1)*mls_v_spacing+mls_height+10
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x-tile_padding, y-tile_padding, tile_width, tile_height))
    out.append(text('Metaloggers', x-tile_padding+tile_width-8, y-tile_padding+tile_height-8, 'tile-title-right'))
    svg_h=max(svg_h, y-tile_padding+tile_height)

    for mls in metaloggers:
      out.append('<line class="net-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x+ms_width, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(mlserver(mls,net_offset_y,strverldr), x, y))
      net_end_y = y+net_offset_y
      y+=mls_v_spacing
    y-=mls_v_spacing
    

  # Print chunk servers
  net_offset_y = 0.6 * cs_height
  odd = True
  cy_1st = cs_top
  cy_2nd = cs_top+cs_v_spacing/2
  net_start_y = min(cy_1st,net_start_y)
  if len(chunkservers) > 0:
    # chunk servers tile
    x=net_backbone_x+15
    y=cy_1st-tile_padding

    tile_width=cs_left_2nd+cs_width+tile_padding-(net_backbone_x+15)
    tile_height=tile_padding+(math.ceil(len(chunkservers)/2)-1)*cs_v_spacing+cs_height+tile_padding
    if len(chunkservers) % 2 == 0:
      tile_height+=(cy_2nd-cy_1st)
    
    out.append('<rect x="%f" y="%f" width="%f" height="%f" rx="4" ry="4" class="tile-bckgrnd"/>' % (x, y, tile_width, tile_height))
    out.append(text('Chunkservers', x+8, y+tile_height-8, 'tile-title-left'))
    svg_h=max(svg_h, y+tile_height)
    svg_w=max(svg_w, x+tile_width)
    for cs in chunkservers:
      if odd:
        x = cs_left_1st
        y = cy_1st
        cy_1st += cs_v_spacing
      else:
        x = cs_left_2nd
        y = cy_2nd
        cy_2nd += cs_v_spacing
      net_end_y = max(net_end_y,y+net_offset_y)
      out.append('<line class="net-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (x, y+net_offset_y, net_backbone_x, y+net_offset_y))
      out.append(translate(cserver(cs,net_offset_y,strverldr), x, y))
      odd = not odd

  svg_h=max(svg_h, cy_1st+cs_height-cs_v_spacing)
  svg_h=max(svg_h, cy_2nd+cs_height-cs_v_spacing)

  
  net_start_y += net_offset_y
  # net_end_y += net_offset_y

  # Print network backbone line
  out.append('<line class="net-line" x1="%f" y1="%f" x2="%f" y2="%f"/>' % (net_backbone_x, net_start_y, net_backbone_x, net_end_y))

  # Print cluster state tile - after printing servers because we need to know servers' health state
  out.append(cluster_state(clusterinfo))

  # Adjust overall height, width and padding
  out.insert(0, '<g transform="matrix(1,0,0,1,%s,%s)">' % (padding, padding))
  svg_h+=2*padding
  svg_w+=2*padding
  out.insert(0, '<svg height="%d" width="%d">' % (svg_h, svg_w))

  out.append('</g>')
  out.append('</svg>')
  # out = []
  # out.insert(0, html_cluster_state(clusterinfo));
  return "\n".join(out)