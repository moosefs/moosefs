import socket 
import struct
import time
from datetime import datetime

from common.constants import *
from common.errors import *

UNIQ_MASK_IP = 1 << (1+ord('Z')-ord('A'))
UNIQ_MASK_RACK = 1 << (2+ord('Z')-ord('A'))

def myunicode(x):
	return str(x)

def gettzoff(t):
	return time.localtime(t).tm_gmtoff

def shiftts(t):
	return t - gettzoff(t)

def safe_int(val, default=0):
	try:
		return int(val)
	except Exception:
		return default

def resolve(strip, donotresolve=0, default=None):
	if donotresolve:
		return strip
	try:
		return (socket.gethostbyaddr(strip))[0]
	except Exception:
		if default is not None:
			return default
		else:
			return strip

def decimal_number(number,sep=' '):
	parts = []
	while number>=1000:
		number,rest = divmod(number,1000)
		parts.append("%03u" % rest)
	parts.append(str(number))
	parts.reverse()
	return sep.join(parts)

def decimal_number_html(number):
	return decimal_number(number,"&#8239;")

# returns decimal number with non-breaking space as separator or 'n/a' if number is None
def decimal_number_na_html(number):
	if number is None:
		return 'n/a'
	return decimal_number(number,"&#8239;")


def humanize_number(number,sep='',suff='B'):
	number*=100
	scale=0
	while number>=99950:
		number = number//1024
		scale+=1
	if number<995 and scale>0:
		b = (number+5)//10
		nstr = "%u.%u" % divmod(b,10)
	else:
		b = (number+50)//100
		nstr = "%u" % b
	if scale>0:
		return "%s%s%si%s" % (nstr,sep,"-KMGTPEZY"[scale],suff)
	else:
		return "%s%s%s" % (nstr,sep,suff)

# return ordinal suffix for a number
def ordinal_suffix(i):
	if i%100 in (11,12,13):
		return "th"
	elif i%10==1:
		return "st"
	elif i%10==2:
		return "nd"
	elif i%10==3:
		return "rd"
	else:
		return "th"

def timeduration_to_shortstr(timeduration, sep=''):
	for l,s in ((604800,'w'),(86400,'d'),(3600,'h'),(60,'m'),(0,'s')):
		if timeduration>=l:
			if l>0:
				n = float(timeduration)/float(l)
			else:
				n = float(timeduration)
			rn = round(n,1)
			if n==round(n,0):
				return "%.0f%s%s" % (n,sep,s)
			else:
				return "%s%.1f%s%s" % (("~"+sep if n!=rn else ""),rn,sep,s)
	return "???"

def timeduration_to_fullstr(timeduration):
	if timeduration>=86400:
		days,dayseconds = divmod(timeduration,86400)
		daysstr = "%u day%s + " % (days,("s" if days!=1 else ""))
	else:
		dayseconds = timeduration
		daysstr = ""
	hours,hourseconds = divmod(dayseconds,3600)
	minutes,seconds = divmod(hourseconds,60)
	if seconds==round(seconds,0):
		return "%u second%s (%s%u:%02u:%02u)" % (timeduration,("" if timeduration==1 else "s"),daysstr,hours,minutes,seconds)
	else:
		seconds,fracsec = divmod(seconds,1)
		return "%.3f seconds (%s%u:%02u:%02u.%03u)" % (timeduration,daysstr,hours,minutes,seconds,round(1000*fracsec,0))

def hours_to_str(hours, sep=''):
	days,hoursinday = divmod(hours,24)
	if days>0 and hours!=24:
		if hoursinday>0:
			return "%u%sd %u%sh" % (days,sep,hoursinday,sep)
		else:
			return "%u%sd" % (days,sep)
	else:
		return "%u%sh" % (hours,sep)

def datetime_to_str(tm):
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(tm))

def date_to_str(tm):
	return time.strftime("%Y-%m-%d",time.localtime(tm))

def time_to_str(tm):
	return time.strftime("%H:%M:%S",time.localtime(tm))

def seconds_since_beginning_of_hour():
	now = datetime.now()  # Get the current date and time
	start_of_hour = now.replace(minute=0, second=0, microsecond=0)  # Set minutes, seconds, and microseconds to 0
	seconds_since = (now - start_of_hour).total_seconds()  # Calculate the difference in seconds
	return int(seconds_since)  # Return the result as an integer

def label_id_to_char(id):
	return chr(ord('A')+id)

def state_name(stateid):
	if   stateid==STATE_DUMMY:    return "DUMMY"
	elif stateid==STATE_USURPER:  return "USURPER"
	elif stateid==STATE_FOLLOWER: return "FOLLOWER"
	elif stateid==STATE_ELECT:    return "ELECT"
	elif stateid==STATE_DEPUTY:   return "DEPUTY"
	elif stateid==STATE_LEADER:   return "LEADER"
	elif stateid==STATE_MASTERCE: return "MASTER"
	else:                         return "???"

def state_color(stateid,sync):
	if stateid==STATE_DUMMY:
		return 8
	elif stateid==STATE_FOLLOWER or stateid==STATE_USURPER:
		if sync:
			return 5
		else:
			return 6
	elif stateid==STATE_ELECT:
		return 3
	elif stateid==STATE_DEPUTY:
		return 2
	elif stateid==STATE_LEADER:
		return 4
	else:
		return 1

def labelmask_to_str(labelmask):
	str = ""
	m = 1
	for i in range(26):
		if labelmask & m:
			str += label_id_to_char(i)
		m <<= 1
	return str

def labelmasks_to_str(labelmasks):
	if labelmasks[0]==0:
		return "*"
	r = []
	for labelmask in labelmasks:
		if labelmask==0:
			break
		r.append(labelmask_to_str(labelmask))
	return "+".join(r)

def labelexpr_to_str(labelexpr, literals = ['*','&','|','~']):
	any = literals[0]
	andop = literals[1]
	orop = literals[2]
	notop = literals[3]
	stack = []
	if len(labelexpr)==0:
		return any
	if labelexpr[0]==0:
		return any              # '*' any label
	for i in labelexpr:
		if i==255:
			stack.append((0,any)) # '*' any label
		elif i>=192 and i<255:
			n = (i-192)
			stack.append((0,chr(ord('A')+n)))
		elif i>=128 and i<192:
			n = (i-128)+2
			if n>len(stack):
				return 'EXPR ERROR'
			m = []
			for _ in range(n):
				l,s = stack.pop()
				if l>1:
					m.append("(%s)" % s)
				else:
					m.append(s)
			m.reverse()
			stack.append((1,andop.join(m)))   # '&' and operator
		elif i>=64 and i<128:
			n = (i-64)+2
			if n>len(stack):
				return 'EXPR ERROR'
			m = []
			for _ in range(n):
				l,s = stack.pop()
				if l>2:
					m.append("(%s)" % s)
				else:
					m.append(s)
			m.reverse()
			stack.append((2,orop.join(m)))   # '|' or operator
		elif i==1:
			if len(stack)==0:
				return 'EXPR ERROR'
			l,s = stack.pop()
			if l>0:
				stack.append((0,notop+"(%s)" % s)) # '~' not operator
			else:
				stack.append((0,notop+"%s" % s))   # '~' not operator
		elif i==0:
			break
		else:
			return 'EXPR ERROR'
	if len(stack)!=1:
		return 'EXPR ERROR'
	l,s = stack.pop()
	return s

def labellist_fold(labellist):
	ll = []
	prev_data = None
	count = 0
	for data in labellist:
		if (data != prev_data):
			if count>0:
				if count>1:
					ll.append(('%u%s' % (count,prev_data[0]),prev_data[1],prev_data[2]))
				else:
					ll.append(prev_data)
			prev_data = data
			count = 1
		else:
			count = count+1
	if count>0:
		if count>1:
			ll.append(('%u%s' % (count,prev_data[0]),prev_data[1],prev_data[2]))
		else:
			ll.append(prev_data)
	return ll

def uniqmask_to_str(uniqmask):
	if uniqmask==0:
		return "-"
	if uniqmask & UNIQ_MASK_IP:
		return "[IP]"
	if uniqmask & UNIQ_MASK_RACK:
		return "[RACK]"
	rstr = ""
	inrange = 0
	for i in range(26):
		if uniqmask & (1<<i):
			if inrange==0:
				rstr += chr(ord('A')+i)
				if i<24 and ((uniqmask>>i)&7)==7:
					inrange = 1
		else:
			if inrange==1:
				rstr += '-'
				rstr += chr(ord('A')+(i-1))
				inrange = 0
	if inrange:
		rstr += '-'
		rstr += chr(ord('A')+25)
	return rstr

def eattr_to_str(seteattr,clreattr):
	eattrs = ["noowner","noattrcache","noentrycache","nodatacache","snapshot","undeletable","appendonly","immutable"]
	outlist = []
	mask = 1
	for eattrname in eattrs:
		if seteattr&mask:
			outlist.append("+%s" % eattrname)
		if clreattr&mask:
			outlist.append("-%s" % eattrname)
		mask = mask << 1
	if len(outlist)>0:
		return ",".join(outlist)
	else:
		return "-"

def get_string_from_packet(data,pos,err):
	rstr = ""
	shift = 0
	if 1+pos > len(data):
		err = 1;
	if err==0:
		shift = data[pos]
		if shift+1+pos > len(data):
			err = 1
		else:
			rstr = data[pos+1:pos+1+shift]
		rstr = rstr.decode('utf-8','replace')
	return (rstr,pos+shift+1,err)

def get_longstring_from_packet(data,pos,err):
	rstr = ""
	shift = 0
	if 2+pos > len(data):
		err = 1;
	if err==0:
		shift = data[pos]*256+data[pos+1]
		if shift+2+pos > len(data):
			err = 1
		else:
			rstr = data[pos+2:pos+2+shift]
		rstr = rstr.decode('utf-8','replace')
	return (rstr,pos+shift+2,err)

def version_convert(versionxyzp):
	if versionxyzp>=(4,0,0) and versionxyzp<(4,11,0):
		return (versionxyzp,0)
	elif versionxyzp>=(2,0,0):
		return ((versionxyzp[0],versionxyzp[1],versionxyzp[2]//2),versionxyzp[2]&1)
	elif versionxyzp>=(1,7,0):
		return (versionxyzp,1)
	elif versionxyzp>(0,0,0):
		return (versionxyzp,0)
	else:
		return (versionxyzp,-1)

def version_str_sort_pro(versionxyzp):
	versionxyz,pro = version_convert(versionxyzp)
	strver = "%u.%u.%u" % versionxyz
	sortver = "%05u_%03u_%03u" % versionxyz
	if pro==1:
		strver += " PRO"
		sortver += "_2"
	elif pro==0:
		sortver += "_1"
	else:
		sortver += "_0"
	if strver == '0.0.0':
		strver = ''
	return (strver,sortver,pro)

# converts redundancy goal and actual to class and column number
# 0: missing, 1: endangered, 2: undergoal, 3: normal, 4: overgoal, 5: delete pending, 6: delete ready
def redundancy2colclass(goal,actual):
	if goal==0:
		if actual==0:
			clz = "DELETEREADY"
			col = MX_COL_DELETEREADY
		else:
			clz = "DELETEPENDING"
			col = MX_COL_DELETEPENDING
	elif actual==0:
		clz = "MISSING"
		col = MX_COL_MISSING
	elif actual>goal:
		clz = "OVERGOAL"
		col = MX_COL_OVERGOAL
	elif actual<goal:
		if actual==1:
			clz = "ENDANGERED"
			col = MX_COL_ENDANGERED
		else:
			clz = "UNDERGOAL"
			col = MX_COL_UNDERGOAL
	else:
		clz = "NORMAL"
		col = MX_COL_STABLE
	return (col,clz)

def disablesmask_to_string_list(disables_mask):
	cmds = ["chown","chmod","symlink","mkfifo","mkdev","mksock","mkdir","unlink","rmdir","rename","move","link","create","readdir","read","write","truncate","setlength","appendchunks","snapshot","settrash","setsclass","seteattr","setxattr","setfacl"]
	l = []
	m = 1
	for cmd in cmds:
		if disables_mask & m:
			l.append(cmd)
		m <<= 1
	return l

def disablesmask_to_string(disables_mask):
	return ",".join(disablesmask_to_string_list(disables_mask))




def charts_convert_data(datalist,mul,div,raw):
	res = []
	nodata = (2**64)-1
	for v in datalist:
		if v==nodata:
			res.append(None)
		else:
			if raw:
				res.append(v)
			else:
				res.append((v*mul)/div)
	return res

# Unpacks chart data from a given (ANTOCL_CHART_DATA) packet
def unpack_chart_packet(data,length):
	if length>=8:
		ranges,series,entries,perc,base = struct.unpack(">BBLBB",data[:8])
		if length==8+ranges*(13+series*entries*8):
			res = {}
			unpackstr = ">%uQ" % entries
			for r in range(ranges):
				rpos = 8 + r * (13+series*entries*8)
				rng,ts,mul,div = struct.unpack(">BLLL",data[rpos:rpos+13])
				rpos += 13
				if series>3:
					series=3
				l1 = None
				l2 = None
				l3 = None
				if series>=1:
					l1 = list(struct.unpack(unpackstr,data[rpos:rpos+entries*8]))
				if series>=2:
					l2 = list(struct.unpack(unpackstr,data[rpos+entries*8:rpos+2*entries*8]))
				if series>=3:
					l3 = list(struct.unpack(unpackstr,data[rpos+2*entries*8:rpos+3*entries*8]))
				res[rng] = (l1,l2,l3,ts,mul,div)
			return perc,base,res
		else:
			return None,None,None
	else:
		return None,None,None

# Handles a single chart data request (CLTOAN_CHART_DATA) to the given server (mfsconn)
def get_charts_multi_data(mfsconn,chartid,dataleng):
	data,length = mfsconn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LLB",chartid,dataleng,1))
	return unpack_chart_packet(data,length)

# Handles multiple, concurrent chart data requests (CLTOAN_CHART_DATA) to many chunkservers at once
def get_charts_multi_data_async(mfsmulticonn,chartid_arr,dataleng):
	all_charts = {}
	dataout_arr = []
	for chartid in chartid_arr:
		dataout_arr.append(struct.pack(">LLB",chartid,dataleng,1))
	answers=mfsmulticonn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,dataout_arr) #ask all registered chunkservers about chart data
	for hostkey in answers: # hostkey is string: 'ip:port'
		data_arr,length_arr = answers[hostkey]
		if len(chartid_arr)!=len(data_arr) or len(chartid_arr)!=len(length_arr):
			raise MFSCommunicationError("charts data length mismatch")
		for i in range(len(chartid_arr)):
			chartid = chartid_arr[i]
			data = data_arr[i]
			length = length_arr[i]
			perc,base,res = unpack_chart_packet(data,length)
			if perc!=None and base!=None and res!=None:
				chartkey = hostkey + ":" + str(chartid)
				all_charts[chartkey] = (perc,base,res)
	return all_charts

# Adjust chart data multiplication/division factors based on the base value
def adjust_muldiv(mul,div,base):
	base -= 2
	if base>0:
		mul = mul*(1000**base)
	if base<0:
		div = div*(1000**(-base))
	return mul,div
