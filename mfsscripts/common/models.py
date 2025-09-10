import json

try:
	from common.constants import *
	from common.utils import *
except:
	pass # imports may be unaccessible in a single mfscli file but they should be already inlined instead

#######################################
# A set of classes representing data models for particluar entities in a cluster (servers, disks, sessions, storage classes etc.)
class MasterServer:
	def __init__(self,host,port,versionxyzp,workingstate,sync,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime):
		self.host = host
		self.port = port
		try:
			iptab = tuple(map(int,host.split('.')))
			self.strip = "%u.%u.%u.%u" % iptab
			self.sortip = "%03u_%03u_%03u_%03u" % iptab
		except Exception:
			self.strip = host
			self.sortip = host
		self.version = versionxyzp
		self.strver, self.sortver, self.pro = version_str_sort_pro(versionxyzp)
		self.workingstate = workingstate
		self.sync = sync
		self.statestr = statestr
		self.statecolor = statecolor
		self.metaversion = metaversion
		self.memusage = memusage
		self.syscpu = syscpu
		self.usercpu = usercpu
		self.lastsuccessfulstore = lastsuccessfulstore
		self.lastsaveseconds = lastsaveseconds
		self.lastsavestatus = lastsavestatus
		self.exportschecksum = exportschecksum
		self.metaid = metaid
		self.lastsavemetaversion = lastsavemetaversion
		self.lastsavemetachecksum = lastsavemetachecksum
		self.usectime = usectime
		self.chlogtime = chlogtime
		self.secdelta = None  #updated separately by update_masterservers_delays
		self.metadelay = None #updated separately by update_masterservers_delays
		
		if self.workingstate==STATE_FOLLOWER:
			if sync==0:	self.statestr += " (DESYNC)"
			if sync==2:	self.statestr += " (DELAYED)"
			if sync==3:	self.statestr += " (INIT)"

	def is_active(self):
		return self.workingstate != STATE_UNREACHABLE

# 		self.featuremask = 0
# 		if self.version>=(3,0,72):
# 			self.featuremask |= (1<<FEATURE_EXPORT_UMASK)
# 		if (self.version>=(3,0,112) and self.version[0]==3) or self.version>=(4,21,0):
# 			self.featuremask |= (1<<FEATURE_EXPORT_DISABLES)
# 		if self.version>=(4,27,0):
# 			self.featuremask |= (1<<FEATURE_SESSION_STATS_28)
# 		if self.version>=(4,29,0):
# 			self.featuremask |= (1<<FEATURE_INSTANCE_NAME)
# 		if self.version>=(4,35,0):
# 			self.featuremask |= (1<<FEATURE_CSLIST_MODE)
# 		if self.version>=(4,44,0):
# 			self.featuremask |= (1<<FEATURE_SCLASS_IN_MATRIX)
# 		if self.version>=(4,51,0):
# 			self.featuremask |= (1<<FEATURE_DEFAULT_GRACEPERIOD)
# 		if self.version>=(4,53,0):
# 			self.featuremask |= (1<<FEATURE_LABELMODE_OVERRIDES)
# 		if self.version>=(4,57,0):
# 			self.featuremask |= (1<<FEATURE_SCLASSGROUPS)	

# 	def version_at_least(self,v1,v2,v3):
# 		return (self.version>=(v1,v2,v3))
# 	def version_less_than(self,v1,v2,v3):
# 		return (self.version<(v1,v2,v3))
# 	def version_is(self,v1,v2,v3):
# 		return (self.version==(v1,v2,v3))
# 	def version_unknown(self):
# 		return (self.version==(0,0,0))
# 	def is_pro(self):
# 		return self.pro
# 	def has_feature(self,featureid):
# 		return True if (self.featuremask & (1<<featureid)) else False
	


class Metalogger:
	# ip should be tuple: (ip1,ip2,ip3,ip4), version should be tuple: (v1,v2,v3)
	def __init__(self, ip, donotresolve, version):
		self.strip = "%u.%u.%u.%u" % ip
		self.sortip = "%03u_%03u_%03u_%03u" % ip
		self.host = resolve(self.strip, donotresolve)
		self.strver,self.sortver,self.pro = version_str_sort_pro(version)

class ClusterInfo:
	def __init__(self, version,memusage,syscpu,usercpu,totalspace,availspace,freespace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks,lastsuccessfulstore,lastsaveseconds,lastsavestatus,metainfomode,
																	allcopies,regularcopies,copychunks,ec8chunks,ec4chunks,chunkcopies,chunkec8parts,chunkec4parts,chunkhypcopies):
		self.version = version
		self.memusage = memusage
		self.syscpu = syscpu
		self.usercpu = usercpu
		self.totalspace = totalspace
		self.availspace = availspace
		self.freespace = freespace
		self.trspace = trspace
		self.trfiles = trfiles
		self.respace = respace
		self.refiles = refiles
		self.nodes = nodes
		self.dirs = dirs
		self.files = files
		self.chunks = chunks
		self.lastsuccessfulstore = lastsuccessfulstore
		self.lastsaveseconds = lastsaveseconds
		self.lastsavestatus = lastsavestatus
		self.metainfomode = metainfomode
		self.allcopies = allcopies
		self.regularcopies = regularcopies
		self.copychunks = copychunks
		self.ec8chunks = ec8chunks
		self.ec4chunks = ec4chunks
		self.chunkcopies = chunkcopies
		self.chunkec8parts = chunkec8parts
		self.chunkec4parts = chunkec4parts
		self.chunkhypcopies = chunkhypcopies
		self.strver,self.sortver,_ = version_str_sort_pro(version)

		if metainfomode==1:
			if (copychunks + ec8chunks + ec4chunks) > 0:
				self.ecchunkspercent = 100.0 * (ec8chunks + ec4chunks) / (copychunks + ec8chunks + ec4chunks)
				self.dataredundancyratio = (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts) / (copychunks + ec8chunks + ec4chunks)
			else:
				self.ecchunkspercent = None
				self.dataredundancyratio = None
			if (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts) > 0.0:
				noecratio = (chunkhypcopies / (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts))
				if noecratio>1.0:
					extranoecspace = noecratio - 1.0
				else:
					extranoecspace = 0.0
				if freespace!=None:
					self.savedbyec = int((totalspace-freespace) * extranoecspace)
				else:
					self.savedbyec = int((totalspace-availspace) * extranoecspace)
			else:
				self.savedbyec = None



class Licence:
	def __init__(self,licver,lictype,v1,v2,v3,licmaxtime,licleft,licmaxsize,currentsize,licid,lictypestr,licissuer,licuser,licjson,addinfo,quota,csmaxsize,csmaxcnt):
		self.licver = licver
		self.lictype = lictype
		self.v1 = v1
		self.v2 = v2
		self.v3 = v3
		self.licmaxtime = licmaxtime
		self.licleft = licleft
		self.licmaxsize = licmaxsize
		self.currentsize = currentsize
		self.licid = licid
		self.lictypestr = lictypestr
		self.licissuer = licissuer
		self.licuser = licuser
		self.licjson = licjson
		self.addinfo = addinfo
		self.quota = quota
		self.csmaxsize = csmaxsize
		self.csmaxcnt = csmaxcnt
		if self.licver and self.licver>=1 and len(self.licjson)>0:
			self.licextrainfo = json.loads(self.licjson)
		else:
			self.licextrainfo = {}

	def get_max_version_str(self):
		if self.v1==0xFFFF or self.licver == LICVER_CE:
			return "unlimited"
		elif self.v2==255:
			return "%u.x" % (self.v1)
		elif self.v3==255:
			return "%u.%u.x" % (self.v1,self.v2)
		else:
			return "%u.%u.%u" % (self.v1,self.v2,self.v3)

	# Returns true if given software version (v1,v2,v3) is allowed by the licence
	def is_allowed_version(self,version):
		if (self.licver == LICVER_CE):
			return True
		v1,v2,v3 = version
		if self.v1==0xFFFF:
			return True #unlimited
		if v1<self.v1:
			return True
		elif v1>self.v1:
			return False
		if self.v2==255:
			True		#A.x unlimited
		if v2<self.v2:
			return True
		elif v2>self.v2:
			return False
		if self.v3==255:
			True		#A.B.x unlimited
		if v3<self.v3:
			return True
		elif v3>self.v3:
			return False
		return True

	def is_time_unlimited(self):
		return self.licleft==0xFFFFFFFF or self.licmaxtime==0xFFFFFFFF or self.licver == LICVER_CE
	
	def is_cs_size_unlimited(self):
		return self.csmaxsize==0xFFFFFFFFFFFFFFFF or self.licver == LICVER_CE
	
	def is_cs_cnt_unlimited(self):
		return self.csmaxcnt==0xFFFF or self.licver == LICVER_CE
	
	def is_size_unlimited(self):
		return self.licmaxsize==0xFFFFFFFFFFFFFFFF or (self.licmaxsize==0 and self.currentsize==0) or self.licver == LICVER_CE


class HealthSelfCheck:
	def __init__(self,loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng,datastr):
		self.loopstart = loopstart
		self.loopend = loopend
		self.files = files
		self.ugfiles = ugfiles
		self.mfiles = mfiles
		self.mtfiles = mtfiles
		self.msfiles = msfiles
		self.chunks = chunks
		self.ugchunks = ugchunks
		self.mchunks = mchunks
		self.msgbuffleng = msgbuffleng
		self.datastr = datastr

# Missing chunk
class MissingChunk:
	def __init__(self,paths,inode,indx,chunkid,mtype):
		self.paths = paths
		self.inode = inode
		self.indx = indx
		self.chunkid = chunkid
		self.mtype = mtype

	def get_mtype_str(self):
		if self.mtype==MISSING_CHUNK_TYPE_NOCOPY:
			return "No copy"
		elif self.mtype==MISSING_CHUNK_TYPE_INVALID_COPIES:
			return "Invalid copies"
		elif self.mtype==MISSING_CHUNK_TYPE_WRONG_VERSIONS:
			return "Wrong versions"
		elif self.mtype==MISSING_CHUNK_TYPE_PARTIAL_EC:
			return "Missing EC parts"
		else:
			return "Other"

	def get_chunkid_str(self):
		return "%016X" % self.chunkid

class MemoryUsage:
	def __init__(self, memlabels,abrlabels,totalused,totalallocated,memusage):
		self.memlabels = memlabels
		self.abrlabels = abrlabels
		self.totalused = totalused
		self.totalallocated = totalallocated
		self.memusage = memusage

class ChunkServer:
	def __init__(self,oip,ip,donotresolve,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus,maintenanceto):
		self.oip = oip
		self.ip = ip
		self.version = (v1,v2,v3)
		self.stroip = "%u.%u.%u.%u" % oip
		self.strip = "%u.%u.%u.%u" % ip
		self.sortip = "%03u_%03u_%03u_%03u" % ip
		self.strver,self.sortver,self.pro = version_str_sort_pro((v1,v2,v3))
		self.host = resolve(self.strip, donotresolve)
		self.port = port
		self.csid = csid
		self.flags = flags
		self.used = used
		self.total = total
		self.chunks = chunks
		self.tdused = tdused
		self.tdtotal = tdtotal
		self.tdchunks = tdchunks
		self.errcnt = errcnt
		self.queue = queue
		self.gracetime = gracetime
		self.labels = labels
		self.mfrstatus = mfrstatus
		self.maintenanceto = maintenanceto
		self.hostkey = "%s:%s" % (self.strip,self.port)

		if self.is_maintenance_off():
			self.mmto = "n/a"
		elif self.maintenanceto==None:
			self.mmto = "unknown"
		elif self.maintenanceto==0xFFFFFFFF:
			self.mmto = "permanent"
		else:
			self.mmto = "%u seconds left" % self.maintenanceto

		if self.gracetime>=0xC0000000:
			self.queue_state = CS_LOAD_OVERLOADED
			self.queue_state_str = "Overloaded"
			self.queue_state_msg = "Overloaded"
			self.queue_state_info = "server queue is heavy loaded (overloaded)"
			self.queue_cgi = "<%u>" % self.queue
		elif self.gracetime>=0x80000000:
			self.queue_state = CS_LOAD_REBALANCE
			self.queue_state_str = "Rebalance"
			self.queue_state_msg = "Rebalancing"
			self.queue_state_info = "internal rebalance in progress"
			self.queue_cgi = "(%u)" % self.queue
		elif self.gracetime>=0x40000000:
			self.queue_state = CS_LOAD_FAST_REBALANCE
			self.queue_state_str = "Fast rebalance"
			self.queue_state_msg = "Fast rebalancing"
			self.queue_state_info = "high speed rebalance in progress"
			self.queue_cgi = "{%u}" % self.queue
			# queue_state_has_link = 1
		elif self.gracetime>0:
			self.queue_state = CS_LOAD_GRACEFUL
			self.queue_state_str = "G(%u)" % self.gracetime
			self.queue_state_msg = "%u secs graceful" % self.gracetime
			self.queue_state_info = "server in graceful period - back to normal after %u seconds" % self.gracetime
			self.queue_cgi = "[%u]" % self.queue
		else:
			self.queue_state = CS_LOAD_NORMAL
			self.queue_state_str = "Normal"
			self.queue_state_msg = "Normal load"
			self.queue_state_info = "server is responsive, working with low load"
			self.queue_cgi = "%u" % self.queue

		if self.labels==0xFFFFFFFF or self.labels==0:
			self.labelstr = "-"
		else:
			labelstab = []
			for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
				if self.labels & (1<<bit):
					labelstab.append(char)
			self.labelstr = ",".join(labelstab)		

	def is_maintenance_off(self):
		return (self.flags&6)==0

	def is_maintenance_on(self):
		return (self.flags&6)==2

	def is_connected(self):
		return (self.flags&1)==0

	def get_mfr_status_str(self):
		if self.tdchunks!=0:
			if self.mfrstatus==MFRSTATUS_VALIDATING:
				return 'not ready for removal (validating)'
			elif self.mfrstatus==MFRSTATUS_INPROGRESS:
				return 'not ready for removal (in progress)'
			elif self.mfrstatus==MFRSTATUS_READY:
				return 'removal ready'
			else:
				return 'not ready for removal (unrecognized status)'
		else:
			return ''
		
# Chunkserver's hard disk drive
class HDD:
	def __init__(self,csvalid,hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus):
		self.csvalid = csvalid
		self.hostkey = hostkey
		self.hoststr = hoststr
		self.hostip = hostip
		self.port = port
		self.hddpath = hddpath
		self.sortippath = sortippath
		self.ippath = ippath
		self.hostpath = hostpath
		self.flags = flags
		self.clearerrorarg = clearerrorarg
		self.errchunkid = errchunkid
		self.errtime = errtime
		self.used = used
		self.total = total
		self.chunkscnt = chunkscnt
		self.rbw = rbw
		self.wbw = wbw
		self.usecreadavg = usecreadavg
		self.usecwriteavg = usecwriteavg
		self.usecfsyncavg = usecfsyncavg
		self.usecreadmax = usecreadmax
		self.usecwritemax = usecwritemax
		self.usecfsyncmax = usecfsyncmax
		self.rops = rops
		self.wops = wops
		self.fsyncops = fsyncops
		self.rbytes = rbytes
		self.wbytes = wbytes
		self.mfrstatus = mfrstatus

	def __lt__(self, other):
		return self.sortippath < other.sortippath
	
	def is_valid(self):
		return self.csvalid==CS_HDD_CS_VALID
	def has_errors(self):
		return self.errtime!=0 or self.errchunkid!=0
	
	def get_statuslist(self):
		statuslist = []
		if (self.csvalid == CS_HDD_CS_TOO_OLD):
			statuslist.append('unknown (too old chunkserver)')
		elif (self.csvalid == CS_HDD_CS_UNREACHABLE):
			statuslist.append('unknown (unreachable chunkserver)')
		else:
			if (self.flags&CS_HDD_INVALID):
				statuslist.append('invalid')
			if (self.flags&CS_HDD_DAMAGED) and (self.flags&CS_HDD_SCANNING)==0 and (self.flags&CS_HDD_INVALID)==0:
				statuslist.append('damaged')
			if self.flags&CS_HDD_MFR:
				if self.mfrstatus==MFRSTATUS_VALIDATING:
					statuslist.append('removal not ready (validating)')
				elif self.mfrstatus==MFRSTATUS_INPROGRESS:
					statuslist.append('removal not ready (in progress)')
				elif self.mfrstatus==MFRSTATUS_READY:
					statuslist.append('removal ready')
				else:
					statuslist.append('removal not ready (unrecognized status)')
			if self.flags&CS_HDD_SCANNING:
				statuslist.append('scanning')
			if self.flags==0:
				if self.has_errors():
					statuslist.append('ok (errors)')
				else:
					statuslist.append('ok')
		return statuslist

	# Returns a string with a status of the HDD
	def get_status_str(self):
		return ", ".join(self.get_statuslist())

class ExportsEntry:
	def __init__(self,fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,path,meta,v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables):
		self.ipfrom = (fip1,fip2,fip3,fip4)
		self.ipto = (tip1,tip2,tip3,tip4)
		self.version = (v1,v2,v3)
		self.stripfrom = "%u.%u.%u.%u" % (fip1,fip2,fip3,fip4)
		self.sortipfrom = "%03u_%03u_%03u_%03u" % (fip1,fip2,fip3,fip4)
		self.stripto = "%u.%u.%u.%u" % (tip1,tip2,tip3,tip4)
		self.sortipto = "%03u_%03u_%03u_%03u" % (tip1,tip2,tip3,tip4)
		self.strver,self.sortver,self.pro = version_str_sort_pro((v1,v2,v3))
		self.meta = meta
		self.path = path
		self.exportflags = exportflags
		self.sesflags = sesflags
		self.umaskval = umaskval
		self.rootuid = rootuid
		self.rootgid = rootgid
		self.mapalluid = mapalluid
		self.mapallgid = mapallgid
		if sclassgroups==None:
			if mingoal==None and maxgoal==None:
				self.sclassgroups = 0xFFFF
			else:
				self.sclassgroups = ((0xFFFF<<mingoal) & (((0xFFFF<<(maxgoal+1))&0xFFFF)^0xFFFF)) | 1
		else:
			self.sclassgroups = sclassgroups
		self.mintrashretention = mintrashretention
		self.maxtrashretention = maxtrashretention
		self.disables = disables

		if self.sclassgroups==0:
			self.sclassgroups_sort = 0
			self.sclassgroups_str = 'NONE'
		elif self.sclassgroups==0xFFFF:
			self.sclassgroups_sort = 0xFFFF
			self.sclassgroups_str = "ALL"
		else:
			self.sclassgroups_sort = 0
			sclassgroups_list = []
			for b in range(16):
				self.sclassgroups_sort<<=1
				if self.sclassgroups & (1<<b):
					sclassgroups_list.append(b)
					self.sclassgroups_sort |= 1
			self.sclassgroups_str = ",".join(map(str,sclassgroups_list))

	def is_alldirs(self):
		return self.exportflags&1
	def is_password(self):
		return self.exportflags&2
	def is_readonly(self):
		return self.sesflags&1
	def is_unrestricted(self):
		return self.sesflags&2
	def ignore_gid(self):
		return self.sesflags&4
	def is_admin(self):
		return self.sesflags&8
	def map_user(self):
		return self.sesflags&16


class Session:
	def __init__(self,sessionid,ip,donotresolve,info,openfiles,nsocks,expire,v1,v2,v3,meta,path,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables,stats_c,stats_l):
		self.ip = ip
		self.version = (v1,v2,v3)
		self.strip = "%u.%u.%u.%u" % ip
		self.sortip = "%03u_%03u_%03u_%03u" % ip
		self.strver,self.sortver,self.pro = version_str_sort_pro((v1,v2,v3))
		self.host = resolve(self.strip, donotresolve)
		self.sessionid = sessionid
		self.info = info
		self.openfiles = openfiles
		self.nsocks = nsocks
		self.expire = expire
		self.meta = meta
		self.path = path
		self.sesflags = sesflags
		self.umaskval = umaskval
		self.rootuid = rootuid
		self.rootgid = rootgid
		self.mapalluid = mapalluid
		self.mapallgid = mapallgid
		if sclassgroups==None:
			if mingoal==None and maxgoal==None:
				self.sclassgroups = 0xFFFF
			else:
				self.sclassgroups = ((0xFFFF<<mingoal) & (((0xFFFF<<(maxgoal+1))&0xFFFF)^0xFFFF)) | 1
		else:
			self.sclassgroups = sclassgroups
		self.mintrashretention = mintrashretention
		self.maxtrashretention = maxtrashretention
		self.disables = disables
		self.stats_c = stats_c
		self.stats_l = stats_l

	def get_sessionstr(self):
		if self.sessionid & 0x80000000:
			return "TMP/%u" % (self.sessionid&0x7FFFFFFF)
		else:
			return "%u" % (self.sessionid)
		
	def is_tmp(self):
		return self.sessionid & 0x80000000

	def get_sclassgroups_sort(self):
		if self.sclassgroups==0:
			sclassgroups_sort = 0
		elif self.sclassgroups==0xFFFF:
			sclassgroups_sort = 0xFFFF
		else:
			sclassgroups_sort = 0
			for b in range(16):
				sclassgroups_sort<<=1
				if self.sclassgroups & (1<<b):
					sclassgroups_sort |= 1
		return sclassgroups_sort

	def get_sclassgroups_str(self):
		if self.sclassgroups==0:
			sclassgroups_str = 'NONE'
		elif self.sclassgroups==0xFFFF:
			sclassgroups_str = "ALL"
		else:
			sclassgroups_list = []
			for b in range(16):
				if self.sclassgroups & (1<<b):
					sclassgroups_list.append(b)
			sclassgroups_str = ",".join(map(str,sclassgroups_list))
		return sclassgroups_str

# Represents just a short info about sclass used for sclass listing
class StorageClassName:
	def __init__(self,name,has_chunks):
		self.name = name
		self.has_chunks = has_chunks

class StorageClass:
	def __init__(self,sclassid,sclassname,sclassdesc,priority,export_group,admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention,files,dirs,states,availableservers,labels_ver):
		self.sclassid = sclassid
		self.sclassname = sclassname
		self.sclassdesc = sclassdesc
		self.priority = priority
		self.export_group = export_group
		self.admin_only = admin_only
		self.labels_mode = labels_mode
		self.arch_mode = arch_mode
		self.arch_delay = arch_delay
		self.arch_min_size = arch_min_size
		self.min_trashretention = min_trashretention
		self.files = files
		self.dirs = dirs
		self.states = states
		self.availableservers = availableservers
		self.labels_ver = labels_ver

		arch_mode_list = []
		if arch_mode&SCLASS_ARCH_MODE_CHUNK:
			arch_mode_list.append("CHUNK")
		elif arch_mode&SCLASS_ARCH_MODE_FAST:
			arch_mode_list.append("FAST")
		else:
			arch_mode_list.append("[")
			if arch_mode&SCLASS_ARCH_MODE_CTIME:
				arch_mode_list.append("C")
			if arch_mode&SCLASS_ARCH_MODE_MTIME:
				arch_mode_list.append("M")
			if arch_mode&SCLASS_ARCH_MODE_ATIME:
				arch_mode_list.append("A")
			arch_mode_list.append("]TIME")
			if arch_mode&SCLASS_ARCH_MODE_REVERSIBLE:
				arch_mode_list.append("/REVERSIBLE")
			else:
				arch_mode_list.append("/ONEWAY")
		self.arch_mode_str = "".join(arch_mode_list)

		self.defined_create = False
		self.defined_archive = False
		self.defined_trash = False	
		for state in self.states:
			if state.name=="CREATE":
				self.defined_create = state.defined
			elif state.name=="ARCH":
				self.defined_archive = state.defined
			elif state.name=="TRASH":
				self.defined_trash = state.defined
		# admin_only_str = "YES" if admin_only else "NO"
		# labels_mode_str = "LOOSE" if labels_mode==0 else "STD" if labels_mode==1 else "STRICT"
		# arch_delay_str = hours_to_str(arch_delay) if (arch_delay>0 and (arch_mode&SCLASS_ARCH_MODE_FAST)==0) else "-"
		# min_trashretention_str = hours_to_str(min_trashretention) if min_trashretention>0 else "-"

	def get_admin_only_str(self):
		return "YES" if self.admin_only else "NO"
	def get_labels_mode_str(self):
		return "LOOSE" if self.labels_mode==0 else "STD" if self.labels_mode==1 else "STRICT"
	def get_min_trashretention_str(self):
		return hours_to_str(self.min_trashretention) if self.min_trashretention>0 else "-"
	def get_arch_delay_str(self):
		return hours_to_str(self.arch_delay) if (self.arch_delay>0 and (self.arch_mode&SCLASS_ARCH_MODE_FAST)==0) else "-"
	def get_arch_mode_str(self):
		return self.arch_mode_str
	def isKeepOnly(self):
		return not (self.defined_create or self.defined_archive or self.defined_trash)

class StorageClassState:
	def __init__(self,defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled):
		self.defined = defined
		self.name = name
		self.counters = counters
		self.ec_level = ec_level
		self.labellist = labellist
		self.uniqmask = uniqmask
		self.labelsmodeover = labelsmodeover
		self.canbefulfilled = canbefulfilled
		self.fullname = "ARCHIVE" if name=="ARCH" else name

		self.ec_data_parts = 0
		self.ec_chksum_parts = 0
		if self.ec_level!=None and self.ec_level>0:
			self.ec_data_parts = self.ec_level >> 4
			self.ec_chksum_parts = self.ec_level & 0xF
			if self.ec_data_parts==0:
				self.ec_data_parts = 8

	def isEC(self):
		return self.ec_level!=None and self.ec_level>0

class OPattern:
	def __init__(self,globname,euid,egid,priority,sclassname,trashretention,seteattr,clreattr):
		self.globname = globname
		self.euid = euid
		self.egid = egid
		self.priority = priority
		self.sclassname = sclassname
		self.trashretention = trashretention
		self.seteattr = seteattr
		self.clreattr = clreattr

		self.euidstr = "ANY" if self.euid==PATTERN_EUGID_ANY else ("%u" % self.euid)
		self.egidstr = "ANY" if self.egid==PATTERN_EUGID_ANY else ("%u" % self.egid)
		self.eattrstr = eattr_to_str(self.seteattr,self.clreattr)

class ChunkTestInfo72:
	def __init__(self,loopstart,loopend,del_invalid,ndel_invalid,del_unused,ndel_unused,del_dclean,ndel_dclean,del_ogoal,ndel_ogoal,rep_ugoal,nrep_ugoal,rep_wlab,nrep_wlab,rebalance,labels_dont_match,locked_unused,locked_used):
		self.loopstart = loopstart
		self.loopend = loopend
		self.del_invalid = del_invalid
		self.ndel_invalid = ndel_invalid
		self.del_unused = del_unused
		self.ndel_unused = ndel_unused
		self.del_dclean = del_dclean
		self.ndel_dclean = ndel_dclean
		self.del_ogoal = del_ogoal
		self.ndel_ogoal = ndel_ogoal
		self.rep_ugoal = rep_ugoal
		self.nrep_ugoal = nrep_ugoal
		self.rep_wlab = rep_wlab
		self.nrep_wlab = nrep_wlab
		self.rebalance = rebalance
		self.labels_dont_match = labels_dont_match
		self.locked_unused = locked_unused
		self.locked_used = locked_used		

class ChunkTestInfo96:
	def __init__(self,loopstart,loopend,fixed,forcekeep,delete_invalid,delete_no_longer_needed,delete_wrong_version,delete_duplicated_ecpart,delete_excess_ecpart,delete_excess_copy,delete_diskclean_ecpart,delete_diskclean_copy,replicate_dupserver_ecpart,replicate_needed_ecpart,replicate_needed_copy,replicate_wronglabels_ecpart,replicate_wronglabels_copy,split_copy_into_ecparts,join_ecparts_into_copy,recover_ecpart,calculate_ecchksum,locked_unused,locked_used,replicate_rebalance):
		self.loopstart = loopstart
		self.loopend = loopend
		self.fixed = fixed
		self.forcekeep = forcekeep
		self.delete_invalid = delete_invalid
		self.delete_no_longer_needed = delete_no_longer_needed
		self.delete_wrong_version = delete_wrong_version
		self.delete_duplicated_ecpart = delete_duplicated_ecpart
		self.delete_excess_ecpart = delete_excess_ecpart
		self.delete_excess_copy = delete_excess_copy
		self.delete_diskclean_ecpart = delete_diskclean_ecpart
		self.delete_diskclean_copy = delete_diskclean_copy
		self.replicate_dupserver_ecpart = replicate_dupserver_ecpart
		self.replicate_needed_ecpart = replicate_needed_ecpart
		self.replicate_needed_copy = replicate_needed_copy
		self.replicate_wronglabels_ecpart = replicate_wronglabels_ecpart
		self.replicate_wronglabels_copy = replicate_wronglabels_copy
		self.split_copy_into_ecparts = split_copy_into_ecparts
		self.join_ecparts_into_copy = join_ecparts_into_copy
		self.recover_ecpart = recover_ecpart
		self.calculate_ecchksum = calculate_ecchksum
		self.locked_unused = locked_unused
		self.locked_used = locked_used
		self.replicate_rebalance = replicate_rebalance
	
class OpenFile:
	def __init__(self,sessionid,host,sortipnum,ipnum,info,inode,paths):
		self.sessionid = sessionid
		self.host = host
		self.sortipnum = sortipnum
		self.ipnum = ipnum
		self.info = info
		self.inode = inode
		self.paths = paths

class AcquiredLock:
	def __init__(self,inode,sessionid,host,sortipnum,ipnum,info,owner,pid,start,end,ctype):
		self.inode = inode
		self.sessionid = sessionid
		self.host = host
		self.sortipnum = sortipnum
		self.ipnum = ipnum
		self.info = info
		self.owner = owner
		self.pid = pid
		self.start = start
		self.end = end
		self.ctype = ctype

		if self.pid==0 and self.start==0 and self.end==0:
			self.locktype = "FLOCK"
		else:
			self.locktype = "POSIX"

class Quota:
	def __init__(self,path,exceeded,qflags,graceperiod,timetoblock,sinodes,slength,ssize,srealsize,hinodes,hlength,hsize,hrealsize,cinodes,clength,csize,crealsize):
		self.path = path
		self.exceeded = exceeded
		self.qflags = qflags
		self.graceperiod = graceperiod
		self.timetoblock = timetoblock
		self.sinodes = sinodes
		self.slength = slength
		self.ssize = ssize
		self.srealsize = srealsize
		self.hinodes = hinodes
		self.hlength = hlength
		self.hsize = hsize
		self.hrealsize = hrealsize
		self.cinodes = cinodes
		self.clength = clength
		self.csize = csize
		self.crealsize = crealsize
