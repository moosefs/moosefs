import struct
from typing import List

try:
	from common.constants import *
	from common.errors import *
	from common.utils import *
	from common.models import *
	from common.conn import *
except:
	pass # imports may be unaccessible in a single mfscli file but they should be already inlined instead

class DataProvider:
	def __init__(self, cluster, donotresolve):
		self.cluster = cluster

		# Caching variables - to prevent many requests to the master:
		self.clusterinfo = None
		self.metaloggers = None
		self.matrices = {}
		self.matrix_summary = None
		self.missing_chunks = None
		self.chunkservers = None
		self.sessions = None
		self.exports = None
		self.sclasses_names = None
		self.sclasses = None
		self.opatterns = None
		self.chunktestinfo = None
		self.openfiles = None
		self.acquiredlocks = None
		self.quotas = None
		self.quotas_maxperc = 0

		self.donotresolve = donotresolve # don't resolve IPs to hostnames
		if self.master()!=None and self.master().has_feature(FEATURE_SESSION_STATS_28):
			self.stats_to_show = 28
		else:
			self.stats_to_show = 16

	def master(self):
		return self.cluster.master()
	
	def get_masterservers(self, IMorder=0, IMrev=False):
		return self.cluster.get_masterservers(IMorder, IMrev)

	def get_metaloggers(self, order=1, rev=False):
		if self.metaloggers==None:
			self.metaloggers=[]
			data,length = self.master().command(CLTOMA_MLOG_LIST,MATOCL_MLOG_LIST)
			if (length%8)==0:
				n = length//8
				for i in range(n):
					d = data[i*8:(i+1)*8]
					v1,v2,v3,ip1,ip2,ip3,ip4 = struct.unpack(">HBBBBBB",d)
					mls = Metalogger((ip1,ip2,ip3,ip4),self.donotresolve,(v1,v2,v3))
					self.metaloggers.append(mls)

		if order is None:
			return self.metaloggers
		
		if   order==1: key=lambda entry: (entry.host)
		elif order==2: key=lambda entry: (entry.sortip)
		elif order==3: key=lambda entry: (entry.sortver)
		else:          key=lambda entry: (entry.host)
		self.metaloggers.sort(key=key)
		if rev:
			self.metaloggers.reverse()
		return self.metaloggers
	
	def get_clusterinfo(self):
		if self.clusterinfo==None:
			masterinfo = self.cluster.masterinfo()
			length = len(masterinfo)
			if length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
				offset = 8 if (length>=137 and length!=173) else 0
				if offset==8:
					v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,freespace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQQLQLLLLL",masterinfo[:84+offset])
				else:
					v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQLQLLLLL",masterinfo[:84])
					freespace = None
				if length==205:
					lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",masterinfo[offset+96:offset+105])
				else:
					lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",masterinfo[offset+92:offset+101])

				if length>=173:
					metainfomode = 1
					if length==205:
						copychunks,ec8chunks,ec4chunks = struct.unpack(">LLL",masterinfo[offset+84:offset+96])
					else:
						copychunks,ec8chunks = struct.unpack(">LL",masterinfo[offset+84:offset+92])
						ec4chunks = 0
					if length==205:
						chunkcopies,chunkec8parts,chunkec4parts,chunkhypcopies = struct.unpack(">QQQQ",masterinfo[offset+153:offset+185])
					else:
						chunkcopies,chunkec8parts,chunkhypcopies = struct.unpack(">QQQ",masterinfo[offset+149:offset+173])
						chunkec4parts = 0
					allcopies = regularcopies = None
				else:
					metainfomode = 0
					allcopies,regularcopies = struct.unpack(">LL",masterinfo[offset+84:offset+92])
					copychunks = ec8chunks = ec4chunks = None
					chunkcopies = chunkec8parts = chunkec4parts = chunkhypcopies = None
				
				syscpu/=10000000.0
				usercpu/=10000000.0
				lastsaveseconds = lastsaveseconds / 1000.0
			else:
				raise Exception("Unrecognized MATOCL_INFO answer from the Master server (too old version?)")
			
			self.clusterinfo = ClusterInfo((v1,v2,v3),memusage,syscpu,usercpu,totalspace,availspace,freespace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks,lastsuccessfulstore,lastsaveseconds,lastsavestatus,metainfomode,
																	allcopies,regularcopies,copychunks,ec8chunks,ec4chunks,chunkcopies,chunkec8parts,chunkec4parts,chunkhypcopies)
		return self.clusterinfo
	
	def get_licence(self):
		return None
		
	def get_matrix(self, sclassid=-1):
		if not sclassid in self.matrices or self.progressstatus!=0:
			if self.master().has_feature(FEATURE_SCLASS_IN_MATRIX):
				if sclassid>=0:
					data,length = self.master().command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX,struct.pack(">BB",0,sclassid))
				else:
					data,length = self.master().command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX)
			else:
				if sclassid>=0:
					raise RuntimeError("storage class id in matrix is not supported by your master")
				data,length = self.master().command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX)
			if length==(1+484*2):
				self.progressstatus = struct.unpack(">B",data[0:1])[0]
				self.matrices[sclassid] = ([],[])
				for x in range(2):
					for i in range(11):
						self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
			elif length==(1+484*6):
				self.progressstatus = struct.unpack(">B",data[0:1])[0]
				self.matrices[sclassid] = ([],[],[],[],[],[])
				for x in range(6):
					for i in range(11):
						self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
			elif length==(1+484*8):
				self.progressstatus = struct.unpack(">B",data[0:1])[0]
				self.matrices[sclassid] = ([],[],[],[],[],[],[],[])
				for x in range(8):
					for i in range(11):
						self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
		return (self.matrices[sclassid], self.progressstatus)

	# Calculate sums of missing, undergoal and endangered (for all sclasses)
	# Returns an array of 8: [missing, endangered, undergoal, stable, overgoal, pending deletion, ready to be removed, and total no of chunks
	def get_matrix_summary(self):
		if self.matrix_summary:
			return (self.matrix_summary, self.progressstatus)
		self.matrix_summary = [0] * (MX_COL_TOTAL+1)
		(matrix,_)=self.get_matrix()

		for goal in range(11):
			for actual in range(11):
				(col,_)=redundancy2colclass(goal,actual)
				for i in range(len(matrix)):
					self.matrix_summary[col]+=matrix[0][goal][actual]
					self.matrix_summary[MX_COL_TOTAL]+=matrix[0][goal][actual]
		return (self.matrix_summary, self.progressstatus)

	def get_health_selfcheck(self):
		if (self.master().version_at_least(2,0,66) and self.master().version_less_than(3,0,0)) or self.master().version_at_least(3,0,19):
			data,length = self.master().command(CLTOMA_FSTEST_INFO,MATOCL_FSTEST_INFO,struct.pack(">B",0))
			pver = 1
		else:
			data,length = self.master().command(CLTOMA_FSTEST_INFO,MATOCL_FSTEST_INFO)
			pver = 0
		if length>=(36 + pver*8):
			if pver==1:
				loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng = struct.unpack(">LLLLLLLLLLL",data[:44])
				datastr = data[44:].decode('utf-8','replace')
			else:
				mtfiles = None
				msfiles = None
				loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,msgbuffleng = struct.unpack(">LLLLLLLLL",data[:36])
				datastr = data[36:].decode('utf-8','replace')
			return HealthSelfCheck(loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng,datastr)
		else:
			raise RuntimeError("MFS packet malformed (MATOCL_FSTEST_INFO)")


	# Returns a list of missing chunks (along with files paths) 
	def get_missing_chunks(self, MForder=0, MFrev=False):
		if self.missing_chunks==None:
			self.missing_chunks = []
			missing_inodes = set()
			missings = []
			if (self.master().version_at_least(3,0,25)):
				# get missing chunks list from master
				data,length = self.master().command(CLTOMA_MISSING_CHUNKS,MATOCL_MISSING_CHUNKS,struct.pack(">B",1))
				if length%17==0:
					n = length//17
					for x in range(n):
						chunkid,inode,indx,mtype = struct.unpack(">QLLB",data[x*17:x*17+17])
						missing_inodes.add(inode)
						missings.append((chunkid,inode,indx,mtype))
			inodepaths = self.resolve_inodes_paths(missing_inodes)
			for chunkid,inode,indx,mtype in missings:
				# resolve paths for inode if possible
				if inode in inodepaths:
					paths = inodepaths[inode]
				else:
					paths = []
				self.missing_chunks.append(MissingChunk(paths,inode,indx,chunkid,mtype))

		if MForder is None:
			return self.missing_chunks

		if   MForder==1:  key=lambda mc: (mc.paths)
		elif MForder==2:  key=lambda mc: (mc.inode)
		elif MForder==3:  key=lambda mc: (mc.indx)
		elif MForder==4:  key=lambda mc: (mc.chunkid)
		elif MForder==4:  key=lambda mc: (mc.mtype)
		else:             key=lambda mc: (mc.paths)

		self.missing_chunks.sort(key=key)
		if MFrev:
			self.missing_chunks.reverse()		
		return self.missing_chunks

	# Resolve paths for a list of inodes
	def resolve_inodes_paths(self, inodes):
		def create_chunks(list_name, n):
			listobj = list(list_name)
			for i in range(0, len(listobj), n):
				yield listobj[i:i + n]
		inodepaths = {}
		for chunk in create_chunks(inodes,100):
			if len(chunk)>0:
				data,length = self.master().command(CLTOMA_MASS_RESOLVE_PATHS,MATOCL_MASS_RESOLVE_PATHS,struct.pack(">"+len(chunk)*"L",*chunk))
				pos = 0
				while pos+8<=length:
					inode,psize = struct.unpack(">LL",data[pos:pos+8])
					pos+=8
					if psize == 0:
						if inode not in inodepaths:
							inodepaths[inode] = []
						inodepaths[inode].append("./META")
					elif pos + psize <= length:
						while psize>=4:
							pleng = struct.unpack(">L",data[pos:pos+4])[0]
							pos+=4
							psize-=4
							path = data[pos:pos+pleng]
							pos+=pleng
							psize-=pleng
							path = path.decode('utf-8','replace')
							if inode not in inodepaths:
								inodepaths[inode] = []
							inodepaths[inode].append(path)
						if psize!=0:
							raise RuntimeError("MFS packet malformed (MATOCL_MASS_RESOLVE_PATHS)")
				if pos!=length:
					raise RuntimeError("MFS packet malformed (MATOCL_MASS_RESOLVE_PATHS)")
		return inodepaths

	def get_memory_usage(self):
		data,length = self.master().command(CLTOMA_MEMORY_INFO,MATOCL_MEMORY_INFO)
		if length>=176 and length%16==0:
			memusage = struct.unpack(">QQQQQQQQQQQQQQQQQQQQQQ",data[:176])
			memlabels = ["Chunk hash","Chunks","CS lists","Edge hash","Edges","Node hash","Nodes","Deleted nodes","Chunk tabs","Symlinks","Quota"]
			abrlabels = ["c.h.","c.","c.l.","e.h.","e.","n.h.","n.","d.n.","c.t.","s.","q."]
			totalused = 0
			totalallocated = 0
			for i in range(len(memusage)>>1):
				totalused += memusage[1+i*2]
				totalallocated += memusage[i*2]
			return MemoryUsage(memlabels,abrlabels,totalused,totalallocated,memusage)
		else:
			raise RuntimeError("MFS packet malformed (MATOCL_MEMORY_INFO)")

	# Returns a list of all chunkservers in the cluster
	# CSorder - order of chunkservers in the list, None - no ordering, Default - order by IP address and port nunmber	
	# CSrev - reverse order in the returned list
	# CScsid - return only chunkserver with given id (used for displaying a single chunkserver info)
	def get_chunkservers(self, CSorder=0, CSrev=False):
		if self.chunkservers==None:
			self.chunkservers=[]
			if self.master().has_feature(FEATURE_CSLIST_MODE):
				data,length = self.master().command(CLTOMA_CSERV_LIST,MATOCL_CSERV_LIST,struct.pack(">B",1))
			else:
				data,length = self.master().command(CLTOMA_CSERV_LIST,MATOCL_CSERV_LIST)
			cs = None
			if self.master().version_at_least(4,35,0) and (length%77)==0:
				n = length//77
				for i in range(n):
					d = data[i*77:(i+1)*77]
					flags,v1,v2,v3,oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus,maintenanceto = struct.unpack(">BBBBBBBBBBBBHHQQLQQLLLLLBL",d)
					cs = ChunkServer((oip1,oip2,oip3,oip4),(ip1,ip2,ip3,ip4),self.donotresolve,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus,maintenanceto)
					self.chunkservers.append(cs)
			elif self.master().version_at_least(4,26,0) and self.master().version_less_than(4,35,0) and (length%73)==0:
				n = length//73
				for i in range(n):
					d = data[i*73:(i+1)*73]
					flags,v1,v2,v3,oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus = struct.unpack(">BBBBBBBBBBBBHHQQLQQLLLLLB",d)
					cs = ChunkServer((oip1,oip2,oip3,oip4),(ip1,ip2,ip3,ip4),self.donotresolve,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus,None)
					self.chunkservers.append(cs)
			elif self.master().version_at_least(3,0,38) and self.master().version_less_than(4,26,0) and (length%69)==0:
				n = length//69
				for i in range(n):
					d = data[i*69:(i+1)*69]
					flags,v1,v2,v3,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus = struct.unpack(">BBBBBBBBHHQQLQQLLLLLB",d)
					cs = ChunkServer((ip1,ip2,ip3,ip4),(ip1,ip2,ip3,ip4),self.donotresolve,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,mfrstatus,None)
					self.chunkservers.append(cs)
			elif self.master().version_at_least(2,1,0) and self.master().version_less_than(3,0,38) and (length%68)==0:
				n = length//68
				for i in range(n):
					d = data[i*68:(i+1)*68]
					flags,v1,v2,v3,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels = struct.unpack(">BBBBBBBBHHQQLQQLLLLL",d)
					cs = ChunkServer((ip1,ip2,ip3,ip4),(ip1,ip2,ip3,ip4),self.donotresolve,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,queue,gracetime,labels,None,None)
					self.chunkservers.append(cs)
			else:
				raise RuntimeError("Invalid chunkservers list data (MATOCL_CSERV_LIST)")

		if CSorder is None:
			return self.chunkservers
		
		if   CSorder==0:  key=lambda cs: (cs.sortip,cs.port)
		elif CSorder==1:  key=lambda cs: (cs.host)
		elif CSorder==2:  key=lambda cs: (cs.sortip,cs.port)
		elif CSorder==3:  key=lambda cs: (cs.port)
		elif CSorder==4:  key=lambda cs: (cs.csid)
		elif CSorder==5:  key=lambda cs: (cs.sortver)
		elif CSorder==6:  key=lambda cs: (cs.gracetime,cs.queue)
		elif CSorder==7:  key=lambda cs: (cs.queue)
		elif CSorder==10: key=lambda cs: (cs.chunks)
		elif CSorder==11: key=lambda cs: (cs.used)
		elif CSorder==12: key=lambda cs: (cs.total)
		elif CSorder==13: key=lambda cs: ((1.0*cs.used)/cs.total if cs.total>0 else 0)
		elif CSorder==20: key=lambda cs: (cs.tdchunks)
		elif CSorder==21: key=lambda cs: (cs.tdused)
		elif CSorder==22: key=lambda cs: (cs.tdtotal)
		elif CSorder==23: key=lambda cs: ((1.0*cs.tdused)/cs.tdtotal if cs.tdtotal>0 else 0)
		else:             key=lambda cs: (0)

		self.chunkservers.sort(key=key)
		if CSrev:
			self.chunkservers.reverse()

		return self.chunkservers

	# Returns a tuple of two lists: first list contains connected chunkservers, 
	# second list contains disconnected chunkservers
	def get_chunkservers_by_state(self, CSorder=0, CSrev=False):
		servers = []
		dservers = []
		for cs in self.get_chunkservers(CSorder, CSrev):
			if cs.is_connected():
				servers.append(cs)
			else:
				dservers.append(cs)
		return servers,dservers


	# Get list of hdds for chunk servers, 
	# HDdata - selects what disks should be returned: "ALL" - all, "ERR" - only with errors, "NOK" - only w/out ok status, "ip:port" - only for a given ip/port chunk server
	# HDorder - sort order
	# HDrev - reverse sort order
	# returns tuple: (hdds,scanhdds)
	def get_hdds(self, HDdata, HDperiod=0, HDtime=0, HDorder=0, HDrev=False):
		# get cs list
		hostlist = []
		multiconn = MFSMultiConn()
		for cs in self.get_chunkservers(None): # don't sort
			if cs.port>0 and cs.is_connected():
				hostip = "%u.%u.%u.%u" % cs.ip
				hostkey = "%s:%u" % (hostip,cs.port)
				sortip = "%03u.%03u.%03u.%03u:%05u" % (cs.ip[0],cs.ip[1],cs.ip[2],cs.ip[3],cs.port)
				if HDdata=="ALL" or HDdata=="ERR" or HDdata=="NOK" or HDdata==hostkey:
					hostlist.append((hostkey,hostip,sortip,cs.port,cs.version,cs.mfrstatus))
					multiconn.register(hostip,cs.port) #register CS to send command to

		if len(hostlist)==0:
			return ([], [])

		# get hdd lists - from all cs at once
		answers=multiconn.command(CLTOCS_HDD_LIST,CSTOCL_HDD_LIST) #ask all registered chunk servers about hdds
		hdds = []
		scanhdds = []
		for hostkey,hostip,sortip,port,version,mfrstatus in hostlist:
			hoststr = resolve(hostip, self.donotresolve)
			if version<=(1,6,8):
				hdds.append((sortip,HDD(CS_HDD_CS_TOO_OLD,hostkey,hoststr,hostip,port,"",sortip,hostkey,hoststr,0,0,0,0,0,0,0,[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],0)))
			elif hostkey not in answers:
				hdds.append((sortip,HDD(CS_HDD_CS_UNREACHABLE,hostkey,hoststr,hostip,port,"",sortip,hostkey,hoststr,0,0,0,0,0,0,0,[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],0)))
			else:
				data,length = answers[hostkey]
				while length>0:
					entrysize = struct.unpack(">H",data[:2])[0]
					entry = data[2:2+entrysize]
					data = data[2+entrysize:]
					length -= 2+entrysize
					plen = entry[0]
					hddpathhex = entry[1:plen+1].hex()
					hddpath = entry[1:plen+1]
					hddpath = hddpath.decode('utf-8','replace')
					hostpath = "%s:%u:%s" % (hoststr,port,hddpath)
					ippath = "%s:%u:%s" % (hostip,port,hddpath)
					clearerrorarg = "%s:%u:%s" % (hostip,port,hddpathhex)
					sortippath = "%s:%s" % (sortip,hddpath)
					flags,errchunkid,errtime,used,total,chunkscnt = struct.unpack(">BQLQQL",entry[plen+1:plen+34])
					rbytes = [0,0,0]
					wbytes = [0,0,0]
					usecreadsum = [0,0,0]
					usecwritesum = [0,0,0]
					usecfsyncsum = [0,0,0]
					rops = [0,0,0]
					wops = [0,0,0]
					fsyncops = [0,0,0]
					usecreadmax = [0,0,0]
					usecwritemax = [0,0,0]
					usecfsyncmax = [0,0,0]
					if entrysize==plen+34+144:
						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],rops[0],wops[0],usecreadmax[0],usecwritemax[0] = struct.unpack(">QQQQLLLL",entry[plen+34:plen+34+48])
						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],rops[1],wops[1],usecreadmax[1],usecwritemax[1] = struct.unpack(">QQQQLLLL",entry[plen+34+48:plen+34+96])
						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],rops[2],wops[2],usecreadmax[2],usecwritemax[2] = struct.unpack(">QQQQLLLL",entry[plen+34+96:plen+34+144])
					elif entrysize==plen+34+192:
						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],usecfsyncsum[0],rops[0],wops[0],fsyncops[0],usecreadmax[0],usecwritemax[0],usecfsyncmax[0] = struct.unpack(">QQQQQLLLLLL",entry[plen+34:plen+34+64])
						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],usecfsyncsum[1],rops[1],wops[1],fsyncops[1],usecreadmax[1],usecwritemax[1],usecfsyncmax[1] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+64:plen+34+128])
						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],usecfsyncsum[2],rops[2],wops[2],fsyncops[2],usecreadmax[2],usecwritemax[2],usecfsyncmax[2] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+128:plen+34+192])
					rbw = [0,0,0]
					wbw = [0,0,0]
					usecreadavg = [0,0,0]
					usecwriteavg = [0,0,0]
					usecfsyncavg = [0,0,0]
					for i in range(3):
						if usecreadsum[i]>0:
							rbw[i] = rbytes[i]*1000000//usecreadsum[i]
						if usecwritesum[i]+usecfsyncsum[i]>0:
							wbw[i] = wbytes[i]*1000000//(usecwritesum[i]+usecfsyncsum[i])
						if rops[i]>0:
							usecreadavg[i] = usecreadsum[i]//rops[i]
						if wops[i]>0:
							usecwriteavg[i] = usecwritesum[i]//wops[i]
						if fsyncops[i]>0:
							usecfsyncavg[i] = usecfsyncsum[i]//fsyncops[i]
					sf = sortippath #default sort field
					if   HDorder==1:	sf = sortippath
					elif HDorder==2:	sf = chunkscnt
					elif HDorder==3:	sf = errtime
					elif HDorder==4:	sf = -flags
					elif HDorder==5:	sf = rbw[HDperiod]
					elif HDorder==6:	sf = wbw[HDperiod]
					elif HDorder==7:	sf = usecreadavg[HDperiod] if HDtime==1 else usecreadmax[HDperiod]
					elif HDorder==8:	sf = usecwriteavg[HDperiod] if HDtime==1 else usecwritemax[HDperiod]
					elif HDorder==9:	sf = usecfsyncavg[HDperiod] if HDtime==1 else usecfsyncmax[HDperiod]
					elif HDorder==10:	sf = rops[HDperiod]
					elif HDorder==11:	sf = wops[HDperiod]
					elif HDorder==12:	sf = fsyncops[HDperiod]
					elif HDorder==20:	sf = used if flags&CS_HDD_SCANNING==0 else 0
					elif HDorder==21:	sf = total if flags&CS_HDD_SCANNING else 0
					elif HDorder==22:	sf = (1.0*used)/total if flags&CS_HDD_SCANNING==0 and total>0 else 0

					if (HDdata=="ERR" and errtime>0) or (HDdata=="NOK" and flags!=0) or (HDdata!="ERR" and HDdata!="NOK"):
						hdd = HDD(CS_HDD_CS_VALID,hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus)
						if flags&CS_HDD_SCANNING: #and not cgimode and ttymode:
							scanhdds.append((sf,hdd))
						else:
							hdds.append((sf,hdd))
		hdds.sort()
		scanhdds.sort()
		if HDrev:
			hdds.reverse()
			scanhdds.reverse()
		return (hdds, scanhdds)
	
	# Summarizes hdd info for given chunkserver,
	# returns tuple: string status of all disks (unknown or ok or warnings or errors) and a list of disks with a few basic fields only
	def cs_hdds_status(self, cs, hdds_all):
		hdds_out = []
		hdds_status = 'ok'
		for (_,hdd) in hdds_all:
			if cs.hostkey!=hdd.hostkey:
				continue
			if (hdd.flags&CS_HDD_INVALID):
				status = 'invalid'
				hdds_status = 'errors'
			elif hdd.flags&CS_HDD_SCANNING:
				status = 'scanning'
			elif (hdd.flags&CS_HDD_DAMAGED) and (hdd.flags&CS_HDD_SCANNING)==0 and (hdd.flags&CS_HDD_INVALID)==0:
				status = 'damaged'
				hdds_status = 'errors'
			elif hdd.flags&CS_HDD_MFR:
				status = 'marked for removal'
			elif hdd.flags==0:
				status = 'ok'
			else:
				status = 'unknown'
				if hdds_status!='errors':
					hdds_status = 'warnings' 
			if hdd.errtime>0:
				status = 'warning' 
				if hdds_status!='errors':
					hdds_status = 'warnings' 
			hdds_out.append({
				'flags': hdd.flags,
				'errtime': hdd.errtime,
				'status': status,
				'used': hdd.used,
				'total': hdd.total
				})
		return (hdds_status, hdds_out)


	
	def get_exports(self, EXorder=0, EXrev=False):
		if self.exports==None:
			self.exports=[]
			if self.master().has_feature(FEATURE_SCLASSGROUPS):
				data,length = self.master().command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",4))
			elif self.master().has_feature(FEATURE_EXPORT_DISABLES):
				data,length = self.master().command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",3))
			elif self.master().has_feature(FEATURE_EXPORT_UMASK):
				data,length = self.master().command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",2))
			else: #if self.master().version_at_least(1,6,26):
				data,length = self.master().command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",1))
			pos = 0
			while pos<length:
				fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,pleng = struct.unpack(">BBBBBBBBL",data[pos:pos+12])
				pos+=12
				path = data[pos:pos+pleng]
				path = path.decode('utf-8','replace')
				pos+=pleng
				if self.master().has_feature(FEATURE_SCLASSGROUPS):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables = struct.unpack(">HBBBBHLLLLHLLL",data[pos:pos+38])
					pos+=38
					mingoal = None
					maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.master().has_feature(FEATURE_EXPORT_DISABLES):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention,disables = struct.unpack(">HBBBBHLLLLBBLLL",data[pos:pos+38])
					pos+=38
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.master().has_feature(FEATURE_EXPORT_UMASK):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">HBBBBHLLLLBBLL",data[pos:pos+34])
					pos+=34
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				else: #if self.master().version_at_least(1,6,26):
					v1,v2,v3,exportflags,sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">HBBBBLLLLBBLL",data[pos:pos+32])
					pos+=32
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
					umaskval = None
				if path=='.':
					meta = 1
					umaskval = None
					disables = 0
				else:
					meta = 0
				expent = ExportsEntry(fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,path,meta,v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables)
				self.exports.append(expent)
		
		if EXorder is None:
			return self.exports
		
		if EXorder==1:  key=lambda ee: (ee.sortipfrom + ee.sortipto)
		elif EXorder==2:  key=lambda ee: (ee.sortipto + ee.sortipfrom)
		elif EXorder==3:  key=lambda ee: (ee.path)
		elif EXorder==4:  key=lambda ee: (ee.sortver)
		elif EXorder==5:  key=lambda ee: (None if ee.meta else ee.is_alldirs())
		elif EXorder==6:  key=lambda ee: (ee.is_password())
		elif EXorder==7:  key=lambda ee: (ee.is_readonly())
		elif EXorder==8:  key=lambda ee: (2-(ee.is_unrestricted()))
		elif EXorder==9:  key=lambda ee: (None if ee.meta else ee.ignore_gid())
		elif EXorder==10: key=lambda ee: (None if ee.meta else ee.is_admin())
		elif EXorder==11: key=lambda ee: (None if ee.meta else ee.rootuid)
		elif EXorder==12: key=lambda ee: (None if ee.meta else ee.rootgid)
		elif EXorder==13: key=lambda ee: (None if ee.meta or (ee.map_user())==0 else ee.mapalluid)
		elif EXorder==14: key=lambda ee: (None if ee.meta or (ee.map_user())==0 else ee.mapallgid)
		elif EXorder==15: key=lambda ee: (ee.sclassgroups)
		elif EXorder==16: key=lambda ee: (ee.sclassgroups)
		elif EXorder==17: key=lambda ee: (ee.mintrashretention)
		elif EXorder==18: key=lambda ee: (ee.maxtrashretention)
		elif EXorder==19: key=lambda ee: (ee.umaskval)
		elif EXorder==20: key=lambda ee: (ee.disables)
		else: key=lambda ee: (ee.sortipfrom + ee.sortipto)

		self.exports.sort(key=key)
		if EXrev:
			self.exports.reverse()

		return self.exports
	
	# Returns a list of all sessions (clients) connected to the cluster
	# MSorder - order of master servers in the list, None - no ordering, default 0 - order by IP
	# MSrev - reverse order in the returned list
	# Returns a list of Session objects
	def get_sessions(self, MSorder=0, MSrev=False):
		if self.sessions==None:
			self.sessions=[]
			if self.master().has_feature(FEATURE_SCLASSGROUPS):
				data,length = self.master().command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",5))
			elif self.master().has_feature(FEATURE_EXPORT_DISABLES):
				data,length = self.master().command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",4))
			elif self.master().has_feature(FEATURE_EXPORT_UMASK):
				data,length = self.master().command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",3))
			else: #if self.master().version_at_least(1,7,8):
				data,length = self.master().command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",2))
			statscnt = struct.unpack(">H",data[0:2])[0]
			pos = 2
			while pos<length:
				sessionid,ip1,ip2,ip3,ip4,v1,v2,v3,openfiles,nsocks,expire,ileng = struct.unpack(">LBBBBHBBLBLL",data[pos:pos+25])
				pos+=25
				info = data[pos:pos+ileng]
				pos+=ileng
				pleng = struct.unpack(">L",data[pos:pos+4])[0]
				pos+=4
				path = data[pos:pos+pleng]
				pos+=pleng
				info = info.decode('utf-8','replace')
				path = path.decode('utf-8','replace')
				if self.master().has_feature(FEATURE_SCLASSGROUPS):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables = struct.unpack(">BHLLLLHLLL",data[pos:pos+33])
					pos+=33
					mingoal = None
					maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.master().has_feature(FEATURE_EXPORT_DISABLES):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention,disables = struct.unpack(">BHLLLLBBLLL",data[pos:pos+33])
					pos+=33
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.master().has_feature(FEATURE_EXPORT_UMASK):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">BHLLLLBBLL",data[pos:pos+29])
					pos+=29
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				else: #if self.master().version_at_least(1,6,26):
					sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">BLLLLBBLL",data[pos:pos+27])
					pos+=27
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
					umaskval = None
				if statscnt<self.stats_to_show:
					stats_c = struct.unpack(">"+"L"*statscnt,data[pos:pos+4*statscnt])+(0,)*(self.stats_to_show-statscnt)
					pos+=statscnt*4
					stats_l = struct.unpack(">"+"L"*statscnt,data[pos:pos+4*statscnt])+(0,)*(self.stats_to_show-statscnt)
					pos+=statscnt*4
				else:
					stats_c = struct.unpack(">"+"L"*self.stats_to_show,data[pos:pos+4*self.stats_to_show])
					pos+=statscnt*4
					stats_l = struct.unpack(">"+"L"*self.stats_to_show,data[pos:pos+4*self.stats_to_show])
					pos+=statscnt*4
				if path=='.':
					meta=1
				else:
					meta=0
				ses = Session(sessionid,(ip1,ip2,ip3,ip4),self.donotresolve,info,openfiles,nsocks,expire,v1,v2,v3,meta,path,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables,stats_c,stats_l)
				self.sessions.append(ses)

		if MSorder is None:
			return self.sessions

		if   MSorder==1:	key=lambda ses: (ses.sessionid)
		elif MSorder==2:	key=lambda ses: (ses.host)
		elif MSorder==3:	key=lambda ses: (ses.sortip)
		elif MSorder==4:	key=lambda ses: (ses.info)
		elif MSorder==5:	key=lambda ses: (ses.openfiles)
		elif MSorder==6:	key=lambda ses: (ses.nsocks if ses.nsocks>0 else ses.expire)
		elif MSorder==7:	key=lambda ses: (ses.sortver)
		elif MSorder==8:	key=lambda ses: (ses.path)
		elif MSorder==9:	key=lambda ses: (ses.sesflags&1)
		elif MSorder==10:	key=lambda ses: (2-(ses.sesflags&2))
		elif MSorder==11:	key=lambda ses: (-1 if ses.meta else ses.sesflags&4)
		elif MSorder==12:	key=lambda ses: (-1 if ses.meta else ses.sesflags&8)
		elif MSorder==13:	key=lambda ses: (-1 if ses.meta else ses.rootuid)
		elif MSorder==14:	key=lambda ses: (-1 if ses.meta else ses.rootgid)
		elif MSorder==15:	key=lambda ses: (-1 if ses.meta or (ses.sesflags&16)==0 else ses.mapalluid)
		elif MSorder==16:	key=lambda ses: (-1 if ses.meta or (ses.sesflags&16)==0 else ses.mapallgid)
		elif MSorder==17:	key=lambda ses: (ses.sclassgroups)
		elif MSorder==18:	key=lambda ses: (ses.sclassgroups)
		elif MSorder==19:	key=lambda ses: (ses.mintrashretention)
		elif MSorder==20:	key=lambda ses: (ses.maxtrashretention)
		elif MSorder==21:	key=lambda ses: (ses.umaskval)
		elif MSorder==22:	key=lambda ses: (ses.disables)
		else:             key=lambda ses: (ses.sortip)

		self.sessions.sort(key=key)
		if MSrev:
			self.sessions.reverse()
		return self.sessions
	
	# Returns a tuple of two lists: first list contains connected sessions (with nsocks>0), second list contains disconnected sessions with nsocks==0
	def get_sessions_by_state(self, MSorder, MSrev):
		sessions = []
		dsessions = []
		for ses in self.get_sessions(MSorder,MSrev):
			if ses.nsocks>0:
				sessions.append(ses)
			else:
				dsessions.append(ses)
		return sessions,dsessions
	
	# Returns a list of sessions ordered by MO (operations) related parameters
	def get_sessions_order_by_mo(self, MOorder=1, MOrev=False, MOdata=0):
		msessions = []
		for ses in self.get_sessions(None): #don't sort
			if ses.path!='.':
				msessions.append(ses)

		if MOorder is None:
			return msessions

		if   MOorder == 1: key=lambda ses: (ses.host)
		elif MOorder == 2: key=lambda ses: (ses.sortip)
		elif MOorder == 3: key=lambda ses: (ses.info)
		elif MOorder >= 100 and MOorder < 100 + self.stats_to_show:
			if MOdata == 0:
				key=lambda ses: (-ses.stats_l[MOorder-100])
			else:
				key=lambda ses: (-ses.stats_c[MOorder-100])
		elif MOorder == 150:
			if MOdata == 0:
				key=lambda ses: (-sum(ses.stats_l))
			else:
				key=lambda ses: (-sum(ses.stats_c))
		else: key=lambda ses: (ses.host)
		
		msessions.sort(key=key)
		if MOrev:
			msessions.reverse()
		return msessions

	# Returns a list of storage classes names (e.g. for knobs or selects)
	def get_used_sclasses_names(self, order=1):
		if self.sclasses_names==None:
			self.sclasses_names = {}
			data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",128))
			pos = 0
			while pos < length:
				sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
				pos += 2
				sclassname = data[pos:pos+sclassnleng]
				sclassname = sclassname.decode('utf-8','replace')
				pos += sclassnleng
				has_chunks = struct.unpack_from(">B",data,pos)[0]
				pos += 1
				sclassobj = StorageClassName(sclassname,has_chunks)
				self.sclasses_names[sclassid] = sclassobj
		if order==1:	
			self.sclasses_names = dict(sorted(self.sclasses_names.items()))
		return self.sclasses_names
	
	# Returns a list of storage classes
	def get_sclasses(self, SCorder=0, SCrev=False):
		if self.sclasses==None:
			self.sclasses = []
			show_copy_and_ec = self.master().version_at_least(4,5,0)
			if self.master().has_feature(FEATURE_SCLASSGROUPS):
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",5))
				fver = 5
			elif self.master().has_feature(FEATURE_LABELMODE_OVERRIDES):
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",4))
				fver = 4
			elif self.master().version_at_least(4,34,0):
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",3))
				fver = 3
			elif self.master().version_at_least(4,5,0):
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",2))
				fver = 2
			elif self.master().version_at_least(4,2,0):
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",1))
				fver = 1
			else:
				data,length = self.master().command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO)
				fver = 0
			availableservers = struct.unpack(">H",data[:2])[0]
			pos = 2
			while pos < length:
				if show_copy_and_ec:
					createcounters = 6*[None]
					keepcounters = 6*[None]
					archcounters = 6*[None]
					trashcounters = 6*[None]
				else:
					createcounters = 3*[None]
					keepcounters = 3*[None]
					archcounters = 3*[None]
					trashcounters = 3*[None]
				if self.master().version_at_least(4,0,0):
					sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
					pos += 2
					sclassname = data[pos:pos+sclassnleng]
					sclassname = sclassname.decode('utf-8','replace')
					pos += sclassnleng
					if fver>=5:
						sclassdleng = data[pos]
						pos+=1
						sclassdesc = data[pos:pos+sclassdleng]
						sclassdesc = sclassdesc.decode('utf-8','replace')
						pos += sclassdleng
					else:
						sclassdesc = ""
						sclassdleng = 0
					files,dirs = struct.unpack_from(">LL",data,pos)
					pos += 8
					if fver>=2:
						for cntid in range(6):
							keepcounters[cntid],archcounters[cntid],trashcounters[cntid] = struct.unpack_from(">QQQ",data,pos)
							pos += 3*8
					else:
						for cntid in range(3):
							keepcounters[cntid],archcounters[cntid],trashcounters[cntid] = struct.unpack_from(">QQQ",data,pos)
							pos += 3*8
					if fver>=5:
						priority,export_group = struct.unpack_from(">LB",data,pos)
						pos += 5
					else:
						priority = None
						export_group = sclassid if sclassid<10 else 0
					if fver>=3:
						admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention = struct.unpack_from(">BBBHQH",data,pos)
						pos += 15
					elif fver>=1:
						admin_only,labels_mode,arch_mode,arch_delay,min_trashretention = struct.unpack_from(">BBBHH",data,pos)
						arch_min_size = 0
						pos += 7
					else:
						admin_only,labels_mode,arch_delay,min_trashretention = struct.unpack_from(">BBHH",data,pos)
						arch_mode = 1
						arch_min_size = 0
						pos += 6
					if fver>=4:
						create_canbefulfilled,create_labelscnt,create_uniqmask,create_labelsmode = struct.unpack_from(">BBLB",data,pos)
						pos += 7
						keep_canbefulfilled,keep_labelscnt,keep_uniqmask,keep_labelsmode = struct.unpack_from(">BBLB",data,pos)
						pos += 7
						arch_canbefulfilled,arch_labelscnt,arch_ec_level,arch_uniqmask,arch_labelsmode = struct.unpack_from(">BBBLB",data,pos)
						pos += 8
						trash_canbefulfilled,trash_labelscnt,trash_ec_level,trash_uniqmask,trash_labelsmode = struct.unpack_from(">BBBLB",data,pos)
						pos += 8
					else:
						create_canbefulfilled,create_labelscnt,create_uniqmask = struct.unpack_from(">BBL",data,pos)
						pos += 6
						keep_canbefulfilled,keep_labelscnt,keep_uniqmask = struct.unpack_from(">BBL",data,pos)
						pos += 6
						arch_canbefulfilled,arch_labelscnt,arch_ec_level,arch_uniqmask = struct.unpack_from(">BBBL",data,pos)
						pos += 7
						trash_canbefulfilled,trash_labelscnt,trash_ec_level,trash_uniqmask = struct.unpack_from(">BBBL",data,pos)
						pos += 7
						create_labelsmode = -1
						keep_labelsmode = -1
						arch_labelsmode = -1
						trash_labelsmode = -1
				elif self.master().version_at_least(3,0,75):
					sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
					pos += 2
					sclassname = data[pos:pos+sclassnleng]
					sclassname = sclassname.decode('utf-8','replace')
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					pos += sclassnleng
					files,dirs,keepcounters[0],archcounters[0],keepcounters[1],archcounters[1],keepcounters[2],archcounters[2],admin_only,labels_mode,arch_delay,create_canbefulfilled,create_labelscnt,keep_canbefulfilled,keep_labelscnt,arch_canbefulfilled,arch_labelscnt = struct.unpack_from(">LLQQQQQQBBHBBBBBB",data,pos)
					pos += 18 + 3 * 16
					if arch_delay==0:
						for cntid in range(3):
							keepcounters[cntid] += archcounters[cntid]
							archcounters[cntid] = None
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
					arch_mode = 1
					arch_min_size = 0
				elif self.master().version_at_least(3,0,9):
					sclassid,files,dirs,keepcounters[0],archcounters[0],keepcounters[1],archcounters[1],keepcounters[2],archcounters[2],labels_mode,arch_delay,create_canbefulfilled,create_labelscnt,keep_canbefulfilled,keep_labelscnt,arch_canbefulfilled,arch_labelscnt = struct.unpack_from(">BLLQQQQQQBHBBBBBB",data,pos)
					pos += 18 + 3 * 16
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					admin_only = 0
					if sclassid<10:
						sclassname = str(sclassid)
					else:
						sclassname = "sclass_%u" % (sclassid-9)
					if arch_delay==0:
						for cntid in range(3):
							keepcounters[cntid] += archcounters[cntid]
							archcounters[cntid] = None
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
					arch_mode = 1
					arch_min_size = 0
				else:
					sclassid,files,create_canbefulfilled,create_labelscnt = struct.unpack_from(">BLBB",data,pos)
					pos+=7
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					admin_only = 0
					if sclassid<10:
						sclassname = str(sclassid)
					else:
						sclassname = "sclass_%u" % (sclassid-9)
					dirs = 0
					if create_canbefulfilled:
						create_canbefulfilled = 3
					keep_canbefulfilled = create_canbefulfilled
					arch_canbefulfilled = create_canbefulfilled
					keep_labelscnt = create_labelscnt
					arch_labelscnt = create_labelscnt
					labels_mode = 1
					arch_mode = 1
					arch_delay = 0
					arch_min_size = 0
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
				if self.master().version_at_least(4,0,0):
					labels_ver = 4
					create_labellist = []
					keep_labellist = []
					arch_labellist = []
					trash_labellist = []
					for i in range(create_labelscnt):
						labelexpr = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						create_labellist.append((labelexpr_to_str(labelexpr),matchingservers,labelexpr))
					for i in range(keep_labelscnt):
						labelexpr = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						keep_labellist.append((labelexpr_to_str(labelexpr),matchingservers,labelexpr))
					for i in range(arch_labelscnt):
						labelexpr = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						arch_labellist.append((labelexpr_to_str(labelexpr),matchingservers,labelexpr))
					for i in range(trash_labelscnt):
						labelexpr = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						trash_labellist.append((labelexpr_to_str(labelexpr),matchingservers,labelexpr))
				else:
					labels_ver = 3
					create_labellist = []
					for i in range(create_labelscnt):
						labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
						pos+=4*MASKORGROUP
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						create_labellist.append((labelmasks_to_str(labelmasks),matchingservers,labelmasks))
					if self.master().version_at_least(3,0,9):
						keep_labellist = []
						for i in range(keep_labelscnt):
							labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
							pos+=4*MASKORGROUP
							matchingservers = struct.unpack_from(">H",data,pos)[0]
							pos+=2
							keep_labellist.append((labelmasks_to_str(labelmasks),matchingservers,labelmasks))
						arch_labellist = []
						for i in range(arch_labelscnt):
							labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
							pos+=4*MASKORGROUP
							matchingservers = struct.unpack_from(">H",data,pos)[0]
							pos+=2
							arch_labellist.append((labelmasks_to_str(labelmasks),matchingservers,labelmasks))
					else:
						keep_labellist = create_labellist
						arch_labellist = create_labellist
					trash_labellist = []
					
				states = []
				if len(create_labellist)>0:
					states.append(StorageClassState(1,"CREATE",createcounters,None,create_labellist,create_uniqmask,create_labelsmode,create_canbefulfilled))
				else:
					states.append(StorageClassState(0,"CREATE",createcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				states.append(StorageClassState(1,"KEEP",keepcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				if len(arch_labellist)>0 or arch_ec_level>0:
					states.append(StorageClassState(1,"ARCH",archcounters,arch_ec_level,arch_labellist,arch_uniqmask,arch_labelsmode,arch_canbefulfilled))
				else:
					states.append(StorageClassState(0,"ARCH",archcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				if len(trash_labellist)>0 or trash_ec_level>0:
					states.append(StorageClassState(1,"TRASH",trashcounters,trash_ec_level,trash_labellist,trash_uniqmask,trash_labelsmode,trash_canbefulfilled))
				else:
					states.append(StorageClassState(0,"TRASH",trashcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
			
				sc = StorageClass(sclassid,sclassname,sclassdesc,priority,export_group,admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention,files,dirs,states,availableservers,labels_ver)
				self.sclasses.append(sc)

		if SCorder is None:
			return self.sclasses

		if   SCorder==1:  key=lambda sc: (sc.sclassid)
		elif SCorder==2:  key=lambda sc: (sc.sclassname)
		elif SCorder==3:  key=lambda sc: (sc.admin_only)
		elif SCorder==4:  key=lambda sc: (sc.labels_mode)
		elif SCorder==5:  key=lambda sc: (sc.arch_mode)
		elif SCorder==6:  key=lambda sc: (sc.arch_delay)
		elif SCorder==7:  key=lambda sc: (sc.arch_min_size)
		elif SCorder==8:  key=lambda sc: (sc.min_trashretention)
		elif SCorder==9:  key=lambda sc: (sc.files)
		elif SCorder==10: key=lambda sc: (sc.dirs)
		else: key=lambda sc: (sc.sclassname)

		self.sclasses.sort(key=key)
		if SCrev:
			self.sclasses.reverse()

		return self.sclasses

	def get_opatterns(self, PAorder=1, PArev=False):
		if self.opatterns==None:
			self.opatterns = []
			data,length = self.master().command(CLTOMA_PATTERN_INFO,MATOCL_PATTERN_INFO)
			pos = 0
			while pos < length:
				globleng = struct.unpack_from(">B",data,pos)[0]
				pos += 1
				globname = data[pos:pos+globleng]
				globname = globname.decode('utf-8','replace')
				pos += globleng
				euid,egid,priority,omask,sclassleng = struct.unpack_from(">LLBBB",data,pos)
				pos += 11
				sclassname = data[pos:pos+sclassleng]
				sclassname = sclassname.decode('utf-8','replace')
				pos += sclassleng
				trashretention,seteattr,clreattr = struct.unpack_from(">HBB",data,pos)
				if (omask&PATTERN_OMASK_SCLASS)==0:
					sclassname = None
				if (omask&PATTERN_OMASK_TRASHRETENTION)==0:
					trashretention = None
				if (omask&PATTERN_OMASK_EATTR)==0:
					seteattr = 0
					clreattr = 0
				pos += 4
				self.opatterns.append(OPattern(globname,euid,egid,priority,sclassname,trashretention,seteattr,clreattr))

		if PAorder is None:
			return self.opatterns

		if PAorder==1: key=lambda op: (op.globname)
		elif PAorder==2: key=lambda op: (op.euid)
		elif PAorder==3: key=lambda op: (op.egid)
		elif PAorder==4: key=lambda op: (op.priority)
		elif PAorder==5: key=lambda op: (op.sclassname)
		elif PAorder==6: key=lambda op: (op.trashretention)
		elif PAorder==7: key=lambda op: (op.seteattr|op.clreattr)
		else: key=lambda op: (op.globname)

		self.opatterns.sort(key=key)
		if PArev:
			self.opatterns.reverse()
		
		return self.opatterns
	
	def get_chunktestinfo(self):
		if self.chunktestinfo==None:
			data,length = self.master().command(CLTOMA_CHUNKSTEST_INFO,MATOCL_CHUNKSTEST_INFO)
			if length==72:
				self.chunktestinfo = ChunkTestInfo72(*struct.unpack(">LLLLLLLLLLLLLLLLLL",data))
			elif length==96:
				self.chunktestinfo = ChunkTestInfo96(*struct.unpack(">"+24*"L",data))
			else:
				raise Exception("Invalid length of chunktestinfo response")
		return self.chunktestinfo
	
	# returns a list of open files for a given session, if sessionid is 0, returns a list of all open files
	def get_openfiles(self, OFsessionid=0, OForder=1, OFrev=False):
		if self.openfiles==None:
			self.openfiles = []
			sessionsdata = {}
			for ses in self.get_sessions():
				if ses.sessionid>0 and ses.sessionid < 0x80000000:
					sessionsdata[ses.sessionid]=(ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles)
			inodes = set()
			data,length = self.master().command(CLTOMA_LIST_OPEN_FILES,MATOCL_LIST_OPEN_FILES,struct.pack(">L",OFsessionid))
			openfiles = []
			if OFsessionid==0:
				n = length//8
				for x in range(n):
					sessionid,inode = struct.unpack(">LL",data[x*8:x*8+8])
					openfiles.append((sessionid,inode))
					inodes.add(inode)
			else:
				n = length//4
				for x in range(n):
					inode = struct.unpack(">L",data[x*4:x*4+4])[0]
					openfiles.append((OFsessionid,inode))
					inodes.add(inode)
			inodepaths = self.resolve_inodes_paths(inodes)

			for sessionid,inode in openfiles:
				if sessionid in sessionsdata:
					host,sortipnum,ipnum,info,openfiles = sessionsdata[sessionid]
				else:
					host = 'unknown'
					sortipnum = ''
					ipnum = ''
					info = 'unknown'
				if inode in inodepaths:
					paths = inodepaths[inode]
				else:
					paths = []
				self.openfiles.append(OpenFile(sessionid,host,sortipnum,ipnum,info,inode,paths))

		if OForder is None:
			return self.openfiles

		if   OForder==1: key=lambda of: (of.sessionid)
		elif OForder==2: key=lambda of: (of.host)
		elif OForder==3: key=lambda of: (of.sortipnum)
		elif OForder==4: key=lambda of: (of.info)
		elif OForder==5: key=lambda of: (of.inode)
		elif OForder==6: key=lambda of: (of.paths)
		else:            key=lambda of: (of.sortipnum)

		self.openfiles.sort(key=key)
		if OFrev:
			self.openfiles.reverse()
		
		return self.openfiles
	
	def get_acquiredlocks(self, ALinode=0, ALorder=1, ALrev=False):
		if self.acquiredlocks==None:
			self.acquiredlocks = []
			sessionsdata = {}
			for ses in self.get_sessions():
				if ses.sessionid>0 and ses.sessionid < 0x80000000:
					sessionsdata[ses.sessionid]=(ses.host,ses.sortip,ses.strip,ses.info)

			data,length = self.master().command(CLTOMA_LIST_ACQUIRED_LOCKS,MATOCL_LIST_ACQUIRED_LOCKS,struct.pack(">L",ALinode))
			locks = []
			if ALinode==0:
				n = length//37
				for x in range(n):
					inode,sessionid,owner,pid,start,end,ctype = struct.unpack(">LLQLQQB",data[x*37:x*37+37])
					locks.append((inode,sessionid,owner,pid,start,end,ctype))
			else:
				n = length//33
				for x in range(n):
					sessionid,owner,pid,start,end,ctype = struct.unpack(">LQLQQB",data[x*33:x*33+33])
					locks.append((ALinode,sessionid,owner,pid,start,end,ctype))
			for inode,sessionid,owner,pid,start,end,ctype in locks:
				if sessionid in sessionsdata:
					host,sortipnum,ipnum,info = sessionsdata[sessionid]
				else:
					host = 'unknown'
					sortipnum = ''
					ipnum = ''
					info = 'unknown'
				self.acquiredlocks.append(AcquiredLock(inode,sessionid,host,sortipnum,ipnum,info,owner,pid,start,end,ctype))

		if ALorder is None:
			return self.acquiredlocks

		if   ALorder==1: key=lambda al: (al.inode)
		elif ALorder==2: key=lambda al: (al.sessionid)
		elif ALorder==3: key=lambda al: (al.host)
		elif ALorder==4: key=lambda al: (al.sortipnum)
		elif ALorder==5: key=lambda al: (al.info)
		elif ALorder==6: key=lambda al: (al.locktype)
		elif ALorder==7: key=lambda al: (al.owner)
		elif ALorder==8: key=lambda al: (al.pid)
		elif ALorder==9: key=lambda al: (al.start)
		elif ALorder==10: key=lambda al: (al.end)
		elif ALorder==11: key=lambda al: (al.ctype)
		else:            key=lambda of: (of.inode)

		self.acquiredlocks.sort(key=key)
		if ALrev:
			self.acquiredlocks.reverse()
		
		return self.acquiredlocks		

	def get_quotas(self, QUorder=1, QUrev=False):
		if self.quotas==None:
			self.quotas = []
			self.quotas_maxperc = 0.0
			if self.master().has_feature(FEATURE_DEFAULT_GRACEPERIOD):
				data,length = self.master().command(CLTOMA_QUOTA_INFO,MATOCL_QUOTA_INFO,struct.pack(">B",1))
			else:
				data,length = self.master().command(CLTOMA_QUOTA_INFO,MATOCL_QUOTA_INFO)
			if length>=4:
				maxperc = 0.0
				pos = 0
				while pos<length:
					inode,pleng = struct.unpack(">LL",data[pos:pos+8])
					pos+=8
					path = data[pos:pos+pleng]
					path = path.decode('utf-8','replace')
					pos+=pleng
					if self.master().version_at_least(3,0,9):
						graceperiod,exceeded,qflags,timetoblock = struct.unpack(">LBBL",data[pos:pos+10])
						pos+=10
					else:
						exceeded,qflags,timetoblock = struct.unpack(">BBL",data[pos:pos+6])
						pos+=6
						graceperiod = 0
					sinodes,slength,ssize,srealsize = struct.unpack(">LQQQ",data[pos:pos+28])
					pos+=28
					hinodes,hlength,hsize,hrealsize = struct.unpack(">LQQQ",data[pos:pos+28])
					pos+=28
					cinodes,clength,csize,crealsize = struct.unpack(">LQQQ",data[pos:pos+28])
					pos+=28
					if (qflags&1) and sinodes>0:
						perc = 100.0*cinodes/sinodes
						if perc>maxperc: maxperc = perc
					if (qflags&2) and slength>0:
						perc = 100.0*clength/slength
						if perc>maxperc: maxperc = perc
					if (qflags&4) and ssize>0:
						perc = 100.0*csize/ssize
						if perc>maxperc: maxperc = perc
					if (qflags&8) and srealsize>0:
						perc = 100.0*crealsize/srealsize
						if perc>maxperc: maxperc = perc
					if (qflags&16) and hinodes>0:
						perc = 100.0*cinodes/hinodes
						if perc>maxperc: maxperc = perc
					if (qflags&32) and hlength>0:
						perc = 100.0*clength/hlength
						if perc>maxperc: maxperc = perc
					if (qflags&64) and hsize>0:
						perc = 100.0*csize/hsize
						if perc>maxperc: maxperc = perc
					if (qflags&128) and hrealsize>0:
						perc = 100.0*crealsize/hrealsize
						if perc>maxperc: maxperc = perc
					self.quotas.append(Quota(path,exceeded,qflags,graceperiod,timetoblock,sinodes,slength,ssize,srealsize,hinodes,hlength,hsize,hrealsize,cinodes,clength,csize,crealsize))
				self.quotas_maxperc = maxperc + 0.01

		if QUorder is None:
			return self.quotas_maxperc,self.quotas
				
		if   QUorder==1:  key=lambda qu: (qu.path)
		elif QUorder==2:  key=lambda qu: (qu.exceeded)
		elif QUorder==9:  key=lambda qu: (qu.graceperiod)
		elif QUorder==10: key=lambda qu: (qu.timetoblock)
		elif QUorder==11: key=lambda qu: (qu.sinodes)
		elif QUorder==12: key=lambda qu: (qu.slength)
		elif QUorder==13: key=lambda qu: (qu.ssize)
		elif QUorder==14: key=lambda qu: (qu.srealsize)
		elif QUorder==21: key=lambda qu: (qu.hinodes)
		elif QUorder==22: key=lambda qu: (qu.hlength)
		elif QUorder==23: key=lambda qu: (qu.hsize)
		elif QUorder==24: key=lambda qu: (qu.hrealsize)
		elif QUorder==31: key=lambda qu: (qu.cinodes)
		elif QUorder==32: key=lambda qu: (qu.clength)
		elif QUorder==33: key=lambda qu: (qu.csize)
		elif QUorder==34: key=lambda qu: (qu.crealsize)
		elif QUorder==41: key=lambda qu: ((-1,0) if (qu.qflags&1)==0 else (1,0) if qu.sinodes==0 else (0,1.0*qu.cinodes/qu.sinodes))
		elif QUorder==42: key=lambda qu: ((-1,0) if (qu.qflags&2)==0 else (1,0) if qu.slength==0 else (0,1.0*qu.clength/qu.slength))
		elif QUorder==43: key=lambda qu: ((-1,0) if (qu.qflags&4)==0 else (1,0) if qu.ssize==0 else (0,1.0*qu.csize/qu.ssize))
		elif QUorder==44: key=lambda qu: ((-1,0) if (qu.qflags&8)==0 else (1,0) if qu.srealsize==0 else (0,1.0*qu.crealsize/qu.srealsize))
		elif QUorder==51: key=lambda qu: ((-1,0) if (qu.qflags&16)==0 else (1,0) if qu.hinodes==0 else (0,1.0*qu.cinodes/qu.hinodes))
		elif QUorder==52: key=lambda qu: ((-1,0) if (qu.qflags&32)==0 else (1,0) if qu.hlength==0 else (0,1.0*qu.clength/qu.hlength))
		elif QUorder==53: key=lambda qu: ((-1,0) if (qu.qflags&64)==0 else (1,0) if qu.hsize==0 else (0,1.0*qu.csize/qu.hsize))
		elif QUorder==54: key=lambda qu: ((-1,0) if (qu.qflags&128)==0 else (1,0) if qu.hrealsize==0 else (0,1.0*qu.crealsize/qu.hrealsize))
		else:             key=lambda of: (of.path)

		self.quotas.sort(key=key)
		if QUrev:
			self.quotas.reverse()
		
		return self.quotas_maxperc,self.quotas
	
