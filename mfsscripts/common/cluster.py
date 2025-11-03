from common.constants import *
from common.utils import *
from common.conn import *
from common.models import *

# # called asynchronously after receiving the answer to the info command from each master server
# # it should decode the answer and built the master server object
# def process_info_answer(host,port,data,length,error):
# 	# print("process_info_answer data: %s\n\n" % data)
# 	version = (0,0,0)
# 	workingstate = STATE_DUMMY
# 	statestr = ""
# 	statecolor = 1
# 	memusage = 0
# 	syscpu = 0
# 	usercpu = 0
# 	lastsuccessfulstore = 0
# 	lastsaveseconds = 0
# 	lastsavestatus = 0
# 	metaversion = 0
# 	exportschecksum = None
# 	metaid = None
# 	lastsavemetaversion = None
# 	lastsavemetachecksum = None
# 	usectime = None
# 	chlogtime = 0
# 	try:
# 		if length==52:
# 			version = (1,4,0)
# 		elif length==60:
# 			version = (1,5,0)
# 		elif length==68 or length==76 or length==101: #(size < 121 => version < 2.0.0)
# 			version = struct.unpack(">HBB",data[:4])
# 		# (size = 121,version >= 2.0.0) (size = 129,version >= 3.0.32) (size = 173,version >= 4.4.0)
# 		elif length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
# 			offset = 8 if (length>=137 and length!=173) else 0
# 			version = struct.unpack(">HBB",data[:4])
# 			memusage,syscpu,usercpu = struct.unpack(">QQQ",data[4:28])
# 			syscpu/=10000000.0
# 			usercpu/=10000000.0
# 			if length==205:
# 				offset += 4
# 			lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",data[offset+92:offset+101])
# 			if version>(2,0,14):
# 				lastsaveseconds = lastsaveseconds / 1000.0
# 			workingstate,nextstate,stablestate,sync,leaderip,changetime,metaversion = struct.unpack(">BBBBLLQ",data[offset+101:offset+121])
# 			if length>=129:
# 				exportschecksum = struct.unpack(">Q",data[offset+121:offset+129])[0]
# 			if length>=173:
# 				metaid,lastsavemetaversion,lastsavemetachecksum = struct.unpack(">QQL",data[offset+129:offset+149])
# 			if length==149 or length==193 or length==205:
# 				usectime,chlogtime = struct.unpack(">QL",data[length-12:length])
# 			if workingstate==STATE_MASTERCE and nextstate==STATE_MASTERCE and stablestate==STATE_MASTERCE and sync==STATE_MASTERCE:
# 				statestr = state_name(workingstate) # "MASTER"
# 				statecolor = 0
# 			elif stablestate==0 or workingstate!=nextstate:
# 				statestr = "transition %s -> %s" % (state_name(workingstate),state_name(nextstate))
# 				statecolor = 8
# 			else:
# 				statestr = state_name(workingstate)
# 				statecolor = state_color(workingstate,sync)
# 		else:
# 			version = (0,0,0) # unknown version
# 		# except Exception:
# 		# 	statestr = STATE_STR_BUSY
# 		# 	statecolor = 7
# 	except Exception:
# 		workingstate = STATE_UNREACHABLE
# 		statestr = STATE_STR_UNREACHABLE
# 	ms = MasterServer(host,port,version,workingstate,sync,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime)
# 	return ms

# Represents state of master servers in the cluster: leader, elect etc.
class Cluster:
	# possible_leader_ip is a hint for possible leading master IP address (the previously known leader)
	def __init__(self, masterhost, masterport, possible_leader_ip=None):
		self.masterhost = masterhost
		self.masterport = masterport

		# Don't use the following underscored variables directly, use the methods instead
		self._masterconn = None
		self._masterlist = None
		self._masterinfo = None
		self._master_exportschecksum = 0
		self._master_metaid = 0
		self._leader_usectime = None

		self._errormsg = "Unknown error"

		self._leaderfound = 0
		self._electfound = 0
		self._usurperfound = 0
		self._followerfound = 0
		self._deputyfound = 0

		# Resolve master hostname to all IP addresses
		self.addresses = []
		for mhost in self.masterhost.replace(';',' ').replace(',',' ').split():
			try:
				for i in socket.getaddrinfo(mhost,self.masterport,socket.AF_INET,socket.SOCK_STREAM,socket.SOL_TCP):
					if i[0]==socket.AF_INET and i[1]==socket.SOCK_STREAM and i[2]==socket.SOL_TCP:
						self.addresses.append(i[4])
						# print("Master address: %s:%u" % i[4])
			except Exception:
				pass

		if len(self.addresses)==0:
			self._errormsg = """Can't resolve the MooseFS Master hostname (%s)""" % (masterhost)
		elif len(self.addresses)==1:
			self._errormsg = """Can't connect to the MooseFS Master server (%s)""" % (masterhost)
		else:
			self._errormsg = """Can't connect to MooseFS Master servers (%s)""" % (masterhost)

		# self.find_all_masters()
		
		if possible_leader_ip:
			# put possible leader to the beginning of the list
			self.addresses.sort(key=lambda x: x[0]!=possible_leader_ip)

		# Find master servers in the cluster, find them all if there is no hint for possible leading master IP address
		self.find_masters(find_all=(possible_leader_ip==None))

	# Returns True if any master server is found in the cluster
	def anyfound(self):
		return self._leaderfound or self._electfound or self._usurperfound or self._followerfound or self._deputyfound
	
	# Returns True if any leading master server is found in the cluster (leader, elect, usurper, or deputy)
	def leadingfound(self):
		return self._leaderfound or self._electfound or self._usurperfound or self._deputyfound

	# Return the current leading master server (connection)
	def master(self):
		return self._masterconn

	def errormsg(self):
		return self._errormsg

	def leaderfound(self):
		return self._leaderfound
	def followerfound(self):
		return self._followerfound
	def electfound(self):
		return self._electfound
	def usurperfound(self):
		return self._usurperfound
	def deputyfound(self):
		return self._deputyfound

	def masterinfo(self):
		return self._masterinfo
	def master_metaid(self):
		return self._master_metaid
	def master_exportschecksum(self):
		return self._master_exportschecksum


	# def find_all_masters(self):
	# 	multiconn = MFSMultiConn(0.5,self.addresses)
	# 	answers=multiconn.command(CLTOMA_INFO,MATOCL_INFO,None,process_info_answer) 



	def find_masters(self, find_all):
		# Don't use the following variables directly, use the methods instead
		self._masterconn = None
		self._masterlist = None
		self._masterinfo = None
		self._master_exportschecksum = 0
		self._master_metaid = 0
		self._leader_usectime = None

		self._leaderfound = 0
		self._electfound = 0
		self._usurperfound = 0
		self._followerfound = 0
		self._deputyfound = 0

		leaderinfo = None
		leader_exportschecksum = None
		leader_metaid = None
		leaderconn = None

		electinfo = None
		elect_exportschecksum = None
		elect_metaid = None
		electconn = None

		usurperinfo = None
		usurper_exportschecksum = None
		usurper_metaid = None
		usurperconn = None

		followerinfo = None
		follower_exportschecksum = None
		follower_metaid = None
		followerconn = None

		if find_all:
			self._masterlist = []

		# find leader
		for mhost,mport in self.addresses:
			conn = None
			version = (0,0,0)
			workingstate = STATE_DUMMY
			sync = 0
			statestr = "???"
			statecolor = 1
			memusage = 0
			syscpu = 0
			usercpu = 0
			lastsuccessfulstore = 0
			lastsaveseconds = 0
			lastsavestatus = 0
			metaversion = 0
			exportschecksum = None
			metaid = None
			lastsavemetaversion = None
			lastsavemetachecksum = None
			usectime = None
			chlogtime = 0
			try:
				conn = MasterConn(mhost,mport)
				try:
					data,length = conn.command(CLTOMA_INFO,MATOCL_INFO)
					if length==52:
						version = (1,4,0)
						conn.set_version(version)
						if self._leaderfound==0:
							leaderconn = conn
							leaderinfo = data
							self._leaderfound = 1
						statestr = "OLD MASTER (LEADER ONLY)"
						statecolor = 0
					elif length==60:
						version = (1,5,0)
						conn.set_version(version)
						if self._leaderfound==0:
							leaderconn = conn
							leaderinfo = data
							self._leaderfound = 1
						statestr = "OLD MASTER (LEADER ONLY)"
						statecolor = 0
					elif length==68 or length==76 or length==101:
						version = struct.unpack(">HBB",data[:4])
						conn.set_version(version)
						if self._leaderfound==0 and version<(1,7,0):
							leaderconn = conn
							leaderinfo = data
							self._leaderfound = 1
						if length==76:
							memusage = struct.unpack(">Q",data[4:12])[0]
						if length==101:
							memusage,syscpu,usercpu = struct.unpack(">QQQ",data[4:28])
							syscpu/=10000000.0
							usercpu/=10000000.0
							lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",data[92:101])
						if version<(1,7,0):
							statestr = "OLD MASTER (LEADER ONLY)"
							statecolor = 0
						else:
							statestr = "UPGRADE THIS UNIT!"
							statecolor = 2
					elif length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
						offset = 8 if (length>=137 and length!=173) else 0
						version = struct.unpack(">HBB",data[:4])
						conn.set_version(version)
						memusage,syscpu,usercpu = struct.unpack(">QQQ",data[4:28])
						syscpu/=10000000.0
						usercpu/=10000000.0
						if length==205:
							offset += 4
						lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",data[offset+92:offset+101])
						if conn.version_at_least(2,0,14):
							lastsaveseconds = lastsaveseconds / 1000.0
						workingstate,nextstate,stablestate,sync,leaderip,changetime,metaversion = struct.unpack(">BBBBLLQ",data[offset+101:offset+121])
						if length>=129:
							exportschecksum = struct.unpack(">Q",data[offset+121:offset+129])[0]
						if length>=173:
							metaid,lastsavemetaversion,lastsavemetachecksum = struct.unpack(">QQL",data[offset+129:offset+149])
						if length==149 or length==193 or length==205:
							usectime,chlogtime = struct.unpack(">QL",data[length-12:length])
						if workingstate==STATE_MASTERCE and nextstate==STATE_MASTERCE and stablestate==STATE_MASTERCE and sync==0xFF:
							if self._leaderfound==0:
								leaderconn = conn
								leaderinfo = data
								self._leaderfound = 1
								leader_exportschecksum = exportschecksum
								leader_metaid = metaid
								self._leader_usectime = usectime
							statestr = state_name(workingstate) # "MASTER"
							statecolor = 0
						elif stablestate==0 or workingstate!=nextstate:
							statestr = "transition %s -> %s" % (state_name(workingstate),state_name(nextstate))
							statecolor = 8
						else:
							statestr = state_name(workingstate)
							statecolor = state_color(workingstate,sync)
							if workingstate==STATE_FOLLOWER:
								# if sync==0:
								# 	statestr += " (DESYNC)"
								# if sync==2:
								# 	statestr += " (DELAYED)"
								# if sync==3:
								# 	statestr += " (INIT)"
								self._followerfound = 1
								followerconn = conn
								followerinfo = data
								follower_exportschecksum = exportschecksum
								follower_metaid = metaid
							if workingstate==STATE_USURPER and self._usurperfound==0:
								self._usurperfound = 1
								usurperconn = conn
								usurperinfo = data
								usurper_exportschecksum = exportschecksum
								usurper_metaid = metaid
							if workingstate==STATE_ELECT and self._electfound==0:
								self._electfound = 1
								electconn = conn
								electinfo = data
								elect_exportschecksum = exportschecksum
								elect_metaid = metaid
							if (workingstate==STATE_LEADER or workingstate==STATE_DEPUTY) and self._leaderfound==0:
								leaderconn = conn
								leaderinfo = data
								self._leaderfound = 1
								if (workingstate==STATE_DEPUTY):
									self._deputyfound = 1
								leader_exportschecksum = exportschecksum
								leader_metaid = metaid
								self._leader_usectime = usectime
					else:
						if len(self.addresses)==1:
							self._errormsg = """Got wrong answer from the MooseFS Master server (%s) - likely the master version is too new""" % (self.masterhost)
						else:
							self._errormsg = """Got wrong answer from MooseFS Master servers (%s) - likely the masters version is too new""" % (self.masterhost)
						statestr = STATE_STR_WRONGANSWER
						statecolor = 1
				except Exception:
					statestr = STATE_STR_BUSY
					statecolor = 7
			except Exception:
				workingstate = STATE_UNREACHABLE
				statestr = STATE_STR_UNREACHABLE

			if conn and conn!=leaderconn and conn!=electconn and conn!=usurperconn and conn!=followerconn:
				del conn
			if find_all:
				self._masterlist.append(MasterServer(mhost,mport,version,workingstate,sync,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime))

			if self._leaderfound:
				self._masterconn = leaderconn
				self._masterinfo = leaderinfo
				self._master_exportschecksum = leader_exportschecksum
				self._master_metaid = leader_metaid
				if not find_all:
					break # don't need to find other masters if leader is found
			elif self._electfound:
				self._masterconn = electconn
				self._masterinfo = electinfo
				self._master_exportschecksum = elect_exportschecksum
				self._master_metaid = elect_metaid
			elif self._usurperfound:
				self._masterconn = usurperconn
				self._masterinfo = usurperinfo
				self._master_exportschecksum = usurper_exportschecksum
				self._master_metaid = usurper_metaid
			elif self._followerfound:
				self._masterconn = followerconn
				self._masterinfo = followerinfo
				self._master_exportschecksum = follower_exportschecksum
				self._master_metaid = follower_metaid



	# Returns a list of all master servers in the cluster along with its highest metaversion and checksum (tuple)
	# Sorts if IMorder - sort order is provided, IMrev - reverse order
	def get_masterservers(self, IMorder=0, IMrev=False):
		if self._masterlist==None: # _masterlist could not be initialized if only the leader was found on initialization
			self.find_masters(True)
			# self.find_all_masters(None, True)

		if IMorder==None:
			return self._masterlist
		masterservers = []
		for ms in self._masterlist:
			if   IMorder==0:	key=lambda ms: (ms.sortip, ms.port)
			elif IMorder==1:	key=lambda ms: (ms.sortip)
			elif IMorder==2:	key=lambda ms: (ms.sortver)
			elif IMorder==3:	key=lambda ms: (ms.statecolor)
			elif IMorder==4:	key=lambda ms: (ms.usectime if ms.usectime!=None else 0)
			elif IMorder==5:	key=lambda ms: (ms.metaversion)
			elif IMorder==6:	key=lambda ms: (ms.metaid)
			elif IMorder==7:	key=lambda ms: (ms.chlogtime if ms.chlogtime!=None else 0)
			elif IMorder==8:	key=lambda ms: (ms.memusage)
			elif IMorder==9:	key=lambda ms: (ms.syscpu+ms.usercpu)
			elif IMorder==10:	key=lambda ms: (ms.lastsuccessfulstore)
			elif IMorder==11:	key=lambda ms: (ms.lastsaveseconds)
			elif IMorder==12:	key=lambda ms: (ms.lastsavestatus)
			elif IMorder==13:	key=lambda ms: (ms.lastsavemetaversion)
			elif IMorder==14:	key=lambda ms: (ms.lastsavemetachecksum)
			elif IMorder==15:	key=lambda ms: (ms.exportschecksum)
			else:				      key=lambda ms: (0)
			masterservers.append(ms)
		masterservers.sort(key=key)
		if IMrev:
			masterservers.reverse()
		return masterservers

	# Returns a list of all master servers in the cluster along with secdelta and metadelay
	# Sorts if IMorder - sort order is provided, IMrev - reverse order
	def update_masterservers_delays(self, IMorder=0, IMrev=False):
		mservers = self.get_masterservers(IMorder, IMrev)
		# Calculate necessary metrics from all metadata servers
		highest_saved_metaversion = 0
		highest_metaversion_checksum = 0
		master_minusectime = None
		master_maxusectime = None
		master_deltausectime = None
		for ms in mservers:
			if ms.lastsavemetaversion!=None and ms.lastsavemetachecksum!=None:
				if ms.lastsavemetaversion>highest_saved_metaversion:
					highest_saved_metaversion = ms.lastsavemetaversion
					highest_metaversion_checksum = ms.lastsavemetachecksum
				elif ms.lastsavemetaversion==highest_saved_metaversion:
					highest_metaversion_checksum |= ms.lastsavemetachecksum
			if self._leader_usectime==None or self._leader_usectime==0:
				if ms.usectime!=None and ms.usectime>0:
					if master_minusectime==None or ms.usectime<master_minusectime:
						master_minusectime = ms.usectime
					if master_maxusectime==None or ms.usectime>master_maxusectime:
						master_maxusectime = ms.usectime
				if master_maxusectime and master_minusectime:
					master_deltausectime = master_maxusectime - master_minusectime
		updated_masterlist = []
		for ms in mservers:
			secdelta = 0.0
			if ms.usectime!=None and ms.usectime!=0:
				if self._leader_usectime==None or self._leader_usectime==0:
					if master_deltausectime!=None:
						secdelta = (master_deltausectime) / 1000000.0
				else:
					if self._leader_usectime > ms.usectime:
						secdelta = (self._leader_usectime - ms.usectime) / 1000000.0
					else:
						secdelta = (ms.usectime - self._leader_usectime) / 1000000.0
			if ms.chlogtime==None or ms.chlogtime==0 or self._leader_usectime==None or self._leader_usectime==0:
				metadelay = None
			else:
				metadelay = self._leader_usectime/1000000.0 - ms.chlogtime
				if metadelay>1.0:
					metadelay-=1.0
				else:
					metadelay=0.0
			ms.secdelta = secdelta
			ms.metadelay = metadelay
			updated_masterlist.append(ms)
		self._masterlist = updated_masterlist
		return highest_saved_metaversion, highest_metaversion_checksum
