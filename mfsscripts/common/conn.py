import asyncio 
import sys
import struct

from common.constants import *
from common.utils import *

# Handles multiple, concurrent commands to many servers at once
class MFSMultiConn:
	# Initializes class, sets timeout for a single task
	def __init__(self,timeout=5,addresses=[]):
		self.timeout = timeout
		self.addresses = addresses

	# Registers a server (host,port), to send a command to
	def register(self,host,port):
		self.addresses.append((host,port))

	# Checks if a given server (host,port) is registered for sending commands
	def is_registered(self,host,port):
		return (host,port) in self.addresses

	# Sends a given command asynchrously to all (previously) registered servers and applies the global timeout on all of them.
	# It is ensured that this function completes within give timeout
	# returns a dictionary hostkey->(datain, length) where hostkey is 'ip:port' (eg. '10.10.10.12:9422), 
	# if a given server can't be reached or gives no valid answer its hostkey will be absent from the dictionary, thus dictionary may be empty if no command completed successfully 
	# postprocess is a function (lambda) that will be applied to each datain before storing it in the dictionary
	def command(self,cmdout,cmdin,dataout=None,postprocess=None):
		if (sys.version_info[0]==3 and sys.version_info[1]<7):
			loop = asyncio.get_event_loop()
			return loop.run_until_complete(self.async_command(cmdout,cmdin,dataout,postprocess))
		else:
			return asyncio.run(self.async_command(cmdout,cmdin,dataout,postprocess))

	# Internal: coordinates async tasks and collects results
	async def async_command(self,cmdout,cmdin,dataout,postprocess):
		results = []
		# Create a list of tasks
		if dataout and isinstance(dataout, list):
			tasks = [self.multiple_command(host, port, cmdout, cmdin, dataout) for host, port in self.addresses]
		else:
			tasks = [self.single_command(host, port, cmdout, cmdin, dataout, postprocess) for host, port in self.addresses]
		if not tasks:
			return dict(results)
		# Schedule tasks
		if (sys.version_info[0]==3 and sys.version_info[1]<7):
			loop = asyncio.get_event_loop()
			tasks = [loop.create_task(task) for task in tasks]
		else:
			tasks = [asyncio.create_task(task) for task in tasks]
		try:
			# Attempt to wait for all tasks with a timeout
			done, pending = await asyncio.wait(tasks, timeout=self.timeout)
			for task in done:
				if not task.cancelled() and not task.exception():
					results.append(task.result()) # Collect a single task output
		except asyncio.TimeoutError:
			raise Exception("Timeout during executing single_command")  # This line will not be hit using this structure.
		for task in pending:
			task.cancel() # Cancel pending tasks after timeout
			try:
				await task
			except asyncio.CancelledError:
				pass #do nothing about it
		return dict(results) # Convert list of tuples (successful task results) to dictionary
	
	# Internal: perform a single mfs i/o command 
	async def single_command(self,host,port,cmdout,cmdin,dataout,postprocess):
		if dataout:
			l = len(dataout)
			msg = struct.pack(">LL",cmdout,l) + dataout
		else:
			msg = struct.pack(">LL",cmdout,0)
		reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=2)
		cmdok = 0
		errcnt = 0
		error = None
		try: 
			while cmdok==0:
				badans = 0
				try:
					writer.write(msg)
					await writer.drain()
					while cmdok==0:
						header = await reader.readexactly(8)
						if header:
							cmd,length = struct.unpack(">LL",header)
							if cmd==cmdin:
								datain = await reader.readexactly(length)
								cmdok = 1
							elif cmd!=ANTOAN_NOP:
								badans = 1
								raise Exception
						else:
							raise Exception
				except Exception:
					if errcnt<3:
						writer.close()
						if not (sys.version_info[0]==3 and sys.version_info[1]<7):
							await writer.wait_closed()
						reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=3)
						errcnt+=1
					else:
						if badans:
							raise MFSCommunicationError("bad answer")
						else:
							raise MFSCommunicationError()
		except Exception as e:
			error = e
		finally: #clean up whatever happened
			writer.close()
			if not (sys.version_info[0]==3 and sys.version_info[1]<7):
				await writer.wait_closed()
		hostkey = "%s:%u" % (host,port)
		if postprocess: # apply postprocess function to datain
			return hostkey,postprocess(host,port,datain,length,error)
		else: # or return datain as is
			return hostkey,(datain,length)

	# Internal: perform synchronously multiple mfs i/o commands with a single peer (during a single connection) sending different data to each command
	# returns a list of answers (datain, length) for all commands
	async def multiple_command(self,host,port,cmdout,cmdin,dataout_arr):
		datain_arr,length_arr = [],[]
		reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=2)
		try:
			for dataout in dataout_arr:
				cmdok = 0
				errcnt = 0
				l = len(dataout)
				msg = struct.pack(">LL",cmdout,l) + dataout
				while cmdok==0:
					badans = 0
					try:
						writer.write(msg)
						await writer.drain()
						while cmdok==0:
							header = await reader.readexactly(8)
							if header:
								cmd,length = struct.unpack(">LL",header)
								if cmd==cmdin:
									datain = await reader.readexactly(length)
									cmdok = 1
								elif cmd!=ANTOAN_NOP:
									badans = 1
									raise Exception
							else:
								raise Exception
					except Exception:
						if errcnt<3:
							writer.close()
							if not (sys.version_info[0]==3 and sys.version_info[1]<7):
								await writer.wait_closed()
							reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=3)
							errcnt+=1
						else:
							if badans:
								raise MFSCommunicationError("bad answer")
							else:
								raise MFSCommunicationError()
				datain_arr.append(datain)
				length_arr.append(length)
		finally: #clean up whatever happened
			writer.close()
			if not (sys.version_info[0]==3 and sys.version_info[1]<7):
				await writer.wait_closed()
		hostkey = "%s:%u" % (host,port)
		return hostkey,(datain_arr,length_arr)


########################################################################################
# Handles a single connection for sending requests and commands to any MooseFS server
class MFSConn:
	def __init__(self,host,port):
		self.host = host
		self.port = port
		self.socket = None
		self.connect()

	def __del__(self):
		try:
			if self.socket:
				self.socket.close()
#				print "connection closed with: %s:%u" % (self.host,self.port)
			self.socket = None
		except AttributeError:
			pass

	def connect(self):
		cnt = 0
		while self.socket == None and cnt<3:
			self.socket = socket.socket()
			self.socket.settimeout(1)
			try:
				self.socket.connect((self.host,self.port))
			except Exception:
				self.socket.close()
				self.socket = None
				cnt += 1
		if self.socket==None:
			self.socket = socket.socket()
			self.socket.settimeout(1)
			self.socket.connect((self.host,self.port))
#		else:
#			print "connected to: %s:%u" % (self.host,self.port)

	def close(self):
		if self.socket:
			self.socket.close()
			self.socket = None

	def mysend(self,msg):
		if self.socket == None:
			self.connect()
		totalsent = 0
		while totalsent < len(msg):
			sent = self.socket.send(msg[totalsent:])
			if sent == 0:
				raise RuntimeError("socket connection broken")
			totalsent = totalsent + sent

	def myrecv(self,leng):
		msg = bytes(0)
		while len(msg) < leng:
			chunk = self.socket.recv(leng-len(msg))
			if len(chunk) == 0:
				raise RuntimeError("socket connection broken")
			msg = msg + chunk
		return msg
	
	def command(self,cmdout,cmdin,dataout=None):
		if dataout:
			l = len(dataout)
			msg = struct.pack(">LL",cmdout,l) + dataout
		else:
			msg = struct.pack(">LL",cmdout,0)
		cmdok = 0
		errcnt = 0
		while cmdok==0:
			badans = 0
			try:
				self.mysend(msg)
				while cmdok==0:
					header = self.myrecv(8)
					cmd,length = struct.unpack(">LL",header)
					if cmd==cmdin:
						datain = self.myrecv(length)
						cmdok = 1
					elif cmd!=ANTOAN_NOP:
						badans = 1
						raise Exception
			except Exception:
				if errcnt<3:
					self.close()
					self.connect()
					errcnt+=1
				else:
					if badans:
						raise MFSCommunicationError("bad answer")
					else:
						raise MFSCommunicationError()
		return datain,length

# Handles a single connection for sending requests and commands to Master server (leader or follower)
class MasterConn(MFSConn):
	def __init__(self,host,port):
		MFSConn.__init__(self,host,port)
		self.version = (0,0,0)
		self.pro = -1
		self.featuremask = 0

	def set_version(self,versionxyzp):
		self.version,self.pro = version_convert(versionxyzp)
		(_,self.sortver,self.pro) = version_str_sort_pro(versionxyzp)
		if self.version>=(3,0,72):
			self.featuremask |= (1<<FEATURE_EXPORT_UMASK)
		if (self.version>=(3,0,112) and self.version[0]==3) or self.version>=(4,21,0):
			self.featuremask |= (1<<FEATURE_EXPORT_DISABLES)
		if self.version>=(4,27,0):
			self.featuremask |= (1<<FEATURE_SESSION_STATS_28)
		if self.version>=(4,29,0):
			self.featuremask |= (1<<FEATURE_INSTANCE_NAME)
		if self.version>=(4,35,0):
			self.featuremask |= (1<<FEATURE_CSLIST_MODE)
		if self.version>=(4,44,0):
			self.featuremask |= (1<<FEATURE_SCLASS_IN_MATRIX)
		if self.version>=(4,51,0):
			self.featuremask |= (1<<FEATURE_DEFAULT_GRACEPERIOD)
		if self.version>=(4,53,0):
			self.featuremask |= (1<<FEATURE_LABELMODE_OVERRIDES)
		if self.version>=(4,57,0):
			self.featuremask |= (1<<FEATURE_SCLASSGROUPS)	

	def version_at_least(self,v1,v2,v3):
		return (self.version>=(v1,v2,v3))
	def version_less_than(self,v1,v2,v3):
		return (self.version<(v1,v2,v3))
	def version_is(self,v1,v2,v3):
		return (self.version==(v1,v2,v3))
	def version_unknown(self):
		return (self.version==(0,0,0))
	def is_pro(self):
		return self.pro
	def has_feature(self,featureid):
		return True if (self.featuremask & (1<<featureid)) else False
	
	def sort_ver(self):
		sortver = "%05u_%03u_%03u" % self.version
		if self.pro==1:
			sortver += "_2"
		elif self.pro==0:
			sortver += "_1"
		else:
			sortver += "_0"
		return sortver
