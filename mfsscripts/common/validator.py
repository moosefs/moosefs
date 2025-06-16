import os
import json
from common.constants import *

# Contains 0, 1 or many issues (errors and/or warnings and/or infos) found by Validator's check_* routine
class Issues:
	# Creates a new Issues object, if 'issue' is provided this is added (initiated) to the list of issues
	def __init__(self, validator, issue=None, css_class_override=None, icon_override=None):
		self.issues_dict = validator.issues_dict
		self.issues = []
		self.css_class_override = css_class_override
		self.icon_override = icon_override
		self.has_error = False
		self.has_warning = False
		self.has_info = False
		self.has_help = False
		if issue:
			self.add_issue(issue)

	def __iter__(self):
		return iter(self.issues)

	def len(self):
		return len(self.issues)
	def any(self):
		return len(self.issues)>0
	
	# Appends an issue with a given id to the returned list of issues; severity of the issue is determined automatically based on issues_dict ('help.json')
	def add_issue(self, id):
		def find_issue_by_id(id):
			if id in self.issues_dict:
				return self.issues_dict[id]  # Return the first matching dictionary
			else:
				return None  # Return None if no match is found
		issue = find_issue_by_id(id)
		if issue == None:
			self.css_class_override = "missing-data-tt"
			self.issues.append('unknown_severity')
			self.issues.append(id)
			return

		self.issues.append(id)

		if issue['severity']=="Error":
			self.has_error = True
		elif issue['severity']=="Warning":
			self.has_warning = True
		elif issue['severity']=="Info":
			self.has_info = True
		elif issue['severity']=="Help":
			self.has_help = True

	# Returns a list of issues ids as comma separated string for use directly in the 'data-tt' attribute
	def data_help(self):
		return ",".join(self.issues)
	
	# A convenient helper: returns a string with a complete <span> tag containing class and data-tt attributes,
	# along with icon and a given msg string as its inner html
	def span(self, inner_html='', clz=''):
		if clz:
			if self.css_class():
				clz='%s %s' % (clz, self.css_class())
		elif self.css_class():
			clz = self.css_class()
		if clz:
			clz = ' class="%s"' % clz
		else:
			clz=''
		if self.data_help():
			data_help = ' data-tt="%s"' % self.data_help()
		else:
			data_help = ''
		return '<span%s%s>%s%s</span>' % (clz, data_help, inner_html, self.icon())

	# A convenient helper: returns a string with a complete <span> tag containing class and data-tt attributes,
	# without icon but with a given msg string as its inner html
	def span_noicon(self, inner_html='', clz=''):
		if clz:
			if self.css_class():
				clz='%s %s' % (clz, self.css_class())
		elif self.css_class():
			clz = self.css_class()
		if clz:
			clz = ' class="%s"' % clz
		else:
			clz=''
		if self.data_help():
			data_help = ' data-tt="%s"' % self.data_help()
		else:
			data_help = ''
		return '<span%s%s>%s</span>' % (clz, data_help, inner_html)
	
	# Returns a string containing html tags (<svg>...</svg>) displaying an icon: either error, warning or info - depending on the highest severity issue in the list
	# The icon tag contains 'data-tt' attribute with the appropriate list of issues to display tooltip
	# returns empty string if there is no issue on the list
	def icon(self, scale=1):
		icon = None
		if self.icon_override!=None:
			icon = self.icon_override
		elif self.has_error: 
			icon="icon-error"
		elif self.has_warning:
			icon="icon-warning"
		elif self.has_info: 
			icon="icon-info"
		scale = ' style="transform: scale(%.2f);"' % float(scale) if scale != 1 else ''
		if icon:
			return """ <svg class="icon" height="12px" width="12px" data-tt="%s"%s><use xlink:href="#%s"/></svg> """ % (self.data_help(), scale, icon)
		else:
			return ""
		
	# Returns a string with a name of a span. or div. class - for displaying red text for errors or orange for warnings 	
	def css_class(self):
		if self.css_class_override!=None:
			return self.css_class_override
		if self.has_error: 
			return "ERROR"
		if self.has_warning: 
			return "WARNING"
		if self.has_info: 
			return "INFO"
		return ""
	
	# Turns off showing icon if only info issues are on the list
	def turn_off_info_icon(self):
		if self.has_info and not self.has_warning and not self.has_error:
			self.icon_override = ''
	
	def override_info_css_class(self, info_css_class_override):
		if self.has_info and not self.has_warning and not self.has_error:
			self.css_class_override=info_css_class_override

	# Returns True if the list contains an issue with a given id
	def contains(self, id):
		return id in self.issues

# A series of independent routines validating a state of particular parts of a cluster. 
# Each of 'check_' function returns None if no issues were found or an 'Issues' object containing a list of found problems 
class Validator:
	def __init__(self,dp):
		self.issues_dict = []
		self.dp = dp
		# help.json is used only to determine issues severity (thus order of appearance in the list)
		# actual tooltips content is retrieved by the javascript
		with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../assets/help.json'), 'r', encoding='utf-8') as file:
			self.issues_dict = json.load(file)

	# Returns just a single issue with a given id		
	def issue(self, id):
		issues = Issues(self)
		issues.add_issue(id)
		return issues

	# Compares (stringified) versions ("4.56.3 PRO"), 
	# returns 1 if ver1>ver2, -1 if ver1<ver2, 0 if ver1==ver2, doesn't take into account "PRO" on not "PRO"
	def cmp_ver_str(self, ver1, ver2):
		(ver1_1, ver1_2, ver1_3)=list(map(int, ver1.upper().replace("PRO","").split('.')))
		(ver2_1, ver2_2, ver2_3)=list(map(int, ver2.upper().replace("PRO","").split('.')))
		if (ver1_1>ver2_1):
			return 1
		elif (ver1_1<ver2_1):
			return -1
		if (ver1_2>ver2_2):
			return 1
		elif (ver1_2<ver2_2):
			return -1
		if (ver1_3>ver2_3):
			return 1
		elif (ver1_3<ver2_3):
			return -1
		return 0

	# Checks if GUI version is newer/older than master and if it's identical in terms of PRO vs. CE
	# returns a list of issues
	def check_gui_ver(self, ver_ms, ver_gui):
		issues = Issues(self)
		ver_diff = self.cmp_ver_str(ver_ms, ver_gui)
		if ver_diff == 0:
			return issues
		if (ver_diff==1): 
			issues.add_issue('ui_older') # master in newer than GUI
		else:
			issues.add_issue('ui_newer') # GUI in newer than master
		# check PRO vs. CE mismatch
		if ("PRO" in ver_ms.upper() and not "PRO" in ver_gui.upper()):
			issues.add_issue('ui_ce_pro')
		elif (not "PRO" in  ver_ms.upper() and "PRO" in ver_gui.upper()):
			issues.add_issue('ui_pro_ce')
		return issues

	def check_ms_metadata_save_older(self, ms, highest_saved_metaversion):
		issues = Issues(self)
		if ms.lastsavemetaversion!=highest_saved_metaversion:
			issues.add_issue('ms_metadata_save_older')
		issues.turn_off_info_icon() # don't show any icon if only info on the list
		issues.override_info_css_class('GRAY') # use GRAY css class for this issue
		return issues


	def check_ms_checksum_mismatch(self, ms, highest_saved_metaversion, highest_metaversion_checksum):
		issues = Issues(self)
		if ms.lastsavemetaversion!=highest_saved_metaversion:
			issues.add_issue('ms_metadata_save_older')
		if ms.lastsavemetaversion==highest_saved_metaversion and ms.lastsavemetachecksum!=highest_metaversion_checksum:
			issues.add_issue('ms_checksum_mismatch')
		issues.turn_off_info_icon() # don't show any icon if only info on the list
		issues.override_info_css_class('GRAY') # use GRAY css class for this issue
		return issues
	
	def check_ms_last_store_status(self,ms):
		issues = Issues(self)
		if ms.lastsavestatus==LASTSTORE_META_STORED_BG:
			issues.add_issue('ms_last_store_background')
		elif ms.lastsavestatus==LASTSTORE_DOWNLOADED:
			issues.add_issue('ms_last_store_downloaded')
		elif ms.lastsavestatus==LASTSTORE_META_STORED_FG:
			issues.add_issue('ms_last_store_foreground')
		elif ms.lastsavestatus==LASTSTORE_CRC_STORED_BG:
			issues.add_issue('ms_last_store_crc_background')
			issues.css_class_override = 'NOTICE'
		else:
			issues.add_issue('ms_last_store_unknown')
		issues.turn_off_info_icon() # don't show any icon if only info on the list
		return issues
	
	# Check if follower is older or newer and if it matches PRO/CE with the Leader
	def check_follower_version(self, ms):
		issues = Issues(self)
		if self.dp.master()==None or not ms.strver:
			return issues
		# check PRO vs. CE mismatch
		if self.dp.master().is_pro() and not ms.pro:
			issues.add_issue('follower_ce_pro')
		elif not self.dp.master().is_pro() and ms.pro:
			issues.add_issue('follower_pro_ce')
		if  self.dp.master().sortver > ms.sortver:
			issues.add_issue('follower_older')
		elif self.dp.master().sortver < ms.sortver:
			issues.add_issue('follower_newer')
		return issues
	
	# Check if metalogger is older or newer and if it matches PRO/CE with the Leader
	def check_ml_version(self, ml):
		issues = Issues(self)
		if self.dp.master()==None or not ml.strver:
			return issues
		# check PRO vs. CE mismatch
		if self.dp.master().is_pro() and not ml.pro:
			issues.add_issue('ml_ce_pro')
		elif not self.dp.master().is_pro() and ml.pro:
			issues.add_issue('ml_pro_ce')
		if  self.dp.master().sortver > ml.sortver:
			issues.add_issue('ml_older')
		elif self.dp.master().sortver < ml.sortver:
			issues.add_issue('ml_newer')
		return issues
	
	# Check if chunkserver is older or newer and if it matches PRO/CE with the Leader
	def check_cs_version(self, cs):
		issues = Issues(self)
		if self.dp.master()==None or not cs.strver:
			return issues
		# check PRO vs. CE mismatch
		if self.dp.master().is_pro() and not cs.pro:
			issues.add_issue('cs_ce_pro')
		elif not self.dp.master().is_pro() and cs.pro:
			issues.add_issue('cs_pro_ce')
		if  self.dp.master().sortver > cs.sortver:
			issues.add_issue('cs_older')
		elif self.dp.master().sortver < cs.sortver:
			issues.add_issue('cs_newer')
		return issues

	# Check if mount is older or newer and if it matches PRO/CE with the Leader
	def check_mount_version(self, mnt):
		issues = Issues(self)
		if self.dp.master()==None or not mnt.strver:
			return issues
		# check PRO vs. CE mismatch
		if self.dp.master().is_pro() and not mnt.pro:
			issues.add_issue('mnt_ce_pro')
		elif not self.dp.master().is_pro() and mnt.pro:
			issues.add_issue('mnt_pro_ce')
		if  self.dp.master().sortver > mnt.sortver:
			issues.add_issue('mnt_older')
		elif self.dp.master().sortver < mnt.sortver:
			issues.add_issue('mnt_newer')
		return issues
	
	# Check if there is enough master servers
	def check_cluster_masters(self, masterservers, num_metaloggers):
		issues = Issues(self)
		active = leader = elect = usurper = 0
		pro = False
		for ms in masterservers:
			if ms.is_active():
				active+=1
				pro = ms.pro or pro
				if ms.workingstate==STATE_LEADER:
					leader += 1
				elif ms.workingstate==STATE_ELECT:
					elect += 1
				elif ms.workingstate==STATE_USURPER:
					usurper += 1
		if active==1 and pro:
			issues.add_issue('ms_single_master_pro')
		if active==1 and num_metaloggers==0:
			issues.add_issue('ms_single')
		if leader==0:
			if elect==1:
				issues.add_issue('ms_elect')
			elif usurper==1:
				issues.add_issue('ms_usurper')
		if leader+elect+usurper>1 and not (leader==1 and usurper==1 and elect==0):
			issues.add_issue('ms_toomany')
		return issues
	
	# Check (explain) the state of the master server (desync, unreachable etc.)
	def check_ms_state(self, ms):
		issues = Issues(self)
		if not ms.is_active():
			issues.add_issue('follower_unreachable')
		if ms.workingstate==STATE_FOLLOWER and "DESYNC" in ms.statestr:
			issues.add_issue('follower_desync')
		return issues
	
	# Is the follower clock in sync with the leader?
	def check_follower_clock(self,secdelta):
		issues = Issues(self)
		if secdelta>2.0:
			issues.add_issue('follower_time_diff')
		elif secdelta>1.0:
			issues.add_issue('follower_time_diff_small')
		return issues
	
	# Is the follower's metaid the same as the leader's?
	def check_follower_metaid(self, ms):
		issues = Issues(self)
		if ms.metaid!=self.dp.cluster.master_metaid():
			issues.add_issue('follower_id_mismatch')
		return issues

	# Does follower checksum match the leader's?
	def check_follower_checksum(self, ms):
		issues = Issues(self)
		if ms.exportschecksum!=self.dp.cluster.master_exportschecksum():
			issues.add_issue('follower_checksum_mismatch')
		return issues

	# What is the follower's dalay in metadata sync with the leader?
	def check_follower_metadelay(self, metadelay):
		issues = Issues(self)
		if metadelay>6.0:
			issues.add_issue('follower_meta_delay')
		elif metadelay>1.0:
			issues.add_issue('follower_meta_delay_small')
		return issues

	# Isn't the last metadata save duration too long?
	def check_last_save_duration(self, ms):
		issues = Issues(self)
		if ms.lastsaveseconds>45*60:
			issues.add_issue('ms_long_duration')
		return issues

	# Check licence expiration time
	def check_licence_time(self, lic):
		issues = Issues(self)
		if lic.licleft:
			if lic.licleft <= 0:
				issues.add_issue('lic_expired')
			elif lic.licleft < 86400 * 30:
				issues.add_issue('lic_expiring')
		return issues
	
	# Check licence size
	def check_licence_size(self, lic):
		issues = Issues(self)
		if lic.licver==0 or lic.licver == LICVER_CE: return issues
		if (lic.currentsize >= lic.licmaxsize):
			issues.add_issue('lic_exhausted')
		elif (lic.currentsize >= lic.licmaxsize*0.95):
			issues.add_issue('lic_almost_exhausted')
		elif (lic.currentsize >= lic.licmaxsize*0.80):
			issues.add_issue('lic_exhausting')
		return issues
	
	# Check version granted
	def check_licence_version(self, lic, ver):
		issues = Issues(self)
		if not lic.is_allowed_version(ver):
			issues.add_issue('lic_max_version')
		return issues
	
	# Check if the chunk matrix numbers are valid
	def check_mx_progress(self, progressstatus):
		issues = Issues(self)
		if progressstatus>0:
			issues.add_issue('data_health_checking')
		return issues

	# Check if there are any missing, endangered or undergoal chunks
	def check_mx_summary(self, mx_summary):
		issues = Issues(self)
		if mx_summary[MX_COL_MISSING]>0:
			issues.add_issue('data_health_missing')
		elif mx_summary[MX_COL_ENDANGERED]>0:
			issues.add_issue('data_health_endangered')
		elif mx_summary[MX_COL_UNDERGOAL]>0:
			issues.add_issue('data_health_undergoal')
		return issues
	
	# Check if there are issues reported by the health self-check loop
	def check_health_selfcheck(self, hsc):
		issues = Issues(self)
		if hsc.mfiles>0 or hsc.mchunks>0:
			issues.add_issue('data_missing_files')
		if hsc.ugfiles>0 or hsc.ugchunks>0:
			issues.add_issue('data_undergoal_files')
		if hsc.mtfiles>0:
			issues.add_issue('data_missing_trash_files')
		return issues
	
	# Return info on a missing chunk status
	def get_missing_chunk_info(self, mc):
		issues = Issues(self)
		if mc.mtype == MISSING_CHUNK_TYPE_NOCOPY:
			issues.add_issue('data_mc_nocopy')
		elif mc.mtype == MISSING_CHUNK_TYPE_INVALID_COPIES:
			issues.add_issue('data_mc_invalid')
		elif mc.mtype == MISSING_CHUNK_TYPE_WRONG_VERSIONS:
			issues.add_issue('data_mc_wrongver')
		elif mc.mtype == MISSING_CHUNK_TYPE_PARTIAL_EC:
			issues.add_issue('data_mc_partialec')
		return issues

	# Check CS queue overload state
	def check_cs_queue_state(self,cs):
		issues = Issues(self)
		if cs.queue_state==CS_LOAD_OVERLOADED:
			issues.add_issue('cs_overload')
		return issues
	
	def check_cs_hdds(self,cs):
		issues = Issues(self)
		if cs.total>0 and cs.used >= cs.total*0.99:
			issues.add_issue('cs_hdds_full')
		elif cs.total>0 and cs.used >= cs.total*0.90:
			issues.add_issue('cs_hdds_nearly_full')
		return issues

	def check_cs_mfr_status(self, cs):
		issues = Issues(self)
		if cs.tdchunks!=0:
			if cs.mfrstatus==MFRSTATUS_VALIDATING:
				issues.add_issue('cs_mfr_validating')
			elif cs.mfrstatus==MFRSTATUS_INPROGRESS:
				issues.add_issue('cs_mfr_inprogress')
			elif cs.mfrstatus==MFRSTATUS_READY:
				issues.add_issue('cs_mfr_ready')
			else:
				issues.add_issue('cs_mfr_unknown')
			issues.turn_off_info_icon()
		return issues

	# Check all possible hdd status issues
	def check_hdd_status(self, hdd):
		issues = Issues(self)
		if (hdd.csvalid == CS_HDD_CS_TOO_OLD):
			issues.add_issue('hdd_cs_too_old')
		elif (hdd.csvalid == CS_HDD_CS_UNREACHABLE):
			issues.add_issue('hdd_cs_unreachable')
		else:
			if (hdd.flags&CS_HDD_INVALID):
				issues.add_issue('hdd_invalid')
			if (hdd.flags&CS_HDD_DAMAGED) and (hdd.flags&CS_HDD_SCANNING)==0 and (hdd.flags&CS_HDD_INVALID)==0:
				issues.add_issue('hdd_damage')
			if hdd.flags&CS_HDD_SCANNING:
				issues.add_issue('hdd_scanning')

			if hdd.flags&CS_HDD_MFR:
				if hdd.mfrstatus==MFRSTATUS_VALIDATING:
					issues.add_issue('hdd_mfr_validating')
				elif hdd.mfrstatus==MFRSTATUS_INPROGRESS:
					issues.add_issue('hdd_mfr_inprogress')
				elif hdd.mfrstatus==MFRSTATUS_READY:
					issues.add_issue('hdd_mfr_ready')
				else:
					issues.add_issue('hdd_mfr_unknown')
				issues.turn_off_info_icon()
			if not hdd.flags&CS_HDD_DAMAGED and (hdd.errtime!=0 or hdd.errchunkid!=0):
				issues.add_issue('hdd_error')
		return issues

	# Check hdd usage (capacity) issues
	def check_hdd_usage(self, hdd):
		issues = Issues(self)
		if (hdd.csvalid != CS_HDD_CS_TOO_OLD) and (hdd.csvalid != CS_HDD_CS_UNREACHABLE):
			if hdd.total>0 and hdd.used >= hdd.total*0.99:
				issues.add_issue('hdd_full')
			elif hdd.total>0 and hdd.used >= hdd.total*0.90:
				issues.add_issue('hdd_nearly_full')
		return issues

	# Check if defining min trash retention is necessary
	def check_sc_mintrashretention(self, sc):
		issues = Issues(self)
		if not sc.defined_trash:
			if sc.min_trashretention>0:
				issues.add_issue('sc_mintrashretention_unnecessary')
			elif sc.min_trashretention==0:
				issues.add_issue('sc_mintrashretention_na')
		else:
			if sc.min_trashretention==0:
				issues.add_issue('sc_mintrashretention0')
		return issues