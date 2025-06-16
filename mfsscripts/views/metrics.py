import subprocess
import os
import json
import time
import traceback
import re

from common.constants import *

def print_render(fields, masterhost, masterport):
	print("Content-Type: text/plain; charset=UTF-8\r\n\r")
	print('# HELP mfs_cgi_info MooseFS CGI general info')
	print('# TYPE mfs_cgi_info gauge')
	print("""mfs_cgi_info{version="%s"} 1.0""" % VERSION)
	def filter_letters_and_commas(input_string):
		# Use list comprehension to include only uppercase letters and commas
		filtered_characters = [char for char in input_string if (char.isalpha() or char == '_' or char == ',')]
		# Join the filtered characters into a new string
		return ''.join(filtered_characters)
	scope = fields.getvalue("scope", default="default")
	if scope is not None:
		if scope.lower() == 'none':
			scope = None 
		else:
			scope = filter_letters_and_commas(scope.upper()).split(",")
	mastercharts = fields.getvalue("mastercharts", default="all")
	if mastercharts is not None:
		if mastercharts.lower() == 'none':
			mastercharts = None
		else:
			mastercharts = filter_letters_and_commas(mastercharts.lower()).split(",")
	else:
		mastercharts = ['all']
	cscharts = fields.getvalue("cscharts", default="all")
	if cscharts is not None:
		if cscharts.lower() == 'none':
			cscharts = None
		else:
			cscharts = filter_letters_and_commas(cscharts.lower()).split(",")
	else:
		cscharts = ['all']
	prefix_whitelist = fields.getvalue("prefix_whitelist", default="")
	if prefix_whitelist is not None and prefix_whitelist:
		prefix_whitelist = filter_letters_and_commas(prefix_whitelist.lower()).split(",")
	else:
		prefix_whitelist = None
	prefix_blacklist = fields.getvalue("prefix_blacklist", default="")
	if prefix_blacklist is not None and prefix_blacklist:
		prefix_blacklist = filter_letters_and_commas(prefix_blacklist.lower()).split(",")
	else:
		prefix_blacklist = None
	
	print(MFSMetrics.get_metrics(masterhost=masterhost, masterport=masterport, scope=scope, mastercharts=mastercharts, cscharts=cscharts, prefix_whitelist=prefix_whitelist, prefix_blacklist=prefix_blacklist))


class MFSMetrics:
	def __init__(self, prefix_whitelist, prefix_blacklist):
		self.prefix_whitelist = prefix_whitelist
		self.prefix_blacklist = prefix_blacklist

		# exclude some keys from metrics: all chunks table doesn't fit well into prometheus metrics, so we exclude it and export summary only
		default_blacklist = [
					# 'mfs_info_missing',
					'mfs_info_chunks_allchunks','mfs_info_chunks_regularchunks',
					'mfs_info_chunks_allchunks_copies','mfs_info_chunks_regularchunks_copies',
					'mfs_info_chunks_allchunks_ec8','mfs_info_chunks_regularchunks_ec8',
					'mfs_info_chunks_allchunks_ec4','mfs_info_chunks_regularchunks_ec4']
		if self.prefix_blacklist is None:
			self.prefix_blacklist = default_blacklist
		else:
			self.prefix_blacklist += default_blacklist

		self.help_dict = []
		with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../assets/help.json'), 'r', encoding='utf-8') as file:
			self.help_dict = json.load(file)

	# Calls 'mfscli' with appropriate (scope, mastercharts, cscharts) parameters and generates Prometheus metrics restricted by prefix_whitelist and prefix_blacklist
	@classmethod
	def get_metrics(clz, masterhost, masterport, scope, mastercharts, cscharts, prefix_whitelist, prefix_blacklist):
		generator = MFSMetrics(prefix_whitelist, prefix_blacklist)
		return generator.generate_metrics(masterhost, masterport, scope, mastercharts, cscharts)

	# 	-SIN : show full master info 			   - NOT allowed because includes possibly too long list of missing chunks/files (SMF)
	# 	-SIM : show only masters states
	# 	-SLI : show only licence info
	# 	-SIG : show only general master (leader) info
	# 	-SMU : show only master memory usage
	# 	-SIC : show only chunks info (target/current redundancy level matrices)
	# 	-SIL : show only loop info (with messages)
	# 	-SMF : show only missing chunks/files - NOT allowed because possibly too long list of missing chunks/files
	# 	-SCS : show connected chunk servers
	# 	-SMB : show connected metadata backup servers
	# 	-SHD : show hdd data
	# 	-SEX : show exports                 - NOT allowed because not much interesting numeric data here
	# 	-SMS : show active mounts           - NOT allowed because not much interesting numeric data here
	# 	-SRS : show resources               - NOT allowed becauase possibly too long list of open files and locks
	# 	-SSC : show storage classes
	# 	-SPA : show patterns override data  - NOT allowed because not much interesting numeric data here
	# 	-SOF : show only open files         - NOT allowed because possibly too long list of open files
	# 	-SAL : show only acquired locks     - NOT allowed because possibly too long list of acquired locks
	# 	-SMO : show operation counters
	# 	-SQU : show quota info
	# 	-SMC : show master charts data      - NOT allowed as scope because covered with cscharts
	# 	-SCC : show chunkserver charts data - NOT allowed as scope because covered with mastercharts

	def generate_metrics(self, masterhost, masterport, scope, mastercharts, cscharts):
		try:
			command = ["mfscli", "-j"]
			allowed_scopes = ["SIM", "SLI", "SIG", "SMU", "SIC", "SIL", "SCS", "SMB", "SHD", "SSC", "SMO", "SQU"]
			default_scopes = ["SIM", "SLI", "SIG",        "SIC", "SIL", "SCS", "SMB", "SHD"                     ]
			show_anything = False
			if scope == ["DEFAULT"]:
				for s in default_scopes:
					command.append("-"+str(s))
					show_anything = True
			elif scope is not None:
				for s in scope:
					if s in allowed_scopes:
						command.append("-"+str(s))
						show_anything = True
			if mastercharts is not None:
				command.append("-SMC")
				command.append("-a")
				command.append("1")
				command.append("-b")
				command.append(",".join(mastercharts))
				show_anything = True
			if cscharts is not None:
				command.append("-SCC")
				command.append("-c")
				command.append("1")
				command.append("-d")
				command.append(",".join(cscharts))
				show_anything = True
			if masterhost is not None:
				command.append("-H")
				command.append(str(masterhost))
			if masterport is not None:
				command.append("-P")
				command.append(str(masterport))
			# print("Command: ", command)
			# if len(command) == 2:
			# 	return "Error: No valid scope selected for MooseFS metrics (see mfscli '-S__' argument documentation), actual: "+str(scope)+" allowed: "+str(allowed_scopes)
			# return f"{scope} {mastercharts} {cscharts} {masterhost} {masterport}"
			if not show_anything:
				return ""
			start_time = time.time()
			proc = subprocess.Popen(command, env=self.make_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			# proc = subprocess.Popen(["mfscli", "-j", "-SIN", "-SCS", "-SHD", "-SQU", "-SMC", "-SMO", "-SMS", "-a", "1", "-b", "all", "-SCC", "-c", "1", "-d", "all"],env=clz.make_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			json_string = ""
			for line in proc.stdout:
				json_string+=line.decode('utf-8')
			execution_time_seconds = time.time() - start_time
			print('# HELP mfs_cli_execution_time CLI (mfscli) command execution time [seconds] used to fetch metrics')
			print('# TYPE mfs_cli_execution_time gauge')
			print("""mfs_cli_execution_time{command="%s"} %f""" % (" ".join(command),execution_time_seconds))
			# output += "Errors:\n"
			# for line in proc.stderr:
			# 	output+=line.decode('ascii')
			proc.stdout.close()
			proc.stderr.close()
			json_data = json.loads(json_string)
			return self.json2metrics(json_data)
		except json.JSONDecodeError as e:
			print("Failed to parse JSON: "+str(e))
			print("First line of JSON: "+json_string.split('\n')[0])
			print("CLI command for retrieving data: "+" ".join(command))
		except Exception as e:
			return "Error: "+str(e)+"\nCLI command: "+" ".join(command)+"\n"+traceback.format_exc()
	
	def make_env(self):
		env = {}
		env['PATH'] = BIN_PATH+":"+os.environ['PATH']
		return env
	
	def json2metrics(self, json_data):
		prefix = 'mfs'
		out = self.iterable2metrics(prefix,json_data['dataset'])
		return out

	# Recursively converts a json subset to prometheus metrics
	def iterable2metrics(self, prefix, subset, ids=None):
		if prefix == 'mfs_mastercharts' or prefix == 'mfs_cscharts':
			return self.charts2metrics(prefix, subset, ids)

		out = ""
		if isinstance(subset, dict):
			out += self.strings2metrics(prefix, subset, ids)
			for key in subset:
				out += self.iterable2metrics(prefix+'_'+key.lower(), subset[key], ids)
		elif isinstance(subset, list):
			for index,element in enumerate(subset):
				ids_this = self.prepare_ids(ids, index, element)
				out += self.iterable2metrics(prefix, element, ids_this)
		elif isinstance(subset, (int, float, bool)):
			out += self.number2metrics(prefix, subset, ids)
		return out
	
	# Converts a list of mastercharts to prometheus metrics
	def charts2metrics(self, prefix, subset, ids):
		out = ''
		for index,element in enumerate(subset):
			# check if we have any valid data?
			if element['data_ranges'] is not None and '0' in element['data_ranges'] and 'data_array_1' in element['data_ranges']['0']:
				value = element['data_ranges']['0']['data_array_1'][0]
				if (value == None):
					continue
				ids_this = self.prepare_ids(ids, index, element,['master','chunkserver'])	
				key = (prefix+'_'+element['name']).lower()
				if self.is_listed(key):
					out += self.help_string(key, element['description'])
					out += '# TYPE ' + key + ' gauge\n'
					out += key +'{'+self.ids2metrics(ids_this)+'}' + ' ' + str(value)+'\n'
		return out

	# Converts a number to a prometheus gauge metric
	def number2metrics(self, key, number, ids):
		if not self.is_listed(key):
			return ''
		out = ''
		out += self.help_string(key)
		out += '# TYPE ' + key + ' gauge\n'+key
		if ids is not None:
			out += '{'+self.ids2metrics(ids)+'}'
		if isinstance(number, bool):
			number = int(number)
		return out+' '+str(number)+'\n'
	
	# Prepares a list of labels for a given element - they serve as a unique identifier for a metric
	def prepare_ids(self, ids, index, element, identifiers=None):
		if identifiers is None:
			identifiers=['ip', 'port', 'hostname', 'name', 'id', 'csid', 'path', 'hostname_path', 'sclassname', 'state'] #, 'ip_path'
		if ids is None:
			ids = {}
		# if ids is None: don't use 'no' 
		# 	ids = {'no': str(index)}
		# else:
		# 	ids['no'] = str(ids['no'])+'_'+str(index)
		if isinstance(element, dict):
			for identifier in identifiers:
				if identifier in element:
					ids[identifier]=element[identifier]
		return ids

	# Converts a list of identifiers (for a lists of values) to a prometheus labels
	def ids2metrics(self, ids):
		out = ''
		for id_name in ids:
			if out != '':
				out+=','
			out+= self.safe_string(id_name)+'="' + str(ids[id_name]).replace('"',"'")+'"' 
		return out

	def safe_string(self, string):
		# replace all non-alphanumeric characters with _ and make sure it doesn't starts with a number
		if not string[0].isalpha():
			string = '_'+string
		return ''.join(e for e in string if e.isalnum() or e == '_')
	
	# Prepares a list of all strings in a subset as a prometheus label
	def strings2metrics(self, prefix, subset, ids):
		prefix += '_misc'
		if not self.is_listed(prefix):
			return ''
		strings = ''
		for key in subset:
			if ids is not None and key in ids.keys():
				continue # skip string if it is already in identifier
			if isinstance(subset[key], (str)) and not key.endswith('_human'):
				if strings !='':
					strings+=','
				strings += self.safe_string(key)+'="'+subset[key].replace('"',"'")+'"'

		if strings != '':
			labels = '{'
			if ids is not None:
				labels += self.ids2metrics(ids)+','
			labels += strings+'}'
			return self.help_string(prefix)+'# TYPE ' + prefix + ' gauge\n'+prefix+labels+' 1\n'
		return ''
	
	def whitelisted(self, prefix):
		# whitelist is present, so we only include metrics that start with the whitelisted prefixes
		if self.prefix_whitelist is None:
			return True
		for prefix_w in self.prefix_whitelist:
			if prefix.startswith(prefix_w):
				return True
		return False

	def blacklisted(self, prefix):
		# blacklist is present, so we exclude metrics that start with the blacklisted prefixes
		if self.prefix_blacklist is None:
			return False
		for prefix_b in self.prefix_blacklist:
			if prefix.startswith(prefix_b):
				return True
		return False
	
	# Filters out metrics based on the prefix whitelist and blacklist
	def is_listed(self, prefix):
		if not self.whitelisted(prefix):
			return False
		if self.blacklisted(prefix):
			return False
		return True

	def help_string(self, key, default=None):
		# skip first 4 letters as help_dict doesn't use 'mfs_' prefix
		if key[4:] in self.help_dict:
			help_msg = ''
			if 'title' in self.help_dict[key[4:]]: 
				help_msg += self.help_dict[key[4:]]['title'] 
				if help_msg != '' and help_msg[-1] == '.': # remove dot if title ends with it
					help_msg = help_msg[:-1]

			if 'unit' in self.help_dict[key[4:]]:
				help_msg += ' ['+self.help_dict[key[4:]]['unit']+']'
			# if 'description' in self.help_dict[key[4:]]:
			# 	if help_msg != '' and help_msg[-1] != '.': # add a dot if title doesn't end with it
			# 		help_msg += '. '
			# 	help_msg += self.help_dict[key[4:]]['description']
			# remove any html tags, leaving only text between them
			help_msg = re.sub(r'<[^>]*>', '', help_msg)
			return '# HELP '+ key + ' ' + help_msg + '\n'
		elif default is not None:
			return '# HELP '+ key + ' ' + default + '\n'
		else:
			return ''