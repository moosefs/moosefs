class Organization:
	def __init__(self, dp, guimode=False, ajax_request=False):
		self.sectionset = None
		self.subsectionset = None
		self.guimode = guimode
		cl = dp.cluster
		# Define menu & subsections structure
		if guimode:
			if cl.leaderfound():
				self.menu_tree = [
					["ST", ["ST"]],
					["IN", ["IG", "IM", "LI", "IC", "FL", "CL", "MU"]],
					["CS", ["CS", "MB"]],
					["HD", ["HD"]],
					["EX", ["EX"]],
					["MD", ["MS", "MO", "OF"]],
					["RP", ["SC", "PA"]],
					["QU", ["QU"]],
					["XC", ["MC", "CC"]]
				]
			elif cl.electfound():
				self.menu_tree = [
					["ST", ["ST"]],
					["IN", ["IG", "IM", "LI", "IC", "FL", "CL", "MU"]],
					["CS", ["CS", "MB"]],
					["HD", ["HD"]],
					["EX", ["EX"]],
					["QU", ["QU"]],
					["XC", ["MC", "CC"]]
				]
			elif cl.followerfound() or cl.usurperfound():
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "FL", "CL", "MU"]],
					["MC", ["MC"]]
				]
			else:
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "FL", "CL", "MU"]]
				]
		else:
			# Define menu & subsections structure
			if cl.leaderfound():
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "IL", "MF", "MU"]],
					["CS", ["CS"]],
					["MB", ["MB"]],
					["HD", ["HD"]],
					["EX", ["EX"]],
					["MD", ["MS", "OF", "AL", "MO"]],
					["RP", ["SC", "PA"]],
					["RS", ["SC", "PA", "OF", "AL"]], # backwards compatibility
					["QU", ["QU"]],
					["MC", ["MC"]],
					["CC", ["CC"]]
				]
			elif cl.electfound():
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "IL", "MF", "MU"]],
					["CS", ["CS"]],
					["MB", ["MB"]],
					["HD", ["HD"]],
					["EX", ["EX"]],
					["QU", ["QU"]],
					["MC", ["MC"]],
					["CC", ["CC"]]
				]
			elif cl.followerfound() or cl.usurperfound():
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "IL", "MF", "MU"]],
					["MC", ["MC"]]
				]
			elif cl.anyfound():
				self.menu_tree = [
					["IN", ["IG", "IM", "LI", "IC", "IL", "MF", "MU"]]
				]
			else:
				self.menu_tree = None


		# sections names and render conditions
		self.sections_defs = {
			"ST":("Status",										 lambda: guimode and (cl.leaderfound() or cl.electfound())),
			"IN":("Info",                      lambda: True),
				"IG":("Cluster Summary",           lambda: cl.master()!=None),
				"IM":("Metadata Servers (Masters)",lambda: len(dp.get_masterservers(None))>0),
				"MB":("Metadata backup loggers",   lambda: cl.leaderfound() and cl.master()!=None),
				"LI":("Licence Info",              lambda: cl.leaderfound() and cl.master()!=None and cl.master().is_pro()),
				"IC":("Chunk Matrix Table",        lambda: cl.leaderfound()),
				"IL":("Self-check Loops",          lambda: cl.leaderfound() and not guimode),
				"MF":("Missing Files",             lambda: cl.leaderfound() and ((cl.master().version_at_least(2,0,66) and cl.master().version_less_than(3,0,0)) or cl.master().version_at_least(3,0,19))),
				"FL":("Filesystem Self-check Loop",lambda: cl.leaderfound() and guimode), #GUI only
				"CL":("Chunks Housekeeping Loop",  lambda: cl.leaderfound() and guimode), #GUI only
				"MU":("Memory Usage",              lambda: cl.leaderfound() and cl.master()!=None),
			"CS":("Chunkservers",                lambda: cl.master()!=None),
			"HD":("Disks",                     lambda: cl.master()!=None),
			"EX":("Exports",                   lambda: cl.master()!=None),
			"MD":("Mounts",                    lambda: cl.leaderfound()),
				"MS":("Parameters",                lambda: cl.leaderfound()),
				"MO":("Operations",                lambda: cl.leaderfound()),
				"OF":("Open files and locks",      lambda: cl.leaderfound()),
				"AL":("Acquired locks",            lambda: cl.leaderfound() and not guimode), #CLI only, for GUI "AL" is in "OF"
			"RP":("Redundancy",                lambda: cl.leaderfound()),
				"SC":("Storage Classes",           lambda: cl.leaderfound()),
				"PA":("Override Patterns",         lambda: cl.leaderfound() and cl.master().version_at_least(4,2,0)),
			"RS":("Resources",                 lambda: cl.leaderfound()),# backwards compatibility
			"QU":("Quotas",                    lambda: cl.master()!=None),
			"XC":("Charts",                    lambda: cl.master()!=None and not ajax_request),
				"MC":("Master servers",            lambda: cl.master()!=None and not ajax_request),
				"CC":("Chunkservers",              lambda: cl.master()!=None and not ajax_request)
		}

	def set_sections(self, sectionset, subsectionset=None):
		self.sectionset = sectionset
		self.subsectionset = subsectionset

	def get_default_section(self):
		return self.menu_tree[0][0]
	
	# returns list of subsections ids for given menu section
	def menu_subitems(self, section):
		for menu_item in self.menu_tree:
			if menu_item[0]==section:
				return menu_item[1]
		return []

	# Decide if section should be rendered
	def shall_render(self, section):
		if self.sectionset==None:
			raise Exception("Organization object is not initialized properly: set_sections() must be called before shall_render()")
		
		# render section if it's in the sectionset
		effective_sectionset = set(self.sectionset)

		if self.guimode:
			if self.subsectionset==None:
				raise Exception("Organization object is not initialized properly: set_sections() must be called before shall_render()")
			# adjust default subsections
			if "RP" in self.sectionset: 
				if len(self.subsectionset.intersection(set(self.menu_subitems("RP"))))==0:
					self.subsectionset |= set(("SC",)) # add Storage Classes section by default if Redundancy Policy is selected
			if "IN" in self.sectionset:
				if len(self.subsectionset.intersection(set(self.menu_subitems("IN"))))==0:
					self.subsectionset |= set((self.menu_subitems("IN"))) # add all subsections by default if Info is selected
			# render subsection only if 
			#    its main section is visible and it's in the subsectionset 
			# or its main section is visible and there no subsections for this main section specified
			for main_section in self.sectionset:
				subitemset = set(self.menu_subitems(main_section))
				itsmainsectionvisible = section in subitemset
				if itsmainsectionvisible and section in self.subsectionset:
					effective_sectionset |= set((section,))
					break
		else:
			# for CLI render all subsections if the main section is selected
			for s in self.sectionset:
				effective_sectionset |= set(self.menu_subitems(s))

		if section in effective_sectionset:
			return self.sections_defs[section][1]() # render this section if condition is met

		return False # don't render



