import sys
import traceback


from common.constants import *
from common.utils import *
from common.utilsgui import *
from common.models import *
from common.conn import MFSConn

def process_commands(cl,fields,html_title):
	# commands in CGI mode
	cmd_success = -1
	if "CSremove" in fields:
		cmd_success = 0
		tracedata = ""
		if cl.leaderfound():
			try:
				serverdata = fields.getvalue("CSremove").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_REMOVE,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = fields.createrawlink({"CSremove":""})
	elif "CSbacktowork" in fields:
		cmd_success = 0
		tracedata = ""
		if cl.leaderfound() and cl.master().version_at_least(1,6,28):
			try:
				serverdata = fields.getvalue("CSbacktowork").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_BACKTOWORK,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = fields.createrawlink({"CSbacktowork":""})
	elif "CSmaintenanceon" in fields:
		cmd_success = 0
		tracedata = ""
		if cl.leaderfound() and cl.master().version_at_least(2,0,11):
			try:
				serverdata = fields.getvalue("CSmaintenanceon").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEON,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = fields.createrawlink({"CSmaintenanceon":""})
	elif "CSmaintenanceoff" in fields:
		cmd_success = 0
		tracedata = ""
		if cl.leaderfound() and cl.master().version_at_least(2,0,11):
			try:
				serverdata = fields.getvalue("CSmaintenanceoff").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEOFF,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = fields.createrawlink({"CSmaintenanceoff":""})
	elif "MSremove" in fields:
		cmd_success = 0
		tracedata = ""
		if cl.leaderfound():
			try:
				sessionid = int(fields.getvalue("MSremove"))
				data,length = cl.master().command(CLTOMA_SESSION_COMMAND,MATOCL_SESSION_COMMAND,struct.pack(">BL",MFS_SESSION_COMMAND_REMOVE,sessionid))
				if length==1:
					status = (struct.unpack(">B",data))[0]
					cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = fields.createrawlink({"MSremove":""})
	elif "CSclearerrors" in fields:
		cmd_success = 0
		tracedata = ""
		try:
			serverdata = fields.getvalue("CSclearerrors").split(":")
			csip = serverdata[0]
			csport = int(serverdata[1])
			pathhex = serverdata[2]
			pleng = len(pathhex)>>1
			pathlist = []
			for x in range(pleng):
				pathlist.append(int(pathhex[2*x:2*x+2],16))
			dout = struct.pack(">L%uB" % pleng,pleng,*pathlist)
			conn = MFSConn(csip,csport)
			data,length = conn.command(ANTOCS_CLEAR_ERRORS,CSTOAN_CLEAR_ERRORS,dout)
			del conn
			if length==1:
				res = (struct.unpack(">B",data))[0]
				if res>0:
					cmd_success = 1
		except Exception:
			tracedata = traceback.format_exc()
		url = fields.createrawlink({"CSclearerrors":""})
	if cmd_success==1:
		print("Status: 302 Found")
		print("Location: %s" % url)
		print("Content-Type: text/html; charset=UTF-8\r\n\r")
		# print("")
		print("""<!DOCTYPE html>""")
		print("""<html lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<meta http-equiv="Refresh" content="0; url=%s" />""" % url.replace('&','&amp;'))
		print("""<title>%s</title>""" % html_title)
		# leave this script a the beginning to prevent screen blinking when using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")		
		print("""<link rel="stylesheet" href="assets/mfs.css" type="text/css" />""")
		print("""</head>""")
		print("""<body>""")
		print("""<h1 align="center"><a href="%s">If you see this then it means that redirection didn't work, so click here</a></h1>""" % url)
		print("""</body>""")
		print("""</html>""")
		sys.exit(0)
	elif cmd_success==0:
		print("Content-Type: text/html; charset=UTF-8\r\n\r")
		print("""<!DOCTYPE html>""")
		print("""<html lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<meta http-equiv="Refresh" content="5; url=%s" />""" % url.replace('&','&amp;'))
		print("""<title>%s</title>""" % html_title)
		# leave this script a the beginning to prevent screen blinking when using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")		
		print("""<link rel="stylesheet" href="assets/mfs.css" type="text/css" />""")
		print("""</head>""")
		print("""<body>""")
		print("""<h3 align="center">Can't perform command - wait 5 seconds for refresh</h3>""")
		if tracedata:
			print("""<hr />""")
			print("""<pre>%s</pre>""" % tracedata)
		print("""</body>""")
		print("""</html>""")
		sys.exit(0)
		