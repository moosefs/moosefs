from common.utilsgui import *

# renders forms with errors and prompts when leader master server is missing
def render(dp, fields, vld):
	cl = dp.cluster
	out = []
	out.append("""<div class="tab_title ERROR">Oops!</div>""")
	out.append("""<table class="FR MESSAGE">""")
	if not cl.anyfound():
		out.append("""	<tr>""")
		out.append("""		<td align="center">""")
		out.append("""			<span class="ERROR">Can't find masters (resolve given name)!</span><br />""")
		out.append("""			<form method="GET">""")
		out.append("""				Input your DNS master name: <input type="text" name="masterhost" value="%s" size="100">""" % htmlentities(cl.masterhost))
		for i in fields.createinputs(["masterhost"]):
			out.append("""				%s""" % (i))
		out.append("""				<input type="submit" value="Try it!">""")
		out.append("""			</form>""")
		out.append("""		</td>""")
		out.append("""	</tr>""")
	else:
		out.append("""	<tr>""")
		out.append("""		<td align="center">""")
		out.append("""			<span class="ERROR">Can't find working masters!</span><br />""")
		out.append("""			<form method="GET">""")
		out.append("""				Input your DNS master name: <input type="text" name="masterhost" value="%s" size="100"><br />""" % htmlentities(cl.masterhost))
		out.append("""				Input your master-client port number: <input type="text" name="masterport" value="%u" size="5"><br />""" % (cl.masterport))
		for i in fields.createinputs(["masterhost","masterport"]):
			out.append("""				%s""" % (i))
		out.append("""				<input type="submit" value="Try it!">""")
		out.append("""			</form>""")
		out.append("""		</td>""")
		out.append("""	</tr>""")
	out.append("""</table>""")
	return out