  # Copyright (c) 2014, Pierre Rochard
  # All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


from flask import render_template
from flask import request
from app import app
import hyperdex.client
import hyperdex.admin
import hyperdexfunc

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
@app.route('/noc', methods=['GET', 'POST'])

def noc():

# Coordinator start/stop

	if request.method == 'POST':
		if request.form['submit'] == 'Start Coordinator':
			try:
				hyperdexfunc.startcoord()
			except: 
				pass

		elif request.form['submit'] == 'Stop Coordinator':
			try:

				hyperdexfunc.stopcoord()

			except:
				pass

# Node start/stop 


		elif request.form['submit'] == 'Start Node':
			try:

				hyperdexfunc.startnode()

			except: pass

		elif request.form['submit'] == 'Stop Node':
			try:

				hyperdexfunc.stopnodes()

			except: pass

# Data Deletion 


		elif request.form['submit'] == 'Delete Node Data':
			try:
				hyperdexfunc.deletenodedata()

			except:
				pass


		elif request.form['submit'] == 'Delete Coordinator Data':
			try:
				hyperdexfunc.deletecoordata()

			except:
				pass

		coordstatus = hyperdexfunc.checkcoordstatus()

		nodestatus = hyperdexfunc.checknodestatus()

		spacelist = hyperdexfunc.listspaces()

		try:
			config = hyperdexfunc.getconfig()

			cluster = config[0]

			version = config[1]

			flags = config[2]
			
			try:

				server = config[3]
			except:
				server = ''

		except:
			cluster = ''

			version = ''

			flags = ''

			server = ''

			pass

		return render_template("noc.html",
			title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist, cluster = cluster, version = version, flags = flags, server = server)


	else:
		coordstatus = hyperdexfunc.checkcoordstatus()

		nodestatus = hyperdexfunc.checknodestatus()

		spacelist = hyperdexfunc.listspaces()

		try:
			config = hyperdexfunc.getconfig()

			cluster = config[0]

			version = config[1]

			flags = config[2]

			try:

				server = config[3]
			except:
				server = ''

		except:
			cluster = ''

			version = ''

			flags = ''

			server = ''

			pass

		return render_template("noc.html",
			title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist, cluster = cluster, version = version, flags = flags, server = server)


@app.route('/spaces/<spacename>')

def show_space(spacename):
	return render_template("space.html",
		title = '%s' % spacename, space = spacename)