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


from flask import render_template, request, redirect, url_for
from app import app
from forms import DesignSpace
import hyperdex.client
import hyperdex.admin
import hyperdexfunc

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
@app.route('/noc', methods=['GET', 'POST'])

def noc():

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
			title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist, 
			cluster = cluster, version = version, flags = flags, server = server)


@app.route('/coordinator/start')
def start_coordinator():
	hyperdexfunc.startcoord()
	return redirect(url_for('noc'))

@app.route('/coordinator/stop')
def stop_coordinator():
	hyperdexfunc.stopcoord()
	return redirect(url_for('noc'))

@app.route('/node/start')
def start_node():
	hyperdexfunc.startnode()
	return redirect(url_for('noc'))

@app.route('/node/stop')
def stop_node():
	hyperdexfunc.stopnodes()
	return redirect(url_for('noc'))

@app.route('/coordinator/delete')
def delete_coordinator():
	hyperdexfunc.deletecoordata()
	return redirect(url_for('noc'))

@app.route('/node/delete')
def delete_node():
	hyperdexfunc.deletenodedata()
	return redirect(url_for('noc'))

@app.route('/delete/<space>')
def delete_space(space):
	hyperdexfunc.delspace(space)
	return redirect(url_for('noc'))

@app.route('/spaces/new', methods=['GET', 'POST'])
def design_space():
	form = DesignSpace()
	if request.method == 'POST':
		print 'ok'	
		spacename = form.spacename.data.encode('utf-8')
		keyname = form.keyname.data.encode('utf-8')
		partitions = form.partitions.data
		failures = form.failures.data
		hyperdexfunc.create_space(spacename, keyname, partitions, failures)
		return redirect(url_for('noc'))
	return render_template('createspace.html', form=form)