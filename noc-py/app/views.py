  # Copyright (c) 2014, Pierre Rochard
  # All rights reserved.

  #   This program is free software: you can redistribute it and/or modify
  #   it under the terms of the Affero GNU General Public License as published by
  #   the Free Software Foundation, either version 3 of the License, or
  #   (at your option) any later version.

  #   This program is distributed in the hope that it will be useful,
  #   but WITHOUT ANY WARRANTY; without even the implied warranty of
  #   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the Affero
  #   GNU General Public License for more details.

  #   You should have received a copy of the Affero GNU General Public License
  #   along with this program.  If not, see <http://www.gnu.org/licenses/>.


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


# Coordinator start/stop logic

	if request.method == 'POST':
		print request.form
		if request.form['submit'] == 'Start Coordinator':
			print 1
			try:
				hyperdexfunc.startcoord()
			except: 
				pass

		elif request.form['submit'] == 'Stop Coordinator':
			print 1
			try:

				hyperdexfunc.stopcoord()

			except:
				pass

# Node start/stop logic


		elif request.form['submit'] == 'Start Node':
			print 1
			try:

				hyperdexfunc.startnode()

			except: pass

		elif request.form['submit'] == 'Stop Node':
			try:

				hyperdexfunc.stopnodes()

			except: pass


		coordstatus = hyperdexfunc.checkcoordstatus()

		nodestatus = hyperdexfunc.checknodestatus()
	
		spacelist = hyperdexfunc.listspaces()


		return render_template("noc.html",
			title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist)


	else:
		coordstatus = hyperdexfunc.checkcoordstatus()

		nodestatus = hyperdexfunc.checknodestatus()
	
		spacelist = hyperdexfunc.listspaces()


		return render_template("noc.html",
			title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist)


