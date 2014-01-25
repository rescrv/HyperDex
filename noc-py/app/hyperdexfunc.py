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


import os
import hyperdex.admin
import subprocess

# Configuration Variables - will be added to settings form eventually

approot = os.path.dirname(os.path.abspath(__file__))

nodedata = os.path.join(approot, 'data/nodedata/')

nodelog = os.path.join(approot, 'data/nodelog/')


coordata = os.path.join(approot, 'data/coordata/')


coorlog = os.path.join(approot, 'data/coorlog/')


coorip = '127.0.0.1'

coorport = '1982'


def checkcoordstatus():
	try:
		results = os.system('ps -C "replicant-daemon"')
		if results == 256:
			coordstatus = 'Offline'
		elif results == 0:
			coordstatus = 'Online'
		else:
				coordstatus = '%s' % results

	except:
		pass

	return coordstatus


def checknodestatus():

	try:
		results = os.system('ps -C "hyperdex-daemon"')
		if results == 256:
			nodestatus = 'Offline'
		elif results == 0:
			nodestatus = 'Online'
		else:
			nodestatus = '%s' % results

	except:
		pass
	return nodestatus


def startnode():

	try:

		startnode = 'hyperdex daemon --daemon --data=%s --log=%s --listen=%s --coordinator-port=%s' % (nodedata, nodelog, coorip, coorport)
		print startnode 
		os.system(startnode)

		return 1

	except:

		return 0

def stopnodes():
	try:
		while checknodestatus() == 'Online':
			stopnodes = 'killall hyperdex-daemon'

			os.system(stopnodes)
		return 1

	except:
		return 0




def startcoord():

	try:

		startcoord = 'hyperdex coordinator --daemon --data=%s --log=%s --listen=%s  --listen-port=%s' % (coordata, coorlog, coorip, coorport)

		os.system(startcoord)

		return 1

	except:

		return 0

def stopcoord():
	try:
		while checkcoordstatus() == 'Online':
			stopnodes = 'killall hyperdex-daemon'

			os.system(stopnodes)

			stopcoord = 'killall replicant-daemon'

			os.system(stopcoord)
		return 1

	except:
		return 0


def listspaces():
	try:
		cmd = 'hyperdex list-spaces'
		proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
		(out,err) = proc.communicate()
		spacelist = out.split('\n')
		spacelist = spacelist[:-1]

		return spacelist

	except:
		pass