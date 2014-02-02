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


import os
import hyperdex.admin
import subprocess


a = hyperdex.admin.Admin('127.0.0.1', 1982)
c = hyperdex.client.Client('127.0.0.1',1982)

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


def getconfig():

	try: 
		cmd = 'hyperdex show-config'
		proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
		(out,err) = proc.communicate()

		print out
		if out != '':
			try:
				configlist = out.split()
				cluster = configlist[1]
				version = configlist[3]
				flags = configlist[5]
				try:
					server = configlist[7]
				except:
					server = ''
				print configlist

				return cluster, version, flags, server

			except: pass
		else:
			pass

	except:
		pass




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

			setreadonly()

			waituntilstable()

			stopcoord = 'killall replicant-daemon'

			os.system(stopcoord)

			stopnodes()


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


def deletenodedata():

	try:

		delnodedata = 'rm -r %s*' % nodedata
		delnodelog = 'rm -r %s*' % nodelog

		os.system(delnodedata)

		os.system(delnodelog)

		return 1

	except:

		return 0



def deletecoordata():

	try:

		delcoordata = 'rm -r %s*' % coordata
		delcoorlog = 'rm -r %s*' % coorlog

		os.system(delcoordata)

		os.system(delcoorlog)

		return 1

	except:

		return 0


def setreadonly():

	try:

		setreadonly = 'hyperdex set-read-only --host=%s  --port=%s' % (coorip, coorport)

		os.system(setreadonly)

		return 1

	except:

		return 0


def setreadwrite():

	try:

		setreadonly = 'hyperdex set-read-write --host=%s  --port=%s' % (coorip, coorport)

		os.system(setreadwrite)

		return 1

	except:

		return 0



def waituntilstable():

	try:

		waituntilstable = 'hyperdex wait-until-stable --host=%s  --port=%s' % (coorip, coorport)

		os.system(waituntilstable)

		return 1

	except:

		return 0

def delspace(space):

	a.rm_space(space.encode('utf-8'))