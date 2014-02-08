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
	cmd = 'ps -C "replicant-daemon"'
	results = os.system(cmd)
	if results == 256:
		coordstatus = False
	elif results == 0:
		coordstatus = True
	else:
		coordstatus = False
	return coordstatus

def getconfig():
	if checkcoordstatus():
		cmd = 'hyperdex show-config'
		proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
		(out,err) = proc.communicate()
		configlist = out.split()
		cluster = configlist[1]
		version = configlist[3]
		flags = configlist[5]
		try:
			server = configlist[7]
		except:
			server = ''
	else:
		cluster = ''
		version = ''
		flags = ''
		server = ''
	return cluster, version, flags, server

def checknodestatus():
	results = os.system('ps -C "hyperdex-daemon"')
	if results == 256:
		nodestatus = False
	elif results == 0:
		nodestatus = True
	else:
		nodestatus = False
	return nodestatus

def startnode():
	startnode = 'hyperdex daemon --daemon --data=%s --log=%s --listen=%s --coordinator-port=%s' % \
	(nodedata, nodelog, coorip, coorport)
	os.system(startnode)

def stopnodes():
	while checknodestatus():
		stopnodes = 'killall hyperdex-daemon'
		os.system(stopnodes)
		
def startcoord():
	startcoord = 'hyperdex coordinator --daemon --data=%s --log=%s --listen=%s  --listen-port=%s' % \
		(coordata, coorlog, coorip, coorport)
	os.system(startcoord)

def setreadonly():
	setreadonly = 'hyperdex set-read-only --host=%s  --port=%s' % (coorip, coorport)
	os.system(setreadonly)

def setreadwrite():
	setreadonly = 'hyperdex set-read-write --host=%s  --port=%s' % (coorip, coorport)
	os.system(setreadwrite)

def waituntilstable():
	waituntilstable = 'hyperdex wait-until-stable --host=%s  --port=%s' % (coorip, coorport)
	os.system(waituntilstable)

def stopcoord():
	setreadonly()
	waituntilstable()
	while checkcoordstatus():
		stopcoord = 'killall replicant-daemon'
		os.system(stopcoord)
		stopnodes()

def listspaces():
	cmd = 'hyperdex list-spaces'
	proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
	(out,err) = proc.communicate()
	spacelist = out.split('\n')
	spacelist = spacelist[:-1]
	return spacelist

def deletenodedata():
	delnodedata = 'rm -r %s*' % nodedata
	delnodelog = 'rm -r %s*' % nodelog
	os.system(delnodedata)
	os.system(delnodelog)
	
def deletecoordata():
	delcoordata = 'rm -r %s*' % coordata
	delcoorlog = 'rm -r %s*' % coorlog
	os.system(delcoordata)
	os.system(delcoorlog)



