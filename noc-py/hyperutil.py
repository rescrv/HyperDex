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

coorip = '127.0.0.1'
coorport = '1982'

def checkcoordstatus():
	cmd = 'ps -C "replicant-daemon"'
	status = os.system(cmd)
	if status == 256:
		coordstatus = False
	elif results == 0:
		coordstatus = True
	else:
		coordstatus = False
	return coordstatus

def checknodestatus():
	results = os.system('ps -C "hyperdex-daemon"')
	if results == 256:
		nodestatus = False
	elif results == 0:
		nodestatus = True
	else:
		nodestatus = False
	return nodestatus

def stopall():
	setreadonly()
	waituntilstable()
	while checkcoordstatus():
		stopcoord()
	while checknodestatus():
		stopnodes()

def stopnodes():
	stopnodes = 'killall hyperdex-daemon'
	os.system(stopnodes)

def stopcoord():
	stopcoord = 'killall replicant-daemon'
	os.system(stopcoord)

def setreadonly():
	setreadonly = 'hyperdex set-read-only --host=%s  --port=%s' % (coorip, coorport)
	os.system(setreadonly)

def waituntilstable():
	waituntilstable = 'hyperdex wait-until-stable --host=%s  --port=%s' % (coorip, coorport)
	os.system(waituntilstable)
