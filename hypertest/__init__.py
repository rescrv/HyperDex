# Copyright (c) 2012, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import random
import subprocess
import time


class HyperDexCoordinator(object):

    def __init__(self, bindto='127.0.0.1', control_port=None, host_port=None):
        if not control_port:
            control_port = random.randint(1025, 65535)
        if not host_port:
            host_port = random.randint(1025, 65535)
        args = ['hyperdex-coordinator',
                '-b', bindto, '-c', str(control_port), '-p', str(host_port)]
        self._daemon = subprocess.Popen(args)
        #                                stdout=open('/dev/null', 'w'),
        #                                stderr=open('/dev/null', 'w'))
        self._bindto = bindto
        self._control_port = control_port
        self._host_port = host_port

    def bindto(self):
        return self._bindto

    def control_port(self):
        return self._control_port

    def host_port(self):
        return self._host_port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._daemon.terminate()
        time.sleep(0.1)
        self._daemon.kill()
        self._daemon.wait()


class HyperDexDaemon(object):

    def __init__(self, datadir='.', host='127.0.0.1', port=1234, threads=1):
        args = ['hyperdex-daemon', '-f', '-D', datadir, '-t', str(threads),
                '-h', host, '-p', str(port)]
        self._daemon = subprocess.Popen(args)
        #                                stdout=open('/dev/null', 'w'),
        #                                stderr=open('/dev/null', 'w'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._daemon.terminate()
        time.sleep(0.1)
        self._daemon.kill()
        self._daemon.wait()
