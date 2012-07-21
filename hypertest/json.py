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

import subprocess
import sys
import time

import pyparsing

import hypercoordinator.client
import hypercoordinator.parser
import hypertest


def runtest(lang, filename):
    f = open(filename)
    spacedesc = f.readline()
    space = None
    if spacedesc.startswith('#'):
        spacedesc = spacedesc[1:].strip()
        try:
            parser = (hypercoordinator.parser.space + pyparsing.stringEnd)
            space = parser.parseString(spacedesc)[0]
        except ValueError as e:
            space = None
        except pyparsing.ParseException as e:
            space = None
    if space is None:
        print('could not find valid space description; aborting test')
        return -1
    with hypertest.HyperDexCoordinator() as coord:
        time.sleep(0.1)
        with hypertest.HyperDexDaemon(host=coord.bindto(), port=coord.host_port()) as daemon:
            time.sleep(0.1)
            c = hypercoordinator.client.Client(coord.bindto(), coord.control_port())
            while c.count_servers() < 1:
                time.sleep(0.1)
            print('JSON TEST:  server up')
            c.add_space(spacedesc)
            print('JSON TEST:  space created')
            while not c.is_stable():
                time.sleep(0.1)
            print('JSON TEST:  stabilized')
            args = ['hyperdex-json-bridge-' + lang,
                    '--host', coord.bindto(), '--port', str(coord.host_port())]
            f.seek(0)
            pipe = subprocess.Popen(args, stdin=f)
            f.close()
            print('JSON TEST:  bridge created')
            pipe.wait()
            print('JSON TEST:  bridge exited', pipe.returncode)
            return pipe.returncode


if __name__ == '__main__':
    sys.exit(runtest(sys.argv[1], sys.argv[2]))
