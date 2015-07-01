# Copyright (c) 2013, Cornell University
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


import collections
import glob
import os
import os.path
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time


DOTDOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
BUILDDIR = os.getenv('HYPERDEX_BUILDDIR') or DOTDOT

sys.path.append(DOTDOT)
sys.path.append(os.path.join(BUILDDIR, './bindings/python'))


import argparse

import hyperdex.admin


Coordinator = collections.namedtuple('Coordinator', ('host', 'port'))
Daemon = collections.namedtuple('Daemon', ('host', 'port'))


class HyperDexCluster(object):

    def __init__(self, coordinators, daemons, clean=False, base=None):
        self.processes = []
        self.coordinators = coordinators
        self.daemons = daemons
        self.clean = clean
        self.base = base
        self.log_output = False

    def setup(self):
        if self.base is None:
            self.base = tempfile.mkdtemp(prefix='hyperdex-test-')
        env = {'GLOG_logtostderr': '',
               'GLOG_minloglevel': '0',
               'PATH': ((os.getenv('PATH') or '') + ':' + BUILDDIR).strip(':')}
        env['CLASSPATH'] = ((os.getenv('CLASSPATH') or '') + BUILDDIR + '/*').strip(':')
        env['GLOG_logbufsecs'] = '0'
        if 'HYPERDEX_BUILDDIR' in os.environ and os.environ['HYPERDEX_BUILDDIR'] != '.':
            env['HYPERDEX_EXEC_PATH'] = BUILDDIR
            env['HYPERDEX_COORD_LIB'] = os.path.join(BUILDDIR, '.libs/libhyperdex-coordinator')
        for i in range(self.coordinators):
            cmd = ['hyperdex', 'coordinator',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(1982 + i)]
            if i > 0:
                cmd += ['--connect', '127.0.0.1', '--connect-port', '1982']
            cwd = os.path.join(self.base, 'coord%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'hyperdex-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(1)
        for i in range(self.daemons):
            cmd = ['hyperdex', 'daemon', '-t', '1',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(2012 + i),
                   '--coordinator', '127.0.0.1', '--coordinator-port', '1982']
            cwd = os.path.join(self.base, 'daemon%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'hyperdex-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(0.5)

    def cleanup(self):
        for i in range(self.coordinators):
            core = os.path.join(self.base, 'coord%i' % i, 'core*')
            if glob.glob(core):
                print('coordinator', i, 'dumped core')
                self.clean = False
        for i in range(self.daemons):
            core = os.path.join(self.base, 'daemon%i' % i, 'core*')
            if glob.glob(core):
                print('daemon', i, 'dumped core')
                self.clean = False
        for p in reversed(self.processes):
            p.terminate()
        time.sleep(0.5)
        for p in reversed(self.processes):
            p.terminate()
        time.sleep(0.5)
        for p in reversed(self.processes):
            p.kill()
            p.wait()
        if self.log_output:
            print("logging output of failed test run's daemons")
            for i in range(self.coordinators):
                log = os.path.join(self.base, 'coord%i' % i, 'hyperdex-test-runner.log')
                print
                print('coordinator', i)
                print(open(log).read())
            for i in range(self.daemons):
                log = os.path.join(self.base, 'daemon%i' % i, 'hyperdex-test-runner.log')
                print
                print('daemon', i)
                print(open(log).read())
        if self.clean and self.base is not None:
            shutil.rmtree(self.base)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--space')
    parser.add_argument('--coordinators', default=1, type=int)
    parser.add_argument('--daemons', default=1, type=int)
    parser.add_argument('args', nargs='*')
    args = parser.parse_args(argv)
    hdc = HyperDexCluster(args.coordinators, args.daemons, True)
    try:
        hdc.setup()
        adm = hyperdex.admin.Admin('127.0.0.1', 1982)
        if args.space is not None:
            time.sleep(0.5)
            adm.add_space(args.space)
        time.sleep(0.5)
        ctx = {'HOST': '127.0.0.1', 'PORT': 1982}
        cmd_args = [arg.format(**ctx) for arg in args.args]
        status = subprocess.call(cmd_args, stderr=subprocess.STDOUT)
        if status != 0:
            print('process exited', status, '; dumping logs')
            hdc.log_output = True
        return status
    finally:
        hdc.cleanup()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
