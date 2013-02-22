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

'''
Hyperunit is a testing module for HyperDex's client bindings.  It sets up
a temporary HyperDex configuration using the Config class, and provides 
Contexts, which are used for individual steps in a unit test.

Hyperunit also provides a convenient REPL for interactively exploring
HyperDex.
'''

# we frequently catch Exception to enable REPL's and control test failures
#pylint: disable=W0703

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import sys
import random
import subprocess
import time
import shutil
import atexit
import imp
import os.path

import hyperclient
import tempfile

__all__ = (
    'Config',
    'Context',
    'Error',
    'ContextError',
    'Mismatch',
    'parse_argv'
)

# A "mode" is a function that when given a context and a possible exception,
# reacts by raising an exception, returning an exception and/or returning
# None.  By assigning a mode to a context, you state your expectations about
# the outcome of the next performed operation.

def must(context, err):
    "indicates that actions must succeed to continue testing"
    if err is not None:
        context.fail(err)
        raise err

def should(context, err):
    "indicates that actions should succeed to continue testing"
    if err is not None:
        context.fail(err)

def mustnt(context, err):
    "indicates that actions must not succeed to continue testing"
    if err is None:
        err = Mustnt(context)
        context.fail(err)
        raise err

def shouldnt(context, err):
    "indicates that actions should not succeed to continue testing"
    if err is None:
        err = Mustnt(context)
        context.fail(err)

class Config(object):
    '''
    maintains a configuration of hyperdex coordinator and daemons for use
    in unit tests
    '''

    __slots__ = (
        'bind_addr', 'coord_port', 'daemon_count', 'data_dir', 
        'temp_dir', 'atexit',
        'running', 'tracing', 'errors', 'log', 'procs'
    )

    def __init__(
        self, bind_addr='127.0.0.1', 
        coord_port=None, 
        daemons=5,
        data_dir=None,
        tracing=False,
        log=None
    ):
        self.bind_addr = bind_addr
        self.coord_port = coord_port
        self.data_dir = data_dir
        self.daemon_count = daemons
        self.data_dir = data_dir
        self.tracing = tracing

        if hasattr(log, 'write'):
            self.log = log
        elif isinstance(log, str):
            self.log = open(log, 'w')
        elif log is None:
            self.log = sys.stderr
        else:
            raise Exception("expected either path, string or file-like log")

        self.temp_dir = None
        self.procs = []
        self.running = False
        self.atexit = None

        self.errors = []

    def require_daemons(self, count):
        'ensures at least count daemons are available in the config'
        while count > self.daemon_count:
            self.start_daemon(self.daemon_count)
            self.daemon_count += 1

    def start(self):
        'ensures that a hyperdex coordinator and daemon are running'

        if self.running: return
        self.running = True

        if self.coord_port is None:
            self.coord_port = random_port()

        self.spam("building datadir")
        if self.data_dir is None:
            self.temp_dir = tempfile.mkdtemp(prefix='hyperunit-')
            self.data_dir = self.temp_dir

        self.spam("starting coordinator")
        self.start_proc(
            'hyperdex', 'coordinator', 
            '-f', '-D', self.data_dir,
            '-l', self.bind_addr,
            '-p', str(self.coord_port)
        )

        time.sleep(2) # let the coordinator settle

        self.spam("starting daemons")
        for n in range(0, self.daemon_count):
            self.start_daemon(n)

        if self.atexit is None:
            self.atexit = self.stop
            atexit.register(self.atexit)

    def stop(self):
        'stops the hyperdex coordinator and daemon'

        if not self.running: return
        self.running = False

        self.spam("halting processes")
        while self.procs:
            self.halt_proc(self.procs.pop())

        if self.temp_dir is not None:
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
            self.data_dir = None

        if self.atexit is not None:
            # python2 lacks unregister
            try:
                #pylint: disable=E1101
                atexit.unregister(self.atexit)
            except AttributeError:
                pass

    def start_daemon(self, n):
        'starts a daemon named "n" and connects it to the coordinator'
        pd = "%s/daemon-%s" % (self.data_dir, n) 
        pn = random_port()
        self.spam("starting daemon %s", pn)
        self.start_proc(
            'hyperdex' ,'daemon', 
            '-f', '-D', pd, 
            '-l', self.bind_addr, 
            '-p', str(pn),
            '-P', str(self.coord_port)
        )
        time.sleep(0.25) # let the coordinator resettle

    def start_proc(self, *cmd):
        'popens a process and appends it to the list of things to halt'
        proc = subprocess.Popen(
            cmd, env={"GLOG_minloglevel" : "2"}, 
                        stdout=self.log, stderr=subprocess.STDOUT
        )
        self.procs.append(proc)
        return proc

    def halt_proc(self, proc):
        'halts a process associated with the test'
        if proc in self.procs: self.procs.remove(proc)
        proc.terminate()
        time.sleep(0.25)
        proc.kill()
        proc.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def client(self):
        'establishes a client to the HyperDex coordinator'
        self.start()
        return hyperclient.Client(self.bind_addr, self.coord_port)

    def must(self):
        'produces a Context that "must" perform instructions'
        return Context(self, self.client(), mode=must, tracing=self.tracing)

    def should(self):
        'produces a Context that "should" perform instructions'
        return Context(self, self.client(), mode=should, tracing=self.tracing)

    def mustnt(self):
        'produces a Context that "mustnt" perform instructions'
        return Context(self, self.client(), mode=mustnt, tracing=self.tracing)

    def shouldnt(self):
        'produces a Context that "shouldnt" perform instructions'
        return Context(self, self.client(), mode=shouldnt, tracing=self.tracing)

    def spam(self, fmt, *args):
        'prints a message to stdout if tracing'
        if not self.tracing: return
        if not args: 
            print('..', fmt)
            return
        print('..', fmt % args)

    def repl(self): 
        'runs a python repl with test config and client locals'
        import code 
        # this is a bulky module..
        # it'd be nice to use something pleasant like IPython, but IPython's
        # embed is not for the faint of heart, and can confuse a user more
        # than it helps
        code.interact(
            'HyperUnit REPL; test config bound as "test", client as ' +
            '"db"', raw_input, 
            {'db' : self.client(), 'test' : self}
        )

    def run_scripts(self, *paths):
        'runs one or more test scripts'

        for path in paths:
            self.run_script(path)

        return self.errors

    def run_script(self, path):
        'runs one test script'
        self.spam('loading %s', path)
        name = os.path.splitext(os.path.basename(path))[0]

        impl = imp.load_source(name, path)
        if impl is None:
            print('!! could not load script', name)
            return

        slots = list(dir(impl))
        slots.sort()

        for slot in slots:
            if slot.startswith("__"):
                continue # fast track so we don't spam about it
            if not slot.startswith("test_"): 
                self.spam("skipping nontest %s", slot)
                continue
            print("##", slot, "in", name)

            test = getattr(impl, slot)
            try:
                test(self)
            except Exception as exc:
                self.fail(exc)                
                if uncontrolled_error(exc): raise

    def fail(self, reason):
        'adds err to the list of errors experienced with this config and prints'
        if reason is None: return
        if reason not in self.errors:
            self.errors.append(reason)
            print("!!", repr(reason))

class Context(object):
    '''
    Maintains test context, providing a simple and immediate wrapper around
    a HyperDex client that simplifies writing unit tests.
    '''

    __slots__ = (
        'config', 'client', 'mode', 'result', 'error', 'tracing', 
        'performing'
    )

    def __init__(
        self, config, client, mode=must, tracing=False, 
        error=None, result=None
    ):
        self.config = config
        self.client = client
        self.mode = mode
        self.result = result
        self.error = error
        self.tracing = tracing
        self.performing = None

    def __repr__(self):
        if self.result or self.error:
            return '<%s>' % (format_status(self.result, self.error),)
        return '<context>'

    def clone(self, **opts):
        'returns a new Context with changed options and same client/config'

        for slot in self.__slots__:
            if slot in ('client', 'config', 'performing'): 
                continue # these items are not options

            if opts.has_key(slot): 
                continue

            opts[slot] = getattr(self, slot)

        return Context(self.config, self.client, **opts)

    def must(self):
        'provides a Context that "must" succeed'
        return self.clone(mode=must)

    def should(self):
        'provides a Context that "should" succeed'
        return self.clone(mode=should)

    def mustnt(self):
        'provides a Context that "mustnt" succeed'
        return self.clone(mode=mustnt)

    def shouldnt(self):
        'provides a Context that "shouldnt" succeed'
        return self.clone(mode=shouldnt)

    def trace(self):
        'provides a Context that will emit spam'
        return self.clone(tracing=True)

    def spam(self, fmt, *args):
        'prints a message to stdout if tracing'
        if not self.tracing: return
        if not args: print('::', fmt)
        print('::', fmt % args)

    def perform(self, func, *args, **opts):
        'if current state is not error, calls func(*args, **opts)'
        if self.error: return # don't do it!
        self.performing = (func, args, opts)
        if self.tracing: self.explain()
    
        try:
            self.result = func(*args, **opts)
        except Exception as exc: 
            if uncontrolled_error(exc): raise
            if exc is not None:
                self.error = exc
            self.mode(self, exc)
        else:
            self.mode(self, None)
        finally:
            self.performing = None

        return self

    def explain(self, tag='..'):
        'explains what we are currently performing'
        if not self.performing: return
        func, args, opts = self.performing
        show = args

        if isinstance(args[0], hyperclient.Client):
            show = args[1:]
        elif isinstance(args[0], Context):
            show = args[1:]

        if show and opts:
            print(tag, "%s %s(*%r, **%r)" % (
                self.mode.__name__, func.__name__, show, opts
            ))
        elif show:
            print(tag, "%s %s(*%r)" % (
                self.mode.__name__, func.__name__, show
            ))
        elif opts:
            print(tag, "%s %s(**%r)" % (
                self.mode.__name__, func.__name__, opts
            ))

    def equal(self, other):
        'compares other to the current result, if self.error is None'
        return self.perform(expect_equal, self, other)

    def equals(self, other):
        'alias for "equal"'
        return self.equal(other)

    def fail(self, reason):
        'indicates that the context has failed with reason'
        if reason is None: return
        self.error = reason
        if not self.tracing: self.explain('!!')
        self.config.fail(reason)

class Error(Exception):
    'denotes an error associated with hyperunit'
    
    pass

class ContextError(Error):
    'denotes an error associated with a context'

    __slots__ = ('context', 'result', 'error')

    def __init__(self, context):
        Error.__init__(self)
        self.context = context
        self.result = context.result
        self.error = context.error

    def __repr__(self):
        class_name = self.__class__.__name__
        if self.result or self.error:
            return '<%s %s>' % (
                class_name,
                format_status(self.result, self.error)
            )
        return '<%s>' % (class_name,)

class Mismatch(ContextError):
    'denotes a mismatch produced by a context comparison'

    __slots__ = ('other')

    def __init__(self, context, other):
        ContextError.__init__(self, context)
        self.other = other

    def __repr__(self):
        if self.other:
            return ContextError.__repr__(self)[:-1] + (
                ' other: %r>' % (self.other,)
            )

        return ContextError.__repr__(self)

class Mustnt(ContextError):
    'indicates that a "mustnt" or "shouldnt" constraint was violated'
    pass

def expect_equal(context, expect):
    'raises Mismatch if context result does not match'
    if hasattr(context.result, "next"):
        # coerce the iterator to a tuple and verify equivalence
        context.result = tuple(x for x in context.result)
        expect_subset(context, context.result, expect)
        expect_subset(context, expect, context.result)
        return
   
    if context.result != expect:
        raise Mismatch(context, expect)

def expect_subset(context, domain, subset):
    'verifies all items in subset are in domain'
    domain = list(x for x in domain) 
    subset = list(x for x in subset)
    for item in domain:
        if item not in subset:
            raise Mismatch(context, item)
        subset.remove(item)

    if len(subset):
        raise Mismatch(context, subset)

def uncontrolled_error(exc):
    'returns true if exception is not associated with hyperdex or hyperunit'

    if exc is None: return False
    if isinstance(exc, Error): return False
    if isinstance(exc, hyperclient.HyperClientException): return False
    return True

def format_status(result, error):
    'describes the status of a context'
    
    if result is not None:
        if error is not None:
            return 'result: %r error:%r' % (
                result, error
            )
        return 'result: %r' % (result,)

    if error:
        return 'error: %r' % (error,)

    return ''

def bind_client_method(name):
    'binds an exposed method of hyperclient.Client into hyperunit.Context'

    impl = getattr(hyperclient.Client, name)
    doc = impl.__doc__
    if not doc:
        doc = 'hyperclient.Client.%s(...)' % name

    binding = lambda self, *args, **opts: self.perform(
        impl, self.client, *args, **opts
    )
    binding.__name__ = name
    binding.__doc__ = doc

    setattr(Context, name, binding)

def bind_client_methods():
    'binds all exposed methods of hyperclient.Client into hyperunit.Context'

    for name in dir(hyperclient.Client):
        if hasattr(Context, name): continue
        if name.startswith('_'): continue
        # we have to use another function, here, to properly bind locals
        bind_client_method(name)

bind_client_methods()

def random_port():
    'guesses a random ip port'

    return random.randint(10000, 65535)

def parse_argv(
    prog, args,
    prolog='Hyperunit runs HyperDex unit tests or provides a ' +
           'repl to test manually.', 
    epilog='If no scripts are provided, a repl is presented with a ' +
           'temporary HyperDex configuration.',
):

    'parses command line returns a list of scripts and a dict of opts'
    import argparse
    parser = argparse.ArgumentParser(prog=prog, description=prolog)
    parser.epilog = epilog
    parser.add_argument(
        '-v', '--verbose', action='store_true', 
        help='enables verbose trace output'
    )
    parser.add_argument(
        '-n', '--daemons', type=int, 
        help='specifies how many daemons to start initially'
    )
    parser.add_argument(
        '-l', '--log', type=str, 
        help='log hyperdex process output to the supplied path'
    )
    parser.add_argument('script', nargs='*')
    result = parser.parse_args(args)

    opts = {}
    if result.verbose: opts['tracing'] = True
    if result.daemons is not None: opts['daemons'] = result.daemons
    if result.log is not None: opts['log'] = result.log

    return result.script, opts

def main():
    'main script entry point'
    args, opts = parse_argv(sys.argv[0], sys.argv[1:])

    print(":: setting up hyperdex test environment")
    ret = 0
    with Config(**opts) as cfg:
        try:
            if len(args) == 0:
                cfg.repl()
            elif cfg.run_scripts(*args):
                ret = 1
        finally:
            print(":: tearing down hyperdex test environment")

    if ret == 0:
        print('## success')
    else:
        print('## failed')

    sys.exit(ret)

if __name__ == '__main__':
    main()
