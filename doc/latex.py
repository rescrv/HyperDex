# Copyright (c) 2013-2015, Robert Escriva
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
#     * Neither the name of this project nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
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


import argparse
import hashlib
import logging
import os.path
import re
import shlex
import subprocess
import sys


# A list of extensions that latex.py acknowledges.
#
# Each entry is a tuple (extension, tex, xtx).  "extension" is a string
# indicating the exension to be watched (e.g., '.tex').  "tex" (and "xtx") are
# booleans that indicate that a rerun of LaTeX (resp., CrossTeX) are necessary
# to obtain a correct build.
#
# If you're adding a new file type, and you're not sure whether it impacts
# LaTeX, CrossTeX, or both, it is safe (but inefficient) to mark both as True.
EXTENSIONS = (('.tex', True,  False, False),
              ('.xtx', False, True,  False),
              ('.aux', True,  True,  False),
              ('.bbl', True,  False, False),
              ('.idx', False, False, True),
              ('.ind', True,  False, False),
              ('.log', True,  False, False))

SKIP_LINES = set([
    r'\write18 enabled.',
    'entering extended mode',
    'ABD: EveryShipout initializing macros'
])

SUPPRESS_PACKAGE_WARNINGS = set([
    ('pgf', 'Your graphic driver pgfsys-dvips.def does not support fadings. This warning is given only once on input line 31.'),
])

latex_version_re = re.compile(r'^This is (?P<tex>.*?), Version (?P<version>.*?) \((?P<distro>.*?)\).*')
package_warning_re = re.compile(r'^Package (?P<package>[-a-zA-Z0-9_.]*?) Warning: (?P<message>.*)')
output_re = re.compile(r'^Output written on (?P<outfile>[-a-zA-Z0-9_.]*?) \((?P<pages>[0-9]+) pages?, (?P<bytes>[0-9]+) bytes?\).')
pagenum_re = re.compile(r'^\[(\d+)\] (.*)')

class TeXLineIterator(object):

    def __init__(self, filename):
        self._file = open(filename)
        self._pushback = []

    def __iter__(self):
        return self

    def next(self):
        if self._pushback:
            ret = self._pushback[0]
            self._pushback = self._pushback[1:]
            return ret
        else:
            return self._file.next()

    def push(self, line):
        self._pushback.append(line)

def process_tex_log(name):
    deps = set([])
    tli = TeXLineIterator(name + '.stdout.log')
    for line in tli:
        line = re.sub('^[ )>]*', '', line).strip()
        # collect deps
        if line.startswith('(') or line.startswith('<'):
            pieces = re.split('[ )>]', line[1:], 1)
            assert len(pieces) in (1, 2)
            x = pieces[0]
            if os.path.exists(x):
                if len(pieces) == 2:
                    tli.push(pieces[1])
                deps.add(x)
                continue
        if not line:
            pass
        elif line in SKIP_LINES:
            pass
        elif pagenum_re.match(line):
            x = pagenum_re.match(line)
            logging.info('page {0}'.format(x.group(1)))
            tli.push(x.group(2))
        elif latex_version_re.match(line):
            x = latex_version_re.match(line)
            logging.info('{distro}: {tex} version {version}'.format(**x.groupdict()))
        elif package_warning_re.match(line):
            x = package_warning_re.match(line)
            pkg = x.groupdict()['package']
            msg = x.groupdict()['message']
            if (pkg, msg) not in SUPPRESS_PACKAGE_WARNINGS:
                logging.error('{0}: {1}'.format(pkg, msg))
        elif output_re.match(line):
            logging.info(line)
        elif line == 'Transcript written on {name}.log.'.format(name=name):
            logging.info(line)
        else:
            for x in SKIP_LINES:
                if line.startswith(x):
                    tli.push(line[len(x):])
                    break
            else:
                logging.error('error: ' + repr(line))
    return set([os.path.normpath(d) for d in deps])

def cat_tex_log(name):
    with open(name + '.log') as f:
        data = f.read()
        sys.stderr.write(data)

def compute_sha1s(name):
    sha1s = {}
    for ext in [x[0] for x in EXTENSIONS]:
        if os.path.exists(name + ext):
            h = hashlib.new('sha1')
            h.update(open(name + ext).read())
            sha1s[name + ext] = h.hexdigest()
        else:
            sha1s[name + ext] = ''
    return sha1s


def write_deps(name, deps):
    f = open(name + '.P', 'w')
    f.write('{0}.dvi: latex.py {1}\n'.format(name, ' '.join(deps)))
    for d in deps:
        d = d[2:] if d.startswith('./') else d
        f.write('{0}:\n'.format(d))
    f.flush()
    f.close()


def older_than(name, target, dep):
    t = name + target
    d = name + dep
    if not os.path.exists(t) or not os.path.exists(d):
        return True
    return os.stat(t).st_mtime < os.stat(d).st_mtime


def build(name, run_xtx=False, run_idx=False):
    need_tex = True
    need_xtx = run_xtx and older_than(name, '.bbl', '.xtx')
    need_idx = run_idx and older_than(name, '.ind', '.idx')
    if not need_tex and not need_xtx and not need_idx:
        need_tex = True
    sha1s = compute_sha1s(name)
    if not need_tex and not need_xtx and not need_idx:
        subprocess.check_call(['touch', name + '.dvi'])
        subprocess.check_call(['touch', name + '.P'])
        return 0
    iters = 0
    deps = set([])
    while need_tex or need_xtx or need_idx:
        if iters > 16:
            logging.error("error, iterated 16 times; you'd think tex would converge by now; bailing")
            return -1
        logging.error("=======================")
        logging.error("beginning iteration %d" % iters)
        logging.error("=======================")
        if need_tex or (need_xtx and not os.path.exists(name + '.aux')):
            env = os.environ.copy()
            env['max_print_line'] = '4096'
            rc = subprocess.call(['latex', '-interaction=nonstopmode',
                                  '-shell-escape', '-halt-on-error', name],
                                 stdin  = open('/dev/null', 'r'),
                                 stdout = open(name + '.stdout.log', 'w'),
                                 stderr = subprocess.STDOUT,
                                 env = env)
            if rc != 0:
                cat_tex_log(name)
                logging.error("LaTeX failed")
                return rc
            deps |= process_tex_log(name)
            need_tex = False
        if need_xtx:
            crosstex_flags = shlex.split(os.environ.get('CROSSTEX_FLAGS', ''), posix=True)
            rc = subprocess.call(['crosstex', name] + crosstex_flags)
            if rc != 0:
                logging.error("CrossTeX failed")
                return rc
            need_xtx = False
        if need_idx:
            rc = subprocess.call(['makeindex', name])
            if rc != 0:
                logging.error("makeindex failed")
                return rc
            need_idx = False
        new_sha1s = compute_sha1s(name)
        for ext, tex, xtx, idx in EXTENSIONS:
            if sha1s[name + ext] != new_sha1s[name + ext]:
                need_tex = need_tex or tex
                need_xtx = run_xtx and (need_xtx or xtx)
                need_idx = run_idx and (need_idx or idx)
        sha1s = new_sha1s
        iters += 1
    write_deps(name, deps)
    return 0

logging.basicConfig(format='%(message)s', level=logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--crosstex', default=False, action='store_true')
    parser.add_argument('--index', default=False, action='store_true')
    parser.add_argument('document')
    args = parser.parse_args()
    rc = build(args.document, run_xtx=args.crosstex, run_idx=args.index)
    if rc != 0:
        os.unlink(args.document + '.dvi')
    sys.exit(rc)
