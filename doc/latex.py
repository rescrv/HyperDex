# Copyright (c) 2013, Robert Escriva
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


# Lines that start with any of these strings will be cut
SKIP_LINES = set(['Babel <'
                 ,'Document Class'
                 ,"Option `squaren' provided!"
                 ,' Command \squaren defined by SIunits package!'
                 ,'\openout1 = `'
                 ,'ABD: EveryShipout initializing macros'
                 ,' %&-line parsing enabled.'
                 ,'entering extended mode'
                 ,' restricted \\write18'
                 ,'LaTeX2e <2011/06/27>'
                 ,'LaTeX Info:'
                 ,'LaTeX Font Info:'
                 ,'(textcomp)'
                 ,'(Font)'
                 ,'File: '
                 ])


SUPPRESS_WARNINGS = set(['Your graphic driver pgfsys-dvips.def does not support fadings. This warning is given only once on input line 31.'
                        ])


latex_version_re = re.compile(r'^This is (?P<tex>.*?), Version (?P<version>.*?) \((?P<distro>.*?)\).*')
constant_re = re.compile(r'\\[a-zA-Z0-9@_-]+=\\[a-zA-Z0-9@_-]+')
package_warning_re = re.compile(r'^Package (?P<package>[-a-zA-Z0-9_.]*?) Warning: (?P<message>.*)')
package_info_re = re.compile(r'^Package (?P<package>[-a-zA-Z0-9_.]*?) Info: (?P<message>.*)')
package_load_re = re.compile(r'Package: (?P<package>[-a-zA-Z0-9_.]*?) \d{4}/\d{2}/\d{2} .*')
output_re = re.compile(r'^Output written on (?P<outfile>[-a-zA-Z0-9_.]*?) \((?P<pages>[0-9]+) pages?, (?P<bytes>[0-9]+) bytes?\).')


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


def cat_tex_log(name):
    with open(name + '.log') as f:
        data = f.read()
        sys.stderr.write(data)


def process_tex_log(name):
    filtered = os.path.exists(name + '.filtered')
    deps = []
    tli = TeXLineIterator(name + '.stdout.log')
    if not filtered:
        logging.getLogger().setLevel(logging.CRITICAL)
    for line in tli:
        line = line.strip('\n')
        if not filtered:
            print line
        if line.startswith('This is'):
            match = latex_version_re.match(line)
            if match:
                logging.info('{distro}: {tex} version {version}'.format(**match.groupdict()))
            else:
                logging.error('unhandled version string case: {0}'.format(line))
        elif line.startswith('Package'):
            match = package_warning_re.match(line)
            if match:
                if match.groupdict()['message'] not in SUPPRESS_WARNINGS:
                    logging.warning('{package}: {message}'.format(**match.groupdict()))
                continue
            match = package_info_re.match(line)
            if match:
                if match.groupdict()['message'] not in SUPPRESS_WARNINGS:
                    logging.debug('{package}: {message}'.format(**match.groupdict()))
                continue
            if not package_load_re.match(line):
                logging.error('unhandled package message case: {0}'.format(line))
        elif line.startswith('Output'):
            match = output_re.match(line)
            if match:
                logging.info(line.strip('.'))
            else:
                logging.error('unhandled output case: {0}'.format(line))
        elif constant_re.match(line):
            logging.debug('constant: {0}'.format(line))
        elif line == "Here is how much of TeX's memory you used:":
            try:
                x = tli.next()
                while x.startswith(' '):
                    x = tli.next()
                tli.push(x)
            except StopIteration:
                pass
        elif any(line.startswith(s) for s in SKIP_LINES):
            logging.debug('skip line: {0}'.format(line))
        elif line == '**{0}'.format(name):
            logging.debug('useless: {0}'.format(line))
        elif line.strip(')( ') == '':
            logging.debug('empty string: {0}'.format(line))
        else:
            paths = [p.strip(')(') for p in line.strip(')( ').split(' ')]
            if all(os.path.exists(p) for p in paths):
                deps += paths
                logging.debug('paths: {0}'.format(', '.join(paths)))
            else:
                logging.error('unhandled input: {0}'.format(line))
    return set(sorted(deps))


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


def build(name):
    need_tex = True
    need_xtx = older_than(name, '.bbl', '.xtx')
    need_idx = older_than(name, '.ind', '.idx')
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
        if iters > 4:
            logging.error("error, iterated 4 times; you'd think tex would converge by now; bailing")
            return 0
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
                need_xtx = need_xtx or xtx
                need_idx = need_idx or idx
        sha1s = new_sha1s
        iters += 1
    write_deps(name, deps)
    return 0

logging.basicConfig(format='%(message)s', level=logging.INFO)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        logging.error("program takes just one argument")
    rc = build(sys.argv[1])
    if rc != 0:
        os.unlink(sys.argv[1] + '.dvi')
    sys.exit(rc)
