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

BAD_STARTS = ['This is pdfTeX, Version',
              r'\write18',
              'entering extended mode',
              'LaTeX2e',
              'Babel',
              'Style option: `fancyvrb\' v2.7a',
              'Document Class: article 2014/09/29 v1.4h Standard LaTeX document class',
              'Option `squaren\' provided!',
              'Package pgf Warning: Your graphic driver pgfsys-dvips.def does not support fadings',
              'Command \\squaren defined by SIunits package!',
              'ABD: EveryShipout initializing macros',
              'see the transcript file for additional information',
              'Output written on ',
              'Transcript written on ',
              ]

pathsoft_re = re.compile(r'\([-/a-zA-Z0-9_.]+[ )]', flags=re.M)
pathhard_re = re.compile(r'<[-/a-zA-Z0-9_.]+[ >]', flags=re.M)
pathsoftnl_re = re.compile(r'\([-/a-zA-Z0-9_.]+$', flags=re.M)
pathhardnl_re = re.compile(r'<[-/a-zA-Z0-9_.]+$', flags=re.M)
pagenum_re = re.compile(r'\[\d+\]')

def eliminate_paths(path_re, log, deps):
    xs = reversed(list(path_re.finditer(log)))
    for x in xs:
        path = log[x.start(0):x.end(0)]
        char = ' '
        if '\n' in path:
            char = '\n'
        path = path.strip('()<> \n')
        if os.path.exists(path):
            deps.add(path)
            log = log[:x.start(0)] + char + log[x.end(0):]
    return log

def sanitize_lines(lines):
    for line in lines:
        xs = pagenum_re.finditer(line)
        breaks = set()
        for x in xs:
            breaks.add(x.start(0))
            breaks.add(x.end(0))
        prev = 0
        for x in sorted(breaks):
            s = line[prev:x]
            s = s.strip('()<> ')
            if s:
                yield s
            prev = x
        if breaks:
            line = line[max(breaks):]
        line = line.strip('()<> ')
        if line:
            yield line

def process_tex_log(name):
    deps = set([])
    log = open(name + '.stdout.log').read()
    log.replace('\t', ' ')
    next_page = 1
    log = eliminate_paths(pathsoft_re, log, deps)
    log = eliminate_paths(pathhard_re, log, deps)
    log = eliminate_paths(pathsoftnl_re, log, deps)
    log = eliminate_paths(pathhardnl_re, log, deps)
    lines = sanitize_lines(log.split('\n'))
    for line in lines:
        skip = False
        for bs in BAD_STARTS:
            if line.startswith(bs):
                skip = True
        if skip:
            continue
        print line
    return deps

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
