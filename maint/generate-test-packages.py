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
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import argparse
import os
import re
import subprocess
import tarfile
import tempfile

def add_from_content(tar, name, content, mode=0644):
    tmp = tempfile.TemporaryFile()
    tmp.write(content)
    tmp.flush()
    tmp.seek(0)
    os.fchmod(tmp.fileno(), mode)
    tinfo = tar.gettarinfo(arcname=name, fileobj=tmp)
    tar.addfile(tinfo, tmp)

def generate_test_tarball(version, name, tests):
    archive = 'test-hyperdex-' + name + '-' + str(version)
    tar = tarfile.open(archive + '.tar.gz', 'w:gz')
    version = subprocess.check_output('grep AC_INIT configure.ac',
                                      shell=True).split(' ')[1].strip('[,] ')
    makefile = '''CLASSPATH := /usr/share/java/*:test/java:.
export CLASSPATH
export HYPERDEX_SRCDIR=.
export HYPERDEX_BUILDDIR=.

check:
{rules}
'''.format(version=version, rules=''.join(['\t%s\n' % s for s, t in tests]))
    add_from_content(tar, archive + '/Makefile', makefile, 0644)
    for sh, test in tests:
        tar.add(sh, archive + '/' + sh)
        tar.add(test, archive + '/' + test)
    tar.add('test/runner.py', archive + '/test/runner.py')
    tar.add('test/doctest-runner.py', archive + '/test/doctest-runner.py')
    if argparse.__file__.endswith('.pyc'):
        tar.add(argparse.__file__[:-1], archive + '/argparse.py')
    tar.close()

version = re.search('^AC_INIT\(\[.*\], \[(.*)\], \[.*\]\)$',
            open('configure.ac').read(), re.MULTILINE).group(1)

generate_test_tarball(version, 'doc', [
    ('test/sh/doc.async-ops.sh', 'test/doc.async-ops.py'),
    ('test/sh/doc.atomic-ops.sh', 'test/doc.atomic-ops.py'),
    ('test/sh/doc.data-types.sh', 'test/doc.data-types.py'),
    ('test/sh/doc.documents.sh', 'test/doc.documents.py'),
    ('test/sh/doc.quick-start.sh', 'test/doc.quick-start.py'),
])

generate_test_tarball(version, 'java', [
    ('test/sh/bindings.java.BasicSearch.sh', 'test/java/BasicSearch.java'),
    ('test/sh/bindings.java.Basic.sh', 'test/java/Basic.java'),
    ('test/sh/bindings.java.DataTypeFloat.sh', 'test/java/DataTypeFloat.java'),
    ('test/sh/bindings.java.DataTypeInt.sh', 'test/java/DataTypeInt.java'),
    ('test/sh/bindings.java.DataTypeListFloat.sh', 'test/java/DataTypeListFloat.java'),
    ('test/sh/bindings.java.DataTypeListInt.sh', 'test/java/DataTypeListInt.java'),
    ('test/sh/bindings.java.DataTypeListString.sh', 'test/java/DataTypeListString.java'),
    ('test/sh/bindings.java.DataTypeMapFloatFloat.sh', 'test/java/DataTypeMapFloatFloat.java'),
    ('test/sh/bindings.java.DataTypeMapFloatInt.sh', 'test/java/DataTypeMapFloatInt.java'),
    ('test/sh/bindings.java.DataTypeMapFloatString.sh', 'test/java/DataTypeMapFloatString.java'),
    ('test/sh/bindings.java.DataTypeMapIntFloat.sh', 'test/java/DataTypeMapIntFloat.java'),
    ('test/sh/bindings.java.DataTypeMapIntInt.sh', 'test/java/DataTypeMapIntInt.java'),
    ('test/sh/bindings.java.DataTypeMapIntString.sh', 'test/java/DataTypeMapIntString.java'),
    ('test/sh/bindings.java.DataTypeMapStringFloat.sh', 'test/java/DataTypeMapStringFloat.java'),
    ('test/sh/bindings.java.DataTypeMapStringInt.sh', 'test/java/DataTypeMapStringInt.java'),
    ('test/sh/bindings.java.DataTypeMapStringString.sh', 'test/java/DataTypeMapStringString.java'),
    ('test/sh/bindings.java.DataTypeSetFloat.sh', 'test/java/DataTypeSetFloat.java'),
    ('test/sh/bindings.java.DataTypeSetInt.sh', 'test/java/DataTypeSetInt.java'),
    ('test/sh/bindings.java.DataTypeSetString.sh', 'test/java/DataTypeSetString.java'),
    ('test/sh/bindings.java.DataTypeString.sh', 'test/java/DataTypeString.java'),
    ('test/sh/bindings.java.LengthString.sh', 'test/java/LengthString.java'),
    ('test/sh/bindings.java.MultiAttribute.sh', 'test/java/MultiAttribute.java'),
    ('test/sh/bindings.java.RangeSearchInt.sh', 'test/java/RangeSearchInt.java'),
    ('test/sh/bindings.java.RangeSearchString.sh', 'test/java/RangeSearchString.java'),
    ('test/sh/bindings.java.RegexSearch.sh', 'test/java/RegexSearch.java'),
])

generate_test_tarball(version, 'python', [
    ('test/sh/bindings.python.BasicSearch.sh', 'test/python/BasicSearch.py'),
    ('test/sh/bindings.python.Basic.sh', 'test/python/Basic.py'),
    ('test/sh/bindings.python.DataTypeFloat.sh', 'test/python/DataTypeFloat.py'),
    ('test/sh/bindings.python.DataTypeInt.sh', 'test/python/DataTypeInt.py'),
    ('test/sh/bindings.python.DataTypeListFloat.sh', 'test/python/DataTypeListFloat.py'),
    ('test/sh/bindings.python.DataTypeListInt.sh', 'test/python/DataTypeListInt.py'),
    ('test/sh/bindings.python.DataTypeListString.sh', 'test/python/DataTypeListString.py'),
    ('test/sh/bindings.python.DataTypeMapFloatFloat.sh', 'test/python/DataTypeMapFloatFloat.py'),
    ('test/sh/bindings.python.DataTypeMapFloatInt.sh', 'test/python/DataTypeMapFloatInt.py'),
    ('test/sh/bindings.python.DataTypeMapFloatString.sh', 'test/python/DataTypeMapFloatString.py'),
    ('test/sh/bindings.python.DataTypeMapIntFloat.sh', 'test/python/DataTypeMapIntFloat.py'),
    ('test/sh/bindings.python.DataTypeMapIntInt.sh', 'test/python/DataTypeMapIntInt.py'),
    ('test/sh/bindings.python.DataTypeMapIntString.sh', 'test/python/DataTypeMapIntString.py'),
    ('test/sh/bindings.python.DataTypeMapStringFloat.sh', 'test/python/DataTypeMapStringFloat.py'),
    ('test/sh/bindings.python.DataTypeMapStringInt.sh', 'test/python/DataTypeMapStringInt.py'),
    ('test/sh/bindings.python.DataTypeMapStringString.sh', 'test/python/DataTypeMapStringString.py'),
    ('test/sh/bindings.python.DataTypeSetFloat.sh', 'test/python/DataTypeSetFloat.py'),
    ('test/sh/bindings.python.DataTypeSetInt.sh', 'test/python/DataTypeSetInt.py'),
    ('test/sh/bindings.python.DataTypeSetString.sh', 'test/python/DataTypeSetString.py'),
    ('test/sh/bindings.python.DataTypeString.sh', 'test/python/DataTypeString.py'),
    ('test/sh/bindings.python.LengthString.sh', 'test/python/LengthString.py'),
    ('test/sh/bindings.python.MultiAttribute.sh', 'test/python/MultiAttribute.py'),
    ('test/sh/bindings.python.RangeSearchInt.sh', 'test/python/RangeSearchInt.py'),
    ('test/sh/bindings.python.RangeSearchString.sh', 'test/python/RangeSearchString.py'),
    ('test/sh/bindings.python.RegexSearch.sh', 'test/python/RegexSearch.py'),
])

generate_test_tarball(version, 'ruby', [
    ('test/sh/bindings.ruby.BasicSearch.sh', 'test/ruby/BasicSearch.rb'),
    ('test/sh/bindings.ruby.Basic.sh', 'test/ruby/Basic.rb'),
    ('test/sh/bindings.ruby.DataTypeFloat.sh', 'test/ruby/DataTypeFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeInt.sh', 'test/ruby/DataTypeInt.rb'),
    ('test/sh/bindings.ruby.DataTypeListFloat.sh', 'test/ruby/DataTypeListFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeListInt.sh', 'test/ruby/DataTypeListInt.rb'),
    ('test/sh/bindings.ruby.DataTypeListString.sh', 'test/ruby/DataTypeListString.rb'),
    ('test/sh/bindings.ruby.DataTypeMapFloatFloat.sh', 'test/ruby/DataTypeMapFloatFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeMapFloatInt.sh', 'test/ruby/DataTypeMapFloatInt.rb'),
    ('test/sh/bindings.ruby.DataTypeMapFloatString.sh', 'test/ruby/DataTypeMapFloatString.rb'),
    ('test/sh/bindings.ruby.DataTypeMapIntFloat.sh', 'test/ruby/DataTypeMapIntFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeMapIntInt.sh', 'test/ruby/DataTypeMapIntInt.rb'),
    ('test/sh/bindings.ruby.DataTypeMapIntString.sh', 'test/ruby/DataTypeMapIntString.rb'),
    ('test/sh/bindings.ruby.DataTypeMapStringFloat.sh', 'test/ruby/DataTypeMapStringFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeMapStringInt.sh', 'test/ruby/DataTypeMapStringInt.rb'),
    ('test/sh/bindings.ruby.DataTypeMapStringString.sh', 'test/ruby/DataTypeMapStringString.rb'),
    ('test/sh/bindings.ruby.DataTypeSetFloat.sh', 'test/ruby/DataTypeSetFloat.rb'),
    ('test/sh/bindings.ruby.DataTypeSetInt.sh', 'test/ruby/DataTypeSetInt.rb'),
    ('test/sh/bindings.ruby.DataTypeSetString.sh', 'test/ruby/DataTypeSetString.rb'),
    ('test/sh/bindings.ruby.DataTypeString.sh', 'test/ruby/DataTypeString.rb'),
    ('test/sh/bindings.ruby.LengthString.sh', 'test/ruby/LengthString.rb'),
    ('test/sh/bindings.ruby.MultiAttribute.sh', 'test/ruby/MultiAttribute.rb'),
    ('test/sh/bindings.ruby.RangeSearchInt.sh', 'test/ruby/RangeSearchInt.rb'),
    ('test/sh/bindings.ruby.RangeSearchString.sh', 'test/ruby/RangeSearchString.rb'),
    ('test/sh/bindings.ruby.RegexSearch.sh', 'test/ruby/RegexSearch.rb'),
])
