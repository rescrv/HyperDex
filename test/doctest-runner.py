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

import datetime
import doctest
import os
import sys

class Sentinel(object): pass

def Document(x): return x

def myeval(e):
    x = (' '.join(e.split('\n'))).strip()
    try:
        if x == '':
            return None
        else:
            return eval(x)
    except:
        return Sentinel

class OutputChecker(doctest.OutputChecker):

    def check_output(self, want, got, optionflags):
        w = myeval(want)
        g = myeval(got)
        if w is not Sentinel and g is not Sentinel:
            if w == g:
                return True
            if isinstance(w, list) and isinstance(g, list) and \
               sorted(w) == sorted(g):
                return True
        return doctest.OutputChecker.check_output(self, want, got, optionflags)

f = sys.argv[1]
text = open(f).read()
runner = doctest.DocTestRunner(checker=OutputChecker(),
                               optionflags=doctest.ELLIPSIS)
parser = doctest.DocTestParser()
test = parser.get_doctest(text, {}, os.path.basename(f), f, 0)
runner.run(test)
result = runner.summarize()
sys.exit(0 if result.failed == 0 else 1)
