# Copyright (c) 2013-2014, Cornell University
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

from __future__ import print_function

import sys

PREAMBLE = {
'python':
'''# File generated from {code} blocks in "{infile}"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
'''}

def transform(code, infile, outfile):
    fin = open(infile, 'r')
    fout = open(outfile, 'w')
    begin = '\\begin{%scode}\n' % code
    end = '\\end{%scode}\n' % code
    fout.write(PREAMBLE[code].format(code=code, infile=infile))
    output = False
    for line in fin:
        if line == begin:
            assert not output
            output = True
        elif line == end:
            output = False
        elif output:
            if line == ">>> a = hyperdex.admin.Admin('127.0.0.1', 1982)\n":
                fout.write(">>> a = hyperdex.admin.Admin(HOST, PORT)\n")
            elif line == ">>> c = hyperdex.client.Client('127.0.0.1', 1982)\n":
                fout.write(">>> c = hyperdex.client.Client(HOST, PORT)\n")
            else:
                fout.write(line)

if len(sys.argv) < 4 or sys.argv[1] is "help":
	print("This script generates test case from the documentation")
	print("Please supply the following arguments:")
	print(" 1. name of code (e.g. python)")
	print(" 2. input file ")
	print(" 3. output file ")
else:
	transform(sys.argv[1], sys.argv[2], sys.argv[3])
