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

import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE)

import bindings
import bindings.c
import bindings.python

def name(x):
    name = x.name
    if name == 'del':
        return 'delete'
    return name

def PYTYPEOF(x):
    if x == bindings.SpaceName:
        return 'bytes'
    elif x == bindings.Key:
        return ''
    elif x == bindings.Attributes:
        return 'dict'
    elif x == bindings.MapAttributes:
        return 'dict'
    elif x == bindings.Predicates:
        return 'dict'
    elif x == bindings.SortBy:
        return 'bytes'
    elif x == bindings.Limit:
        return 'int'
    elif x == bindings.MaxMin:
        return 'str'
    print x
    assert False

def indent(x):
    ret = ''
    for line in x.split('\n'):
        if line:
            line = ' ' * 4 + line
        ret += line + '\n'
    return ret

def arg_name(a):
    return a.__name__.lower()

def generate_prototype(x, lib):
    func = bindings.c.generate_func(x, lib)
    func = func.replace('\n', ' ')
    func = ' '.join([x for x in func.split() if x])
    func = func.replace('enum ', '')
    func = func.replace('const ', '')
    func = func.replace('struct ', '')
    func = func.rstrip(';')
    return func

def generate_function_ptr(x, name, lib):
    func = bindings.c.generate_func_ptr(x, lib)
    func = func.replace('(*f)', name)
    func = func.replace('enum ', '')
    func = func.replace('const ', '')
    func = func.replace('struct ', '')
    func = func.rstrip(';')
    return func

def generate_worker_asynccall(call, x):
    typed_args = ', '.join([(PYTYPEOF(arg) + ' ' + arg_name(arg)).strip()
                             for arg in x.args_in])
    func  = 'cdef {0}(self, {2} f, {1}):\n'.format(call, typed_args, call + '_fptr')
    func += '    cdef Deferred d = Deferred(self)\n'
    for arg in x.args_in:
        for p, n in arg.args:
            if p.startswith('const '):
                p = p[len('const '):]
            if p.startswith('struct '):
                p = p[len('struct '):]
            func += '    cdef ' + p + ' in_' + n + '\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    self.convert_{0}(d.arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
    func += '    d.reqid = f(self.client, {0}, {1});\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                                ', '.join(['&d.' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if d.reqid < 0:\n'
    func += '        raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))\n'
    func += '    d.encode_return = hyperdex_python_client_deferred_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + '\n'
    func += '    self.ops[d.reqid] = d\n'
    func += '    return d'
    return indent(func)

def generate_worker_iterator(call, x):
    typed_args = ', '.join([(PYTYPEOF(arg) + ' ' + arg_name(arg)).strip()
                             for arg in x.args_in])
    func  = 'cdef {0}(self, {2} f, {1}):\n'.format(call, typed_args, call + '_fptr')
    func += '    cdef Iterator it = Iterator(self)\n'

    for arg in x.args_in:
        for p, n in arg.args:
            if p.startswith('const '):
                p = p[len('const '):]
            if p.startswith('struct '):
                p = p[len('struct '):]
            func += '    cdef ' + p + ' in_' + n + '\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    self.convert_{0}(it.arena, {0}, {1});\n'.format(arg.__name__.lower(), args)

    func += '    it.reqid = f(self.client, {0}, {1});\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                                 ', '.join(['&it.' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if it.reqid < 0:\n'
    func += '        raise HyperDexClientException(it.status, hyperdex_client_error_message(self.client))\n'
    func += '    it.encode_return = hyperdex_python_client_iterator_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + '\n'
    func += '    self.ops[it.reqid] = it\n'
    func += '    return it'
    return indent(func)

def generate_function_pointer_typedefs(xs, lib):
    calls = set([])
    for x in xs:
        call = bindings.call_name(x)
        if call in calls:
            continue
        assert x.form in (bindings.AsyncCall, bindings.Iterator)
        yield 'ctypedef ' + generate_function_ptr(x, call + '_fptr', lib)
        calls.add(call)

def generate_workers(xs):
    calls = set([])
    for x in xs:
        call = bindings.call_name(x)
        if call in calls:
            continue
        assert x.form in (bindings.AsyncCall, bindings.Iterator)
        if x.form == bindings.AsyncCall:
            yield generate_worker_asynccall(call, x)
        if x.form == bindings.Iterator:
            yield generate_worker_iterator(call, x)
        calls.add(call)

def generate_method(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    typed_args = ', '.join([(PYTYPEOF(arg) + ' ' + arg_name(arg)).strip()
                             for arg in x.args_in])
    arg_list = ', '.join([arg_name(arg) for arg in x.args_in])
    if x.form == bindings.AsyncCall:
        meth  = 'def async_{0}(self, {1}):\n'.format(name(x), typed_args)
        meth += '    return self.{0}(hyperdex_client_{1}, {2})\n'.format(bindings.call_name(x), x.name, arg_list)
        meth += 'def {0}(self, {1}):\n'.format(name(x), typed_args)
        meth += '    return self.async_{0}({1}).wait()\n'.format(name(x), arg_list)
    if x.form == bindings.Iterator:
        meth  = 'def {0}(self, {1}):\n'.format(name(x), typed_args)
        meth += '    return self.{0}(hyperdex_client_{1}, {2})\n'.format(bindings.call_name(x), x.name, arg_list)
    return indent(meth)[:-1]

def generate_api_norm_func(func):
    arg_list = ', '.join([arg_name(arg) for arg in func.args_in])
    return 'def {0}(self, {1})\n'.format(name(func), arg_list)

def generate_api_async_func(func):
    arg_list = ', '.join([arg_name(arg) for arg in func.args_in])
    return 'def async_{0}(self, {1})\n'.format(name(func), arg_list)

def generate_api_block(x, lib):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsubsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:python:%s}\n' % x.name
    block += '\\index{%s!Python API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/api/desc/%s}\n\n' % x.name
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{pythoncode}\n'
    block += generate_api_norm_func(x)
    block += '\\end{pythoncode}\n\n'
    block += '\\paragraph{Parameters:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_in, 'python/' + lib + '/in',
                                         label_maker=bindings.parameters_script_style)
    block += '\n\\paragraph{Returns:}\n'
    frag  = 'python/' + lib + '/return_' + x.form.__name__.lower()
    frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
    block += '\\input{\\topdir/api/fragments/' + frag + '}\n'
    if x.form == bindings.AsyncCall:
        block += '\n\\pagebreak\n'
        block += '\\subsubsection{\code{async\\_%s}}\n' % bindings.LaTeX(x.name)
        block += '\\label{api:python:async_%s}\n' % x.name
        block += '\\index{async\_%s!Python API}\n' % bindings.LaTeX(x.name)
        block += '\\input{\\topdir/api/desc/%s}\n\n' % x.name
        block += '\\paragraph{Definition:}\n'
        block += '\\begin{pythoncode}\n'
        block += generate_api_async_func(x)
        block += '\\end{pythoncode}\n\n'
        block += '\\paragraph{Parameters:}\n'
        block += bindings.doc_parameter_list(x.form, x.args_in, 'python/' + lib + '/in',
                                             label_maker=bindings.parameters_script_style)
        block += '\n\\paragraph{Returns:}\n'
        frag  = 'python/' + lib + '/return_async_' + x.form.__name__.lower()
        frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
        block += '\\input{\\topdir/api/fragments/' + frag + '}\n'
        block += '\n\\paragraph{See also:}  This is the asynchronous form of '
        block += '\code{%s}.\n' % bindings.LaTeX(x.name)
    return block

def generate_client_python():
    fout = open(os.path.join(BASE, 'bindings/python/hyperdex/client.pyx'), 'w')
    template = open(os.path.join(BASE, 'bindings/python/hyperdex/client.pyx.in')).read()
    prototypes = indent('\n'.join([generate_prototype(c, 'client') for c in bindings.Client]))
    fps = '\n'.join(generate_function_pointer_typedefs(bindings.Client, 'client'))
    fout.write(template.format(prototypes=prototypes, function_pointers=fps))
    fout.write('\n'.join(generate_workers(bindings.Client)))
    fout.write('\n')
    fout.write('\n'.join([generate_method(c) for c in bindings.Client]))

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/api/python.client.tex'), 'w')
    fout.write(bindings.copyright('%', '2014'))
    fout.write('\n% This LaTeX file is generated by bindings/python.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'client') for c in bindings.Client]))

if __name__ == '__main__':
    generate_client_python()
    generate_client_doc()
