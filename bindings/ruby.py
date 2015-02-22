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

import copy
import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE)

import bindings
import bindings.c
import bindings.ruby

def generate_worker(call, x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    if x.form == bindings.AsyncCall:
        cls = 'deferred'
    if x.form == bindings.Iterator:
        cls = 'iterator'
    fptr = bindings.c.generate_func_ptr(x, 'client')
    func = 'static VALUE\n'
    func += 'hyperdex_ruby_client_{0}({1}, VALUE self'.format(call, fptr)
    for arg in x.args_in:
        func += ', VALUE ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    VALUE op;\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '    ' + p + ' in_' + n + ';\n'
    func += '    struct hyperdex_client* client;\n'
    func += '    struct hyperdex_ruby_client_{0}* o;\n'.format(cls)
    func += '    op = rb_class_new_instance(1, &self, class_{0});\n'.format(cls)
    func += '    rb_iv_set(self, "tmp", op);\n'
    func += '    Data_Get_Struct(self, struct hyperdex_client, client);\n'
    func += '    Data_Get_Struct(op, struct hyperdex_ruby_client_{0}, o);\n'.format(cls)
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    hyperdex_ruby_client_convert_{0}(o->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
    func += '    o->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                             ', '.join(['&o->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (o->reqid < 0)\n    {\n'
    func += '        hyperdex_ruby_client_throw_exception(o->status, hyperdex_client_error_message(client));\n'
    func += '    }\n\n'
    func += '    o->encode_return = hyperdex_ruby_client_{0}_encode_'.format(cls)
    func += '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    rb_hash_aset(rb_iv_get(self, "ops"), LONG2NUM(o->reqid), op);\n'
    func += '    rb_iv_set(self, "tmp", Qnil);\n'
    func += '    return op;\n'
    func += '}\n'
    return func

def generate_workers(xs):
    calls = set([])
    for x in xs:
        if x.form is bindings.MicrotransactionCall:
            continue #TODO support microtransactions in Ruby
        call = bindings.call_name(x)
        if call in calls:
            continue
        yield generate_worker(call, x)
        calls.add(call)

def generate_definition(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = 'static VALUE\nhyperdex_ruby_client_{0}(VALUE self'.format(x.name)
    for arg in x.args_in:
        func += ', VALUE ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    return hyperdex_ruby_client_{0}(hyperdex_client_{1}, self'.format(bindings.call_name(x), x.name)
    for arg in x.args_in:
        func += ', ' + arg.__name__.lower()
    func += ');\n}\n'
    if x.form == bindings.AsyncCall:
        func += 'VALUE\nhyperdex_ruby_client_wait_{0}(VALUE self'.format(x.name)
        for arg in x.args_in:
            func += ', VALUE ' + arg.__name__.lower()
        func += ')\n{\n'
        func += '    VALUE deferred = hyperdex_ruby_client_{0}(self'.format(x.name)
        for arg in x.args_in:
            func += ', ' + arg.__name__.lower()
        func += ');\n'
        func += '    return rb_funcall(deferred, rb_intern("wait"), 0);\n}\n'
    return func

def generate_prototype(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    if x.form == bindings.AsyncCall:
        proto1 = 'rb_define_method(class_client, "async_{0}", hyperdex_ruby_client_{0}, {1});\n'.format(x.name, len(x.args_in))
        proto2 = 'rb_define_method(class_client, "{0}", hyperdex_ruby_client_wait_{0}, {1});\n'.format(x.name, len(x.args_in))
        return proto1 + proto2
    if x.form == bindings.Iterator:
        return 'rb_define_method(class_client, "{0}", hyperdex_ruby_client_{0}, {1});\n'.format(x.name, len(x.args_in))

def generate_api_func(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = '%s(' % x.name
    padd = ' ' * 16
    func += ', '.join([str(arg).lower()[17:-2] for arg in x.args_in])
    func += ')\n'
    return func

def generate_api_norm_func(x):
    return generate_api_func(x)

def generate_api_async_func(x):
    xp = copy.deepcopy(x)
    xp.name = 'async_' + x.name
    return generate_api_func(xp)

def generate_api_block(x, lib):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsubsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:ruby:%s}\n' % x.name
    block += '\\index{%s!Ruby API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{rubycode}\n'
    block += generate_api_norm_func(x)
    block += '\\end{rubycode}\n\n'
    block += '\\paragraph{Parameters:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_in, 'ruby/' + lib + '/fragments/in',
                                         label_maker=bindings.parameters_script_style)
    block += '\n\\paragraph{Returns:}\n'
    frag  = 'ruby/' + lib + '/fragments/return_' + x.form.__name__.lower()
    frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
    block += '\\input{\\topdir/' + frag + '}\n'
    if x.form == bindings.AsyncCall:
        block += '\n\\pagebreak\n'
        block += '\\subsubsection{\code{async\\_%s}}\n' % bindings.LaTeX(x.name)
        block += '\\label{api:ruby:async_%s}\n' % x.name
        block += '\\index{async\_%s!Ruby API}\n' % bindings.LaTeX(x.name)
        block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
        block += '\\paragraph{Definition:}\n'
        block += '\\begin{rubycode}\n'
        block += generate_api_async_func(x)
        block += '\\end{rubycode}\n\n'
        block += '\\paragraph{Parameters:}\n'
        block += bindings.doc_parameter_list(x.form, x.args_in, 'ruby/' + lib + '/fragments/in',
                                             label_maker=bindings.parameters_script_style)
        block += '\n\\paragraph{Returns:}\n'
        frag  = 'ruby/' + lib + '/fragments/return_async_' + x.form.__name__.lower()
        frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
        block += '\\input{\\topdir/' + frag + '}\n'
        block += '\n\\paragraph{See also:}  This is the asynchronous form of '
        block += '\code{%s}.\n' % bindings.LaTeX(x.name)
    return block

def generate_client_prototypes():
    fout = open(os.path.join(BASE, 'bindings/ruby/prototypes.c'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write('\n/* This file is generated by bindings/ruby.py */\n\n')
    fout.write(''.join([generate_prototype(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))

def generate_client_definitions():
    fout = open(os.path.join(BASE, 'bindings/ruby/definitions.c'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write('\n/* This file is generated by bindings/ruby.py */\n\n')
    fout.write('\n'.join(generate_workers(bindings.Client)))
    fout.write('\n'.join([generate_definition(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/ruby/client/ops.tex'), 'w')
    fout.write(bindings.copyright('%', '2013-2014'))
    fout.write('\n% This LaTeX file is generated by bindings/ruby.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'client') for c in bindings.Client
                          if c.name not in bindings.DoNotDocument if c.form is not bindings.MicrotransactionCall]))

if __name__ == '__main__':
    generate_client_prototypes()
    generate_client_definitions()
    generate_client_doc()
