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
import bindings.ruby

DOCS_IN = {(bindings.AsyncCall, bindings.SpaceName): 'The name of the space as a string or symbol.'
          ,(bindings.AsyncCall, bindings.Key): 'The key for the operation as a Ruby type'
          ,(bindings.AsyncCall, bindings.Attributes): 'A hash specifying attributes '
           'to modify and their respective values.'
          ,(bindings.AsyncCall, bindings.MapAttributes): 'A hash specifying map '
           'attributes to modify and their respective key/values.'
          ,(bindings.AsyncCall, bindings.Predicates): 'A hash of predicates '
           'to check against.'
          ,(bindings.Iterator, bindings.SpaceName): 'The name of the space as string or symbol.'
          ,(bindings.Iterator, bindings.SortBy): 'The attribute to sort by.'
          ,(bindings.Iterator, bindings.Limit): 'The number of results to return.'
          ,(bindings.Iterator, bindings.MaxMin): 'Maximize (!= 0) or minimize (== 0).'
          ,(bindings.Iterator, bindings.Predicates): 'A hash of predicates '
           'to check against.'
          }

def generate_worker(call, x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    if x.form == bindings.AsyncCall:
        cls = 'deferred'
    if x.form == bindings.Iterator:
        cls = 'iterator'
    fptr = bindings.c.generate_func_ptr(x)
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

def generate_api_func(x, cls):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = 'Client :: %s(' % x.name
    padd = ' ' * 16
    func += ', '.join([str(arg).lower()[17:-2] for arg in x.args_in])
    func += ')\n'
    return func

def generate_api_block(x, cls='Client'):
    block  = ''
    block += '\\paragraph{{\code{{{0}}}}}\n'.format(bindings.LaTeX(x.name))
    block += '\\label{{api:ruby:{0}}}\n'.format(x.name)
    block += '\\index{{{0}!Ruby API}}\n'.format(bindings.LaTeX(x.name))
    block += '\\begin{rubycode}\n'
    block += generate_api_func(x, cls=cls)
    block += '\\end{rubycode}\n'
    block += '\\funcdesc \input{{\\topdir/api/desc/{0}}}\n\n'.format(x.name)
    block += '\\noindent\\textbf{Parameters:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_in, DOCS_IN,
                                         label_maker=bindings.parameters_script_style)
    block += '\n\\noindent\\textbf{Returns:}\n'
    if x.args_out == (bindings.Status,):
        if bindings.Predicates in x.args_in:
            block += 'True if predicate, False if not predicate.  Raises exception on error.'
        else:
            block += 'True.  Raises exception on error.'
    elif x.args_out == (bindings.Status, bindings.Attributes):
        block += 'Object if found, nil if not found.  Raises exception on error.'
    elif x.args_out == (bindings.Status, bindings.Count):
        block += 'Number of objects found.  Raises exception on error.'
    elif x.args_out == (bindings.Status, bindings.Description):
        block += 'Description of search.  Raises exception on error.'
    else:
        assert False
    block += '\n'
    return block

def generate_client_prototypes():
    fout = open(os.path.join(BASE, 'bindings/ruby/prototypes.c'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write('\n/* This file is generated by bindings/ruby.py */\n\n')
    fout.write(''.join([generate_prototype(c) for c in bindings.Client]))

def generate_client_definitions():
    fout = open(os.path.join(BASE, 'bindings/ruby/definitions.c'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write('\n/* This file is generated by bindings/ruby.py */\n\n')
    fout.write('\n'.join(generate_workers(bindings.Client)))
    fout.write('\n'.join([generate_definition(c) for c in bindings.Client]))

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/api/ruby.client.tex'), 'w')
    fout.write(bindings.copyright('%', '2013-2014'))
    fout.write('\n% This LaTeX file is generated by bindings/ruby.py\n\n')
    fout.write('\n'.join([generate_api_block(c) for c in bindings.Client]))

if __name__ == '__main__':
    generate_client_prototypes()
    generate_client_definitions()
    generate_client_doc()
