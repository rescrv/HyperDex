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

import copy
import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '../..')

sys.path.append(os.path.join(BASE, 'bindings'))
sys.path.append(os.path.join(BASE, 'bindings/c'))

import generator
import gen_client_header

copyright = '''/* Copyright (c) 2013, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* GENERATED!  Do not modify this file directly */

'''

def call_name(x):
    call  = x.form.__name__.lower()
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_in])
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_out])
    return call

def generate_worker_asynccall(call, x):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    c_func = gen_client_header.generate_func(xp)
    c_func = c_func.replace('hyperdex_client_WXYZ', '(*f)')
    c_func = ' '.join([c.strip() for c in c_func.split('\n')])
    c_func = c_func.strip('; ')
    func  = 'static VALUE\n'
    func += '_hyperdex_ruby_client_{0}({1}, VALUE self'.format(call, c_func)
    for arg in x.args_in:
        func += ', VALUE ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    VALUE dfrd;\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '    ' + p + ' in_' + n + ';\n'
    func += '    struct hyperdex_client* client;\n'
    func += '    struct hyperdex_ruby_client_deferred* d;\n'
    func += '    dfrd = rb_class_new_instance(1, &self, class_deferred);\n'
    func += '    rb_iv_set(self, "tmp", dfrd);\n'
    func += '    Data_Get_Struct(self, struct hyperdex_client, client);\n'
    func += '    Data_Get_Struct(dfrd, struct hyperdex_ruby_client_deferred, d);\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    hyperdex_ruby_client_convert_{0}(d->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
    func += '    d->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                             ', '.join(['&d->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (d->reqid < 0)\n    {\n'
    func += '        hyperdex_ruby_client_throw_exception(d->status, hyperdex_client_error_message(client));\n'
    func += '    }\n\n'
    func += '    d->encode_return = hyperdex_ruby_client_deferred_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    rb_hash_aset(rb_iv_get(self, "ops"), LONG2NUM(d->reqid), dfrd);\n'
    func += '    rb_iv_set(self, "tmp", Qnil);\n'
    func += '    return dfrd;\n'
    func += '}\n'
    return func

def generate_worker_iterator(call, x):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    c_func = gen_client_header.generate_func(xp)
    c_func = c_func.replace('hyperdex_client_WXYZ', '(*f)')
    c_func = ' '.join([c.strip() for c in c_func.split('\n')])
    c_func = c_func.strip('; ')
    func  = 'static VALUE\n'
    func += '_hyperdex_ruby_client_{0}({1}, VALUE self'.format(call, c_func)
    for arg in x.args_in:
        func += ', VALUE ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    VALUE iter;\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '    ' + p + ' in_' + n + ';\n'
    func += '    struct hyperdex_client* client;\n'
    func += '    struct hyperdex_ruby_client_iterator* it;\n'
    func += '    iter = rb_class_new_instance(1, &self, class_iterator);\n'
    func += '    rb_iv_set(self, "tmp", iter);\n'
    func += '    Data_Get_Struct(self, struct hyperdex_client, client);\n'
    func += '    Data_Get_Struct(iter, struct hyperdex_ruby_client_iterator, it);\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    hyperdex_ruby_client_convert_{0}(it->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
    func += '    it->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                              ', '.join(['&it->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (it->reqid < 0)\n    {\n'
    func += '        hyperdex_ruby_client_throw_exception(it->status, hyperdex_client_error_message(client));\n'
    func += '    }\n\n'
    func += '    it->encode_return = hyperdex_ruby_client_iterator_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    rb_hash_aset(rb_iv_get(self, "ops"), LONG2NUM(it->reqid), iter);\n'
    func += '    rb_iv_set(self, "tmp", Qnil);\n'
    func += '    return iter;\n'
    func += '}\n'
    return func

def generate_workers(xs):
    calls = set([])
    for x in xs:
        call = call_name(x)
        if call in calls:
            continue
        assert x.form in (generator.AsyncCall, generator.Iterator)
        if x.form == generator.AsyncCall:
            yield generate_worker_asynccall(call, x)
        if x.form == generator.Iterator:
            yield generate_worker_iterator(call, x)
        calls.add(call)

def generate_definition(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    func = 'static VALUE\nhyperdex_ruby_client_{0}(VALUE self'.format(x.name)
    for arg in x.args_in:
        func += ', VALUE ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    return _hyperdex_ruby_client_{0}(hyperdex_client_{1}, self'.format(call_name(x), x.name)
    for arg in x.args_in:
        func += ', ' + arg.__name__.lower()
    func += ');\n}\n'
    if x.form == generator.AsyncCall:
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
    assert x.form in (generator.AsyncCall, generator.Iterator)
    if x.form == generator.AsyncCall:
        proto1 = 'rb_define_method(class_client, "async_{0}", hyperdex_ruby_client_{0}, {1});\n'.format(x.name, len(x.args_in))
        proto2 = 'rb_define_method(class_client, "{0}", hyperdex_ruby_client_wait_{0}, {1});\n'.format(x.name, len(x.args_in))
        return proto1 + proto2
    if x.form == generator.Iterator:
        return 'rb_define_method(class_client, "{0}", hyperdex_ruby_client_{0}, {1});\n'.format(x.name, len(x.args_in))

if __name__ == '__main__':
    with open(os.path.join(BASE, 'bindings/ruby/definitions.c'), 'w') as fout:
        fout.write(copyright)
        fout.write('\n'.join(generate_workers(generator.Client)))
        fout.write('\n\n')
        fout.write('\n'.join([generate_definition(c) for c in generator.Client]))

    with open(os.path.join(BASE, 'bindings/ruby/prototypes.c'), 'w') as fout:
        fout.write(copyright)
        fout.write(''.join([generate_prototype(c) for c in generator.Client]))
