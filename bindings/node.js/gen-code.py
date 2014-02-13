
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

copyright = '''// Copyright (c) 2014, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// GENERATED!  Do not modify this file directly

'''

def call_name(x):
    call  = x.form.__name__.lower()
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_in])
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_out])
    return call

def generate_function_pointer(x):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    c_func = gen_client_header.generate_func(xp)
    c_func = c_func.replace('hyperdex_client_WXYZ', '(*f)')
    c_func = ' '.join([c.strip() for c in c_func.split('\n')])
    c_func = c_func.strip('; ')
    return c_func

def generate_worker_declarations(xs):
    calls = set([])
    for x in xs:
        call = call_name(x)
        if call in calls:
            continue
        assert x.form in (generator.AsyncCall, generator.Iterator)
        c_func = generate_function_pointer(x)
        yield 'static v8::Handle<v8::Value> {0}({1}, const v8::Arguments& args);'.format(call, c_func)
        calls.add(call)

def generate_declaration(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    func = 'static v8::Handle<v8::Value> {0}(const v8::Arguments& args);'
    return func.format(x.name)

def generate_worker_definitions(xs):
    calls = set([])
    for x in xs:
        call = call_name(x)
        if call in calls:
            continue
        assert x.form in (generator.AsyncCall, generator.Iterator)
        c_func = generate_function_pointer(x)
        func  = 'v8::Handle<v8::Value>\nHyperDexClient :: '
        func += '{0}({1}, const v8::Arguments& args)\n'.format(call, c_func)
        func += '{\n'
        func += '    v8::HandleScope scope;\n'
        func += '    v8::Local<v8::Object> client_obj = args.This();\n'
        func += '    HyperDexClient* client = node::ObjectWrap::Unwrap<HyperDexClient>(client_obj);\n'
        func += '    e::intrusive_ptr<Operation> op(new Operation(client_obj, client));\n'
        for idx, arg in enumerate(x.args_in):
            for p, n in arg.args:
                func += '    ' + p + ' in_' + n + ';\n'
            args = ', '.join(['&in_' + n for p, n in arg.args])
            func += '    v8::Local<v8::Value> {0} = args[{1}];\n'.format(arg.__name__.lower(), idx)
            func += '    if (!op->convert_{0}({0}, {1})) return scope.Close(v8::Undefined());\n'.format(arg.__name__.lower(), args)
        func += '    v8::Local<v8::Function> func = args[{0}].As<v8::Function>();\n'.format(len(x.args_in))
        func += '\n    if (func.IsEmpty() || !func->IsFunction())\n'.format(len(x.args_in))
        func += '    {\n'
        func += '        v8::ThrowException(v8::String::New("Callback must be a function"));\n'
        func += '        return scope.Close(v8::Undefined());\n'
        func += '    }\n\n'
        if x.form == generator.AsyncCall:
            func += '    if (!op->set_callback(func, 2)) { return scope.Close(v8::Undefined()); }\n'
        if x.form == generator.Iterator:
            func += '    if (!op->set_callback(func, 3)) { return scope.Close(v8::Undefined()); }\n'
        func += '    op->reqid = f(client->client(), {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                                            ', '.join(['&op->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
        func += '    if (op->reqid < 0)\n    {\n'
        func += '        op->callback_error_from_status();\n'
        func += '        return scope.Close(v8::Undefined());\n'
        func += '    }\n\n'
        args = '_'.join([arg.__name__.lower() for arg in x.args_out])
        form = x.form.__name__.lower()
        func += '    op->encode_return = &Operation::encode_{0}_{1};\n'.format(form, args)
        func += '    client->add(op->reqid, op);\n'
        func += '    return scope.Close(v8::Undefined());\n'
        func += '}\n'
        yield func
        calls.add(call)

def generate_definition(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    func  = 'v8::Handle<v8::Value>\n'
    func += 'HyperDexClient :: ' + x.name
    func += '(const v8::Arguments& args)\n{\n'
    func += '    return {0}(hyperdex_client_{1}, args);\n'.format(call_name(x), x.name)
    func += '}\n'
    return func

def generate_prototype(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    return 'NODE_SET_PROTOTYPE_METHOD(tpl, "{0}", HyperDexClient::{0});'.format(x.name)

if __name__ == '__main__':
    with open(os.path.join(BASE, 'bindings/node.js/client.declarations.cc'), 'w') as fout:
        fout.write(copyright)
        fout.write('\n'.join(generate_worker_declarations(generator.Client)))
        fout.write('\n\n')
        fout.write('\n'.join([generate_declaration(c) for c in generator.Client]))
    with open(os.path.join(BASE, 'bindings/node.js/client.definitions.cc'), 'w') as fout:
        fout.write(copyright)
        fout.write('\n'.join(generate_worker_definitions(generator.Client)))
        fout.write('\n\n')
        fout.write('\n'.join([generate_definition(c) for c in generator.Client]))
    with open(os.path.join(BASE, 'bindings/node.js/client.prototypes.cc'), 'w') as fout:
        fout.write(copyright)
        fout.write('\n'.join([generate_prototype(c) for c in generator.Client]))
