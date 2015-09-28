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
import bindings.nodejs

def generate_worker_declarations(xs, lib):
    calls = set([])
    for x in xs:
        call = bindings.call_name(x)
        if call in calls:
            continue
        if x.form is bindings.MicrotransactionCall:
            continue #TODO support microtransactions in NodeJS

        assert x.form in (bindings.AsyncCall, bindings.Iterator)
        fptr = bindings.c.generate_func_ptr(x, lib)
        yield 'static v8::Handle<v8::Value> {0}({1}, const v8::Arguments& args);'.format(call, fptr)
        calls.add(call)

def generate_declaration(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = 'static v8::Handle<v8::Value> {0}(const v8::Arguments& args);'
    return func.format(x.name)

# 20 Sept 2015 - William Whitacre
# Patch against 1.8.1: Enable macaroon authorization.
def generate_worker_definitions(xs, lib):
    calls = set([])
    for x in xs:
        call = bindings.call_name(x)
        if call in calls:
            continue
        if x.form is bindings.MicrotransactionCall:
            continue #TODO support microtransactions in NodeJS
        assert x.form in (bindings.AsyncCall, bindings.Iterator)
        fptr = bindings.c.generate_func_ptr(x, lib)
        func  = 'v8::Handle<v8::Value>\nHyperDexClient :: '
        func += '{0}({1}, const v8::Arguments& args)\n'.format(call, fptr)
        func += '{\n'
        func += '    v8::HandleScope scope;\n'
        func += '    v8::Local<v8::Object> client_obj = args.This();\n'
        func += '    HyperDexClient* client = node::ObjectWrap::Unwrap<HyperDexClient>(client_obj);\n'
        func += '    e::intrusive_ptr<Operation> op(new Operation(client_obj, client));\n'
        wrap_auth_context, arg_count = False, len(x.args_in)
        if x.form == bindings.AsyncCall:
            func += '    const size_t base_args_sz = {0};\n'.format(arg_count)
            func += '    const bool bDoAuth = ((size_t)args.Length() > base_args_sz + 1);\n'
            func += '    const size_t i_Func = bDoAuth ? base_args_sz + 1 : base_args_sz;\n'

            func += '    v8::Local<v8::Function> func = args[i_Func].As<v8::Function>();\n'
            func += '\n    if (func.IsEmpty() || !func->IsFunction())\n'
            func += '    {\n'
            func += '        v8::ThrowException(v8::String::New("Callback must be a function"));\n'
            func += '        return scope.Close(v8::Undefined());\n'
            func += '    }\n\n'
            func += '    if (!op->set_callback(func)) { return scope.Close(v8::Undefined()); }\n\n'

            wrap_auth_context = True
        if x.form == bindings.Iterator:
            func += '    v8::Local<v8::Function> func = args[{0}].As<v8::Function>();\n'.format(len(x.args_in))
            func += '\n    if (func.IsEmpty() || !func->IsFunction())\n'
            func += '    {\n'
            func += '        v8::ThrowException(v8::String::New("Callback must be a function"));\n'
            func += '        return scope.Close(v8::Undefined());\n'
            func += '    }\n\n'
            func += '    v8::Local<v8::Function> done = args[{0}].As<v8::Function>();\n'.format(len(x.args_in) + 1)
            func += '\n    if (done.IsEmpty() || !done->IsFunction())\n'.format(len(x.args_in))
            func += '    {\n'
            func += '        v8::ThrowException(v8::String::New("Callback must be a function"));\n'
            func += '        return scope.Close(v8::Undefined());\n'
            func += '    }\n\n'
            func += '    if (!op->set_callback(func, done)) { return scope.Close(v8::Undefined()); }\n'

            wrap_auth_context = False

        for idx, arg in enumerate(x.args_in):
            for p, n in arg.args:
                func += '    ' + p + ' in_' + n + ';\n'
            args = ', '.join(['&in_' + n for p, n in arg.args])
            func += '    v8::Local<v8::Value> {0} = args[{1}];\n'.format(arg.__name__.lower(), idx)
            func += '    if (!op->convert_{0}({0}, {1})) return scope.Close(v8::Undefined());\n'.format(arg.__name__.lower(), args)

        if wrap_auth_context:
            func += '    if (bDoAuth)\n    {\n'
            func += '        v8::Handle<v8::Value> M = args[base_args_sz];\n'
            func += '        if (!op->set_auth_context(M)) { return scope.Close(v8::Undefined()); }\n    }\n\n'

        func += '    op->reqid = f(client->client(), {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                                            ', '.join(['&op->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))

        if wrap_auth_context:
            func += '    if (bDoAuth) op->clear_auth_context();\n'

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
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func  = 'v8::Handle<v8::Value>\n'
    func += 'HyperDexClient :: ' + x.name
    func += '(const v8::Arguments& args)\n{\n'
    func += '    return {0}(hyperdex_client_{1}, args);\n'.format(bindings.call_name(x), x.name)
    func += '}\n'
    return func

def generate_prototype(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    return 'NODE_SET_PROTOTYPE_METHOD(tpl, "{0}", HyperDexClient::{0});'.format(x.name)

def generate_api_func(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = '%s(' % x.name
    padd = ' ' * 16
    func += ', '.join([str(arg).lower()[17:-2] for arg in x.args_in])
    if x.args_out == (bindings.Status,):
        func += ', function (success, err) {}'
    elif x.form == bindings.AsyncCall and x.args_out == (bindings.Status, bindings.Attributes):
        func += ', function (obj, done, err) {}'
    elif x.form == bindings.Iterator and x.args_out == (bindings.Status, bindings.Attributes):
        func += ', function (obj, err) {}'
    elif x.args_out == (bindings.Status, bindings.Description):
        func += ', function (desc, err) {}'
    elif x.args_out == (bindings.Status, bindings.Count):
        func += ', function (count, err) {}'
    else:
        print x.args_out
        assert False
    func += ')\n'
    if len(func) > 85:
        funcx, funcy = func.split('(', 1)
        funcz = ''
        for x in funcy.split(', '):
            if funcz.count('(') + 1 == funcz.count(')'):
                if funcz:
                    funcz += ',\n' + ' ' * 8
            elif funcz:
                funcz += ', '
            funcz += x
        func = funcx + '(\n' + ' ' * 8 + funcz
    return func

def generate_api_block(x, lib):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsubsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:nodejs:%s}\n' % x.name
    block += '\\index{%s!Node.js API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{javascriptcode}\n'
    block += generate_api_func(x)
    block += '\\end{javascriptcode}\n'
    block += '\\paragraph{Parameters:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_in, 'node.js/' + lib + '/fragments/in',
                                         label_maker=bindings.parameters_script_style)
    block += '\n\\paragraph{Returns:}\n'
    frag  = 'node.js/' + lib + '/fragments/return_' + x.form.__name__.lower()
    frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
    block += '\\input{\\topdir/' + frag + '}\n'
    return block

def generate_client_declarations():
    fout = open(os.path.join(BASE, 'bindings/node.js/client.declarations.cc'), 'w')
    fout.write(bindings.copyright('/', '2014'))
    fout.write('\n// This file is generated by bindings/nodejs.py\n\n')
    fout.write('#ifdef HYPERDEX_NODE_INCLUDED_CLIENT_CC\n\n')
    fout.write('\n'.join(generate_worker_declarations(bindings.Client, 'client')))
    fout.write('\n\n')
    fout.write('\n'.join([generate_declaration(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))
    fout.write('\n\n#endif // HYPERDEX_NODE_INCLUDED_CLIENT_CC\n')

def generate_client_definitions():
    fout = open(os.path.join(BASE, 'bindings/node.js/client.definitions.cc'), 'w')
    fout.write(bindings.copyright('/', '2014'))
    fout.write('\n// This file is generated by bindings/nodejs.py\n\n')
    fout.write('#ifdef HYPERDEX_NODE_INCLUDED_CLIENT_CC\n\n')
    fout.write('\n'.join(generate_worker_definitions(bindings.Client, 'client')))
    fout.write('\n\n')
    fout.write('\n'.join([generate_definition(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))
    fout.write('\n#endif // HYPERDEX_NODE_INCLUDED_CLIENT_CC\n')

def generate_client_prototypes():
    fout = open(os.path.join(BASE, 'bindings/node.js/client.prototypes.cc'), 'w')
    fout.write(bindings.copyright('/', '2014'))
    fout.write('\n// This file is generated by bindings/nodejs.py\n\n')
    fout.write('#ifdef HYPERDEX_NODE_INCLUDED_CLIENT_CC\n\n')
    fout.write('\n'.join([generate_prototype(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))
    fout.write('\n\n#endif // HYPERDEX_NODE_INCLUDED_CLIENT_CC\n')

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/node.js/client/ops.tex'), 'w')
    fout.write(bindings.copyright('%', '2014'))
    fout.write('\n% This LaTeX file is generated by bindings/nodejs.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'client') for c in bindings.Client
                          if c.name not in bindings.DoNotDocument and c.form is not bindings.MicrotransactionCall]))

if __name__ == '__main__':
    generate_client_declarations()
    generate_client_definitions()
    generate_client_prototypes()
    generate_client_doc()
