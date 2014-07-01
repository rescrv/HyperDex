# Copyright (c) 2014, United Networks
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

import itertools

import bindings
import bindings.c
import bindings.go


def GOTYPEOF(x):
    if x == bindings.SpaceName:
        return 'string'
    elif x == bindings.Key:
        return 'Value'
    elif x == bindings.Attributes:
        return 'Attributes'
    elif x == bindings.MapAttributes:
        return 'MapAttributes'
    elif x == bindings.AttributeNames:
        return 'AttributeNames'
    elif x == bindings.Predicates:
        return '[]Predicate'
    elif x == bindings.SortBy:
        return 'string'
    elif x == bindings.Limit:
        return 'uint64'
    elif x == bindings.MaxMin:
        return 'string'
    print x
    assert False

def arg_name(a):
    return a.__name__.lower()

def return_type_async(args_out):
    if args_out == (bindings.Status,):
        return 'err Error'
    elif args_out == (bindings.Status, bindings.Attributes):
        return 'attrs Attributes, err Error'
    elif args_out == (bindings.Status, bindings.Description):
        return 'desc string, err Error'
    elif args_out == (bindings.Status, bindings.Count):
        return 'count uint64, err Error'
    print args_out
    assert False

def return_value_async(args_out):
    if args_out == (bindings.Status,):
        ret  = '\treturn\n'
        return ret
    elif args_out == (bindings.Status, bindings.Attributes):
        ret  = '\tif c_status == SUCCESS {\n'
        ret += '\t\tvar er error\n'
        ret += '\t\tattrs, er = client.buildAttributes(c_attrs, c_attrs_sz)\n'
        ret += '\t\tif er != nil {\n'
        ret += '\t\t\terr = Error{Status(SERVERERROR), er.Error(), ""}\n'
        ret += '\t\t}\n'
        ret += '\t\tC.hyperdex_client_destroy_attrs(c_attrs, c_attrs_sz)\n'
        ret += '\t}\n'
        ret += '\treturn\n'
        return ret
    elif args_out == (bindings.Status, bindings.Description):
        ret  = '\tif c_status == SUCCESS {\n'
        ret += '\t\tdesc = C.GoString(c_description)\n'
        ret += '\t}\n'
        ret += '\treturn\n'
        return ret
    elif args_out == (bindings.Status, bindings.Count):
        ret  = '\tif c_status == SUCCESS {\n'
        ret += '\t\tcount = uint64(c_count)\n'
        ret += '\t}\n'
        ret += '\treturn\n'
        return ret
    print args_out
    assert False

def return_type_iter(args_out):
    if args_out == (bindings.Status, bindings.Attributes):
        return 'attrs chan Attributes, errs chan Error'
    print args_out
    assert False

def return_value_iter(args_out):
    if args_out == (bindings.Status, bindings.Attributes):
        return '\treturn\n'
    print args_out
    assert False

def GoIfy(sym):
    return ''.join([x.capitalize() for x in sym.split('_')])

def CGoIfy(sym):
    x = 'C.' + sym.replace(' ', '_')
    while x.endswith('*'):
        x = '*' + x[:-1]
    x = x.replace('const_', '')
    return x

def generate_stub_args(x, prefix='', in_prefix=None, out_prefix=None):
    in_prefix = in_prefix or prefix
    out_prefix = out_prefix or prefix
    stub_args = []
    stub_args_list = []
    for arg in x.args_in:
        for p, n in arg.args:
            stub_args.append('{2}{0} {1}'.format(n, CGoIfy(p), in_prefix))
            stub_args_list.append(in_prefix + n)
    for arg in x.args_out:
        for p, n in arg.args:
            stub_args.append('{2}{0} *{1}'.format(n, CGoIfy(p), out_prefix))
            stub_args_list.append(out_prefix + n)
    stub_args = ', '.join(stub_args)
    stub_args_list = ', '.join(stub_args_list)
    return stub_args, stub_args_list

def generate_worker_asynccall(call, x):
    typed_args_in = ', '.join([(arg_name(arg) + ' ' + GOTYPEOF(arg)).strip()
                               for arg in x.args_in])
    typed_args_out = return_type_async(x.args_out)
    stub_args, stub_args_list = generate_stub_args(x, prefix='c_')
    func  = 'func (client *Client) {0}(stub func(client *C.struct_hyperdex_client, {3}) int64, {1}) ({2}) {{\n'.format(GoIfy(call), typed_args_in, typed_args_out, stub_args)
    func += '\tarena := C.hyperdex_ds_arena_create()\n'
    func += '\tdefer C.hyperdex_ds_arena_destroy(arena)\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '\tvar c_{0} {1}\n'.format(n, CGoIfy(p))
    for arg in x.args_in:
        args = ', '.join(['&c_' + n for p, n in arg.args])
        func += '\tclient.convert{0}(arena, {1}, {2})\n'.format(GoIfy(arg.__name__), arg_name(arg), args)
    for arg in x.args_out:
        for p, n in arg.args:
            func += '\tvar c_{0} {1}\n'.format(n, CGoIfy(p))
    stub_args, stub_args_list = generate_stub_args(x, in_prefix='c_', out_prefix='&c_')
    func += '\tdone := make(chan Error)\n'
    func += '\tclient.mutex.Lock()\n'
    func += '\treqid := stub(client.ptr, {0})\n'.format(stub_args_list)
    func += '\tif reqid >= 0 {\n'
    func += '\t\tclient.ops[reqid] = done\n'
    func += '\t} else {\n'
    func += '\t\terr = Error{Status(c_status),\n'
    func += '\t\t            C.GoString(C.hyperdex_client_error_message(client.ptr)),\n'
    func += '\t\t            C.GoString(C.hyperdex_client_error_location(client.ptr))}\n'
    func += '\t}\n'
    func += '\tclient.mutex.Unlock()\n'
    func += '\tif reqid >= 0 {\n'
    func += '\t\terr = <-done\n'
    func += '\t\terr.Status = Status(c_status)\n'
    func += '\t}\n'
    func += return_value_async(x.args_out)
    func += '}\n'
    return func

def generate_worker_iterator(call, x):
    typed_args_in = ', '.join([(arg_name(arg) + ' ' + GOTYPEOF(arg)).strip()
                               for arg in x.args_in])
    typed_args_out = return_type_iter(x.args_out)
    stub_args, stub_args_list = generate_stub_args(x, prefix='c_')
    func  = 'func (client *Client) {0}(stub func(client *C.struct_hyperdex_client, {3}) int64, {1}) ({2}) {{\n'.format(GoIfy(call), typed_args_in, typed_args_out, stub_args)
    func += '\tarena := C.hyperdex_ds_arena_create()\n'
    func += '\tdefer C.hyperdex_ds_arena_destroy(arena)\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '\tvar c_{0} {1}\n'.format(n, CGoIfy(p))
    for arg in x.args_in:
        args = ', '.join(['&c_' + n for p, n in arg.args])
        func += '\tclient.convert{0}(arena, {1}, {2})\n'.format(GoIfy(arg.__name__), arg_name(arg), args)
    func += '\tvar c_iter cIterator\n'
    func += '\tc_iter = cIterator{C.HYPERDEX_CLIENT_GARBAGE, nil, 0, make(chan Attributes, 10), make(chan Error, 10)}\n'
    func += '\tattrs = c_iter.attrChan\n'
    func += '\terrs = c_iter.errChan\n'
    stub_args, stub_args_list = generate_stub_args(x, in_prefix='c_', out_prefix='&c_iter.')
    func += '\tvar err Error\n'
    func += '\tclient.mutex.Lock()\n'
    func += '\treqid := stub(client.ptr, {0})\n'.format(stub_args_list)
    func += '\tif reqid >= 0 {\n'
    func += '\t\tclient.searches[reqid] = &c_iter\n'
    func += '\t} else {\n'
    func += '\t\terr = Error{Status(c_iter.status),\n'
    func += '\t\t            C.GoString(C.hyperdex_client_error_message(client.ptr)),\n'
    func += '\t\t            C.GoString(C.hyperdex_client_error_location(client.ptr))}\n'
    func += '\t}\n'
    func += '\tclient.mutex.Unlock()\n'
    func += '\tif reqid < 0 {\n'
    func += '\t\terrs<-err\n'
    func += '\t\tclose(attrs)\n'
    func += '\t\tclose(errs)\n'
    func += '\t}\n'
    func += return_value_iter(x.args_out)
    func += '}\n'
    return func

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

def generate_method(x, lib):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    typed_args_in = ', '.join([(arg_name(arg) + ' ' + GOTYPEOF(arg)).strip()
                               for arg in x.args_in])
    if x.form == bindings.AsyncCall:
        typed_args_out = return_type_async(x.args_out)
    if x.form == bindings.Iterator:
        typed_args_out = return_type_iter(x.args_out)
    arg_list = ', '.join([arg_name(arg) for arg in x.args_in])
    stub_args, stub_args_list = generate_stub_args(x)
    meth  = 'func stub_{0}(client *C.struct_hyperdex_client, {1}) int64 {{\n'.format(x.name, stub_args)
    meth += '\treturn int64(C.hyperdex_client_{0}(client, {1}))\n'.format(x.name, stub_args_list)
    meth += '}\n'
    meth += 'func (client *Client) {0}({1}) ({2}) {{\n'.format(GoIfy(x.name), typed_args_in, typed_args_out)
    meth += '\treturn client.{0}(stub_{1}, {2})\n'.format(GoIfy(bindings.call_name(x)), x.name, arg_list)
    meth += '}\n'
    return meth

def generate_client_code():
    code = ''
    code += '\n'.join(generate_workers(bindings.Client))
    code += '\n'
    code += '\n'.join([generate_method(c, 'Client') for c in bindings.Client])
    code += '\n'
    goclient = os.path.join(BASE, 'bindings/go/client.go')
    current = open(goclient, 'r').read()
    START = '// Begin Automatically Generated Code\n'
    END   = '// End Automatically Generated Code\n'
    head, tail = current.split(START)
    dead, tail = tail.split(END)
    fout = open(goclient, 'w')
    fout.write(head + START + code + END + tail)

if __name__ == '__main__':
    generate_client_code()
