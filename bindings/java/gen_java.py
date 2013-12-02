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

java_header = '''/* Copyright (c) 2013, Cornell University
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

package org.hyperdex.client;

import java.util.Map;
import java.util.HashMap;

public class Client
{
    static
    {
        System.loadLibrary("hyperdex-client-java");
        initialize();
    }

    private long ptr = 0;
    private Map<Long, Operation> ops = null;

    public Client(String host, Integer port)
    {
        create(host, port.intValue());
        this.ops = new HashMap<Long, Operation>();
    }

    public Client(String host, int port)
    {
        create(host, port);
        this.ops = new HashMap<Long, Operation>();
    }

    protected void finalize() throws Throwable
    {
        try
        {
            if (ptr != 0)
            {
                destroy();
            }
        }
        finally
        {
            super.finalize();
        }
    }

    /* utilities */
    public Operation loop()
    {
        long ret = inner_loop();
        Operation o = ops.get(ret);

        if (o != null)
        {
            o.callback();
        }

        return o;
    }

    /* cached IDs */
    private static native void initialize();
    private static native void terminate();
    /* ctor/dtor */
    private native void create(String host, int port);
    private native void destroy();
    /* utilities */
    private native long inner_loop();
    private void add_op(long l, Operation op)
    {
        ops.put(l, op);
    }
    private void remove_op(long l)
    {
        ops.remove(l);
    }
    /* operations */
'''

definitions_header = '''/* Copyright (c) 2013, Cornell University
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

def JTYPEOF(x):
    if x == generator.Attributes:
        return 'Map<String, Object>'
    elif x == generator.MapAttributes:
        return 'Map<String, Map<Object, Object>>'
    elif x == generator.Predicates:
        return 'Map<String, Object>'
    elif x == generator.SpaceName:
        return 'String'
    elif x == generator.SortBy:
        return 'String'
    elif x == generator.Limit:
        return 'int'
    elif x == generator.MaxMin:
        return 'boolean'
    else:
        return 'Object'

def CTYPEOF(x):
    if x == generator.SpaceName:
        return 'jstring'
    elif x == generator.SortBy:
        return 'jstring'
    elif x == generator.Limit:
        return 'jint'
    elif x== generator.MaxMin:
        return 'jboolean'
    else:
        return 'jobject'

def generate_prototype(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    args_list = ', '.join([JTYPEOF(arg) + ' ' + arg.__name__.lower() for arg in x.args_in])
    if x.form == generator.AsyncCall:
        if x.args_out == (generator.Status, generator.Attributes):
            ret = 'Map<String, Object>'
        elif x.args_out == (generator.Status, generator.Description):
            ret = 'String'
        elif x.args_out == (generator.Status, generator.Count):
            ret = 'Long'
        elif x.args_out == (generator.Status,):
            ret = 'Boolean'
        else:
            print x.args_out
            assert False
        proto = '    public native Deferred async_{0}({1}) throws HyperDexClientException;\n'.format(x.name, args_list)
        proto += '    public {1} {0}('.format(x.name, ret)
        proto += args_list
        proto += ') throws HyperDexClientException\n    {\n'
        args_list = ', '.join([arg.__name__.lower() for arg in x.args_in])
        proto += '        return ({2}) async_{0}({1}).waitForIt();\n'.format(x.name, args_list, ret)
        proto += '    }\n'
        return proto
    if x.form == generator.Iterator:
        return '    public native Iterator {0}({1});\n'.format(x.name, args_list)

def generate_worker_asynccall(call, x):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    c_func = gen_client_header.generate_func(xp)
    c_func = c_func.replace('hyperdex_client_WXYZ', '(*f)')
    c_func = ' '.join([c.strip() for c in c_func.split('\n')])
    c_func = c_func.strip('; ')
    func = 'JNIEXPORT jobject JNICALL\n'
    func += '_hyperdex_java_client_{0}(JNIEnv* env, jobject obj, {1}'.format(call, c_func)
    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')'
    func += ';\n\n' + func
    func += '\n{\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '    ' + p + ' in_' + n + ';\n'
    func += '    int success = 0;\n'
    func += '    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);\n'
    func += '    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);\n'
    func += '    struct hyperdex_java_client_deferred* d = NULL;\n'
    func += '    ERROR_CHECK(0);\n'
    func += '    d = hyperdex_get_deferred_ptr(env, dfrd);\n'
    func += '    ERROR_CHECK(0);\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    success = hyperdex_java_client_convert_{0}(env, obj, d->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
        func += '    if (success < 0) return 0;\n'
    func += '    d->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                             ', '.join(['&d->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (d->reqid < 0)\n    {\n'
    func += '        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));\n'
    func += '        return 0;\n'
    func += '    }\n\n'
    func += '    d->encode_return = hyperdex_java_client_deferred_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);\n'
    func += '    ERROR_CHECK(0);\n'
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
    func = 'JNIEXPORT jobject JNICALL\n'
    func += '_hyperdex_java_client_{0}(JNIEnv* env, jobject obj, {1}'.format(call, c_func)
    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')'
    func += ';\n\n' + func
    func += '\n{\n'
    for arg in x.args_in:
        for p, n in arg.args:
            func += '    ' + p + ' in_' + n + ';\n'
    func += '    int success = 0;\n'
    func += '    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);\n'
    func += '    jobject iter = (*env)->NewObject(env, _iterator, _iterator_init, obj);\n'
    func += '    struct hyperdex_java_client_iterator* it = NULL;\n'
    func += '    ERROR_CHECK(0);\n'
    func += '    it = hyperdex_get_iterator_ptr(env, iter);\n'
    func += '    ERROR_CHECK(0);\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    hyperdex_java_client_convert_{0}(env, obj, it->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
        func += '    if (success < 0) return 0;\n'
    func += '    it->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                             ', '.join(['&it->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (it->reqid < 0)\n    {\n'
    func += '        hyperdex_java_client_throw_exception(env, it->status, hyperdex_client_error_message(client));\n'
    func += '        return 0;\n'
    func += '    }\n\n'
    func += '    it->encode_return = hyperdex_java_client_iterator_encode_' + '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    (*env)->CallObjectMethod(env, obj, _client_add_op, it->reqid, iter);\n'
    func += '    ERROR_CHECK(0);\n'
    func += '    return iter;\n'
    func += '}\n'
    return func

def call_name(x):
    call  = x.form.__name__.lower()
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_in])
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_out])
    return call

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
    func = 'JNIEXPORT jobject JNICALL\n'
    if x.form == generator.AsyncCall:
        func += 'Java_org_hyperdex_client_Client_async_1{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    if x.form == generator.Iterator:
        func += 'Java_org_hyperdex_client_Client_{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    return _hyperdex_java_client_{0}(env, obj, hyperdex_client_{1}'.format(call_name(x), x.name)
    for arg in x.args_in:
        func += ', ' + arg.__name__.lower()
    func += ');\n}\n'
    return func

if __name__ == '__main__':
    with open(os.path.join(BASE, 'bindings/java/org/hyperdex/client/Client.java'), 'w') as fout:
        fout.write(java_header)
        fout.write('\n'.join([generate_prototype(c) for c in generator.Client]))
        fout.write('}\n')
        fout.flush();
    os.system('cd bindings/java && javac -cp . org/hyperdex/client/Client.java')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    with open(os.path.join(BASE, 'bindings/java/org_hyperdex_client_Client.definitions.c'), 'w') as fout:
        fout.write(definitions_header)
        fout.write('\n'.join(generate_workers(generator.Client)))
        fout.write('\n\n')
        fout.write('\n'.join([generate_definition(c) for c in generator.Client]))
