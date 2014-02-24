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
import bindings.java

DOCS_IN = {(bindings.AsyncCall, bindings.SpaceName): 'The name of the space as a string.'
          ,(bindings.AsyncCall, bindings.Key): 'The key for the operation as a Java object'
          ,(bindings.AsyncCall, bindings.Attributes): 'A map specifying attributes '
           'to modify and their respective values.'
          ,(bindings.AsyncCall, bindings.MapAttributes): 'A map specifying map '
           'attributes to modify and their respective key/values.'
          ,(bindings.AsyncCall, bindings.Predicates): 'A map of predicates '
           'to check against.'
          ,(bindings.Iterator, bindings.SpaceName): 'The name of the space as string.'
          ,(bindings.Iterator, bindings.SortBy): 'The attribute to sort by.'
          ,(bindings.Iterator, bindings.Limit): 'The number of results to return.'
          ,(bindings.Iterator, bindings.MaxMin): 'Maximize or minimize (e.g., '
          '"max", "min").'
          ,(bindings.Iterator, bindings.Predicates): 'A map of predicates '
           'to check against.'
          }

def JTYPEOF(x):
    if x == bindings.Attributes:
        return 'Map<String, Object>'
    elif x == bindings.MapAttributes:
        return 'Map<String, Map<Object, Object>>'
    elif x == bindings.Predicates:
        return 'Map<String, Object>'
    elif x == bindings.SpaceName:
        return 'String'
    elif x == bindings.SortBy:
        return 'String'
    elif x == bindings.Limit:
        return 'int'
    elif x == bindings.MaxMin:
        return 'boolean'
    else:
        return 'Object'

def CTYPEOF(x):
    if x == bindings.SpaceName:
        return 'jstring'
    elif x == bindings.SortBy:
        return 'jstring'
    elif x == bindings.Limit:
        return 'jint'
    elif x== bindings.MaxMin:
        return 'jboolean'
    else:
        return 'jobject'

def generate_prototype(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    args_list = ', '.join([JTYPEOF(arg) + ' ' + arg.__name__.lower() for arg in x.args_in])
    if x.form == bindings.AsyncCall:
        if x.args_out == (bindings.Status, bindings.Attributes):
            ret = 'Map<String, Object>'
        elif x.args_out == (bindings.Status, bindings.Description):
            ret = 'String'
        elif x.args_out == (bindings.Status, bindings.Count):
            ret = 'Long'
        elif x.args_out == (bindings.Status,):
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
    if x.form == bindings.Iterator:
        return '    public native Iterator {0}({1});\n'.format(x.name, args_list)

def generate_worker(call, x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    if x.form == bindings.AsyncCall:
        cls = 'deferred'
    if x.form == bindings.Iterator:
        cls = 'iterator'
    fptr = bindings.c.generate_func_ptr(x)
    func = 'JNIEXPORT HYPERDEX_API jobject JNICALL\n'
    func += 'hyperdex_java_client_{0}(JNIEnv* env, jobject obj, {1}'.format(call, fptr)
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
    func += '    jobject op = (*env)->NewObject(env, _{0}, _{0}_init, obj);\n'.format(cls)
    func += '    struct hyperdex_java_client_{0}* o = NULL;\n'.format(cls)
    func += '    ERROR_CHECK(0);\n'
    func += '    o = hyperdex_get_{0}_ptr(env, op);\n'.format(cls)
    func += '    ERROR_CHECK(0);\n'
    for arg in x.args_in:
        args = ', '.join(['&in_' + n for p, n in arg.args])
        func += '    success = hyperdex_java_client_convert_{0}(env, obj, o->arena, {0}, {1});\n'.format(arg.__name__.lower(), args)
        func += '    if (success < 0) return 0;\n'
    func += '    o->reqid = f(client, {0}, {1});\n\n'.format(', '.join(['in_' + n for p, n in sum([list(a.args) for a in x.args_in], [])]),
                                                             ', '.join(['&o->' + n for p, n in sum([list(a.args) for a in x.args_out], [])]))
    func += '    if (o->reqid < 0)\n    {\n'
    func += '        hyperdex_java_client_throw_exception(env, o->status, hyperdex_client_error_message(client));\n'
    func += '        return 0;\n'
    func += '    }\n\n'
    func += '    o->encode_return = hyperdex_java_client_{0}_encode_'.format(cls)
    func += '_'.join([arg.__name__.lower() for arg in x.args_out]) + ';\n'
    func += '    (*env)->CallObjectMethod(env, obj, _client_add_op, o->reqid, op);\n'
    func += '    ERROR_CHECK(0);\n'
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
    func = 'JNIEXPORT HYPERDEX_API jobject JNICALL\n'
    if x.form == bindings.AsyncCall:
        func += 'Java_org_hyperdex_client_Client_async_1{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    if x.form == bindings.Iterator:
        func += 'Java_org_hyperdex_client_Client_{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    return hyperdex_java_client_{0}(env, obj, hyperdex_client_{1}'.format(bindings.call_name(x), x.name)
    for arg in x.args_in:
        func += ', ' + arg.__name__.lower()
    func += ');\n}\n'
    return func

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
    block += '\\label{{api:java:{0}}}\n'.format(x.name)
    block += '\\index{{{0}!Java API}}\n'.format(bindings.LaTeX(x.name))
    block += '\\begin{javacode}\n'
    block += generate_api_func(x, cls=cls)
    block += '\\end{javacode}\n'
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
        block += 'Object if found, null if not found.  Raises exception on error.'
    elif x.args_out == (bindings.Status, bindings.Count):
        block += 'Number of objects found.  Raises exception on error.'
    elif x.args_out == (bindings.Status, bindings.Description):
        block += 'Description of search.  Raises exception on error.'
    else:
        assert False
    block += '\n'
    return block

def generate_client_java():
    fout = open(os.path.join(BASE, 'bindings/java/org/hyperdex/client/Client.java'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write(bindings.java.JAVA_HEAD)
    fout.write('\n'.join([generate_prototype(c) for c in bindings.Client]))
    fout.write('}\n')
    fout.flush()
    os.system('cd bindings/java && javac -cp . org/hyperdex/client/Client.java')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Deferred')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.GreaterEqual')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.GreaterThan')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Iterator')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.LengthEquals')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.LengthGreaterEqual')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.LengthLessEqual')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.LessEqual')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.LessThan')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Range')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Regex')
    os.system('cd bindings/java && sed -i -e "s/JNIEXPORT/JNIEXPORT HYPERDEX_API/" *.h')
    fout = open(os.path.join(BASE, 'bindings/java/org_hyperdex_client_Client.definitions.c'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write(bindings.java.DEFINITIONS_HEAD)
    fout.write('\n'.join(generate_workers(bindings.Client)))
    fout.write('\n'.join([generate_definition(c) for c in bindings.Client]))

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/api/java.client.tex'), 'w')
    fout.write(bindings.copyright('%', '2014'))
    fout.write('\n% This LaTeX file is generated by bindings/java.py\n\n')
    fout.write('\n'.join([generate_api_block(c) for c in bindings.Client]))

if __name__ == '__main__':
    generate_client_java()
    generate_client_doc()

JAVA_HEAD = '''
/* This file is generated by bindings/java.py */

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
        _create(host, port.intValue());
        this.ops = new HashMap<Long, Operation>();
    }

    public Client(String host, int port)
    {
        _create(host, port);
        this.ops = new HashMap<Long, Operation>();
    }

    protected void finalize() throws Throwable
    {
        try
        {
            destroy();
        }
        finally
        {
            super.finalize();
        }
    }

    public synchronized void destroy()
    {
        if (ptr != 0)
        {
            _destroy();
            ptr = 0;
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
    private native void _create(String host, int port);
    private native void _destroy();
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

DEFINITIONS_HEAD = '''
/* This file is generated by bindings/java.py */

#include "visibility.h"

'''
