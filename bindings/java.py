# Copyright (c) 2013-2015, Cornell University
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
import bindings.java

def JTYPEOF(x):
    if x == bindings.Attributes:
        return 'Map<String, Object>'
    elif x == bindings.MapAttributes:
        return 'Map<String, Map<Object, Object>>'
    elif x == bindings.AttributeNames:
        return 'List<String>'
    elif x == bindings.Predicates:
        return 'Map<String, Object>'
    elif x in (bindings.SpaceName, bindings.SortBy):
        return 'String'
    elif x in (bindings.Limit, bindings.Count):
        return 'int'
    elif x == bindings.MaxMin:
        return 'boolean'
    elif x == bindings.Microtransaction:
        return 'Microtransaction'
    elif x in (bindings.Status, bindings.Key, bindings.Description):
        return 'Object'
    else:
        raise RuntimeError('Unknown type: ' + str(x))

def CTYPEOF(x):
    if x in (bindings.SpaceName, bindings.SortBy):
        return 'jstring'
    elif x == bindings.Limit:
        return 'jint'
    elif x == bindings.MaxMin:
        return 'jboolean'
    elif x in (bindings.AttributeNames,  bindings.Key, bindings.Attributes, bindings.MapAttributes,
               bindings.Predicates, bindings.Status, bindings.Description, bindings.Microtransaction):
        return 'jobject'
    else:
        raise RuntimeError('Unknown type: ' + str(x))

def generate_microtransaction_prototype(x):
    return '    public native int {0}(Map<String,Object> attrs) throws HyperDexClientException;\n'.format(x.name.replace("uxact_", ""))

def generate_client_prototype(x):
    args_list = ', '.join([JTYPEOF(arg) + ' ' + arg.__name__.lower() for arg in x.args_in])
    if x.form is bindings.AsyncCall:
        if x.args_out == (bindings.Status, bindings.Attributes):
            ret = 'Map<String, Object>'
        elif x.args_out == (bindings.Status, bindings.Description):
            ret = 'String'
        elif x.args_out == (bindings.Status, bindings.Count):
            ret = 'Long'
        elif x.args_out == (bindings.Status,):
            ret = 'Boolean'
        else:
            raise RuntimeError('Cannot handle args: ' + str(x.args_out))
            
        proto = '    public native Deferred async_{0}({1}) throws HyperDexClientException;\n'.format(x.name, args_list)
        proto += '    public {1} {0}('.format(x.name, ret)
        proto += args_list
        proto += ') throws HyperDexClientException\n    {\n'
        args_list = ', '.join([arg.__name__.lower() for arg in x.args_in])
        proto += '        return ({2}) async_{0}({1}).waitForIt();\n'.format(x.name, args_list, ret)
        proto += '    }\n'
        return proto
    elif x.form is bindings.Iterator:
        return '    public native Iterator {0}({1});\n'.format(x.name, args_list)
    else:
        raise RuntimeError('Unknown function type: ' + str(x.form))

def generate_worker(call, x):
    if x.form == bindings.AsyncCall:
        cls = 'deferred'
    elif x.form == bindings.Iterator:
        cls = 'iterator'
    else:
        raise RuntimeError('Unknown function type: ' + str(x.form))
        
    fptr = bindings.c.generate_func_ptr(x, 'client')
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
        if x.form is bindings.MicrotransactionCall:
            continue
        yield generate_worker(call, x)
        calls.add(call)
        
def generate_microtransaction_definition(x):
    func = 'JNIEXPORT HYPERDEX_API jint JNICALL\n'
    func += 'Java_org_hyperdex_client_Microtransaction_{0}(JNIEnv* env '.format(x.name.replace('uxact_', '').replace('_', '_1'))

    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')\n{\n'
    
    # Function Body 
    func += '    jobject client;\n'
    func += '    struct hyperdex_client* client_ptr;\n'
    func += '    struct hyperdex_client_microtransaction* uxact;\n'
    func += '    const struct hyperdex_client_attribute *_attrs;\n'
    func += '    struct hyperdex_ds_arena *arena;\n'
    func += '    size_t _attrs_sz;\n\n'
    func += '    client = hyperdex_uxact_get_client(env, microtransaction);\n'
    func += '    client_ptr = hyperdex_uxact_get_client_ptr(env, microtransaction);\n'
    func += '    uxact = hyperdex_uxact_get_uxact_ptr(env, microtransaction);\n'
    func += '    arena = hyperdex_uxact_get_arena_ptr(env, microtransaction);\n'
    func += '    hyperdex_java_client_convert_attributes(env, client, arena, attributes, &_attrs, &_attrs_sz);\n'
    func += '    return (jint)hyperdex_client_{0}(client_ptr, uxact, _attrs, _attrs_sz);\n'.format(x.name)

    func += '}\n'
    return func

def generate_client_definition(x):
    func = 'JNIEXPORT HYPERDEX_API jobject JNICALL\n'
    if x.form == bindings.AsyncCall:
        func += 'Java_org_hyperdex_client_Client_async_1{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    elif x.form == bindings.Iterator:
        func += 'Java_org_hyperdex_client_Client_{0}(JNIEnv* env, jobject obj'.format(x.name.replace('_', '_1'))
    else:
        raise RuntimeError('Unknown function type ' + str(x.form))
        
    for arg in x.args_in:
        func += ', ' + CTYPEOF(arg) + ' ' + arg.__name__.lower()
    func += ')\n{\n'
    func += '    return hyperdex_java_client_{0}(env, obj, hyperdex_client_{1}'.format(bindings.call_name(x), x.name)
    for arg in x.args_in:
        func += ', ' + arg.__name__.lower()
    func += ');\n}\n'
    return func

def generate_api_func_cleanup(func):
    func = func.strip().strip(';')
    func = func.replace('native ', '')
    if len(func) > 85:
        funcx, funcy = func.split('(', 1)
        funcz = ''
        for x in funcy.split(', '):
            if funcz.count('<') == funcz.count('>'):
                if funcz:
                    funcz += ',\n' + ' ' * 8
            elif funcz:
                funcz += ', '
            funcz += x
        func = funcx + '(\n' + ' ' * 8 + funcz
    return func + '\n'

def generate_api_norm_func(func):
    xs = generate_client_prototype(func)
    if xs is '':
        return ''
    
    xs = [x.strip() for x in xs.split('\n')]
    xs = [x for x in xs if x.startswith('public')]
    xs = [x for x in xs if 'async_' not in x]
    assert len(xs) == 1
    xs = xs[0]
    return generate_api_func_cleanup(xs)

def generate_api_async_func(func):
    xs = generate_client_prototype(func)
    xs = [x.strip() for x in xs.split('\n')]
    xs = [x for x in xs if x.startswith('public')]
    xs = [x for x in xs if 'async_' in x]
    assert len(xs) == 1
    xs = xs[0]
    return generate_api_func_cleanup(xs)

def generate_api_block(x, lib):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsubsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:java:%s}\n' % x.name
    block += '\\index{%s!Java API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{javacode}\n'
    block += generate_api_norm_func(x)
    block += '\\end{javacode}\n\n'
    block += '\\paragraph{Parameters:}\n'
    def java_label(arg):
        return '\\code{' + bindings.LaTeX(JTYPEOF(arg) + ' ' + arg.__name__.lower()) + '}'
    block += bindings.doc_parameter_list(x.form, x.args_in, 'java/' + lib + '/fragments/in',
                                         label_maker=java_label)
    block += '\n\\paragraph{Returns:}\n'
    frag  = 'java/' + lib + '/fragments/return_' + x.form.__name__.lower()
    frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
    block += '\\input{\\topdir/' + frag + '}\n'
    if x.form == bindings.AsyncCall:
        block += '\n\\pagebreak\n'
        block += '\\subsubsection{\code{async\\_%s}}\n' % bindings.LaTeX(x.name)
        block += '\\label{api:java:async_%s}\n' % x.name
        block += '\\index{async\_%s!Java API}\n' % bindings.LaTeX(x.name)
        block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
        block += '\\paragraph{Definition:}\n'
        block += '\\begin{javacode}\n'
        block += generate_api_async_func(x)
        block += '\\end{javacode}\n\n'
        block += '\\paragraph{Parameters:}\n'
        block += bindings.doc_parameter_list(x.form, x.args_in, 'java/' + lib + '/fragments/in',
                                             label_maker=java_label)
        block += '\n\\paragraph{Returns:}\n'
        frag  = 'java/' + lib + '/fragments/return_async_' + x.form.__name__.lower()
        frag += '__' + '_'.join([a.__name__.lower() for a in x.args_out])
        block += '\\input{\\topdir/' + frag + '}\n'
        block += '\n\\paragraph{See also:}  This is the asynchronous form of '
        block += '\code{%s}.\n' % bindings.LaTeX(x.name)
    return block

def generate_client_java():
    fout = open(os.path.join(BASE, 'bindings/java/org/hyperdex/client/Client.java'), 'w')
    fout.write(bindings.copyright('*', '2013-2015'))
    fout.write(bindings.java.JAVA_HEAD)
    fout.write('\n'.join([generate_client_prototype(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))
    fout.write('}\n')
    fout.flush()
    #fout = open(os.path.join(BASE, 'bindings/java/org/hyperdex/client/Microtransaction.java'), 'w')
    #fout.write(bindings.copyright('*', '2015'))
    #fout.write(bindings.java.MICROTRANSACTION_HEAD)
    #fout.write('\n'.join([generate_microtransaction_prototype(c) for c in bindings.Client if c.form is bindings.MicrotransactionCall]))
    #fout.write('}\n')
    #fout.flush()
    os.system('cd bindings/java && javac -cp . org/hyperdex/client/Client.java')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    os.system('cd bindings/java && javah -cp . org.hyperdex.client.Client')
    #os.system('cd bindings/java && javac -cp . org/hyperdex/client/Microtransaction.java')
    #os.system('cd bindings/java && javah -cp . org.hyperdex.client.Microtransaction')
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
    fout.write(bindings.copyright('*', '2013-2015'))
    fout.write(bindings.java.DEFINITIONS_HEAD)
    fout.write('\n'.join(generate_workers(bindings.Client)))
    fout.write('\n'.join([generate_client_definition(c) for c in bindings.Client if c.form is not bindings.MicrotransactionCall]))
    #fout = open(os.path.join(BASE, 'bindings/java/org_hyperdex_client_Microtransaction.definitions.c'), 'w')
    #fout.write(bindings.copyright('*', '2015'))
    #fout.write(bindings.java.DEFINITIONS_HEAD)
    #fout.write('\n'.join([generate_microtransaction_definition(c) for c in bindings.Client if c.form is bindings.MicrotransactionCall]))

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/java/client/ops.tex'), 'w')
    fout.write(bindings.copyright('%', '2014-2015'))
    fout.write('\n% This LaTeX file is generated by bindings/java.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'client') for c in bindings.Client
                          if c.form not in (bindings.DoNotDocument, bindings.MicrotransactionCall)]))

if __name__ == '__main__':
    generate_client_java()
    generate_client_doc()
    
MICROTRANSACTION_HEAD = '''
/* This file is generated by bindings/java.py */

package org.hyperdex.client;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Microtransaction
{
    static
    {
        initialize();
    }

    protected Microtransaction(Client client, String space)
    {
        this.client = client;
        this._create(space);
    }
    
    private Client client;
    
    private long uxact_ptr;
    private long deferred_ptr;
    
    private native void _create(String space);
    private native void _destroy();
    
    /* cached IDs */
    private static native void initialize();
    private static native void terminate();
    
    public Boolean commit(String key) throws HyperDexClientException {
        return (Boolean) async_commit(key).waitForIt();
    }
    
    public native Deferred async_commit(String key) throws HyperDexClientException;
    
    public Long group_commit(Map<String,Object> checks) throws HyperDexClientException {
        return (Long) async_group_commit(checks).waitForIt();
    }
    
    public native Deferred async_group_commit(Map<String,Object> checks) throws HyperDexClientException;
    
    public Boolean cond_commit(String key, Map<String,Object> checks) throws HyperDexClientException {
        return (Boolean) async_cond_commit(key, checks).waitForIt();
    }
    
    public native Deferred async_cond_commit(String key, Map<String,Object> checks) throws HyperDexClientException;
'''

JAVA_HEAD = '''
/* This file is generated by bindings/java.py */

package org.hyperdex.client;

import java.util.List;
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
    public Operation loop() throws HyperDexClientException
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
    private native long inner_loop() throws HyperDexClientException;
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
