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
import itertools
import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE)

import bindings
import bindings.c

################################## enum values #################################

# 0 represents a comment

# hyperdex_client_returncode occupies [8448, 8576)
CLIENT_ENUM = [(8448, 'SUCCESS'),
               (8449, 'NOTFOUND'),
               (8450, 'SEARCHDONE'),
               (8451, 'CMPFAIL'),
               (8452, 'READONLY'),
               (0, 'Error conditions'),
               (8512, 'UNKNOWNSPACE'),
               (8513, 'COORDFAIL'),
               (8514, 'SERVERERROR'),
               (8515, 'POLLFAILED'),
               (8516, 'OVERFLOW'),
               (8517, 'RECONFIGURE'),
               (8519, 'TIMEOUT'),
               (8520, 'UNKNOWNATTR'),
               (8521, 'DUPEATTR'),
               (8523, 'NONEPENDING'),
               (8524, 'DONTUSEKEY'),
               (8525, 'WRONGTYPE'),
               (8526, 'NOMEM'),
               (8530, 'INTERRUPTED'),
               (8531, 'CLUSTER_JUMP'),
               (8533, 'OFFLINE'),
               (8534, 'UNAUTHORIZED'),
               (0, 'This should never happen.  It indicates a bug'),
               (8573, 'INTERNAL'),
               (8574, 'EXCEPTION'),
               (8575, 'GARBAGE'),
]

# hyperdex_admin_returncode occupies [8704, 8832)
ADMIN_ENUM = [(8704, 'SUCCESS'),
              (0, 'Error conditions'),
              (8768, 'NOMEM'),
              (8769, 'NONEPENDING'),
              (8770, 'POLLFAILED'),
              (8771, 'TIMEOUT'),
              (8772, 'INTERRUPTED'),
              (8773, 'SERVERERROR'),
              (8774, 'COORDFAIL'),
              (8775, 'BADSPACE'),
              (8776, 'DUPLICATE'),
              (8777, 'NOTFOUND'),
              (8778, 'LOCALERROR'),
              (0, 'This should never happen.  It indicates a bug'),
              (8829, 'INTERNAL'),
              (8830, 'EXCEPTION'),
              (8831, 'GARBAGE'),
]

def generate_enum(prefix, E):
    width = len(prefix) + max([len(s) for n, s in E if n != 0])
    enum = ''
    for n, s in E:
        if n == 0:
            enum += '\n'
            enum += ' ' * 4 + '/* ' + s + ' */\n'
            continue
        enum += ' ' * 4 + (prefix + s).ljust(width) + ' = ' + str(n) + ',\n'
    return enum.rstrip('\n,')

################################ Code Generation ###############################

def generate_func(x, lib, struct=None, sep='\n', namesep='_', padd=None):
    assert x.form in (bindings.AsyncCall, bindings.SyncCall, bindings.MicrotransactionCall,
                      bindings.NoFailCall, bindings.Iterator)
    struct = struct or lib
    return_type = 'int64_t'
    if x.form == bindings.SyncCall:
        return_type = 'int'
    if x.form == bindings.NoFailCall:
        return_type = 'void'
    name = 'hyperdex_' + lib + namesep + x.name
    func = '{return_type}{sep}{name}(struct hyperdex_{struct}* {lib}'.format(**locals())
    padd = ' ' * (len(name) + 1) if padd is None else ' ' * padd
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ');\n'
    return func

def generate_func_ptr(x, lib):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    fptr = generate_func(xp, lib)
    fptr = fptr.replace('hyperdex_' + lib + '_WXYZ', '(*f)')
    fptr = ' ' .join([c.strip() for c in fptr.split('\n')])
    fptr = fptr.strip('; ')
    return fptr

def generate_client_c_wrapper(x):
    assert x.form in (bindings.AsyncCall, bindings.Iterator, bindings.MicrotransactionCall)
    func = 'HYPERDEX_API int64_t\nhyperdex_client_%s(struct hyperdex_client* _cl' % x.name
    padd = ' ' * (len('hyperdex_client_') + len(x.name) + 1)
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ')\n{\n'
    
    if x.name.startswith('uxact_'):
        func += '    hyperdex::microtransaction* tx = reinterpret_cast<hyperdex::microtransaction*>(microtransaction);\n'
        func += '    hyperdex_client_returncode *status = tx->status;\n'
        func += '    C_WRAP_EXCEPT(\n'
        args = ('tx',  'opinfo')
        if bindings.Attributes in x.args_in:
            args += ('attrs', 'attrs_sz')
        else:
            args += ('NULL', '0')
        if bindings.MapAttributes in x.args_in:
            args += ('mapattrs', 'mapattrs_sz')
        else:
            args += ('NULL', '0')
        func += '    const hyperdex_client_keyop_info* opinfo;\n'
        func += '    opinfo = hyperdex_client_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));\n'.format(x.name.replace('uxact_', ''))
        func += '    return cl->uxact_add_funcall('
        func += ', '.join([a for a in args])
        func += ');\n'
        func += '    );\n'
    else:
        func += '    C_WRAP_EXCEPT(\n'
        if x.name == 'get':
            func += '    return cl->get(space, key, key_sz, status, attrs, attrs_sz);\n'
        elif x.name == 'get_partial':
            func += '    return cl->get_partial(space, key, key_sz, attrnames, attrnames_sz, status, attrs, attrs_sz);\n'
        elif x.name == 'search':
            func += '    return cl->search(space, checks, checks_sz, status, attrs, attrs_sz);\n'
        elif x.name == 'search_describe':
            func += '    return cl->search_describe(space, checks, checks_sz, status, description);\n'
        elif x.name == 'sorted_search':
            func += '    return cl->sorted_search(space, checks, checks_sz, sort_by, limit, maxmin, status, attrs, attrs_sz);\n'
        elif x.name == 'count':
            func += '    return cl->count(space, checks, checks_sz, status, count);\n'
        elif x.name.startswith('group_'):
            args = ('opinfo', 'space', )
            if bindings.Predicates in x.args_in:
                args += ('checks', 'checks_sz')
            else:
                args += ('NULL', '0')
            if bindings.Attributes in x.args_in:
                args += ('attrs', 'attrs_sz')
            else:
                args += ('NULL', '0')
            if bindings.MapAttributes in x.args_in:
                args += ('mapattrs', 'mapattrs_sz')
            else:
                args += ('NULL', '0')
            args += ('status', 'count')
            func += '    const hyperdex_client_keyop_info* opinfo;\n'
            func += '    opinfo = hyperdex_client_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));\n'.format(x.name)
            func += '    return cl->perform_group_funcall('
            func += ', '.join([a for a in args])
            func += ');\n'
        else:
            args = ('opinfo', 'space', 'key', 'key_sz')
            if bindings.Predicates in x.args_in:
                args += ('checks', 'checks_sz')
            else:
                args += ('NULL', '0')
            if bindings.Attributes in x.args_in:
                args += ('attrs', 'attrs_sz')
            else:
                args += ('NULL', '0')
            if bindings.MapAttributes in x.args_in:
                args += ('mapattrs', 'mapattrs_sz')
            else:
                args += ('NULL', '0')
            args += ('status',)
            func += '    const hyperdex_client_keyop_info* opinfo;\n'
            func += '    opinfo = hyperdex_client_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));\n'.format(x.name)
            func += '    return cl->perform_funcall('
            func += ', '.join([a for a in args])
            func += ');\n'
        func += '    );\n'
    func += '}\n'
    return func

def generate_admin_c_wrapper(x):
    assert x.form in (bindings.AsyncCall, bindings.SyncCall,
                      bindings.NoFailCall, bindings.Iterator)
    return_type = 'int64_t'
    if x.form == bindings.SyncCall:
        return_type = 'int'
    if x.form == bindings.NoFailCall:
        return_type = 'void'
    func = 'HYPERDEX_API {return_type}\nhyperdex_admin_{name}(struct hyperdex_admin* _adm'.format(name=x.name, **locals())
    padd = ' ' * (len('hyperdex_admin_') + len(x.name) + 1)
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ')\n{\n'
    if x.form != bindings.NoFailCall:
        func += '    C_WRAP_EXCEPT(\n'
    func += '    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);\n'
    func += '    return adm->%s(' % x.name
    args = itertools.chain(*[list(a.args) for a in x.args_in + x.args_out])
    func += ', '.join([a[1] for a in args])
    func += ');\n'
    if x.form != bindings.NoFailCall:
        func += '    );\n'
    func += '}\n'
    return func

################################# Documentation ################################

def generate_api_block(x, lib):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:c:%s}\n' % x.name
    block += '\\index{%s!C API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/%s/fragments/%s}\n\n' % (lib, x.name)
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{ccode}\n'
    block += generate_func(x, lib, sep=' ', padd=8)
    block += '\\end{ccode}\n\n'
    block += '\\paragraph{Parameters:}\n'
    def c_in_label(arg):
        args = [p + ' ' + n for p, n in arg.args]
        return '\\code{' + bindings.LaTeX(', '.join(args)) + '}'
    if lib == 'client':
        args = (bindings.StructClient,) + x.args_in
    elif lib == 'admin':
        args = (bindings.StructAdmin,) + x.args_in
    else:
        assert False
    block += bindings.doc_parameter_list(x.form, args, 'c/' + lib + '/fragments/in',
                                         label_maker=c_in_label)
    block += '\n\\paragraph{Returns:}\n'
    def c_out_label(arg):
        args = [p + '* ' + n for p, n in arg.args]
        return '\\code{' + bindings.LaTeX(', '.join(args)) + '}'
    block += bindings.doc_parameter_list(x.form, x.args_out, 'c/' + lib + '/fragments/out',
                                         label_maker=c_out_label)
    return block

############################### Output Generators ##############################

def generate_client_header():
    fout = open(os.path.join(BASE, 'include/hyperdex/client.h'), 'w')
    fout.write(bindings.copyright('*', '2011-2014'))
    fout.write(bindings.c.CLIENT_HEADER_HEAD)
    fout.write('\n'.join([generate_func(c, 'client') for c in bindings.Client]))
    fout.write(bindings.c.CLIENT_HEADER_FOOT)

def generate_client_wrapper():
    fout = open(os.path.join(BASE, 'client/c.cc'), 'w')
    fout.write(bindings.copyright('/', '2013-2014'))
    fout.write(bindings.c.CLIENT_WRAPPER_HEAD)
    fout.write('\n'.join([generate_client_c_wrapper(c) for c in bindings.Client]))
    fout.write(bindings.c.CLIENT_WRAPPER_FOOT)

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/c/client/ops.tex'), 'w')
    fout.write(bindings.copyright('%', '2013-2014'))
    fout.write('\n% This LaTeX file is generated by bindings/c.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'client') for c in bindings.Client
                          if c.name not in bindings.DoNotDocument and c.form is
                          not bindings.MicrotransactionCall]))

def generate_admin_header():
    fout = open(os.path.join(BASE, 'include/hyperdex/admin.h'), 'w')
    fout.write(bindings.copyright('*', '2013-2014'))
    fout.write(bindings.c.ADMIN_HEADER_HEAD)
    fout.write('\n'.join([generate_func(c, 'admin') for c in bindings.Admin]))
    fout.write(bindings.c.ADMIN_HEADER_FOOT)

def generate_admin_wrapper():
    fout = open(os.path.join(BASE, 'admin/c.cc'), 'w')
    fout.write(bindings.copyright('/', '2013-2014'))
    fout.write(bindings.c.ADMIN_WRAPPER_HEAD)
    fout.write('\n'.join([generate_admin_c_wrapper(c) for c in bindings.Admin]))
    fout.write(bindings.c.ADMIN_WRAPPER_FOOT)

def generate_admin_doc():
    fout = open(os.path.join(BASE, 'doc/c/admin/ops.tex'), 'w')
    fout.write(bindings.copyright('%', '2013-2014'))
    fout.write('\n% This LaTeX file is generated by bindings/c.py\n\n')
    fout.write('\n'.join([generate_api_block(c, 'admin') for c in bindings.Admin]))

if __name__ == '__main__':
    generate_client_header()
    generate_client_wrapper()
    generate_client_doc()
    generate_admin_header()
    generate_admin_wrapper()
    generate_admin_doc()

################################### Templates ##################################

CLIENT_HEADER_HEAD = '''
#ifndef hyperdex_client_h_
#define hyperdex_client_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperdex_client;
struct hyperdex_client_microtransaction;

struct hyperdex_client_attribute
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
};

struct hyperdex_client_map_attribute
{
    const char* attr; /* NULL-terminated */
    const char* map_key;
    size_t map_key_sz;
    enum hyperdatatype map_key_datatype;
    const char* value;
    size_t value_sz;
    enum hyperdatatype value_datatype;
};

struct hyperdex_client_attribute_check
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
    enum hyperpredicate predicate;
};

/* hyperdex_client_returncode occupies [8448, 8576) */
enum hyperdex_client_returncode
{
''' + generate_enum('HYPERDEX_CLIENT_', CLIENT_ENUM) + '''
};

#define HYPERDEX_ATTRIBUTE_SECRET "__secret"

struct hyperdex_client*
hyperdex_client_create(const char* coordinator, uint16_t port);
struct hyperdex_client*
hyperdex_client_create_conn_str(const char* conn_str);
void
hyperdex_client_destroy(struct hyperdex_client* client);

struct macaroon;

void
hyperdex_client_clear_auth_context(struct hyperdex_client* client);
void
hyperdex_client_set_auth_context(struct hyperdex_client* client,
                                 const char** macaroons, size_t macaroons_sz);

struct hyperdex_client_microtransaction*
hyperdex_client_uxact_init(struct hyperdex_client* _cl,
                      const char* space,
                      enum hyperdex_client_returncode *status);

int64_t
hyperdex_client_uxact_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const char* key, size_t key_sz);
                                
int64_t
hyperdex_client_uxact_group_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const struct hyperdex_client_attribute_check *chks, size_t chks_sz,
                                uint64_t *count);
                                
int64_t
hyperdex_client_uxact_cond_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const char* key, size_t key_sz,
                                const struct hyperdex_client_attribute_check *chks, size_t chks_sz);

'''

CLIENT_HEADER_FOOT = '''
int64_t
hyperdex_client_loop(struct hyperdex_client* client, int timeout,
                     enum hyperdex_client_returncode* status);

int
hyperdex_client_poll_fd(struct hyperdex_client* client);

int
hyperdex_client_block(struct hyperdex_client* client, int timeout);

enum hyperdatatype
hyperdex_client_attribute_type(struct hyperdex_client* client,
                               const char* space, const char* name,
                               enum hyperdex_client_returncode* status);

const char*
hyperdex_client_error_message(struct hyperdex_client* client);

const char*
hyperdex_client_error_location(struct hyperdex_client* client);

const char*
hyperdex_client_returncode_to_string(enum hyperdex_client_returncode);

void
hyperdex_client_destroy_attrs(const struct hyperdex_client_attribute* attrs, size_t attrs_sz);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_client_h_ */
'''

CLIENT_WRAPPER_HEAD = '''
// This file is generated by bindings/c.py

#define __STDC_LIMIT_MACROS

// POSIX
#include <signal.h>

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/client.h>
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"

#define FAKE_STATUS \
    hyperdex_client_returncode _status; \
    hyperdex_client_returncode* status = &_status

#define SIGNAL_PROTECT_ERR(X) \\
    sigset_t old_sigs; \\
    sigset_t all_sigs; \\
    sigfillset(&all_sigs); \\
    if (pthread_sigmask(SIG_BLOCK, &all_sigs, &old_sigs) < 0) \\
    { \\
        *status = HYPERDEX_CLIENT_INTERNAL; \\
        return (X); \\
    } \\
    e::guard g = e::makeguard(pthread_sigmask, SIG_SETMASK, (sigset_t*)&old_sigs, (sigset_t*)NULL)

#define SIGNAL_PROTECT SIGNAL_PROTECT_ERR(-1);
inline void return_void() {}
#define SIGNAL_PROTECT_VOID SIGNAL_PROTECT_ERR(return_void());

#define C_WRAP_EXCEPT(X) \\
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl); \\
    SIGNAL_PROTECT; \\
    try \\
    { \\
        X \\
    } \\
    catch (std::bad_alloc& ba) \\
    { \\
        errno = ENOMEM; \\
        *status = HYPERDEX_CLIENT_NOMEM; \\
        cl->set_error_message("out of memory"); \\
        return -1; \\
    } \\
    catch (...) \\
    { \\
        *status = HYPERDEX_CLIENT_EXCEPTION; \\
        cl->set_error_message("unhandled exception was thrown"); \\
        return -1; \\
    } \\

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

HYPERDEX_API hyperdex_client*
hyperdex_client_create(const char* coordinator, uint16_t port)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    try
    {
        return reinterpret_cast<hyperdex_client*>(new hyperdex::client(coordinator, port));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

HYPERDEX_API hyperdex_client*
hyperdex_client_create_conn_str(const char* conn_str)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    try
    {
        return reinterpret_cast<hyperdex_client*>(new hyperdex::client(conn_str));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

HYPERDEX_API void
hyperdex_client_destroy(hyperdex_client* client)
{
    delete reinterpret_cast<hyperdex::client*>(client);
}

HYPERDEX_API const char*
hyperdex_client_error_message(hyperdex_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->error_message();
}

HYPERDEX_API const char*
hyperdex_client_error_location(hyperdex_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->error_location();
}

HYPERDEX_API const char*
hyperdex_client_returncode_to_string(enum hyperdex_client_returncode stat)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    switch (stat)
    {
        CSTRINGIFY(HYPERDEX_CLIENT_SUCCESS);
        CSTRINGIFY(HYPERDEX_CLIENT_NOTFOUND);
        CSTRINGIFY(HYPERDEX_CLIENT_SEARCHDONE);
        CSTRINGIFY(HYPERDEX_CLIENT_CMPFAIL);
        CSTRINGIFY(HYPERDEX_CLIENT_READONLY);
        CSTRINGIFY(HYPERDEX_CLIENT_UNKNOWNSPACE);
        CSTRINGIFY(HYPERDEX_CLIENT_COORDFAIL);
        CSTRINGIFY(HYPERDEX_CLIENT_SERVERERROR);
        CSTRINGIFY(HYPERDEX_CLIENT_POLLFAILED);
        CSTRINGIFY(HYPERDEX_CLIENT_OVERFLOW);
        CSTRINGIFY(HYPERDEX_CLIENT_RECONFIGURE);
        CSTRINGIFY(HYPERDEX_CLIENT_TIMEOUT);
        CSTRINGIFY(HYPERDEX_CLIENT_UNKNOWNATTR);
        CSTRINGIFY(HYPERDEX_CLIENT_DUPEATTR);
        CSTRINGIFY(HYPERDEX_CLIENT_NONEPENDING);
        CSTRINGIFY(HYPERDEX_CLIENT_DONTUSEKEY);
        CSTRINGIFY(HYPERDEX_CLIENT_WRONGTYPE);
        CSTRINGIFY(HYPERDEX_CLIENT_NOMEM);
        CSTRINGIFY(HYPERDEX_CLIENT_INTERRUPTED);
        CSTRINGIFY(HYPERDEX_CLIENT_CLUSTER_JUMP);
        CSTRINGIFY(HYPERDEX_CLIENT_OFFLINE);
        CSTRINGIFY(HYPERDEX_CLIENT_UNAUTHORIZED);
        CSTRINGIFY(HYPERDEX_CLIENT_INTERNAL);
        CSTRINGIFY(HYPERDEX_CLIENT_EXCEPTION);
        CSTRINGIFY(HYPERDEX_CLIENT_GARBAGE);
        default:
            return "unknown hyperdex_client_returncode";
    }
}

HYPERDEX_API enum hyperdatatype
hyperdex_client_attribute_type(hyperdex_client* _cl,
                               const char* space, const char* name,
                               enum hyperdex_client_returncode* status)
{
    SIGNAL_PROTECT_ERR(HYPERDATATYPE_GARBAGE);
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl); \\

    try
    {
        return cl->attribute_type(space, name, status);
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_CLIENT_NOMEM;
        cl->set_error_message("out of memory");
        return HYPERDATATYPE_GARBAGE;
    }
    catch (...)
    {
        *status = HYPERDEX_CLIENT_EXCEPTION;
        cl->set_error_message("unhandled exception was thrown");
        return HYPERDATATYPE_GARBAGE;
    }
}

HYPERDEX_API void
hyperdex_client_destroy_attrs(const hyperdex_client_attribute* attrs, size_t /*attrs_sz*/)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_VOID;
    free(const_cast<hyperdex_client_attribute*>(attrs));
}

HYPERDEX_API void
hyperdex_client_clear_auth_context(struct hyperdex_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_VOID;
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    cl->clear_auth_context();
}

HYPERDEX_API void
hyperdex_client_set_auth_context(struct hyperdex_client* _cl,
                                 const char** macaroons, size_t macaroons_sz)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_VOID;
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    cl->set_auth_context(macaroons, macaroons_sz);
}

HYPERDEX_API struct hyperdex_client_microtransaction*
hyperdex_client_uxact_init(struct hyperdex_client* _cl,
                      const char* space,
                      enum hyperdex_client_returncode *status)
{

    SIGNAL_PROTECT_ERR(NULL);
    hyperdex::client *cl = reinterpret_cast<hyperdex::client*>(_cl);
    hyperdex::microtransaction *tx = cl->uxact_init(space, status);

    return reinterpret_cast<struct hyperdex_client_microtransaction*>(tx);
}

HYPERDEX_API int64_t
hyperdex_client_uxact_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const char* key, size_t key_sz)
{
    hyperdex::microtransaction* tx = reinterpret_cast<hyperdex::microtransaction*>(transaction);
    hyperdex_client_returncode *status = tx->status;

    C_WRAP_EXCEPT(
    return cl->uxact_commit(tx, key, key_sz);
    );
}

HYPERDEX_API int64_t
hyperdex_client_uxact_group_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const hyperdex_client_attribute_check *chks, size_t chks_sz,
                                uint64_t *count)
{
    hyperdex::microtransaction* tx = reinterpret_cast<hyperdex::microtransaction*>(transaction);
    hyperdex_client_returncode *status = tx->status;

    C_WRAP_EXCEPT(
    return cl->uxact_group_commit(tx, chks, chks_sz, count);
    );
}

HYPERDEX_API int64_t
hyperdex_client_uxact_cond_commit(struct hyperdex_client* _cl,
                                struct hyperdex_client_microtransaction *transaction,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check *chks, size_t chks_sz)
{
    hyperdex::microtransaction* tx = reinterpret_cast<hyperdex::microtransaction*>(transaction);
    hyperdex_client_returncode *status = tx->status;

    C_WRAP_EXCEPT(
    return cl->uxact_cond_commit(tx, key, key_sz, chks, chks_sz);
    );
}

'''

CLIENT_WRAPPER_FOOT = '''
HYPERDEX_API int64_t
hyperdex_client_loop(hyperdex_client* _cl, int timeout,
                     hyperdex_client_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->loop(timeout, status);
    );
}

HYPERDEX_API int
hyperdex_client_poll_fd(hyperdex_client* _cl)
{
    FAKE_STATUS;
    C_WRAP_EXCEPT(
    return cl->poll_fd();
    );
}

HYPERDEX_API int
hyperdex_client_block(hyperdex_client* _cl, int timeout)
{
    FAKE_STATUS;
    C_WRAP_EXCEPT(
    return cl->block(timeout);
    );
}

HYPERDEX_API void
hyperdex_client_set_type_conversion(hyperdex_client* _cl, bool enabled)
{
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    cl->set_type_conversion(enabled);
}

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
'''

ADMIN_HEADER_HEAD = '''
#ifndef hyperdex_admin_h_
#define hyperdex_admin_h_

/* C */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperdex_admin;

struct hyperdex_admin_perf_counter
{
    uint64_t id;
    uint64_t time;
    const char* property;
    uint64_t measurement;
};

/* hyperdex_admin_returncode occupies [8704, 8832) */
enum hyperdex_admin_returncode
{
''' + generate_enum('HYPERDEX_ADMIN_', ADMIN_ENUM) + '''
};

struct hyperdex_admin*
hyperdex_admin_create(const char* coordinator, uint16_t port);

void
hyperdex_admin_destroy(struct hyperdex_admin* admin);

int64_t
hyperdex_admin_dump_config(struct hyperdex_admin* admin,
                           enum hyperdex_admin_returncode* status,
                           const char** config);
'''

ADMIN_HEADER_FOOT = '''
int64_t
hyperdex_admin_loop(struct hyperdex_admin* admin, int timeout,
                    enum hyperdex_admin_returncode* status);

int
hyperdex_admin_raw_backup(const char* host, uint16_t port,
                          const char* name,
                          enum hyperdex_admin_returncode* status);

const char*
hyperdex_admin_error_message(struct hyperdex_admin* admin);
const char*
hyperdex_admin_error_location(struct hyperdex_admin* admin);

const char*
hyperdex_admin_returncode_to_string(enum hyperdex_admin_returncode);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_admin_h_ */
'''

ADMIN_WRAPPER_HEAD = '''
// This file is generated by bindings/c.py

#define __STDC_LIMIT_MACROS

// POSIX
#include <signal.h>

// e
#include <e/guard.h>

// HyperDex
#include "include/hyperdex/admin.h"
#include "visibility.h"
#include "common/macros.h"
#include "admin/admin.h"

#define SIGNAL_PROTECT_ERR(X) \\
    do \\
    { \\
        sigset_t old_sigs; \\
        sigset_t all_sigs; \\
        sigfillset(&all_sigs); \\
        if (pthread_sigmask(SIG_BLOCK, &all_sigs, &old_sigs) < 0) \\
        { \\
            *status = HYPERDEX_ADMIN_INTERNAL; \\
            return (X); \\
        } \\
        e::guard g = e::makeguard(pthread_sigmask, SIG_SETMASK, (sigset_t*)&old_sigs, (sigset_t*)NULL); \\
        g.use_variable(); \\
    } \\
    while (0)

#define SIGNAL_PROTECT SIGNAL_PROTECT_ERR(-1);
inline void return_void() {}
#define SIGNAL_PROTECT_VOID SIGNAL_PROTECT_ERR(return_void());

#define C_WRAP_EXCEPT(X) \\
    SIGNAL_PROTECT; \\
    try \\
    { \\
        X \\
    } \\
    catch (std::bad_alloc& ba) \\
    { \\
        errno = ENOMEM; \\
        *status = HYPERDEX_ADMIN_NOMEM; \\
        return -1; \\
    } \\
    catch (...) \\
    { \\
        *status = HYPERDEX_ADMIN_EXCEPTION; \\
        return -1; \\
    }

extern "C"
{

HYPERDEX_API struct hyperdex_admin*
hyperdex_admin_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<struct hyperdex_admin*>(new hyperdex::admin(coordinator, port));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

HYPERDEX_API void
hyperdex_admin_destroy(struct hyperdex_admin* admin)
{
    delete reinterpret_cast<hyperdex::admin*>(admin);
}

HYPERDEX_API int64_t
hyperdex_admin_dump_config(struct hyperdex_admin* _adm,
                           hyperdex_admin_returncode* status,
                           const char** config)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->dump_config(status, config);
    );
}
'''

ADMIN_WRAPPER_FOOT = '''
HYPERDEX_API int64_t
hyperdex_admin_loop(struct hyperdex_admin* _adm, int timeout,
                    enum hyperdex_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->loop(timeout, status);
    );
}

HYPERDEX_API const char*
hyperdex_admin_error_message(struct hyperdex_admin* _adm)
{
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->error_message();
}

HYPERDEX_API const char*
hyperdex_admin_error_location(struct hyperdex_admin* _adm)
{
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->error_location();
}

HYPERDEX_API const char*
hyperdex_admin_returncode_to_string(enum hyperdex_admin_returncode status)
{
    switch (status)
    {
        CSTRINGIFY(HYPERDEX_ADMIN_SUCCESS);
        CSTRINGIFY(HYPERDEX_ADMIN_NOMEM);
        CSTRINGIFY(HYPERDEX_ADMIN_NONEPENDING);
        CSTRINGIFY(HYPERDEX_ADMIN_POLLFAILED);
        CSTRINGIFY(HYPERDEX_ADMIN_TIMEOUT);
        CSTRINGIFY(HYPERDEX_ADMIN_INTERRUPTED);
        CSTRINGIFY(HYPERDEX_ADMIN_SERVERERROR);
        CSTRINGIFY(HYPERDEX_ADMIN_COORDFAIL);
        CSTRINGIFY(HYPERDEX_ADMIN_BADSPACE);
        CSTRINGIFY(HYPERDEX_ADMIN_DUPLICATE);
        CSTRINGIFY(HYPERDEX_ADMIN_NOTFOUND);
        CSTRINGIFY(HYPERDEX_ADMIN_LOCALERROR);
        CSTRINGIFY(HYPERDEX_ADMIN_INTERNAL);
        CSTRINGIFY(HYPERDEX_ADMIN_EXCEPTION);
        CSTRINGIFY(HYPERDEX_ADMIN_GARBAGE);
        default:
            return "unknown hyperdex_admin_returncode";
    }
}

} // extern "C"
'''
