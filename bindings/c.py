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
               (0, 'This should never happen.  It indicates a bug'),
               (8573, 'INTERNAL'),
               (8574, 'EXCEPTION'),
               (8575, 'GARBAGE'),
]

DOCS_IN = {(bindings.AsyncCall, bindings.SpaceName): 'The name of the space as a c-string.'
          ,(bindings.AsyncCall, bindings.Key): 'The key for the operation where '
           '\\code{key} is a bytestring and \\code{key\\_sz} specifies '
           'the number of bytes in \\code{key}.'
          ,(bindings.AsyncCall, bindings.Attributes): 'The set of attributes '
           'to modify and their respective values.  \\code{attrs} points to '
           'an array of length \\code{attrs\_sz}.'
          ,(bindings.AsyncCall, bindings.MapAttributes): 'The set of map '
           'attributes to modify and their respective key/values.  '
           '\\code{mapattrs} points to an array of length '
           '\\code{mapattrs\_sz}.  Each entry specify an attribute that is a '
           'map and a key within that map.'
          ,(bindings.AsyncCall, bindings.Predicates): 'A set of predicates '
           'to check against.  \\code{checks} points to an array of length '
           '\\code{checks\_sz}.'
          ,(bindings.Iterator, bindings.SpaceName): 'The name of the space as a c-string.'
          ,(bindings.Iterator, bindings.SortBy): 'The attribute to sort by.'
          ,(bindings.Iterator, bindings.Limit): 'The number of results to return.'
          ,(bindings.Iterator, bindings.MaxMin): 'Maximize (!= 0) or minimize (== 0).'
          ,(bindings.Iterator, bindings.Predicates): 'A set of predicates '
           'to check against.  \\code{checks} points to an array of length '
           '\\code{checks\_sz}.'
          }
DOCS_OUT = {(bindings.AsyncCall, bindings.Status): 'The status of the '
            'operation.  The client library will fill in this variable before '
            'returning this operation\'s request id from '
            '\\code{hyperdex\\_client\\_loop}.  The pointer must remain valid '
            'until then, and the pointer should not be aliased to the status '
            'for any other outstanding operation.'
           ,(bindings.AsyncCall, bindings.Attributes): 'An array of attributes '
            'that comprise a returned object.  The application must free the '
            'returned values with \\code{hyperdex\_client\_destroy\_attrs}.  The '
            'pointers must remain valid until the operation completes.'
           ,(bindings.AsyncCall, bindings.Count): 'The number of objects which '
            'match the predicates.'
           ,(bindings.AsyncCall, bindings.Description): 'The description of '
            'the search.  This is a c-string that the client must free.'
           ,(bindings.Iterator, bindings.Status): 'The status of the '
            'operation.  The client library will fill in this variable before '
            'returning this operation\'s request id from '
            '\\code{hyperdex\\_client\\_loop}.  The pointer must remain valid '
            'until the operation completes, and the pointer should not be '
            'aliased to the status for any other outstanding operation.'
           ,(bindings.Iterator, bindings.Attributes): 'An array of attributes '
            'that comprise a returned object.  The application must free the '
            'returned values with \\code{hyperdex\_client\_destroy\_attrs}.  The '
            'pointers must remain valid until the operation completes.'
           }

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

def generate_func(x, struct='hyperdex_client', sep='\n', padd=None):
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    name = struct + '_' + x.name
    func = 'int64_t%s%s(struct %s* client' % (sep, name, struct)
    padd = ' ' * (len(name) + 1) if padd is None else ' ' * padd
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ');\n'
    return func

def generate_func_ptr(x, struct='hyperdex_client'):
    xp = copy.deepcopy(x)
    xp.name = 'WXYZ'
    fptr = generate_func(xp)
    fptr = fptr.replace('hyperdex_client_WXYZ', '(*f)')
    fptr = ' ' .join([c.strip() for c in fptr.split('\n')])
    fptr = fptr.strip('; ')
    return fptr

def generate_c_wrapper(x):
    def rename(x):
        return x.replace('enum ', '').replace('struct ', '')
    assert x.form in (bindings.AsyncCall, bindings.Iterator)
    func = 'HYPERDEX_API int64_t\nhyperdex_client_%s(hyperdex_client* _cl' % x.name
    padd = ' ' * (len('hyperdex_client_') + len(x.name) + 1)
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([rename(p) + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([rename(p) + '* ' + n for p, n in arg.args])
    func += ')\n{\n'
    func += '    C_WRAP_EXCEPT(\n'
    if x.name == 'get':
        func += '    return cl->get(space, key, key_sz, status, attrs, attrs_sz);\n'
    elif x.name == 'search':
        func += '    return cl->search(space, checks, checks_sz, status, attrs, attrs_sz);\n'
    elif x.name == 'search_describe':
        func += '    return cl->search_describe(space, checks, checks_sz, status, description);\n'
    elif x.name == 'sorted_search':
        func += '    return cl->sorted_search(space, checks, checks_sz, sort_by, limit, maxmin, status, attrs, attrs_sz);\n'
    elif x.name == 'group_del':
        func += '    return cl->group_del(space, checks, checks_sz, status);\n'
    elif x.name == 'count':
        func += '    return cl->count(space, checks, checks_sz, status, count);\n'
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
        func += ', '.join([rename(a) for a in args])
        func += ');\n'
    func += '    );\n'
    func += '}\n'
    return func

def generate_api_block(x, struct='hyperdex_client'):
    block  = ''
    block += '%' * 20 + ' ' + x.name + ' ' + '%' * 20 + '\n'
    block += '\\pagebreak\n'
    block += '\\subsubsection{\code{%s}}\n' % bindings.LaTeX(x.name)
    block += '\\label{api:c:%s}\n' % x.name
    block += '\\index{%s!C API}\n' % bindings.LaTeX(x.name)
    block += '\\input{\\topdir/api/desc/%s}\n\n' % x.name
    block += '\\paragraph{Definition:}\n'
    block += '\\begin{ccode}\n'
    block += generate_func(x, struct=struct, sep=' ', padd=8)
    block += '\\end{ccode}\n\n'
    block += '\\paragraph{Parameters:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_in, DOCS_IN,
                                         label_maker=bindings.parameters_c_style)
    block += '\n\\paragraph{Returns:}\n'
    block += bindings.doc_parameter_list(x.form, x.args_out, DOCS_OUT,
                                         label_maker=bindings.parameters_c_style)
    return block

def generate_client_header():
    fout = open(os.path.join(BASE, 'include/hyperdex/client.h'), 'w')
    fout.write(bindings.copyright('*', '2011-2014'))
    fout.write(bindings.c.HEADER_HEAD)
    fout.write('\n'.join([generate_func(c) for c in bindings.Client]))
    fout.write(bindings.c.HEADER_FOOT)

def generate_client_wrapper():
    fout = open(os.path.join(BASE, 'client/c.cc'), 'w')
    fout.write(bindings.copyright('/', '2013-2014'))
    fout.write(bindings.c.WRAPPER_HEAD)
    fout.write('\n'.join([generate_c_wrapper(c) for c in bindings.Client]))
    fout.write(bindings.c.WRAPPER_FOOT)

def generate_client_doc():
    fout = open(os.path.join(BASE, 'doc/api/c.client.tex'), 'w')
    fout.write(bindings.copyright('%', '2013-2014'))
    fout.write('\n% This LaTeX file is generated by bindings/c.py\n\n')
    fout.write('\n'.join([generate_api_block(c) for c in bindings.Client]))

if __name__ == '__main__':
    generate_client_header()
    generate_client_wrapper()
    generate_client_doc()

HEADER_HEAD = '''
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

struct hyperdex_client*
hyperdex_client_create(const char* coordinator, uint16_t port);
void
hyperdex_client_destroy(struct hyperdex_client* client);

'''

HEADER_FOOT = '''
int64_t
hyperdex_client_loop(struct hyperdex_client* client, int timeout,
                     enum hyperdex_client_returncode* status);

int
hyperdex_client_poll(struct hyperdex_client* client);

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

WRAPPER_HEAD = '''
// This file is generated by bindings/c.py

// HyperDex
#include <hyperdex/client.h>
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"

#define C_WRAP_EXCEPT(X) \\
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl); \\
    try \\
    { \\
        X \\
    } \\
    catch (po6::error& e) \\
    { \\
        errno = e; \\
        *status = HYPERDEX_CLIENT_EXCEPTION; \\
        cl->set_error_message("unhandled exception was thrown"); \\
        return -1; \\
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
    }

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

HYPERDEX_API hyperdex_client*
hyperdex_client_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<hyperdex_client*>(new hyperdex::client(coordinator, port));
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
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
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->error_message();
}

HYPERDEX_API const char*
hyperdex_client_error_location(hyperdex_client* _cl)
{
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->error_location();
}

HYPERDEX_API const char*
hyperdex_client_returncode_to_string(enum hyperdex_client_returncode status)
{
    switch (status)
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
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl); \\

    try
    {
        return cl->attribute_type(space, name, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERDEX_CLIENT_EXCEPTION;
        cl->set_error_message("unhandled exception was thrown");
        return HYPERDATATYPE_GARBAGE;
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
    free(const_cast<hyperdex_client_attribute*>(attrs));
}

'''

WRAPPER_FOOT = '''
HYPERDEX_API int64_t
hyperdex_client_loop(hyperdex_client* _cl, int timeout,
                     hyperdex_client_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->loop(timeout, status);
    );
}

HYPERDEX_API int
hyperdex_client_poll(hyperdex_client* _cl)
{
    hyperdex_client_returncode _status;
    hyperdex_client_returncode* status = &_status;
    C_WRAP_EXCEPT(
    return cl->poll();
    );
}

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
'''
