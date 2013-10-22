import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '../..')

sys.path.append(os.path.join(BASE, 'bindings'))

import generator

header = '''// Copyright (c) 2013, Cornell University
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
        CSTRINGIFY(HYPERDEX_CLIENT_BADCONFIG);
        CSTRINGIFY(HYPERDEX_CLIENT_DUPLICATE);
        CSTRINGIFY(HYPERDEX_CLIENT_INTERRUPTED);
        CSTRINGIFY(HYPERDEX_CLIENT_CLUSTER_JUMP);
        CSTRINGIFY(HYPERDEX_CLIENT_COORD_LOGGED);
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

footer = '''
HYPERDEX_API int64_t
hyperdex_client_loop(hyperdex_client* _cl, int timeout,
                     hyperdex_client_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->loop(timeout, status);
    );
}

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
'''

def rename(x):
    return x.replace('enum ', '').replace('struct ', '')

def generate_func(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
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
        if generator.Predicates in x.args_in:
            args += ('checks', 'checks_sz')
        else:
            args += ('NULL', '0')
        if generator.Attributes in x.args_in:
            args += ('attrs', 'attrs_sz')
        else:
            args += ('NULL', '0')
        if generator.MapAttributes in x.args_in:
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

if __name__ == '__main__':
    with open(os.path.join(BASE, 'client/c.cc'), 'w') as fout:
        fout.write(header)
        fout.write('\n'.join([generate_func(c) for c in generator.Client]))
        fout.write(footer)
