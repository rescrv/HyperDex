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


import collections


Call = collections.namedtuple('Call', ['name', 'form', 'args', 'ret'])


API = [Call(name='add_space', form='sync', args=('c-str'), ret='returncode'),
       Call(name='rm_space', form='sync', args=('c-str'), ret='returncode'),
       Call(name='get', form='async', args=('c-str', 'key'), ret='status-object'),
       Call(name='put', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_put', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='put_if_not_exist', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='del', form='async', args=('c-str', 'key'), ret='status'),
       Call(name='cond_del', form='async', args=('c-str', 'key', 'checks'), ret='status'),
       Call(name='atomic_add', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_add', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_sub', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_sub', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_mul', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_mul', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_div', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_div', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_mod', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_mod', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_and', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_and', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_or', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_or', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='atomic_xor', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_atomic_xor', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='string_prepend', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_string_prepend', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='string_append', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_string_append', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='list_lpush', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_list_lpush', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='list_rpush', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_list_rpush', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='set_add', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_set_add', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='set_remove', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_set_remove', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='set_intersect', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_set_intersect', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='set_union', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_set_union', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='map_add', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_add', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_remove', form='async', args=('c-str', 'key', 'attrs'), ret='status'),
       Call(name='cond_map_remove', form='async', args=('c-str', 'key', 'checks', 'attrs'), ret='status'),
       Call(name='map_atomic_add', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_add', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_sub', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_sub', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_mul', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_mul', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_div', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_div', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_mod', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_mod', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_and', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_and', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_or', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_or', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_atomic_xor', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_atomic_xor', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_string_prepend', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_string_prepend', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='map_string_append', form='async', args=('c-str', 'key', 'mapattrs'), ret='status'),
       Call(name='cond_map_string_append', form='async', args=('c-str', 'key', 'checks', 'mapattrs'), ret='status'),
       Call(name='search', form='iterator', args=('c-str', 'checks'), ret='status-object'),
       Call(name='search_describe', form='async', args=('c-str', 'checks'), ret='status-c-str'),
       Call(name='sorted_search', form='iterator', args=('c-str', 'checks', 'c-str', 'uint64', 'max/min'), ret='status-object'),
       Call(name='group_del', form='async', args=('c-str', 'checks'), ret='status'),
       Call(name='count', form='async', args=('c-str', 'checks'), ret='status-uint64')]


def generate_c():
    fout = open('client/hyperclient.cc', 'w')
    fout.write('''// Copyright (c) 2013, Cornell University
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

// HyperDex
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"
#include "client/hyperclient.h"

#define C_WRAP_EXCEPT(X) \\
    try \\
    { \\
        X \\
    } \\
    catch (po6::error& e) \\
    { \\
        errno = e; \\
        *status = HYPERCLIENT_EXCEPTION; \\
        return -1; \\
    } \\
    catch (std::bad_alloc& ba) \\
    { \\
        errno = ENOMEM; \\
        *status = HYPERCLIENT_NOMEM; \\
        return -1; \\
    } \\
    catch (...) \\
    { \\
        *status = HYPERCLIENT_EXCEPTION; \\
        return -1; \\
    }

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

HYPERDEX_API struct hyperclient*
hyperclient_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<struct hyperclient*>(new hyperdex::client(coordinator, port));
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
hyperclient_destroy(struct hyperclient* client)
{
    delete reinterpret_cast<hyperdex::client*>(client);
}

HYPERDEX_API void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t /*attrs_sz*/)
{
    free(attrs);
}
''')
    for call in API:
        if call.form == 'sync' and \
           call.args == 'c-str' and \
           call.ret == 'returncode':
            fout.write('''
HYPERDEX_API enum hyperclient_returncode
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* str)
{{
    try
    {{
        hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
        return cl->{0}(str);
    }}
    catch (po6::error& e)
    {{
        errno = e;
        return HYPERCLIENT_EXCEPTION;
    }}
    catch (std::bad_alloc& ba)
    {{
        errno = ENOMEM;
        return HYPERCLIENT_NOMEM;
    }}
    catch (...)
    {{
        return HYPERCLIENT_EXCEPTION;
    }}
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, NULL, 0, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.name == 'get' and \
             call.form == 'async' and \
             call.args == ('c-str', 'key') and \
             call.ret == 'status-object':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} enum hyperclient_returncode* status,
            {1} struct hyperclient_attribute** attrs, size_t* attrs_sz)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, key, key_sz, status, attrs, attrs_sz);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, NULL, 0, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'attrs') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} const struct hyperclient_attribute* attrs, size_t attrs_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks', 'attrs') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} const struct hyperclient_attribute* attrs, size_t attrs_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'mapattrs') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks', 'mapattrs') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const char* key, size_t key_sz,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR({0}), strlen(XSTR({0})));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'iterator' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-object':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} enum hyperclient_returncode* status,
            {1} struct hyperclient_attribute** attrs, size_t* attrs_sz)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, checks, checks_sz, status, attrs, attrs_sz);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-c-str':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} enum hyperclient_returncode* status,
            {1} const char** str)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, checks, checks_sz, status, str);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'iterator' and \
             call.args == ('c-str', 'checks', 'c-str', 'uint64', 'max/min') and \
             call.ret == 'status-object':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} const char* sort_by, uint64_t limit, int maximize,
            {1} enum hyperclient_returncode* status,
            {1} struct hyperclient_attribute** attrs, size_t* attrs_sz)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, checks, checks_sz, sort_by, limit, maximize, status, attrs, attrs_sz);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} enum hyperclient_returncode* status)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, checks, checks_sz, status);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-uint64':
            fout.write('''
HYPERDEX_API int64_t
hyperclient_{0}(struct hyperclient* _cl,
            {1} const char* space,
            {1} const struct hyperclient_attribute_check* checks, size_t checks_sz,
            {1} enum hyperclient_returncode* status, uint64_t* result)
{{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->{0}(space, checks, checks_sz, status, result);
    );
}}\n'''.format(call.name, ' ' * len(call.name)))
        else:
            print "c doesn't support", call.name
    fout.write('''
HYPERDEX_API int64_t
hyperclient_loop(struct hyperclient* client, int timeout,
                 enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(client);
    return cl->loop(timeout, status);
    );
}

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
''')
    fout.flush()
    fout.close()


def generate_cc():
    fout = open('client/hyperclient.hpp', 'w')
    fout.write('''// Copyright (c) 2013, Cornell University
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

#ifndef hyperclient_hpp_
#define hyperclient_hpp_

// HyperDex
#include <hyperclient.h>

class HyperClient
{
    public:
        HyperClient(const char* coordinator, uint16_t port)
            : m_cl(hyperclient_create(coordinator, port))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        ~HyperClient() throw ()
        {
            hyperclient_destroy(m_cl);
        }

    public:
''')
    for call in API:
        if call.form == 'sync' and \
           call.args == 'c-str' and \
           call.ret == 'returncode':
            fout.write('''        enum hyperclient_returncode {0}(const char* str)
            {{ return hyperclient_{0}(m_cl, str); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key') and \
             call.ret == 'status-object':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}hyperclient_returncode* status,
                 {1}struct hyperclient_attribute** attrs, size_t* attrs_sz)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, status, attrs, attrs_sz); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, checks, checks_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'attrs') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}const struct hyperclient_attribute* attrs, size_t attrs_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, attrs, attrs_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks', 'attrs') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}const struct hyperclient_attribute* attrs, size_t attrs_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'mapattrs') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'key', 'checks', 'mapattrs') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space, const char* key, size_t key_sz,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                 {1}hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'iterator' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-object':
            fout.write('''        int64_t {0}(const char* space,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}enum hyperclient_returncode* status,
                 {1}struct hyperclient_attribute** attrs, size_t* attrs_sz)
            {{ return hyperclient_{0}(m_cl, space, checks, checks_sz, status, attrs, attrs_sz); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-c-str':
            fout.write('''        int64_t {0}(const char* space,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}enum hyperclient_returncode* status, const char** str)
            {{ return hyperclient_{0}(m_cl, space, checks, checks_sz, status, str); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'iterator' and \
             call.args == ('c-str', 'checks', 'c-str', 'uint64', 'max/min') and \
             call.ret == 'status-object':
            fout.write('''        int64_t {0}(const char* space,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}const char* sort_by, uint64_t limit, int maximize,
                 {1}enum hyperclient_returncode* status,
                 {1}struct hyperclient_attribute** attrs, size_t* attrs_sz)
            {{ return hyperclient_{0}(m_cl, space, checks, checks_sz, sort_by, limit, maximize, status, attrs, attrs_sz); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status':
            fout.write('''        int64_t {0}(const char* space,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}enum hyperclient_returncode* status)
            {{ return hyperclient_{0}(m_cl, space, checks, checks_sz, status); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        elif call.form == 'async' and \
             call.args == ('c-str', 'checks') and \
             call.ret == 'status-uint64':
            fout.write('''        int64_t {0}(const char* space,
                 {1}const struct hyperclient_attribute_check* checks, size_t checks_sz,
                 {1}enum hyperclient_returncode* status, uint64_t* result)
            {{ return hyperclient_{0}(m_cl, space, checks, checks_sz, status, result); }}\n'''
            .format(call.name, ' ' * len(call.name)))
        else:
            print "cc doesn't support", call.name
    fout.write('''
    public:
        int64_t loop(int timeout, hyperclient_returncode* status)
            { return hyperclient_loop(m_cl, timeout, status); }

    private:
        HyperClient(const HyperClient&);
        HyperClient& operator = (const HyperClient&);

    private:
        hyperclient* m_cl;
};

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs);

#endif // hyperclient_hpp_
''')
    fout.flush()
    fout.close()


if __name__ == '__main__':
    import os
    import sys
    if not os.path.exists('client/hyperclient.h'):
        print 'run from the top level of the HyperDex repo'
    for lang in sys.argv[1:]:
        if 'generate_' + lang in globals():
            func = globals()['generate_' + lang]
            func()
        else:
            print lang, 'is not yet supported'
