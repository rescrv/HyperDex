import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '../..')

sys.path.append(os.path.join(BASE, 'bindings'))

import generator

header = '''/* Copyright (c) 2011-2013, Cornell University
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

/* HyperClient returncode occupies [8448, 8576) */
enum hyperdex_client_returncode
{
    HYPERDEX_CLIENT_SUCCESS      = 8448,
    HYPERDEX_CLIENT_NOTFOUND     = 8449,
    HYPERDEX_CLIENT_SEARCHDONE   = 8450,
    HYPERDEX_CLIENT_CMPFAIL      = 8451,
    HYPERDEX_CLIENT_READONLY     = 8452,

    /* Error conditions */
    HYPERDEX_CLIENT_UNKNOWNSPACE = 8512,
    HYPERDEX_CLIENT_COORDFAIL    = 8513,
    HYPERDEX_CLIENT_SERVERERROR  = 8514,
    HYPERDEX_CLIENT_POLLFAILED   = 8515,
    HYPERDEX_CLIENT_OVERFLOW     = 8516,
    HYPERDEX_CLIENT_RECONFIGURE  = 8517,
    HYPERDEX_CLIENT_TIMEOUT      = 8519,
    HYPERDEX_CLIENT_UNKNOWNATTR  = 8520,
    HYPERDEX_CLIENT_DUPEATTR     = 8521,
    HYPERDEX_CLIENT_NONEPENDING  = 8523,
    HYPERDEX_CLIENT_DONTUSEKEY   = 8524,
    HYPERDEX_CLIENT_WRONGTYPE    = 8525,
    HYPERDEX_CLIENT_NOMEM        = 8526,
    HYPERDEX_CLIENT_BADCONFIG    = 8527,
    HYPERDEX_CLIENT_DUPLICATE    = 8529,
    HYPERDEX_CLIENT_INTERRUPTED  = 8530,
    HYPERDEX_CLIENT_CLUSTER_JUMP = 8531,
    HYPERDEX_CLIENT_COORD_LOGGED = 8532,
    HYPERDEX_CLIENT_OFFLINE      = 8533,

    /* This should never happen.  It indicates a bug */
    HYPERDEX_CLIENT_INTERNAL     = 8573,
    HYPERDEX_CLIENT_EXCEPTION    = 8574,
    HYPERDEX_CLIENT_GARBAGE      = 8575
};

struct hyperdex_client*
hyperdex_client_create(const char* coordinator, uint16_t port);
void
hyperdex_client_destroy(struct hyperdex_client* client);

'''

footer = '''
int64_t
hyperdex_client_loop(struct hyperdex_client* client, int timeout,
                     enum hyperdex_client_returncode* status);

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

def generate_func(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    func = 'int64_t\nhyperdex_client_%s(struct hyperdex_client* client' % x.name
    padd = ' ' * (len('hyperdex_client_') + len(x.name) + 1)
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ');\n'
    return func

if __name__ == '__main__':
    with open(os.path.join(BASE, 'include/hyperdex/client.h'), 'w') as fout:
        fout.write(header)
        fout.write('\n'.join([generate_func(c) for c in generator.Client]))
        fout.write(footer)
