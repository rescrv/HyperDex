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
import bindings.cc

################################ Code Generation ###############################

def generate_func(x, lib, struct=None, sep='\n', namesep='_', padd=None):
    assert x.form in (bindings.AsyncCall, bindings.SyncCall,
                      bindings.NoFailCall, bindings.Iterator)
    struct = struct or lib
    return_type = 'int64_t'
    if x.form == bindings.SyncCall:
        return_type = 'int'
    if x.form == bindings.NoFailCall:
        return_type = 'void'
    func = '        ' + return_type + ' ' + x.name + '('
    padd = ' ' * len(func) if padd is None else ' ' * padd
    first = True
    for arg in x.args_in:
        if not first:
            func += ',\n' + padd
        first = False
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        if not first:
            func += ',\n' + padd
        first = False
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ')\n'
    func = func.replace('struct ', '')
    func = func.replace('enum ', '')
    name = 'hyperdex_' + lib + namesep + x.name
    func += ' ' * 12 + '{ return ' + name + '(m_cl'
    for arg in x.args_in:
        for p, n in arg.args:
            func += ', ' + n
    for arg in x.args_out:
        for p, n in arg.args:
            func += ', ' + n
    func += '); }'
    return func

############################### Output Generators ##############################

def generate_client_header():
    fout = open(os.path.join(BASE, 'include/hyperdex/client.hpp'), 'w')
    fout.write(bindings.copyright('/', '2013-2015'))
    fout.write(bindings.cc.CLIENT_HEADER_HEAD)
    fout.write('\n'.join([generate_func(c, 'client') for c in bindings.Client
        if not c.form is bindings.MicrotransactionCall]))
    fout.write(bindings.cc.CLIENT_HEADER_FOOT)

if __name__ == '__main__':
    generate_client_header()

################################### Templates ##################################

CLIENT_HEADER_HEAD = '''
#ifndef hyperdex_client_hpp_
#define hyperdex_client_hpp_

// C++
#include <iostream>

// HyperDex
#include <hyperdex/client.h>

namespace hyperdex
{

class Client
{
    public:
        Client(const char* coordinator, uint16_t port)
            : m_cl(hyperdex_client_create(coordinator, port))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        Client(const char* conn_str)
            : m_cl(hyperdex_client_create_conn_str(conn_str))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        ~Client() throw ()
        {
            hyperdex_client_destroy(m_cl);
        }

    public:
'''

CLIENT_HEADER_FOOT = '''

    public:
        void clear_auth_context()
            { return hyperdex_client_clear_auth_context(m_cl); }
        void set_auth_context(const char** macaroons, size_t macaroons_sz)
            { return hyperdex_client_set_auth_context(m_cl, macaroons, macaroons_sz); }

    public:
        int64_t loop(int timeout, hyperdex_client_returncode* status)
            { return hyperdex_client_loop(m_cl, timeout, status); }
        int poll_fd()
            { return hyperdex_client_poll_fd(m_cl); }
        int block(int timeout)
            { return hyperdex_client_block(m_cl, timeout); }
        std::string error_message()
            { return hyperdex_client_error_message(m_cl); }
        std::string error_location()
            { return hyperdex_client_error_location(m_cl); }

    private:
        Client(const Client&);
        Client& operator = (const Client&);

    private:
        hyperdex_client* m_cl;
};

} // namespace hyperdex

std::ostream&
operator << (std::ostream& lhs, hyperdex_client_returncode rhs);

#endif // hyperdex_client_hpp_
'''
