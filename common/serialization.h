// Copyright (c) 2012, Cornell University
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

#ifndef hyperdex_common_serialization_h_
#define hyperdex_common_serialization_h_

// po6
#include <po6/net/location.h>
#include <po6/net/hostname.h>

// e
#include <e/buffer.h>

// HyperDex
#include "namespace.h"
#include "hyperdex.h"
#include "common/attribute_check.h"
#include "common/funcall.h"

BEGIN_HYPERDEX_NAMESPACE

e::buffer::packer
operator << (e::buffer::packer lhs, const po6::net::ipaddr& rhs);
e::unpacker
operator >> (e::unpacker lhs, po6::net::ipaddr& rhs);
size_t
pack_size(const po6::net::ipaddr& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const po6::net::location& rhs);
e::unpacker
operator >> (e::unpacker lhs, po6::net::location& rhs);
size_t
pack_size(const po6::net::location& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const po6::net::hostname& rhs);
e::unpacker
operator >> (e::unpacker lhs, po6::net::hostname& rhs);
size_t
pack_size(const po6::net::hostname& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const attribute_check& rhs);
e::unpacker
operator >> (e::unpacker lhs, attribute_check& rhs);
size_t
pack_size(const attribute_check& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const funcall_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, funcall_t& rhs);
size_t
pack_size(const funcall_t& f);

e::buffer::packer
operator << (e::buffer::packer lhs, const funcall& rhs);
e::unpacker
operator >> (e::unpacker lhs, funcall& rhs);
size_t
pack_size(const funcall& f);

e::buffer::packer
operator << (e::buffer::packer lhs, const hyperdatatype& rhs);
e::unpacker
operator >> (e::unpacker lhs, hyperdatatype& rhs);
size_t
pack_size(const hyperdatatype& h);

e::buffer::packer
operator << (e::buffer::packer lhs, const hyperpredicate& rhs);
e::unpacker
operator >> (e::unpacker lhs, hyperpredicate& rhs);
size_t
pack_size(const hyperpredicate& p);

inline size_t
pack_size(uint64_t) { return sizeof(uint64_t); }

size_t
pack_size(const e::slice& s);

template <typename T>
size_t
pack_size(const std::vector<T>& v)
{
    size_t sz = sizeof(uint32_t);

    for (size_t i = 0; i < v.size(); ++i)
    {
        sz += pack_size(v[i]);
    }

    return sz;
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_serialization_h_
