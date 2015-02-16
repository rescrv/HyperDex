// Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_daemon_index_int64_h_
#define hyperdex_daemon_index_int64_h_

// HyperDex
#include "namespace.h"
#include "daemon/index_primitive.h"

BEGIN_HYPERDEX_NAMESPACE

class index_int64 : public index_primitive
{
    public:
        index_int64();
        virtual ~index_int64() throw ();

    public:
        virtual hyperdatatype datatype() const;
};

class index_encoding_int64 : public index_encoding
{
    public:
        index_encoding_int64();
        virtual ~index_encoding_int64() throw ();

    public:
        virtual bool encoding_fixed() const;
        virtual size_t encoded_size(const e::slice& decoded) const;
        virtual char* encode(const e::slice& decoded, char* encoded) const;
        virtual size_t decoded_size(const e::slice& encoded) const;
        virtual char* decode(const e::slice& encoded, char* decoded) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_index_int64_h_
