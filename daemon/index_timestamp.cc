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

// e
#include <e/endian.h>

// HyperDex
#include "common/datatype_info.h"
#include "common/ordered_encoding.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/index_timestamp.h"
#include <iomanip>
using hyperdex::index_timestamp;
using hyperdex::index_encoding_timestamp;

index_timestamp :: index_timestamp(hyperdatatype t)
    : index_primitive(index_encoding::lookup(t))
    , m_type(t)
{
    assert(CONTAINER_TYPE(m_type) == HYPERDATATYPE_TIMESTAMP_GENERIC);
}

index_timestamp :: ~index_timestamp() throw ()
{
}

hyperdatatype
index_timestamp :: datatype() const
{
    return m_type;
}

index_encoding_timestamp :: index_encoding_timestamp()
    : m_iei()
{
}

index_encoding_timestamp :: ~index_encoding_timestamp() throw ()
{
}

bool
index_encoding_timestamp :: encoding_fixed() const
{
    return m_iei.encoding_fixed();
}

size_t
index_encoding_timestamp :: encoded_size(const e::slice& x) const
{
    return m_iei.encoded_size(x);
}

char*
index_encoding_timestamp :: encode(const e::slice& decoded, char* encoded) const
{
    return m_iei.encode(decoded, encoded);
}

size_t
index_encoding_timestamp :: decoded_size(const e::slice& x) const
{
    return m_iei.decoded_size(x);
}

char*
index_encoding_timestamp :: decode(const e::slice& encoded, char* decoded) const
{
    return m_iei.decode(encoded, decoded);
}
