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
#include "common/datatypes.h"
#include "common/ordered_encoding.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/index_int64.h"

using hyperdex::index_int64;

index_int64 :: index_int64()
{
}

index_int64 :: ~index_int64() throw ()
{
}

bool
index_int64 :: encoding_fixed()
{
    return true;
}

size_t
index_int64 :: encoded_size(const e::slice&)
{
    return sizeof(int64_t);
}

char*
index_int64 :: encode(const e::slice& decoded, char* encoded)
{
    return e::pack64be(datatype_info::lookup(HYPERDATATYPE_INT64)->hash(decoded), encoded);
}

size_t
index_int64 :: decoded_size(const e::slice&)
{
    return sizeof(int64_t);
}

char*
index_int64 :: decode(const e::slice& encoded, char* decoded)
{
    uint64_t x = 0;

    if (encoded.size() == sizeof(int64_t))
    {
        e::unpack64be(encoded.data(), &x);
    }

    int64_t number = ordered_decode_int64(x);
    return e::pack64le(number, decoded);
}
