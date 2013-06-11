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
#include "daemon/index_float.h"

using hyperdex::index_float;

index_float :: index_float()
{
}

index_float :: ~index_float() throw ()
{
}

bool
index_float :: encoding_fixed()
{
    return true;
}

size_t
index_float :: encoded_size(const e::slice&)
{
    return 2 * sizeof(double);
}

char*
index_float :: encode(const e::slice& decoded, char* encoded)
{
    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    double number = 0;

    if (di->validate(decoded) && decoded.size() == sizeof(double))
    {
        e::unpackdoublele(decoded.data(), &number);
    }

    char* ptr = encoded;
    ptr = e::pack64be(di->hash(decoded), ptr);
    ptr = e::packdoublele(number, ptr);
    return ptr;
}

size_t
index_float :: decoded_size(const e::slice&)
{
    return sizeof(double);
}

char*
index_float :: decode(const e::slice& encoded, char* decoded)
{
    double number = 0;

    if (encoded.size() == 2 * sizeof(double))
    {
        e::unpackdoublele(decoded + sizeof(double), &number);
    }

    return e::packdoublele(number, decoded);
}
