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

#ifndef hyperdex_microop_h_
#define hyperdex_microop_h_

// e
#include <e/buffer.h>
#include <e/slice.h>

// HyperDex
#include "hyperdex.h"

namespace hyperdex
{

enum microop_type
{
    OP_FAIL,

    OP_SET,

    OP_STRING_APPEND,
    OP_STRING_PREPEND,

    OP_INT64_ADD,
    OP_INT64_SUB,
    OP_INT64_MUL,
    OP_INT64_DIV,
    OP_INT64_MOD,
    OP_INT64_AND,
    OP_INT64_OR,
    OP_INT64_XOR,

    OP_LIST_LPUSH,
    OP_LIST_RPUSH,

    OP_SET_ADD,
    OP_SET_REMOVE,
    OP_SET_INTERSECT,
    OP_SET_UNION,

    OP_MAP_ADD,
    OP_MAP_REMOVE
};

class microop
{
    public:
        microop();

    public:
        uint16_t attr;
        hyperdatatype type;
        microop_type action;
        e::slice arg1;
        e::slice arg2;
};

e::buffer::packer
operator << (e::buffer::packer lhs, const microop& rhs);

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, microop& rhs);

} // namespace hyperdex

#endif // hyperdex_microop_h_
