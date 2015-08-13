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

#ifndef hyperdex_common_funcall_h_
#define hyperdex_common_funcall_h_

// C
#include <stdint.h>

// STL
#include <memory>

// e
#include <e/arena.h>
#include <e/slice.h>
#include <e/serialization.h>

// HyperDex
#include "namespace.h"
#include "hyperdex.h"
#include "common/schema.h"

BEGIN_HYPERDEX_NAMESPACE

enum funcall_t
{
    FUNC_FAIL = 0,

    FUNC_SET = 1,

    FUNC_STRING_APPEND  = 2,
    FUNC_STRING_PREPEND = 3,
    FUNC_STRING_LTRIM   = 24,
    FUNC_STRING_RTRIM   = 25,

    FUNC_NUM_ADD = 4,
    FUNC_NUM_SUB = 5,
    FUNC_NUM_MUL = 6,
    FUNC_NUM_DIV = 7,
    FUNC_NUM_MOD = 8,
    FUNC_NUM_AND = 9,
    FUNC_NUM_OR  = 10,
    FUNC_NUM_XOR = 11,
    FUNC_NUM_MAX = 12,
    FUNC_NUM_MIN = 13,

    FUNC_LIST_LPUSH = 14,
    FUNC_LIST_RPUSH = 15,

    FUNC_SET_ADD       = 16,
    FUNC_SET_REMOVE    = 17,
    FUNC_SET_INTERSECT = 18,
    FUNC_SET_UNION     = 19,

    FUNC_MAP_ADD    = 20,
    FUNC_MAP_REMOVE = 21,

    FUNC_DOC_RENAME = 22,
    FUNC_DOC_UNSET  = 23
};

class funcall
{
    public:
        funcall();
        ~funcall() throw ();

    public:
        uint16_t attr;
        funcall_t name;
        e::slice arg1;
        hyperdatatype arg1_datatype;
        e::slice arg2;
        hyperdatatype arg2_datatype;
};

bool
validate_func(const schema& sc, const funcall& func);

size_t
validate_funcs(const schema& sc,
               const std::vector<funcall>& funcs);

size_t
apply_funcs(const schema& sc,
            const std::vector<funcall>& funcs,
            const e::slice& key,
            const std::vector<e::slice>& old_value,
            e::arena* new_memory,
            std::vector<e::slice>* new_value);

bool
operator < (const funcall& lhs, const funcall& rhs);

e::packer
operator << (e::packer lhs, const funcall_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, funcall_t& rhs);
size_t
pack_size(const funcall_t& f);

e::packer
operator << (e::packer lhs, const funcall& rhs);
e::unpacker
operator >> (e::unpacker lhs, funcall& rhs);
size_t
pack_size(const funcall& f);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_funcall_h_
