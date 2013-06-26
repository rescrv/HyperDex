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
#include <tr1/memory>

// e
#include <e/buffer.h>
#include <e/slice.h>

// HyperDex
#include "namespace.h"
#include "hyperdex.h"
#include "common/schema.h"

BEGIN_HYPERDEX_NAMESPACE

enum funcall_t
{
    FUNC_FAIL,

    FUNC_SET,

    FUNC_STRING_APPEND,
    FUNC_STRING_PREPEND,

    FUNC_NUM_ADD,
    FUNC_NUM_SUB,
    FUNC_NUM_MUL,
    FUNC_NUM_DIV,
    FUNC_NUM_MOD,
    FUNC_NUM_AND,
    FUNC_NUM_OR,
    FUNC_NUM_XOR,

    FUNC_LIST_LPUSH,
    FUNC_LIST_RPUSH,

    FUNC_SET_ADD,
    FUNC_SET_REMOVE,
    FUNC_SET_INTERSECT,
    FUNC_SET_UNION,

    FUNC_MAP_ADD,
    FUNC_MAP_REMOVE
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
            std::auto_ptr<e::buffer>* backing,
            std::vector<e::slice>* new_value);

bool
operator < (const funcall& lhs, const funcall& rhs);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_funcall_h_
