// Copyright (c) 2014, Cornell University
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

#ifndef hyperdex_common_index_h_
#define hyperdex_common_index_h_

// e
#include <e/buffer.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/range_searches.h"

BEGIN_HYPERDEX_NAMESPACE

class index
{
    public:
        enum index_t { NORMAL, DOCUMENT };

    public:
        index();
        index(index_t t, index_id i, uint16_t a, const e::slice& e);
        ~index() throw ();

    public:
        index& operator = (const index&);

    public:
        index_t type;
        index_id id;
        uint16_t attr;
        e::slice extra;
};

std::ostream&
operator << (std::ostream& lhs, const index& rhs);

e::packer
operator << (e::packer, const index& t);
e::unpacker
operator >> (e::unpacker, index& t);
size_t
pack_size(const index& t);

e::packer
operator << (e::packer, const index::index_t& t);
e::unpacker
operator >> (e::unpacker, index::index_t& t);
size_t
pack_size(const index::index_t& t);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_index_h_
