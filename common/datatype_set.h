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

#ifndef hyperdex_common_datatype_set_h_
#define hyperdex_common_datatype_set_h_

// HyperDex
#include "namespace.h"
#include "common/datatype_info.h"

BEGIN_HYPERDEX_NAMESPACE

class datatype_set : public datatype_info
{
    public:
        datatype_set(datatype_info* elem);
        virtual ~datatype_set() throw ();

    public:
        virtual hyperdatatype datatype() const;
        virtual bool validate(const e::slice& value) const;
        virtual bool check_args(const funcall& func) const;
        virtual bool apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           e::arena* new_memory,
                           e::slice* new_value) const;

    public:
        virtual bool indexable() const;

    public:
        virtual bool has_length() const;
        virtual uint64_t length(const e::slice& value) const;

    public:
        virtual bool has_contains() const;
        virtual hyperdatatype contains_datatype() const;
        virtual bool contains(const e::slice& value, const e::slice& needle) const;

    private:
        datatype_set(const datatype_set&);
        datatype_set& operator = (const datatype_set&);

    private:
        datatype_info* m_elem;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_datatype_set_h_
