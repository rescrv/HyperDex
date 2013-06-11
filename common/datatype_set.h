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
#include "common/datatypes.h"

namespace hyperdex
{

class datatype_set : public datatype_info
{
    public:
        datatype_set(datatype_info* elem);
        virtual ~datatype_set() throw ();

    public:
        virtual hyperdatatype datatype();
        virtual bool validate(const e::slice& value);
        virtual bool check_args(const funcall& func);
        virtual uint8_t* apply(const e::slice& old_value,
                               const funcall* funcs, size_t funcs_sz,
                               uint8_t* writeto);

    public:
        virtual bool indexable();

    public:
        virtual bool has_length();
        virtual uint64_t length(const e::slice& value);
        virtual bool has_contains();
        virtual hyperdatatype contains_datatype();
        virtual bool contains(const e::slice& value, const e::slice& needle);

    private:
        datatype_set(const datatype_set&);
        datatype_set& operator = (const datatype_set&);

    private:
        datatype_info* m_elem;
};

} // namespace hyperdex

#endif // hyperdex_common_datatype_set_h_
