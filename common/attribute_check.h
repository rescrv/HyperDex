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

#ifndef hyperdex_common_attribute_check_h_
#define hyperdex_common_attribute_check_h_

// e
#include <e/slice.h>

// HyperDex
#include "namespace.h"
#include "hyperdex.h"
#include "common/schema.h"

BEGIN_HYPERDEX_NAMESPACE

class attribute_check
{
    public:
        attribute_check();
        ~attribute_check() throw ();

    public:
        uint16_t attr;
        e::slice value;
        hyperdatatype datatype;
        hyperpredicate predicate;
};

bool
validate_attribute_check(const schema& sc,
                         const attribute_check& chk);

size_t
validate_attribute_checks(const schema& sc,
                          const std::vector<hyperdex::attribute_check>& checks);

bool
passes_attribute_check(const schema& sc,
                       const attribute_check& chk,
                       const e::slice& value);

size_t
passes_attribute_checks(const schema& sc,
                        const std::vector<hyperdex::attribute_check>& checks,
                        const e::slice& key,
                        const std::vector<e::slice>& value);

bool
operator < (const attribute_check& lhs,
            const attribute_check& rhs);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_attribute_check_h_
