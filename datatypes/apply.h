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

#ifndef datatypes_apply_h_
#define datatypes_apply_h_

// STL
#ifdef _MSC_VER
#include <memory>
#else
#include <tr1/memory>
#endif

// e
#include <e/buffer.h>
#include <e/slice.h>

// HyperDex
#include "hyperdex.h"
#include "common/attribute_check.h"
#include "common/funcall.h"
#include "common/schema.h"
#include "datatypes/microerror.h"

bool
passes_attribute_check(hyperdatatype type,
                       const hyperdex::attribute_check& check,
                       const e::slice& value,
                       microerror* error);

size_t
perform_checks_and_apply_funcs(const hyperdex::schema* sc,
                               const std::vector<hyperdex::attribute_check>& checks,
                               const std::vector<hyperdex::funcall>& funcs,
                               const e::slice& key,
                               const std::vector<e::slice>& old_value,
                               std::tr1::shared_ptr<e::buffer>* backing,
                               std::vector<e::slice>* new_value,
                               microerror* error);

#endif // datatypes_apply_h_
