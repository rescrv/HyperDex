// Copyright (c) 2011-2012, Cornell University
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

#ifndef hyperdex_client_util_h_
#define hyperdex_client_util_h_

// e
#include <e/error.h>

// HyperDex
#include <hyperdex/client.h>
#include "namespace.h"
#include "common/configuration.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

// Convert the key and value vector returned by entity to an array of
// hyperdex_attribute using the given configuration.
bool
value_to_attributes(const configuration& config,
                    const region_id& rid,
                    const uint8_t* key,
                    size_t key_sz,
                    const std::vector<e::slice>& value,
                    hyperdex_client_returncode* op_status,
                    e::error* op_error,
                    const hyperdex_client_attribute** attrs,
                    size_t* attrs_sz,
                    bool convert_types);

bool
value_to_attributes(const configuration& config,
                    const region_id& rid,
                    const std::vector<std::pair<uint16_t, e::slice> >& value,
                    hyperdex_client_returncode* op_status,
                    e::error* op_error,
                    const hyperdex_client_attribute** attrs,
                    size_t* attrs_sz,
                    bool convert_types);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_util_h_
