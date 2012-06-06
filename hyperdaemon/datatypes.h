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

#ifndef hyperdaemon_datatypes_h_
#define hyperdaemon_datatypes_h_

// C
#include <cstdlib>

// HyperDex
#include "hyperdex/hyperdex/microop.h"
#include "hyperdex/hyperdex/network_constants.h"

namespace hyperdaemon
{

// Utilities for manipulating and verifying datastructures

// Validate that the bytestring conforms to the given type
bool
validate_datatype(hyperdatatype datatype, const e::slice& data);

// How much more space does the microop need in the contiguous buffer?
size_t
sizeof_microop(const hyperdex::microop& op);

// Modify the old_value by applying ops sequentially.
uint8_t*
apply_microops(hyperdatatype type,
               const e::slice& old_value,
               hyperdex::microop* ops,
               size_t num_ops,
               uint8_t* writeto,
               hyperdex::network_returncode* error);

} // namespace hyperdaemon

#endif // hyperdaemon_replication_manager_h_
