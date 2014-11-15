// Copyright (c) 2011-2014, Cornell University
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

#ifndef hyperdex_client_request_h_
#define hyperdex_client_request_h_

#include "hyperdex/client.h"
#include "client/client.h"

BEGIN_HYPERDEX_NAMESPACE

// Use this prepare an atomic request
// Can only be used once, i.e. create one for each funcall
class request
{
protected:
    request(client& cl_, const coordinator_link& coord_, const std::string& space_);
    virtual ~request() {};

    // Get a reference to the schema of the space we are using
    // Throws an exception if non existant (i.e. no such space)
    const schema& get_schema() const throw(std::runtime_error);

    size_t prepare_checks(const schema& sc,
                              const hyperdex_client_attribute_check* chks, size_t chks_sz,
                              hyperdex_client_returncode& status,
                              std::vector<attribute_check>* checks);

    client& cl;
    const coordinator_link& coord;
    const std::string space;

    client::arena_t allocate;
};

END_HYPERDEX_NAMESPACE

#endif // header guard

