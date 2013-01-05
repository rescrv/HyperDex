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

#ifndef hyperdex_coordinator_coordinator_h_
#define hyperdex_coordinator_coordinator_h_

// C
#include <cstdlib>

// STL
#include <list>
#include <memory>
#include <utility>
#include <vector>

// po6
#include <po6/net/location.h>

// Replicant
#include <replicant_state_machine.h>

// HyperDex
#include "common/hyperspace.h"
#include "common/server_id.h"

namespace hyperdex
{

class coordinator
{
    public:
        coordinator();
        ~coordinator() throw ();

    public:
        void initial_layout(space* s);
        void regenerate(struct replicant_state_machine_context* ctx);

    public:
        uint64_t version;
        uint64_t counter;
        std::auto_ptr<e::buffer> latest_config;
        std::vector<std::pair<server_id, po6::net::location> > servers;
        std::list<space> spaces;
        char resp[sizeof(uint16_t)];

    private:
        drand48_data m_seed;
};

} // namespace hyperdex

#endif // hyperdex_coordinator_coordinator_h_
