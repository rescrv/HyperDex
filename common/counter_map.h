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

#ifndef hyperdex_common_counter_map_h_
#define hyperdex_common_counter_map_h_

// STL
#include <map>
#include <utility>
#include <vector>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

// The only thread-safe call is "lookup".  "adopt", "peek", and "take_max" all
// require external synchronization.

BEGIN_HYPERDEX_NAMESPACE

class counter_map
{
    public:
        counter_map();
        ~counter_map() throw ();

    public:
        void adopt(const std::vector<region_id>& ris);
        void peek(std::map<region_id, uint64_t>* ris);
        bool lookup(const region_id& ri, uint64_t* count);
        bool take_max(const region_id& ri, uint64_t count);

    private:
        counter_map(const counter_map&);
        counter_map& operator = (const counter_map&);

    private:
        std::vector<std::pair<region_id, uint64_t> > m_counters;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_counter_map_h_
