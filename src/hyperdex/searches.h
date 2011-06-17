// Copyright (c) 2011, Cornell University
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

#ifndef hyperdex_searches_h_
#define hyperdex_searches_h_

// po6
#include <po6/threads/mutex.h>

// e
#include <e/map.h>

// HyperDex
#include <hyperdex/datalayer.h>
#include <hyperdex/ids.h>
#include <hyperdex/logical.h>
#include <hyperdex/search.h>

namespace hyperdex
{

class searches
{
    public:
        searches(datalayer* data, logical* comm);
        ~searches() throw ();

    public:
        void start(const entityid& client, uint32_t nonce, const regionid& r, const search& s);
        void next(const entityid& client, uint32_t nonce);
        void stop(const entityid& client, uint32_t nonce);

    private:
        class search_state
        {
            public:
                search_state(const regionid& region,
                             const search& terms,
                             e::intrusive_ptr<region::snapshot> snap);
                ~search_state() throw ();

            public:
                po6::threads::mutex lock;
                const regionid& region;
                const uint64_t point;
                const uint64_t mask;
                const search terms;
                e::intrusive_ptr<region::snapshot> snap;
                uint64_t count;

            private:
                friend class e::intrusive_ptr<search_state>;

            private:
                size_t m_ref;
        };

    private:
        searches(const searches&);

    private:
        searches& operator = (const searches&);

    private:
        datalayer* m_data;
        logical* m_comm;
        e::map<std::pair<entityid, uint32_t>, e::intrusive_ptr<search_state> > m_searches;
};

} // namespace hyperdex

#endif // hyperdex_searches_h_
