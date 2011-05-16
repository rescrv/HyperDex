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

#ifndef hyperdex_datalayer_h_
#define hyperdex_datalayer_h_

// C
#include <stdint.h>

// STL
#include <map>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/zone.h>

namespace hyperdex
{

class datalayer
{
    public:
        datalayer();

    // Table operations.
    public:
        void create(uint32_t table, uint16_t subspace, uint8_t prefix,
                    uint64_t mask, uint16_t numcolumns);
        void drop(uint32_t table, uint16_t subspace, uint8_t prefix,
                  uint64_t mask);

    // Key-Value store operations.
    public:
        bool get(uint32_t table, uint16_t subspace, uint8_t prefix, uint64_t mask,
                 const e::buffer& key, std::vector<e::buffer>* value);
        bool put(uint32_t table, uint16_t subspace, uint8_t prefix, uint64_t mask,
                 const e::buffer& key, const std::vector<e::buffer>& value);
        bool del(uint32_t table, uint16_t subspace, uint8_t prefix, uint64_t mask,
                 const e::buffer& key);

    private:
        struct zoneid
        {
            zoneid(uint32_t t, uint16_t s, uint8_t p, uint64_t m)
                : table(t), subspace(s), prefix(p), mask(m) {}

            bool operator < (const zoneid& other) const;
            bool operator == (const zoneid& other) const;
            bool operator != (const zoneid& other) const;

            const uint32_t table;
            const uint16_t subspace;
            const uint8_t prefix;
            const uint64_t mask;
        };


    private:
        datalayer(const datalayer&);

    private:
        datalayer& operator = (const datalayer&);

    private:
        po6::threads::rwlock m_lock;
        std::map<zoneid, std::tr1::shared_ptr<hyperdex::zone> > m_zones;
};

} // namespace hyperdex

#endif // hyperdex_datalayer_h_
