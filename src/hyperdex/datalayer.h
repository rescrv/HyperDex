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

// STL
#include <map>
#include <set>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/threads/rwlock.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/ids.h>
#include <hyperdex/region.h>
#include <hyperdex/result_t.h>

namespace hyperdex
{

class datalayer
{
    public:
        datalayer();
        ~datalayer() throw ();

    // Space operations.
    public:
        std::set<regionid> regions();
        void create(const regionid& ri, uint16_t numcolumns);
        void drop(const regionid& ri);

    // Key-Value store operations.
    public:
        result_t get(const regionid& ri, const e::buffer& key, std::vector<e::buffer>* value, uint64_t* version);
        result_t put(const regionid& ri, const e::buffer& key, const std::vector<e::buffer>& value, uint64_t version);
        result_t del(const regionid& ri, const e::buffer& key);

    private:
        datalayer(const datalayer&);

    private:
        e::intrusive_ptr<hyperdex::region> get_region(const regionid& ri);

    private:
        datalayer& operator = (const datalayer&);

    private:
        po6::threads::rwlock m_lock;
        std::map<regionid, e::intrusive_ptr<hyperdex::region> > m_regions;
};

} // namespace hyperdex

#endif // hyperdex_datalayer_h_
