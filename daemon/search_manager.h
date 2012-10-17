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

#ifndef hyperdex_daemon_search_manager_h_
#define hyperdex_daemon_search_manager_h_

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "daemon/datalayer.h"
#include "daemon/reconfigure_returncode.h"
#include "hyperdaemon/logical.h"

namespace hyperdex
{
// Forward declarations
class daemon;

class search_manager
{
    public:
        search_manager(daemon*);
        ~search_manager() throw ();

    public:
        reconfigure_returncode prepare(const configuration& old_config,
                                       const configuration& new_config,
                                       const instance& us);
        reconfigure_returncode reconfigure(const configuration& old_config,
                                           const configuration& new_config,
                                           const instance& us);
        reconfigure_returncode cleanup(const configuration& old_config,
                                       const configuration& new_config,
                                       const instance& us);

    public:
#if 0
        void start(const entityid& us,
                   const entityid& client,
                   uint64_t search_num,
                   uint64_t nonce,
                   std::auto_ptr<e::buffer> msg,
                   const hyperspacehashing::search& wc);
        void next(const entityid& us,
                  const entityid& client,
                  uint64_t search_num,
                  uint64_t nonce);
        void stop(const entityid& us,
                  const entityid& client,
                  uint64_t search_num);
        void group_keyop(const entityid& us,
                         const entityid& client,
                         uint64_t nonce,
                         const hyperspacehashing::search& terms,
                         enum network_msgtype,
                         const e::slice& remain);
        void count(const entityid& us,
                   const entityid& client,
                   uint64_t nonce,
                   const hyperspacehashing::search& terms);
        void sorted_search(const entityid& us,
                           const entityid& client,
                           uint64_t nonce,
                           const hyperspacehashing::search& terms,
                           uint64_t num,
                           uint16_t sort_by,
                           bool maximize);
#endif

    private:
        class id;
        class state;

    private:
        search_manager(const search_manager&);
        search_manager& operator = (const search_manager&);

    private:
        static uint64_t hash(const id&);

    private:
        daemon* m_daemon;
        e::lockfree_hash_map<id, e::intrusive_ptr<state>, hash> m_searches;
};

} // namespace hyperdex

#endif // hyperdex_daemon_search_manager_h_
