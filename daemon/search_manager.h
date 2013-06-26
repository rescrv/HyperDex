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
#include <e/lockfree_hash_map.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "daemon/datalayer.h"
#include "daemon/reconfigure_returncode.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class search_manager
{
    public:
        search_manager(daemon*);
        ~search_manager() throw ();

    public:
        bool setup();
        void teardown();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);

    public:
        void start(const server_id& from,
                   const virtual_server_id& to,
                   std::auto_ptr<e::buffer> msg,
                   uint64_t nonce,
                   uint64_t search_id,
                   std::vector<attribute_check>* checks);
        void next(const server_id& from,
                  const virtual_server_id& to,
                  uint64_t nonce,
                  uint64_t search_id);
        void stop(const server_id& from,
                  const virtual_server_id& to,
                  uint64_t search_id);
        void sorted_search(const server_id& from,
                           const virtual_server_id& to,
                           uint64_t nonce,
                           std::vector<attribute_check>* checks,
                           uint64_t limit,
                           uint16_t sort_by,
                           bool maximize);
        void group_keyop(const server_id& from,
                         const virtual_server_id& to,
                         uint64_t nonce,
                         std::vector<attribute_check>* checks,
                         network_msgtype mt,
                         const e::slice& remain,
                         network_msgtype resp);
        void count(const server_id& from,
                   const virtual_server_id& to,
                   uint64_t nonce,
                   std::vector<attribute_check>* checks);
        void search_describe(const server_id& from,
                             const virtual_server_id& to,
                             uint64_t nonce,
                             std::vector<attribute_check>* checks);

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

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_search_manager_h_
