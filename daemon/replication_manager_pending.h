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

#ifndef hyperdex_daemon_replication_manager_pending_h_
#define hyperdex_daemon_replication_manager_pending_h_

// STL
#include <memory>

// HyperDex
#include "daemon/replication_manager.h"

class hyperdex::replication_manager::pending
{
    public:
        pending(std::auto_ptr<e::buffer> backing,
                const region_id& reg_id,
                uint64_t seq_id,
                bool fresh,
                bool has_value,
                const std::vector<e::slice>& value,
                server_id _client, uint64_t _nonce,
                uint64_t _recv_config_version,
                const virtual_server_id& _recv);
        ~pending() throw ();

    public:
        std::auto_ptr<e::buffer> backing;
        region_id reg_id;
        uint64_t seq_id;
        bool has_value;
        std::vector<e::slice> value;
        uint64_t recv_config_version;
        virtual_server_id recv; // we recv from here
        uint64_t sent_config_version;
        virtual_server_id sent; // we sent to here
        bool fresh;
        bool acked;
        server_id client;
        uint64_t nonce;
        std::vector<uint64_t> old_hashes;
        std::vector<uint64_t> new_hashes;
        region_id this_old_region;
        region_id this_new_region;
        region_id prev_region;
        region_id next_region;

    private:
        friend class e::intrusive_ptr<pending>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        size_t m_ref;
};

#endif // hyperdex_daemon_replication_manager_pending_h_
