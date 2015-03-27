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

#ifndef hyperdex_daemon_key_operation_h_
#define hyperdex_daemon_key_operation_h_

// STL
#include <memory>

// e
#include <e/arena.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class key_operation
{
    public:
        key_operation(uint64_t old_version,
                      uint64_t new_version,
                      bool fresh,
                      bool has_value,
                      const std::vector<e::slice>& value,
                      std::auto_ptr<e::arena> memory);
        ~key_operation() throw ();

    public:
        uint64_t prev_version() const { return m_prev_version; }
        uint64_t this_version() const { return m_this_version; }

        // the state of this op
        void mark_acked() { m_acked = true; }
        // can we ack this op AND remove it from local state?
        bool ackable() const { return m_acked; }

        // who we recv/sent from/to along the chain
        void set_recv(uint64_t version, const virtual_server_id& vsi)
        { m_recv_config_version = version; m_recv = vsi; }
        bool recv_from(uint64_t version) const { return version == m_recv_config_version; }
        virtual_server_id recv_from() const { return m_recv; }
        void set_sent(uint64_t version, const virtual_server_id& vsi)
        { m_sent_config_version = version; m_sent = vsi; }
        virtual_server_id sent_to() const { return m_sent; }
        uint64_t sent_version() const { return m_sent_config_version; }
        bool sent_to(uint64_t version, const virtual_server_id& vsi) const
        { return m_sent_config_version == version && m_sent == vsi; }

        // the path of the op through the value-dependent chain
        bool is_continuous() { return m_type == CONTINUOUS; }
        bool is_discontinuous() { return m_type == DISCONTINUOUS; }
        // call before hashing
        void set_continuous();
        // call after hashing; must call above first
        void set_continuous_hashes(const region_id& prev_region,
                                   const region_id& this_old_region,
                                   const region_id& this_new_region,
                                   const region_id& next_region);
        void set_discontinuous(const region_id& prev_region,
                               const region_id& this_old_region,
                               const region_id& this_new_region,
                               const region_id& next_region);
        region_id prev_region() const { return m_prev_region; }
        region_id this_old_region() const { return m_this_old_region; }
        region_id this_new_region() const { return m_this_new_region; }
        region_id next_region() const { return m_next_region; }

        // the value set by this op
        bool is_fresh() { return m_fresh; }
        bool has_value() { return m_has_value; }
        const std::vector<e::slice>& value() { return m_value; }

        void debug_dump();

    private:
        friend class e::intrusive_ptr<key_operation>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
        const uint64_t m_prev_version;
        const uint64_t m_this_version;
        bool m_acked;
        bool m_fresh;
        const bool m_has_value;

        uint64_t m_recv_config_version;
        virtual_server_id m_recv; // we recv from here
        uint64_t m_sent_config_version;
        virtual_server_id m_sent; // we sent to here

        const std::vector<e::slice> m_value;
        const std::auto_ptr<e::arena> m_memory;

        enum { UNKNOWN, CONTINUOUS, DISCONTINUOUS } m_type;
        region_id m_this_old_region;
        region_id m_this_new_region;
        region_id m_prev_region;
        region_id m_next_region;

    private:
        key_operation(const key_operation&);
        key_operation& operator = (const key_operation&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_key_operation_h_
