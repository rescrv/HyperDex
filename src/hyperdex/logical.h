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

#ifndef hyperdex_logical_h_
#define hyperdex_logical_h_

// STL
#include <map>

// libev
#include <ev++.h>

// po6
#include <po6/net/location.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/instance.h>
#include <hyperdex/physical.h>
#include <hyperdex/stream_no.h>

namespace hyperdex
{

class logical
{
    public:
        logical(ev::loop_ref lr, const po6::net::ipaddr& us);
        ~logical();

    public:
        const instance& inst() const { return m_us; }

    // Reconfigure this layer.
    public:
        void prepare(const configuration& newconfig);
        void reconfigure(const configuration& newconfig);
        void cleanup(const configuration& newconfig);

    // Pause/unpause or completely stop recv of messages.  Paused threads will
    // not hold locks, and therefore will not pose risk of deadlock.
    public:
        void pause() { m_physical.pause(); }
        void unpause() { m_physical.unpause(); }
        size_t num_paused() { return m_physical.num_paused(); }
        void shutdown() { m_physical.shutdown(); }

    // Send and recv messages.
    public:
        // Send from one specific entity to another specific entity.
        bool send(const hyperdex::entityid& from, const hyperdex::entityid& to,
                  const uint8_t msg_type, const e::buffer& msg);
        // Send from one region to a specific entity.  This will find
        // our offset in the chain for the "from" region, and use that
        // entity as the source.
        bool send(const hyperdex::regionid& from, const hyperdex::entityid& to,
                  const uint8_t msg_type, const e::buffer& msg);
        // Send msg1 along a chain in the direction indicated (forward =
        // ascending numbers, backward = descending numbers).  If we hit the end
        // of the chain in that direction, then send msg2 to the head or tail of
        // the region which overlaps with the "otherwise" region.
        bool send_forward_else_head(const hyperdex::regionid& chain,
                                    stream_no::stream_no_t msg1_type,
                                    const e::buffer& msg1,
                                    const hyperdex::regionid& otherwise,
                                    stream_no::stream_no_t msg2_type,
                                    const e::buffer& msg2);
        bool send_forward_else_tail(const hyperdex::regionid& chain,
                                    stream_no::stream_no_t msg1_type,
                                    const e::buffer& msg1,
                                    const hyperdex::regionid& otherwise,
                                    stream_no::stream_no_t msg2_type,
                                    const e::buffer& msg2);
        bool send_backward(const hyperdex::regionid& chain,
                           stream_no::stream_no_t msg_type,
                           const e::buffer& msg);
        bool send_backward_else_tail(const hyperdex::regionid& chain,
                                     stream_no::stream_no_t msg1_type,
                                     const e::buffer& msg1,
                                     const hyperdex::regionid& otherwise,
                                     stream_no::stream_no_t msg2_type,
                                     const e::buffer& msg2);
        bool recv(hyperdex::entityid* from, hyperdex::entityid* to,
                  uint8_t* msg_type, e::buffer* msg);

    private:
        logical(const logical&);

    private:
        logical& operator = (const logical&);

    private:
        // These assume that the mapping lock is held for at least reading.
        bool send_you_hold_lock(const entityid& from,
                                const entityid& to,
                                const uint8_t msg_type,
                                const e::buffer& msg);
        bool our_position(const regionid& r, entityid* e);
        bool chain_tail(const regionid& r, entityid* e);
        bool find_overlapping(const regionid& r, regionid* over);

    private:
        instance m_us;
        po6::threads::rwlock m_client_lock;
        std::map<entityid, instance> m_mapping;
        std::map<po6::net::location, uint64_t> m_client_nums;
        std::map<uint64_t, po6::net::location> m_client_locs;
        uint64_t m_client_counter;
        hyperdex::physical m_physical;
};

} // namespace hyperdex

#endif // hyperdex_logical_h_
