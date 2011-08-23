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

#ifndef hyperdaemon_logical_h_
#define hyperdaemon_logical_h_

// STL
#include <map>

// po6
#include <po6/net/location.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>
#include <e/lockfree_hash_map.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/instance.h>
#include <hyperdex/network_constants.h>

// HyperDaemon
#include "physical.h"

// Forward Declarations
namespace hyperdex
{
class coordinatorlink;
class entityid;
class regionid;
}

namespace hyperdaemon
{

class logical
{
    public:
        logical(hyperdex::coordinatorlink* cl, const po6::net::ipaddr& us,
                in_port_t incoming, in_port_t outgoing);
        ~logical() throw ();

    public:
        hyperdex::instance inst() const { return m_us; }

    // Reconfigure this layer.
    public:
        void prepare(const hyperdex::configuration& newconfig, const hyperdex::instance& newinst);
        void reconfigure(const hyperdex::configuration& newconfig, const hyperdex::instance& newinst);
        void cleanup(const hyperdex::configuration& newconfig, const hyperdex::instance& newinst);

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
                  const hyperdex::network_msgtype msg_type, const e::buffer& msg);
        // Send from one region to a specific entity.  This will find
        // our offset in the chain for the "from" region, and use that
        // entity as the source.
        bool send(const hyperdex::regionid& from, const hyperdex::entityid& to,
                  const hyperdex::network_msgtype msg_type, const e::buffer& msg);
        // Send msg1 along a chain in the direction indicated (forward =
        // ascending numbers, backward = descending numbers).  If we hit the end
        // of the chain in that direction, then send msg2 to the head or tail of
        // the region which overlaps with the "otherwise" region.
        bool send_forward_else_head(const hyperdex::regionid& chain,
                                    hyperdex::network_msgtype msg1_type,
                                    const e::buffer& msg1,
                                    const hyperdex::regionid& otherwise,
                                    hyperdex::network_msgtype msg2_type,
                                    const e::buffer& msg2);
        bool send_forward_else_tail(const hyperdex::regionid& chain,
                                    hyperdex::network_msgtype msg1_type,
                                    const e::buffer& msg1,
                                    const hyperdex::regionid& otherwise,
                                    hyperdex::network_msgtype msg2_type,
                                    const e::buffer& msg2);
        bool send_backward(const hyperdex::regionid& chain,
                           hyperdex::network_msgtype msg_type,
                           const e::buffer& msg);
        bool send_backward_else_tail(const hyperdex::regionid& chain,
                                     hyperdex::network_msgtype msg1_type,
                                     const e::buffer& msg1,
                                     const hyperdex::regionid& otherwise,
                                     hyperdex::network_msgtype msg2_type,
                                     const e::buffer& msg2);
        bool recv(hyperdex::entityid* from, hyperdex::entityid* to,
                  hyperdex::network_msgtype* msg_type, e::buffer* msg);

    private:
        static uint64_t id(const uint64_t& i) { return i; }

    private:
        logical(const logical&);

    private:
        logical& operator = (const logical&);

    private:
        void handle_connectfail(const po6::net::location& loc);
        void handle_disconnect(const po6::net::location& loc);

        // These assume that the mapping lock is held for at least reading.
        bool send_you_hold_lock(const hyperdex::entityid& from,
                                const hyperdex::entityid& to,
                                const hyperdex::network_msgtype msg_type,
                                const e::buffer& msg);
        bool our_position(const hyperdex::regionid& r, hyperdex::entityid* e);

    private:
        hyperdex::coordinatorlink* m_cl;
        hyperdex::instance m_us;
        hyperdex::configuration m_config;
        std::map<hyperdex::entityid, hyperdex::instance> m_mapping;
        e::lockfree_hash_map<po6::net::location, uint64_t, po6::net::location::hash> m_client_nums;
        e::lockfree_hash_map<uint64_t, po6::net::location, id> m_client_locs;
        uint64_t m_client_counter;
        physical m_physical;
};

} // namespace hyperdaemon

#endif // hyperdaemon_logical_h_
