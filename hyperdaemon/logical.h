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
#include <e/lockfree_fifo.h>
#include <e/lockfree_hash_map.h>

// BusyBee
#include <busybee_mta.h>

// HyperDex
#include "daemon/reconfigure_returncode.h"
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/instance.h"
#include "hyperdex/hyperdex/network_constants.h"

// Forward Declarations
namespace hyperdex
{
class coordinatorlink;
class daemon;
class entityid;
class regionid;
}

namespace hyperdaemon
{

class logical
{
    public:
        logical(hyperdex::daemon*);
        ~logical() throw ();

    public:
        size_t header_size() const;
        int in_port() { return m_busybee.inbound().port; }
        int out_port() { return m_busybee.outbound().port; }

    // Reconfigure this layer.
    public:
        hyperdex::reconfigure_returncode prepare(const hyperdex::configuration& old_config,
                                                 const hyperdex::configuration& new_config,
                                                 const hyperdex::instance& us);
        hyperdex::reconfigure_returncode reconfigure(const hyperdex::configuration& old_config,
                                                     const hyperdex::configuration& new_config,
                                                     const hyperdex::instance& us);
        hyperdex::reconfigure_returncode cleanup(const hyperdex::configuration& old_config,
                                                 const hyperdex::configuration& new_config,
                                                 const hyperdex::instance& us);

    // Pause/unpause or completely stop recv of messages.  Paused threads will
    // not hold locks, and therefore will not pose risk of deadlock.
    public:
        void pause() { m_busybee.pause(); }
        void unpause() { m_busybee.unpause(); }
        void shutdown() { m_busybee.shutdown(); }

    // Send and recv messages.
    public:
        // Send from one specific entity to another specific entity.
        bool send(const hyperdex::entityid& from, const hyperdex::entityid& to,
                  const hyperdex::network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        // Receive one message.
        bool recv(hyperdex::entityid* from, hyperdex::entityid* to,
                  hyperdex::network_msgtype* msg_type,
                  std::auto_ptr<e::buffer>* msg);

    private:
        static uint64_t id(const uint64_t& i) { return i; }

    private:
        class early_message;

    private:
        logical(const logical&);

    private:
        logical& operator = (const logical&);

    private:
        void handle_connectfail(const po6::net::location& loc);
        void handle_disconnect(const po6::net::location& loc);

    private:
        hyperdex::daemon* m_daemon;
        e::lockfree_fifo<early_message> m_early_messages;
        e::lockfree_hash_map<po6::net::location, uint64_t, po6::net::location::hash> m_client_nums;
        e::lockfree_hash_map<uint64_t, po6::net::location, id> m_client_locs;
        uint64_t m_client_counter;
        busybee_mta m_busybee;
};

} // namespace hyperdaemon

#endif // hyperdaemon_logical_h_
