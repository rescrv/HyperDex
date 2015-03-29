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

#ifndef hyperdex_daemon_communication_h_
#define hyperdex_daemon_communication_h_

// STL
#include <memory>

// BusyBee
#include <busybee_constants.h>
#include <busybee_mta.h>

// e
#include <e/buffer.h>
#include <e/lockfree_fifo.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/mapper.h"
#include "common/network_msgtype.h"
#include "daemon/reconfigure_returncode.h"

#define HYPERDEX_HEADER_SIZE_VC (BUSYBEE_HEADER_SIZE \
                                 + sizeof(uint8_t) /*message type*/ \
                                 + sizeof(uint64_t) /*virt from*/)
#define HYPERDEX_HEADER_SIZE_SV (BUSYBEE_HEADER_SIZE \
                                 + sizeof(uint8_t) /*message type*/ \
                                 + sizeof(uint8_t) /*flags*/ \
                                 + sizeof(uint64_t) /*config version*/ \
                                 + sizeof(uint64_t) /*virt to*/)
#define HYPERDEX_HEADER_SIZE_VV (BUSYBEE_HEADER_SIZE \
                                 + sizeof(uint8_t) /*message type*/ \
                                 + sizeof(uint8_t) /*flags*/ \
                                 + sizeof(uint64_t) /*config version*/ \
                                 + sizeof(uint64_t) /*virt to*/ \
                                 + sizeof(uint64_t) /*virt from*/)
#define HYPERDEX_HEADER_SIZE_VS HYPERDEX_HEADER_SIZE_VV

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class communication
{
    public:
        communication(daemon* d);
        ~communication() throw ();

    public:
        void pause() { m_busybee->pause(); }
        void unpause() { m_busybee->unpause(); }
        void shutdown() { m_busybee->shutdown(); }
        void wake_one() { m_busybee->wake_one(); }

    public:
        bool setup(const po6::net::location& bind_to,
                   unsigned threads);
        void teardown();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);

    public:
        // Send data to another server (pretending to be a client)
        bool send_client(const virtual_server_id& from,
                         const server_id& to,
                         network_msgtype msg_type,
                         std::auto_ptr<e::buffer> msg);
        bool send(const virtual_server_id& from,
                  const server_id& to,
                  network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        bool send(const virtual_server_id& from,
                  const virtual_server_id& to,
                  network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        bool send(const virtual_server_id& to,
                  network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        bool send_exact(const virtual_server_id& from,
                        const virtual_server_id& to,
                        network_msgtype msg_type,
                        std::auto_ptr<e::buffer> msg);
        bool recv(e::garbage_collector::thread_state* ts,
                  server_id* from,
                  virtual_server_id* vfrom,
                  virtual_server_id* vto,
                  network_msgtype* msg_type,
                  std::auto_ptr<e::buffer>* msg,
                  e::unpacker* up);

    private:
        class early_message;

    private:
        void handle_disruption(uint64_t id);

    private:
        communication(const communication&);
        communication& operator = (const communication&);

    private:
        daemon* m_daemon;
        mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        e::lockfree_fifo<early_message> m_early_messages;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_communication_h_
