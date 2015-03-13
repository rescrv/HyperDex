// Copyright (c) 2015, Cornell University
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

#ifndef hyperdex_daemon_wan_manager_h_
#define hyperdex_daemon_wan_manager_h_

// C
#include <stdint.h>

// STL
#include <memory>
#include <queue>
#include <vector>
#include <set>

// po6
#include <po6/net/location.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_mta.h>

// e
#include <e/intrusive_ptr.h>
#include <e/buffer.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "daemon/background_thread.h"
#include "common/coordinator_link.h"
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

class wan_manager
{
    public:
        wan_manager(daemon* d);
        ~wan_manager() throw ();

    // primary coordinator link
    public:
        void set_coordinator_address(const char* host, uint16_t port);
        void set_is_backup(bool isbackup);
        bool maintain_link();
        void copy_config(configuration* config);
        configuration get_config();

    private:
        void background_maintenance();
        void do_sleep();
        void reset_sleep();
        void enter_critical_section();
        void exit_critical_section();
        void enter_critical_section_killable();
        void enter_critical_section_background();
        void exit_critical_section_killable();

    // state pull from primary
    public:
        void kick();
        void wake_one();
        bool setup();
        void teardown();
        void pause();
        void unpause();
        void reconfigure(configuration *config);
        void handle_handshake(const server_id& from,
                              const virtual_server_id& vfrom,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up);
        void recv_data(const server_id& from,
                              const virtual_server_id& vfrom,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up);
        void send_more_data(const server_id& from,
                              const virtual_server_id& vfrom,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up);
        void handle_ack(const server_id& from,
                              const virtual_server_id& vfrom,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up);
    private:
        class pending;
        class transfer_in_state;
        class transfer_out_state;
        class background_thread;

    private:
        void handle_disruption();
        void run();
        std::vector<space> config_space_overlap(configuration primary, configuration backup);
        void setup_transfer_state(std::vector<space> overlap);
        // msg loop and handling fns
        void loop();
        void wan_xfer(const transfer_id& xid,
                     uint64_t seq_no,
                     bool has_value,
                     uint64_t version,
                     std::auto_ptr<e::buffer> msg,
                     const e::slice& key,
                     const std::vector<e::slice>& value,
                     const region_id rid);
        void make_keychanges(transfer_in_state* tis);
        bool recv(server_id* from,
                  virtual_server_id* vfrom,
                  virtual_server_id* vto,
                  network_msgtype* msg_type,
                  std::auto_ptr<e::buffer>* msg,
                  e::unpacker* up);
        bool send(const virtual_server_id& to,
                  network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        bool send(const virtual_server_id& from,
                  const server_id& to,
                  network_msgtype msg_type,
                  std::auto_ptr<e::buffer> msg);
        void transfer_more_state(transfer_out_state* tos);
        void give_me_more_state(transfer_in_state* tis);
        // get the appropriate state
        transfer_in_state* get_tis(const transfer_id& xid);
        transfer_out_state* get_tos(const transfer_id& xid);
        // network message fns
        void send_handshake_syn(const transfer& xfer);
        void send_ask_for_more(const transfer& xfer);
        void send_object(const transfer& xfer, pending* op);
        void send_ack(const transfer& xfer, uint64_t seq_id);

    private:
        wan_manager(const wan_manager&);
        wan_manager& operator = (const wan_manager&);

    private:
        daemon* m_daemon;
        po6::threads::thread m_poller;
        std::auto_ptr<coordinator_link> m_coord;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_is_backup;
        bool m_poller_started;
        bool m_teardown;
        std::queue<std::pair<int64_t, replicant_returncode> > m_deferred;
        bool m_locked;
        bool m_kill;
        pthread_t m_to_kill;
        uint64_t m_waiting;
        uint64_t m_sleep;
        int64_t m_online_id;
        bool m_shutdown_requested;
        // background transfer
        std::set<virtual_server_id> m_transfer_vids;
        uint64_t m_xid;
        std::vector<e::intrusive_ptr<transfer_in_state> > m_transfers_in;
        std::vector<e::intrusive_ptr<transfer_out_state> > m_transfers_out;
        const std::auto_ptr<background_thread> m_background_thread;
        // busybee
        configuration m_config;
        mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        po6::threads::thread m_msg_thread;
        po6::threads::thread m_link_thread;
        bool m_paused;
        po6::threads::mutex m_protect_pause;
        po6::threads::cond m_can_pause;
        bool m_has_config;
        bool m_busybee_running;

};

END_HYPERDEX_NAMESPACE

#endif /* end of include guard: hyperdex_daemon_wan_manager_h_ */
