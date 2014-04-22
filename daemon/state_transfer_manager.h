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

#ifndef hyperdex_daemon_state_transfer_manager_h_
#define hyperdex_daemon_state_transfer_manager_h_

// STL
#include <memory>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "daemon/background_thread.h"
#include "daemon/reconfigure_returncode.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class state_transfer_manager
{
    public:
        state_transfer_manager(daemon*);
        ~state_transfer_manager() throw ();

    // Reconfigure this layer.
    public:
        bool setup();
        void teardown();
        void pause();
        void unpause();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);
        void debug_dump();

    public:
        void handshake_syn(const virtual_server_id& from,
                           const transfer_id& xid);
        void handshake_synack(const server_id& from,
                              const virtual_server_id& to,
                              const transfer_id& xid,
                              uint64_t timestamp);
        void handshake_ack(const virtual_server_id& from,
                           const transfer_id& xid,
                           bool wipe);
        void handshake_wiped(const server_id& from,
                             const virtual_server_id& to,
                             const transfer_id& xid);
        void report_wiped(const transfer_id& xid);
        void xfer_op(const virtual_server_id& from,
                     const transfer_id& xid,
                     uint64_t seq_no,
                     bool has_value,
                     uint64_t version,
                     std::auto_ptr<e::buffer> msg,
                     const e::slice& key,
                     const std::vector<e::slice>& value);
        void xfer_ack(const server_id& from,
                      const virtual_server_id& to,
                      const transfer_id& xid,
                      uint64_t seq_no);

    private:
        class pending;
        class transfer_in_state;
        class transfer_out_state;
        class background_thread;

    private:
        // get the appropriate state
        transfer_in_state* get_tis(const transfer_id& xid);
        transfer_out_state* get_tos(const transfer_id& xid);
        // caller must hold mtx on tos
        void transfer_more_state(transfer_out_state* tos);
        void retransmit(transfer_out_state* tos);
        // caller must hold mtx on tis
        void put_to_disk_and_send_acks(transfer_in_state* tis);
        // caller must hold mtx on tos
        // send the last object in tos
        void send_handshake_syn(const transfer& xfer);
        void send_handshake_synack(const transfer& xfer, uint64_t timestamp);
        void send_handshake_ack(const transfer& xfer, bool wipe);
        void send_handshake_wiped(const transfer& xfer);
        void send_object(const transfer& xfer, pending* op);
        void send_ack(const transfer& xfer, uint64_t seq_id);

    private:
        state_transfer_manager(const state_transfer_manager&);
        state_transfer_manager& operator = (const state_transfer_manager&);

    private:
        daemon* m_daemon;
        std::vector<e::intrusive_ptr<transfer_in_state> > m_transfers_in;
        std::vector<e::intrusive_ptr<transfer_out_state> > m_transfers_out;
        const std::auto_ptr<background_thread> m_background_thread;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_state_transfer_manager_h_
