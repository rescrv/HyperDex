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

#ifndef hyperdaemon_ongoing_state_transfers_h_
#define hyperdaemon_ongoing_state_transfers_h_

// STL
#include <memory>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>
#include <e/slice.h>

// Forward Declarations
namespace hyperdex
{
class configuration;
class entityid;
class instance;
class regionid;
}
namespace hyperdaemon
{
class datalayer;
class logical;
class replication_manager;
}

namespace hyperdaemon
{

class ongoing_state_transfers
{
    public:
        ongoing_state_transfers(datalayer* data,
                                logical* comm,
                                hyperdex::coordinatorlink* cl);
        ~ongoing_state_transfers() throw ();

    // Reconfigure this layer.
    public:
        void prepare(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void reconfigure(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void cleanup(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void shutdown();

    // Netowrk workers call these methods.
    public:
        void region_transfer_send(const hyperdex::entityid& from, const hyperdex::entityid& to);
        void region_transfer_recv(const hyperdex::entityid& from,
                                  uint16_t xfer_id, uint64_t xfer_num,
                                  bool has_value, uint64_t version,
                                  std::auto_ptr<e::buffer> backing,
                                  const e::slice& key,
                                  const std::vector<e::slice>& value);
        void region_transfer_done(const hyperdex::entityid& from, const hyperdex::entityid& to);

    // Interactions with the replication layer.
    public:
        void add_trigger(const hyperdex::regionid& reg,
                         std::tr1::shared_ptr<e::buffer> backing,
                         const e::slice& key,
                         uint64_t rev);
        void set_replication_manager(replication_manager* repl);

    private:
        class transfer_in;
        class transfer_out;
        static uint64_t transfer_in_hash(const uint16_t& ti) { return ti; }
        static uint64_t transfer_out_hash(const uint16_t& to) { return to; }
        typedef e::lockfree_hash_map<uint16_t, e::intrusive_ptr<transfer_in>, transfer_in_hash>
                transfers_in_map_t;
        typedef e::lockfree_hash_map<uint16_t, e::intrusive_ptr<transfer_out>, transfer_out_hash>
                transfers_out_map_t;

    private:
        ongoing_state_transfers(const ongoing_state_transfers&);

    private:
        void periodic();
        void start_transfers();
        void finish_transfers();

    private:
        ongoing_state_transfers& operator = (const ongoing_state_transfers&);

    private:
        hyperdaemon::datalayer* m_data;
        hyperdaemon::logical* m_comm;
        hyperdex::coordinatorlink* m_cl;
        hyperdaemon::replication_manager* m_repl;
        hyperdex::configuration m_config;
        transfers_in_map_t m_transfers_in;
        transfers_out_map_t m_transfers_out;
        bool m_shutdown;
        po6::threads::mutex m_periodic_lock;
        po6::threads::thread m_periodic_thread;
};

} // namespace hyperdaemon

#endif // hyperdaemon_ongoing_state_transfers_h_
