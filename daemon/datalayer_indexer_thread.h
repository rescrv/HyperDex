// Copyright (c) 2014, Cornell University
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

#ifndef hyperdex_daemon_datalayer_indexer_h_
#define hyperdex_daemon_datalayer_indexer_h_

// HyperDex
#include "daemon/background_thread.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/datalayer_wiper_indexer_mediator.h"
#include "daemon/index_info.h"
#include "daemon/leveldb.h"

class hyperdex::datalayer::indexer_thread : public hyperdex::background_thread
{
    public:
        indexer_thread(daemon* d, wiper_indexer_mediator* m);
        ~indexer_thread() throw ();

    public:
        virtual const char* thread_name();
        virtual bool have_work();
        virtual void copy_work();
        virtual void do_work();

    public:
        void debug_dump();
        void kick();
        bool mark_usable(const region_id& ri, const index_id& ii);

    private:
        bool interrupted();
        region_iterator* play(const region_id& ri, const schema* sc);
        replay_iterator* replay(const region_id& ri,
                                const std::string& timestamp);
        bool wipe(const region_id& ri, const index_id& ii);
        bool wipe_common(uint8_t c, const region_id& ri, const index_id& ii);
        bool index_from_iterator(region_iterator* it,
                                 const schema* sc,
                                 const region_id& ri,
                                 const std::vector<const index*>& idxs);
        bool index_from_replay_iterator(replay_iterator* rit,
                                        const schema* sc,
                                        const region_id& ri,
                                        const std::vector<const index*>& idxs);

    private:
        daemon* m_daemon;
        wiper_indexer_mediator* m_mediator;
        configuration m_config;
        bool m_have_current;
        region_id m_current_region;
        index_id m_current_index;
        uint64_t m_interrupted_count;
        bool m_interrupted;

    private:
        indexer_thread(const indexer_thread&);
        indexer_thread& operator = (const indexer_thread&);
};

#endif // hyperdex_daemon_datalayer_indexer_h_
