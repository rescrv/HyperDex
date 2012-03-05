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

#ifndef hyperdaemon_replication_manager_pending
#define hyperdaemon_replication_manager_pending

// HyperDaemon
#include "hyperdaemon/replication/clientop.h"

class hyperdaemon::replication_manager::pending
{
    public:
        pending(bool has_value,
                std::tr1::shared_ptr<e::buffer> backing,
                const e::slice& key,
                const std::vector<e::slice>& value,
                const hyperdaemon::replication::clientop& co = hyperdaemon::replication::clientop());
        ~pending() throw ();

    public:
        std::tr1::shared_ptr<e::buffer> backing;
        e::slice key;
        std::vector<e::slice> value;
        const bool has_value;
        bool fresh;
        bool acked;
        hyperdaemon::replication::clientop co;
        hyperdex::network_msgtype retcode;
        e::intrusive_ptr<pending> backing2;
        hyperdisk::reference ref;

        hyperdex::entityid recv_e; // We recv from here
        hyperdex::instance recv_i; // We recv from here
        hyperdex::entityid sent_e; // We sent to here
        hyperdex::instance sent_i; // We sent to here
        uint16_t subspace_prev;
        uint16_t subspace_next;
        uint64_t point_prev;
        uint64_t point_this;
        uint64_t point_next;
        uint64_t point_next_next;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: replication_manager :: pending :: pending(bool hv,
                                                         std::tr1::shared_ptr<e::buffer> b,
                                                         const e::slice& k,
                                                         const std::vector<e::slice>& val,
                                                         const hyperdaemon::replication::clientop& c)
    : backing(b)
    , key(k)
    , value(val)
    , has_value(hv)
    , fresh(false)
    , acked(false)
    , co(c)
    , retcode()
    , backing2()
    , ref()
    , recv_e()
    , recv_i()
    , sent_e()
    , sent_i()
    , subspace_prev(0)
    , subspace_next(0)
    , point_prev(0)
    , point_this(0)
    , point_next(0)
    , point_next_next(0)
    , m_ref(0)
{
}

hyperdaemon :: replication_manager :: pending :: ~pending()
                                                 throw ()
{
}

#endif // hyperdaemon_replication_manager_pending
