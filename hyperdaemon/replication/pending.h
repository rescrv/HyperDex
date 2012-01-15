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

#ifndef hyperdaemon_replication_pending_h_
#define hyperdaemon_replication_pending_h_

// e
#include <e/intrusive_ptr.h>

namespace hyperdaemon
{
namespace replication
{

class pending
{
    public:
        pending(bool has_value,
                std::auto_ptr<e::buffer> backing,
                const std::vector<e::slice>& value,
                const clientop& co = clientop());
        pending(bool has_value,
                std::tr1::shared_ptr<e::buffer> backing,
                const std::vector<e::slice>& value,
                const clientop& co = clientop());

    public:
        std::tr1::shared_ptr<e::buffer> backing;
        std::vector<e::slice> value;
        const bool has_value;
        bool fresh;
        bool acked;
        bool mayack; // True if it is OK to receive ACK messages.
        uint8_t retransmit;
        clientop co;
        hyperdex::network_msgtype retcode;
        hyperdex::regionid prev;
        hyperdex::regionid this_old;
        hyperdex::regionid this_new;
        hyperdex::regionid next;
        e::intrusive_ptr<pending> backing2;
        hyperdisk::reference ref;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

inline
pending :: pending(bool hv,
                   std::auto_ptr<e::buffer> b,
                   const std::vector<e::slice>& val,
                   const clientop& c)
    : backing(b.release())
    , value(val)
    , has_value(hv)
    , fresh(false)
    , acked(false)
    , mayack(false)
    , retransmit(0)
    , co(c)
    , retcode()
    , prev()
    , this_old()
    , this_new()
    , next()
    , backing2()
    , ref()
    , m_ref(0)
{
}

inline
pending :: pending(bool hv,
                   std::tr1::shared_ptr<e::buffer> b,
                   const std::vector<e::slice>& val,
                   const clientop& c)
    : backing(b)
    , value(val)
    , has_value(hv)
    , fresh(false)
    , acked(false)
    , mayack(false)
    , retransmit(0)
    , co(c)
    , retcode()
    , prev()
    , this_old()
    , this_new()
    , next()
    , backing2()
    , ref()
    , m_ref(0)
{
}

} // namespace replication
} // namespace hyperdaemon

#endif // hyperdaemon_replication_pending_h_
