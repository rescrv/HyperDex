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

#ifndef hyperdex_daemon_datalayer_wiper_indexer_mediator_h_
#define hyperdex_daemon_datalayer_wiper_indexer_mediator_h_

using hyperdex::datalayer;

class datalayer::wiper_indexer_mediator
{
    public:
        wiper_indexer_mediator();
        ~wiper_indexer_mediator() throw ();

    public:
        void debug_dump();
        bool region_conflicts_with_wiper(const region_id& ri);
        bool region_conflicts_with_indexer(const region_id& ri);
        bool set_wiper_region(const region_id& ri);
        bool set_indexer_region(const region_id& ri);
        void clear_wiper_region();
        void clear_indexer_region();

    private:
        wiper_indexer_mediator(const wiper_indexer_mediator&);
        wiper_indexer_mediator& operator = (const wiper_indexer_mediator&);

    private:
        po6::threads::mutex m_protect;
        region_id m_wiper;
        region_id m_indexer;
};

inline
datalayer :: wiper_indexer_mediator :: wiper_indexer_mediator()
    : m_protect()
    , m_wiper()
    , m_indexer()
{
}

inline
datalayer :: wiper_indexer_mediator :: ~wiper_indexer_mediator() throw ()
{
}

inline void
datalayer :: wiper_indexer_mediator :: debug_dump()
{
    po6::threads::mutex::hold hold(&m_protect);
    LOG(INFO) << "wiper-indexer mediator ========================================================";
    LOG(INFO) << "wiper=" << m_wiper;
    LOG(INFO) << "indexer=" << m_indexer;
}

inline bool
datalayer :: wiper_indexer_mediator :: region_conflicts_with_wiper(const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);
    return m_wiper == ri;
}

inline bool
datalayer :: wiper_indexer_mediator :: region_conflicts_with_indexer(const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);
    return m_indexer == ri;
}

inline bool
datalayer :: wiper_indexer_mediator :: set_wiper_region(const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);

    if (m_indexer != ri)
    {
        m_wiper = ri;
        return true;
    }

    return false;
}

inline bool
datalayer :: wiper_indexer_mediator :: set_indexer_region(const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);

    if (m_wiper != ri)
    {
        m_indexer = ri;
        return true;
    }

    return false;
}

inline void
datalayer :: wiper_indexer_mediator :: clear_wiper_region()
{
    po6::threads::mutex::hold hold(&m_protect);
    m_wiper = region_id();
}

inline void
datalayer :: wiper_indexer_mediator :: clear_indexer_region()
{
    po6::threads::mutex::hold hold(&m_protect);
    m_indexer = region_id();
}

#endif // hyperdex_daemon_datalayer_wiper_indexer_mediator_h_
