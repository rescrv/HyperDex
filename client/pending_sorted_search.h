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

#ifndef hyperdex_client_pending_sorted_search_h_
#define hyperdex_client_pending_sorted_search_h_

// STL
#include <tr1/memory>

// HyperDex
#include "client/pending.h"

class hyperclient::pending_sorted_search : public hyperclient::pending
{
    public:
        class state;

    public:
        pending_sorted_search(int64_t searchid,
                              e::intrusive_ptr<state> st,
                              hyperclient_returncode* status,
                              hyperclient_attribute** attrs,
                              size_t* attrs_sz);
        virtual ~pending_sorted_search() throw ();

    public:
        virtual hyperdex::network_msgtype request_type();
        virtual int64_t handle_response(hyperclient* cl,
                                        const server_id& id,
                                        std::auto_ptr<e::buffer> msg,
                                        hyperdex::network_msgtype type,
                                        hyperclient_returncode* status);
        virtual int64_t return_one(hyperclient* cl,
                                   hyperclient_returncode* status);

    private:
        pending_sorted_search(const pending_sorted_search& other);

    private:
        pending_sorted_search& operator = (const pending_sorted_search& rhs);

    private:
        e::intrusive_ptr<state> m_state;
        hyperclient_attribute** m_attrs;
        size_t* m_attrs_sz;
};

class hyperclient::pending_sorted_search::state
{
    public:
        state(std::auto_ptr<e::buffer>* backings,
              uint64_t limit, uint16_t sort_by,
              hyperdatatype type, bool maximize);
        ~state() throw ();

    private:
        friend class e::intrusive_ptr<hyperclient::pending_sorted_search::state>;
        friend class hyperclient::pending_sorted_search;
        class item;

    private:
        state(const state&);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        state& operator = (const state&);

    private:
        size_t m_ref;
        const uint64_t m_limit;
        const uint16_t m_sort_by;
        hyperdatatype m_sort_type;
        bool m_maximize;
        std::vector<item> m_results;
        std::auto_ptr<e::buffer>* m_backings;
        size_t m_backing_idx;
        size_t m_returned;
};

#endif // hyperdex_client_pending_sorted_search_h_
