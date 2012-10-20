// Copyright (c) 2011-2012, Cornell University
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

// STL
#include <algorithm>

// e
#include <e/endian.h>

// HyperClient
#include "datatypes/compare.h"
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending_sorted_search.h"
#include "hyperclient/util.h"

class hyperclient::pending_sorted_search::state::item
{
    public:
        item();
        item(pending_sorted_search::state* st,
             const e::slice& key,
             const std::vector<e::slice>& value);
        item(const item&);
        ~item() throw ();

    public:
        item& operator = (const item&);
        bool operator < (const item&) const;
        bool operator > (const item&) const;

    public:
        pending_sorted_search::state* st;
        e::slice key;
        std::vector<e::slice> value;
};

hyperclient :: pending_sorted_search :: pending_sorted_search(int64_t searchid,
                                                              e::intrusive_ptr<state> st,
                                                              hyperclient_returncode* status,
                                                              hyperclient_attribute** attrs,
                                                              size_t* attrs_sz)
    : pending(status)
    , m_state(st)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
    this->set_client_visible_id(searchid);
}

hyperclient :: pending_sorted_search :: ~pending_sorted_search() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_sorted_search :: request_type()
{
    return hyperdex::REQ_SORTED_SEARCH;
}

int64_t
hyperclient :: pending_sorted_search :: handle_response(hyperclient* cl,
                                                        const po6::net::location& sender,
                                                        std::auto_ptr<e::buffer> msg,
                                                        hyperdex::network_msgtype type,
                                                        hyperclient_returncode* status)
{
    assert(m_state->m_ref > 0);
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_SORTED_SEARCH)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    e::buffer::unpacker up = msg->unpack_from(HYPERCLIENT_HEADER_SIZE);
    uint64_t num_results = 0;
    up = up >> num_results;

    if (up.error())
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    for (uint64_t i = 0; i < num_results; ++i)
    {
        e::slice key;
        std::vector<e::slice> value;
        up = up >> key >> value;

        if (up.error())
        {
            cl->killall(sender, HYPERCLIENT_SERVERERROR);
            return 0;
        }

        m_state->m_results.push_back(state::item(m_state.get(), key, value));
        std::push_heap(m_state->m_results.begin(), m_state->m_results.end());

        if (m_state->m_results.size() > m_state->m_limit)
        {
            std::pop_heap(m_state->m_results.begin(), m_state->m_results.end());
            m_state->m_results.pop_back();
        }
    }

    m_state->m_backings[m_state->m_backing_idx] = msg;
    ++m_state->m_backing_idx;

    if (m_state->m_ref == 1)
    {
        std::sort(m_state->m_results.begin(), m_state->m_results.end(), std::greater<state::item>());

        for (size_t i = 0; i < m_state->m_results.size(); ++i)
        {
            int64_t nonce = cl->m_server_nonce;
            cl->m_incomplete.insert(std::make_pair(nonce, this));
            cl->m_complete_succeeded.push(nonce);
            ++cl->m_server_nonce;
        }

        if (m_state->m_results.empty())
        {
            cl->m_complete_failed.push(completedop(this, HYPERCLIENT_SEARCHDONE, 0));
        }
    }

    return 0;
}

int64_t
hyperclient :: pending_sorted_search :: return_one(hyperclient* cl,
                                                   hyperclient_returncode* status)
{
    assert(m_state->m_returned < m_state->m_results.size());
    hyperclient_returncode op_status;
    e::slice& key(m_state->m_results[m_state->m_returned].key);
    std::vector<e::slice>& value(m_state->m_results[m_state->m_returned].value);

    if (value_to_attributes(*cl->m_config, this->entity(), key.data(), key.size(),
                            value, status, &op_status, m_attrs, m_attrs_sz))
    {
        set_status(HYPERCLIENT_SUCCESS);
    }
    else
    {
        set_status(op_status);
    }

    ++m_state->m_returned;

    if (m_state->m_returned == m_state->m_results.size())
    {
        cl->m_complete_failed.push(completedop(this, HYPERCLIENT_SEARCHDONE, 0));
    }

    return client_visible_id();
}

hyperclient :: pending_sorted_search :: state :: item :: item()
    : st(NULL)
    , key()
    , value()
{
}

hyperclient :: pending_sorted_search :: state :: item :: item(pending_sorted_search::state* _st,
                                                              const e::slice& _key,
                                                              const std::vector<e::slice>& _value)
    : st(_st)
    , key(_key)
    , value(_value)
{
}

hyperclient :: pending_sorted_search :: state :: item :: item(const item& other)
    : st(other.st)
    , key(other.key)
    , value(other.value)
{
}

hyperclient :: pending_sorted_search :: state :: item :: ~item() throw ()
{
}

hyperclient::pending_sorted_search::state::item&
hyperclient :: pending_sorted_search :: state :: item :: operator = (const item& other)
{
    st = other.st;
    key = other.key;
    value = other.value;
    return *this;
}

bool
hyperclient :: pending_sorted_search :: state :: item :: operator < (const item& rhs) const
{
    const item& lhs(*this);
    assert(lhs.st == rhs.st);
    int cmp = 0;

    if (st->m_sort_by == 0)
    {
        cmp = compare_as_type(lhs.key, rhs.key, st->m_sort_type);
    }
    else
    {
        cmp = compare_as_type(lhs.value[st->m_sort_by - 1],
                              rhs.value[st->m_sort_by - 1],
                              st->m_sort_type);
    }

    if (st->m_maximize)
    {
        return cmp < 0;
    }
    else
    {
        return cmp > 0;
    }
}

bool
hyperclient :: pending_sorted_search :: state :: item :: operator > (const item& rhs) const
{
    const item& lhs(*this);
    assert(lhs.st == rhs.st);
    int cmp = 0;

    if (st->m_sort_by == 0)
    {
        cmp = compare_as_type(lhs.key, rhs.key, st->m_sort_type);
    }
    else
    {
        cmp = compare_as_type(lhs.value[st->m_sort_by - 1],
                              rhs.value[st->m_sort_by - 1],
                              st->m_sort_type);
    }

    if (st->m_maximize)
    {
        return cmp > 0;
    }
    else
    {
        return cmp < 0;
    }
}

hyperclient :: pending_sorted_search :: state :: state(std::auto_ptr<e::buffer>* backings,
                                                       uint64_t _limit,
                                                       uint16_t _sort_by,
                                                       hyperdatatype type,
                                                       bool maximize)
    : m_ref(0)
    , m_limit(_limit)
    , m_sort_by(_sort_by)
    , m_sort_type(type)
    , m_maximize(maximize)
    , m_results()
    , m_backings(backings)
    , m_backing_idx(0)
    , m_returned(0)
{
}

hyperclient :: pending_sorted_search :: state :: ~state() throw ()
{
    if (m_backings)
    {
        delete[] m_backings;
    }
}
