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
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending_sorted_search.h"
#include "hyperclient/util.h"

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
    uint16_t retcode;
    uint64_t num_results = 0;
    up = up >> retcode >> num_results;

    if (up.error() || static_cast<hyperdex::network_returncode>(retcode) != hyperdex::NET_SUCCESS)
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

        m_state->m_results.push_back(sorted_search_item(key, value, m_state->m_sort_by));
        std::push_heap(m_state->m_results.begin(), m_state->m_results.end(), m_state->m_cmp);

        if (m_state->m_results.size() > m_state->m_limit)
        {
            std::pop_heap(m_state->m_results.begin(), m_state->m_results.end(), m_state->m_cmp);
            // XXX may optimize here:  assume server results are sorted.  If we pop
            // what was just pushed, then break.
            m_state->m_results.pop_back();
        }
    }

    m_state->m_backings[m_state->m_backing_idx] = msg;
    ++m_state->m_backing_idx;

    if (m_state->m_ref == 1)
    {
        std::sort(m_state->m_results.begin(), m_state->m_results.end(), m_state->m_cmp);

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

sorted_search_item :: sorted_search_item(const e::slice& _key,
                                         const std::vector<e::slice>& _value,
                                         uint16_t _sort_by)
    : sort_by(_sort_by)
    , key(_key)
    , value(_value)
{
}

sorted_search_item :: ~sorted_search_item() throw ()
{
}

// XXX This duplicates code in the daemon.
static bool
sorted_search_lt_string(const sorted_search_item& ssilhs,
                        const sorted_search_item& ssirhs)
{
    assert(ssilhs.sort_by == ssirhs.sort_by);
    e::slice lhs(ssilhs.sort_by > 0 ? ssilhs.value[ssilhs.sort_by - 1] : ssilhs.key);
    e::slice rhs(ssirhs.sort_by > 0 ? ssirhs.value[ssirhs.sort_by - 1] : ssirhs.key);
    int cmp = memcmp(lhs.data(), rhs.data(), std::min(lhs.size(), rhs.size()));

    if (cmp == 0)
    {
        return lhs.size() < rhs.size();
    }

    return cmp < 0;
}

static bool
sorted_search_gt_string(const sorted_search_item& lhs,
                        const sorted_search_item& rhs)
{
    return sorted_search_lt_string(rhs, lhs);
}

static bool
sorted_search_lt_int64(const sorted_search_item& ssilhs,
                       const sorted_search_item& ssirhs)
{
    assert(ssilhs.sort_by == ssirhs.sort_by);
    e::slice lhs(ssilhs.sort_by > 0 ? ssilhs.value[ssilhs.sort_by - 1] : ssilhs.key);
    e::slice rhs(ssirhs.sort_by > 0 ? ssirhs.value[ssirhs.sort_by - 1] : ssirhs.key);
    uint8_t buflhs[sizeof(int64_t)];
    uint8_t bufrhs[sizeof(int64_t)];
    memset(buflhs, 0, sizeof(int64_t));
    memset(bufrhs, 0, sizeof(int64_t));
    memmove(buflhs, lhs.data(), lhs.size());
    memmove(bufrhs, rhs.data(), rhs.size());
    int64_t ilhs;
    int64_t irhs;
    e::unpack64le(buflhs, &ilhs);
    e::unpack64le(bufrhs, &irhs);
    return ilhs < irhs;
}

static bool
sorted_search_gt_int64(const sorted_search_item& lhs,
                       const sorted_search_item& rhs)
{
    return sorted_search_lt_int64(rhs, lhs);
}

hyperclient :: pending_sorted_search :: state :: state(std::auto_ptr<e::buffer>* backings,
                                                       uint64_t _limit,
                                                       uint16_t _sort_by,
                                                       hyperdatatype type,
                                                       bool maximize)
    : m_ref(0)
    , m_limit(_limit)
    , m_sort_by(_sort_by)
    , m_results()
    , m_backings(backings)
    , m_backing_idx(0)
    , m_returned(0)
    , m_cmp()
{
    if (type == HYPERDATATYPE_STRING)
    {
        if (maximize)
        {
            m_cmp = sorted_search_gt_string;
        }
        else
        {
            m_cmp = sorted_search_lt_string;
        }
    }
    else if (type == HYPERDATATYPE_INT64)
    {
        if (maximize)
        {
            m_cmp = sorted_search_gt_int64;
        }
        else
        {
            m_cmp = sorted_search_lt_int64;
        }
    }
    else
    {
        abort();
    }
}

hyperclient :: pending_sorted_search :: state :: ~state() throw ()
{
    if (m_backings)
    {
        delete[] m_backings;
    }
}
