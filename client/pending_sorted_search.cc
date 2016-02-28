// Copyright (c) 2012-2013, Cornell University
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

// HyperDex
#include "client/client.h"
#include "client/pending_sorted_search.h"
#include "client/util.h"
#include <functional>

using hyperdex::datatype_info;
using hyperdex::pending_sorted_search;

pending_sorted_search :: pending_sorted_search(client* cl,
                                               uint64_t id,
                                               bool maximize,
                                               uint64_t limit,
                                               uint16_t sort_by_idx,
                                               datatype_info* sort_by_di,
                                               hyperdex_client_returncode* status,
                                               const hyperdex_client_attribute** attrs,
                                               size_t* attrs_sz)
    : pending_aggregation(id, status)
    , m_cl(cl)
    , m_yield(false)
    , m_ri()
    , m_maximize(maximize)
    , m_limit(limit)
    , m_sort_by_idx(sort_by_idx)
    , m_sort_by_di(sort_by_di)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
    , m_results()
    , m_results_idx(0)
{
}

pending_sorted_search :: ~pending_sorted_search() throw ()
{
}

bool
pending_sorted_search :: can_yield()
{
    return m_yield;
}

bool
pending_sorted_search :: yield(hyperdex_client_returncode* status, e::error* err)
{
    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();
    m_yield = false;

    if (this->aggregation_done() && m_results_idx >= m_results.size())
    {
        set_status(HYPERDEX_CLIENT_SEARCHDONE);
        set_error(e::error());
        return true;
    }

    m_yield = true;

    hyperdex_client_returncode op_status;
    e::error op_error;
    const e::slice& key(m_results[m_results_idx].key);
    const std::vector<e::slice>& value(m_results[m_results_idx].value);
    ++m_results_idx;

    if (!value_to_attributes(m_cl->m_config, m_ri, key.data(), key.size(),
                             value, &op_status, &op_error, m_attrs, m_attrs_sz, m_cl->m_convert_types))
    {
        set_status(op_status);
        set_error(op_error);
        return true;
    }

    set_status(HYPERDEX_CLIENT_SUCCESS);
    set_error(e::error());
    return true;
}

void
pending_sorted_search :: handle_sent_to(const server_id& si,
                                        const virtual_server_id& vsi)
{
    if (m_ri == region_id())
    {
        m_ri = m_cl->m_config.get_region_id(vsi);
    }

    return pending_aggregation::handle_sent_to(si, vsi);
}

void
pending_sorted_search :: handle_failure(const server_id& si,
                                        const virtual_server_id& vsi)
{
    m_yield = true;
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << vsi << "/" << si;
    return pending_aggregation::handle_failure(si, vsi);
}

namespace hyperdex 
{

bool
operator < (const pending_sorted_search::item& lhs, const pending_sorted_search::item& rhs)
{
    assert(lhs.ssc_ptr == rhs.ssc_ptr);
    const sorted_search_comparator* ssc_ptr = lhs.ssc_ptr;

    if (ssc_ptr->m_sort_by_idx > lhs.value.size() ||
        ssc_ptr->m_sort_by_idx > rhs.value.size() ||
        lhs.value.size() != rhs.value.size())
    {
        return false;
    }

    int cmp = 0;

    if (ssc_ptr->m_sort_by_idx == 0)
    {
        cmp = ssc_ptr->m_sort_by_di->compare(lhs.key, rhs.key);
    }
    else
    {
		cmp = ssc_ptr->m_sort_by_di->compare(lhs.value[ssc_ptr->m_sort_by_idx - 1], rhs.value[ssc_ptr->m_sort_by_idx - 1]);
    }

    if (ssc_ptr->m_maximize)
    {
        return cmp < 0;
    }
    else
    {
        return cmp > 0;
    }
}

bool
operator > (const pending_sorted_search::item& lhs, const pending_sorted_search::item& rhs)
{
    assert(lhs.ssc_ptr == rhs.ssc_ptr);
    const sorted_search_comparator* ssc_ptr = lhs.ssc_ptr;

    if (ssc_ptr->m_sort_by_idx > lhs.value.size() ||
        ssc_ptr->m_sort_by_idx > rhs.value.size() ||
        lhs.value.size() != rhs.value.size())
    {
        return false;
    }

    int cmp = 0;

    if (ssc_ptr->m_sort_by_idx == 0)
    {
        cmp = ssc_ptr->m_sort_by_di->compare(lhs.key, rhs.key);
    }
    else
    {
		cmp = ssc_ptr->m_sort_by_di->compare(lhs.value[ssc_ptr->m_sort_by_idx - 1], rhs.value[ssc_ptr->m_sort_by_idx - 1]);
    }

    if (ssc_ptr->m_maximize)
    {
        return cmp > 0;
    }
    else
    {
        return cmp < 0;
    }
}

}

bool
pending_sorted_search :: handle_message(client* cl,
                                        const server_id& si,
                                        const virtual_server_id& vsi,
                                        network_msgtype mt,
                                        std::auto_ptr<e::buffer> msg,
                                        e::unpacker up,
                                        hyperdex_client_returncode* status,
                                        e::error* err)
{
    bool handled = pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();

    if (mt != RESP_SORTED_SEARCH)
    {
        PENDING_ERROR(SERVERERROR) << "server " << vsi << " responded to SORTED_SEARCH with " << mt;
        m_yield = true;
        return true;
    }

    uint64_t num_results = 0;
    up = up >> num_results;

    if (up.error())
    {
        PENDING_ERROR(SERVERERROR) << "communication error: server "
                                   << vsi << " sent corrupt message="
                                   << msg->as_slice().hex()
                                   << " in response to a SORTED_SEARCH";
        m_yield = true;
        return true;
    }

    sorted_search_comparator ssc(m_maximize, m_sort_by_idx, m_sort_by_di);
    e::compat::shared_ptr<e::buffer> backing(msg.release());

    for (uint64_t i = 0; i < num_results; ++i)
    {
        e::slice key;
        std::vector<e::slice> value;
        up = up >> key >> value;

        if (up.error())
        {
            PENDING_ERROR(SERVERERROR) << "communication error: server "
                                       << vsi << " sent corrupt message="
                                       << msg->as_slice().hex()
                                       << " in response to a SORTED_SEARCH";
            m_yield = true;
            return true;
        }

        m_results.push_back(item(&ssc, key, value, backing));
        std::push_heap(m_results.begin(), m_results.end());

        if (m_results.size() > m_limit)
        {
            std::pop_heap(m_results.begin(), m_results.end());
            m_results.pop_back();
        }
    }

    m_yield = this->aggregation_done();
    set_status(HYPERDEX_CLIENT_SUCCESS);
    set_error(e::error());

    if (m_yield)
    {
        std::sort(m_results.begin(), m_results.end(), std::greater<pending_sorted_search::item>());
    }

    return true;
}

pending_sorted_search :: item :: item(const sorted_search_comparator *sscobj, 
		                              const e::slice& _key,
                                      const std::vector<e::slice>& _value,
                                      e::compat::shared_ptr<e::buffer> _backing)
    : ssc_ptr(sscobj)
	, key(_key)
    , value(_value)
    , backing(_backing)
{
}

pending_sorted_search :: item :: item(const item& other)
    : ssc_ptr(other.ssc_ptr)
    , key(other.key)
    , value(other.value)
    , backing(other.backing)
{
}

pending_sorted_search :: item :: ~item() throw ()
{
}

pending_sorted_search::item&
pending_sorted_search :: item :: operator = (const item& other)
{
    if (this != &other)
    {
		ssc_ptr = other.ssc_ptr;
        key = other.key;
        value = other.value;
        backing = other.backing;
    }

    return *this;
}
