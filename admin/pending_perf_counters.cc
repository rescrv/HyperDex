// Copyright (c) 2013, Cornell University
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

#define __STDC_LIMIT_MACROS

// HyperDex
#include "admin/admin.h"
#include "admin/constants.h"
#include "admin/pending_perf_counters.h"

using hyperdex::pending_perf_counters;

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#define CLOCK_REALTIME 0x01
#define CLOCK_MONOTONIC 0x02
int clock_gettime(int ign, struct timespec * ts){
	clock_serv_t cclock;
	mach_timespec_t mts;
	host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
	clock_get_time(cclock, &mts);
	mach_port_deallocate(mach_task_self(), cclock);
	ts->tv_sec = mts.tv_sec;
	ts->tv_nsec = mts.tv_nsec;
	return 1;
}
#endif

static uint64_t
monotonic_time()
{
    timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    {
        throw po6::error(errno);
    }

    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

struct pending_perf_counters::perf_counter
{
    perf_counter(uint64_t i, uint64_t t, const std::string& p, uint64_t m)
        : id(i), time(t), property(p), measurement(m) {}
    uint64_t id;
    uint64_t time;
    std::string property;
    uint64_t measurement;
};

pending_perf_counters :: pending_perf_counters(uint64_t id,
                                               hyperdex_admin_returncode* s,
                                               hyperdex_admin_perf_counter* pc)
    : pending(id, s)
    , m_pc(pc)
    , m_next_send(0)
    , m_pcs()
    , m_scratch()
    , m_cutoffs()
{
}

pending_perf_counters :: ~pending_perf_counters() throw ()
{
}

void
pending_perf_counters :: send_perf_reqs(admin* adm,
                                        const configuration* config,
                                        hyperdex_admin_returncode* status)
{
    std::vector<std::pair<server_id, po6::net::location> > addrs;
    config->get_all_addresses(&addrs);
    uint64_t failed = 0;

    for (size_t i = 0; i < addrs.size(); ++i)
    {
        uint64_t nonce = adm->m_next_server_nonce;
        ++adm->m_next_server_nonce;
        uint64_t when = 0;
        std::map<server_id, uint64_t>::iterator it = m_cutoffs.find(addrs[i].first);

        if (it == m_cutoffs.end())
        {
            m_cutoffs[addrs[i].first] = 0;
        }
        else
        {
            when = it->second;
        }

        size_t sz = HYPERDEX_ADMIN_HEADER_SIZE_REQ + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_ADMIN_HEADER_SIZE_REQ) << when;

        if (!adm->send(PERF_COUNTERS, addrs[i].first, nonce, msg, this, status))
        {
            ++failed;
        }
    }

    std::map<server_id, uint64_t>::iterator it = m_cutoffs.begin();

    while (it != m_cutoffs.end())
    {
        bool found = false;

        for (size_t i = 0; i < addrs.size(); ++i)
        {
            if (it->first == addrs[i].first)
            {
                found = true;
                break;
            }
        }

        if (found)
        {
            ++it;
        }
        else
        {
            m_cutoffs.erase(it);
            it = m_cutoffs.begin();
        }
    }
}

int
pending_perf_counters :: millis_to_next_send()
{
    const uint64_t one_second = 1000ULL * 1000ULL * 1000ULL;
    uint64_t now = monotonic_time();

    if (now >= m_next_send)
    {
        m_next_send = now + one_second;
        return 0;
    }

    return ((m_next_send - now) / one_second) + 1;
}

bool
pending_perf_counters :: can_yield()
{
    return !m_pcs.empty();
}

bool
pending_perf_counters :: yield(hyperdex_admin_returncode* status)
{
    assert(can_yield());
    m_scratch = m_pcs.front().property;
    m_pc->id = m_pcs.front().id;
    m_pc->time = m_pcs.front().time;
    m_pc->property = m_scratch.c_str();
    m_pc->measurement = m_pcs.front().measurement;
    m_pcs.pop_front();
    *status = HYPERDEX_ADMIN_SUCCESS;
    return true;
}

void
pending_perf_counters :: handle_sent_to(const server_id&)
{
}

void
pending_perf_counters :: handle_failure(const server_id&)
{
}

bool
pending_perf_counters :: handle_message(admin*,
                                        const server_id& si,
                                        network_msgtype mt,
                                        std::auto_ptr<e::buffer>,
                                        e::unpacker up,
                                        hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;
    set_status(HYPERDEX_ADMIN_SUCCESS);

    if (mt != PERF_COUNTERS)
    {
        set_status(HYPERDEX_ADMIN_SERVERERROR);
        return true;
    }

    e::slice rem = up.as_slice();
    char* ptr = const_cast<char*>(reinterpret_cast<const char*>(rem.data()));
    char* end = ptr + rem.size() - 1;
    uint64_t max_time = 0;

    // parse one line at a time
    while (ptr < end)
    {
        char* eol = reinterpret_cast<char*>(memchr(ptr, '\n', end - ptr));
        eol = eol ? eol : end;
        *eol = '\0';
        char* tmp = ptr;

        // parse the time
        tmp = strchr(ptr, ' ');
        tmp = tmp ? tmp : eol;
        *tmp = '\0';
        uint64_t time = strtoull(ptr, NULL, 0);
        max_time = std::max(time, max_time);
        ptr = tmp + 1;

        while (ptr < eol)
        {
            tmp = strchr(ptr, ' ');
            tmp = tmp ? tmp : eol;
            *tmp = '\0';

            char* equals = strchr(ptr, '=');
            equals = equals ? equals: tmp;
            *equals = '\0';
            uint64_t value = 0;

            if (equals < tmp)
            {
                ++equals;
                value = strtoull(equals, NULL, 0);
            }

            m_pcs.push_back(perf_counter(si.get(), time, ptr, value));
            ptr = tmp + 1;
        }

        ptr = eol + 1;
    }

    m_cutoffs[si] = max_time;
    return true;
}
