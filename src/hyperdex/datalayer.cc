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

#define __STDC_LIMIT_MACROS

// C
#include <cassert>
#include <cstdlib>

// POSIX
#include <sys/stat.h>
#include <sys/types.h>

// C++
#include <limits>

// STL
#include <iomanip>
#include <set>
#include <sstream>
#include <tr1/functional>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/pathname.h>

// HyperDex
#include <hyperdex/datalayer.h>

typedef e::intrusive_ptr<hyperdex::region> region_ptr;
typedef std::map<e::buffer, std::pair<uint64_t, std::vector<e::buffer> > > region_map_t;

hyperdex :: datalayer :: datalayer()
    : m_shutdown(false)
    , m_base("")
    , m_flusher(std::tr1::bind(&datalayer::flush_loop, this))
    , m_lock()
    , m_regions()
{
    m_flusher.start();
}

hyperdex :: datalayer :: ~datalayer() throw ()
{
    if (!m_shutdown)
    {
        shutdown();
    }

    m_flusher.join();
}

void
hyperdex :: datalayer :: prepare(const configuration& newconfig, const instance& us)
{
    // Create new regions which we do not currently have.
    std::map<regionid, e::intrusive_ptr<hyperdex::region> > regions;

    // Grab a copy of all the regions we do have.
    {
        po6::threads::rwlock::rdhold hold(&m_lock);
        regions = m_regions;
    }

    for (std::map<entityid, instance>::const_iterator e = newconfig.entity_mapping().begin();
            e != newconfig.entity_mapping().end(); ++e)
    {
        if (e->first.space != std::numeric_limits<uint32_t>::max() - 1 && e->second == us
            && regions.find(e->first.get_region()) == regions.end())
        {
            std::map<hyperdex::regionid, size_t>::iterator region_size;
            region_size = newconfig.regions().find(e->first.get_region());

            if (region_size != newconfig.regions().end())
            {
                create_region(e->first.get_region(), region_size->second);
            }
            else
            {
                LOG(ERROR) << "There is a logic error in the configuration object.";
            }
        }
    }

    std::map<uint16_t, regionid> transfers = newconfig.transfers_to(us);

    for (std::map<uint16_t, regionid>::const_iterator t = transfers.begin();
            t != transfers.end(); ++t)
    {
        if (regions.find(t->second) == regions.end())
        {
            std::map<hyperdex::regionid, size_t>::iterator region_size;
            region_size = newconfig.regions().find(t->second);

            if (region_size != newconfig.regions().end())
            {
                create_region(t->second, region_size->second);
            }
            else
            {
                LOG(ERROR) << "There is a logic error in the configuration object.";
            }
        }
    }
}

void
hyperdex :: datalayer :: reconfigure(const configuration&, const instance&)
{
    // Do nothing.
}

void
hyperdex :: datalayer :: cleanup(const configuration& newconfig, const instance& us)
{
    // Delete regions which are no longer in the config.
    std::map<regionid, e::intrusive_ptr<hyperdex::region> > regions;
    std::map<uint16_t, regionid> transfers = newconfig.transfers_to(us);

    // Grab a copy of all the regions we do have.
    {
        po6::threads::rwlock::rdhold hold(&m_lock);
        regions = m_regions;
    }

    for (std::map<regionid, e::intrusive_ptr<hyperdex::region> >::iterator r = regions.begin();
            r != regions.end(); ++r)
    {
        bool keep = false;
        std::map<entityid, instance>::const_iterator start;
        std::map<entityid, instance>::const_iterator end;
        start = newconfig.entity_mapping().lower_bound(entityid(r->first, 0));
        end = newconfig.entity_mapping().upper_bound(entityid(r->first, UINT8_MAX));

        for (; start != end; ++start)
        {
            if (start->second == us)
            {
                keep = true;
            }
        }

        for (std::map<uint16_t, regionid>::const_iterator t = transfers.begin();
                t != transfers.end(); ++t)
        {
            if (t->second == r->first)
            {
                keep = true;
            }
        }

        if (!keep)
        {
            drop_region(r->first);
        }
    }
}

void
hyperdex :: datalayer :: shutdown()
{
    m_shutdown = true;
}

e::intrusive_ptr<hyperdex::region::snapshot>
hyperdex :: datalayer :: make_snapshot(const regionid& ri)
{
    e::intrusive_ptr<hyperdex::region> r = get_region(ri);

    if (!r)
    {
        return e::intrusive_ptr<hyperdex::region::snapshot>();
    }

    return r->make_snapshot();
}

e::intrusive_ptr<hyperdex::region::rolling_snapshot>
hyperdex :: datalayer :: make_rolling_snapshot(const regionid& ri)
{
    e::intrusive_ptr<hyperdex::region> r = get_region(ri);

    if (!r)
    {
        return e::intrusive_ptr<hyperdex::region::rolling_snapshot>();
    }

    return r->make_rolling_snapshot();
}

hyperdex :: result_t
hyperdex :: datalayer :: get(const regionid& ri,
                             const e::buffer& key,
                             std::vector<e::buffer>* value,
                             uint64_t* version)
{
    e::intrusive_ptr<hyperdex::region> r = get_region(ri);

    if (!r)
    {
        return INVALID;
    }

    return r->get(key, value, version);
}

hyperdex :: result_t
hyperdex :: datalayer :: put(const regionid& ri,
                             const e::buffer& key,
                             const std::vector<e::buffer>& value,
                             uint64_t version)
{
    e::intrusive_ptr<hyperdex::region> r = get_region(ri);

    if (!r)
    {
        return INVALID;
    }

    return r->put(key, value, version);
}

hyperdex :: result_t
hyperdex :: datalayer :: del(const regionid& ri,
                             const e::buffer& key)
{
    e::intrusive_ptr<hyperdex::region> r = get_region(ri);

    if (!r)
    {
        return INVALID;
    }

    return r->del(key);
}

e::intrusive_ptr<hyperdex::region>
hyperdex :: datalayer :: get_region(const regionid& ri)
{
    po6::threads::rwlock::rdhold hold(&m_lock);
    std::map<regionid, e::intrusive_ptr<region> >::iterator i;
    i = m_regions.find(ri);

    if (i == m_regions.end())
    {
        return e::intrusive_ptr<hyperdex::region>();
    }
    else
    {
        return i->second;
    }
}

void
hyperdex :: datalayer :: flush_loop()
{
    LOG(WARNING) << "Flush thread started.";

    while (!m_shutdown)
    {
        std::set<e::intrusive_ptr<region> > to_flush;
        bool sleep = true;

        { // Hold the lock only in this scope.
            po6::threads::rwlock::rdhold hold(&m_lock);

            for (std::map<regionid, e::intrusive_ptr<region> >::iterator i = m_regions.begin();
                    i != m_regions.end(); ++i)
            {
                to_flush.insert(i->second);
            }
        }

        for (std::set<e::intrusive_ptr<region> >::iterator i = to_flush.begin();
                    i != to_flush.end(); ++i)
        {
            if ((*i)->flush() > 0)
            {
                sleep = false;
            }

            (*i)->async();
        }

        if (sleep)
        {
            timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000000; // 100ms
            nanosleep(&ts, &ts); // If interrupted, it is no big deal.
        }
    }
}

void
hyperdex :: datalayer :: create_region(const regionid& ri, uint16_t num_columns)
{
    LOG(INFO) << "Creating " << ri << " with " << num_columns << " columns.";
    region_ptr reg;
    reg = new region(ri, m_base, num_columns);
    po6::threads::rwlock::wrhold hold(&m_lock);
    m_regions.insert(std::make_pair(ri, reg));
}

void
hyperdex :: datalayer :: drop_region(const regionid& ri)
{
    po6::threads::rwlock::wrhold hold(&m_lock);
    std::map<regionid, region_ptr>::iterator i;
    i = m_regions.find(ri);

    if (i != m_regions.end())
    {
        LOG(INFO) << "Dropping " << ri << ".";
        i->second->drop();
        m_regions.erase(i);
    }
}
