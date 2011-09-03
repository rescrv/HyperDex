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

// e
#include <e/timer.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/coordinatorlink.h>

// HyperDaemon
#include "datalayer.h"

using hyperdex::regionid;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::configuration;
using hyperdex::coordinatorlink;

typedef e::intrusive_ptr<hyperdisk::disk> disk_ptr;
typedef std::map<hyperdex::regionid, disk_ptr> disk_map_t;

hyperdaemon :: datalayer :: datalayer(coordinatorlink* cl, const po6::pathname& base)
    : m_cl(cl)
    , m_shutdown(false)
    , m_base(base)
    , m_flusher(std::tr1::bind(&datalayer::flush_loop, this))
    , m_lock()
    , m_disks()
{
    m_flusher.start();
}

hyperdaemon :: datalayer :: ~datalayer() throw ()
{
    if (!m_shutdown)
    {
        shutdown();
    }

    m_flusher.join();
}

void
hyperdaemon :: datalayer :: prepare(const configuration& newconfig, const instance& us)
{
    // Create new disks which we do not currently have.
    std::map<regionid, e::intrusive_ptr<hyperdisk::disk> > disks;

    // Grab a copy of all the disks we do have.
    {
        po6::threads::rwlock::rdhold hold(&m_lock);
        disks = m_disks;
    }

    for (std::map<entityid, instance>::const_iterator e = newconfig.entity_mapping().begin();
            e != newconfig.entity_mapping().end(); ++e)
    {
        if (e->first.space != std::numeric_limits<uint32_t>::max() - 1 && e->second == us
            && disks.find(e->first.get_region()) == disks.end())
        {
            std::map<regionid, size_t>::iterator region_size;
            region_size = newconfig.regions().find(e->first.get_region());

            if (region_size != newconfig.regions().end())
            {
                create_disk(e->first.get_region(), region_size->second);
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
        if (disks.find(t->second) == disks.end())
        {
            std::map<regionid, size_t>::iterator region_size;
            region_size = newconfig.regions().find(t->second);

            if (region_size != newconfig.regions().end())
            {
                create_disk(t->second, region_size->second);
            }
            else
            {
                LOG(ERROR) << "There is a logic error in the configuration object.";
            }
        }
    }
}

void
hyperdaemon :: datalayer :: reconfigure(const configuration&, const instance&)
{
    // Do nothing.
}

void
hyperdaemon :: datalayer :: cleanup(const configuration& newconfig, const instance& us)
{
    // Delete disks which are no longer in the config.
    disk_map_t disks;
    std::map<uint16_t, regionid> transfers = newconfig.transfers_to(us);

    // Grab a copy of all the regions we do have.
    {
        po6::threads::rwlock::rdhold hold(&m_lock);
        disks = m_disks;
    }

    for (disk_map_t::iterator d = disks.begin(); d != disks.end(); ++d)
    {
        bool keep = false;
        std::map<entityid, instance>::const_iterator start;
        std::map<entityid, instance>::const_iterator end;
        start = newconfig.entity_mapping().lower_bound(entityid(d->first, 0));
        end = newconfig.entity_mapping().upper_bound(entityid(d->first, UINT8_MAX));

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
            if (t->second == d->first)
            {
                keep = true;
            }
        }

        if (!keep)
        {
            drop_disk(d->first);
        }
    }
}

void
hyperdaemon :: datalayer :: shutdown()
{
    m_shutdown = true;
}

e::intrusive_ptr<hyperdisk::snapshot>
hyperdaemon :: datalayer :: make_snapshot(const regionid& ri)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (!r)
    {
        return e::intrusive_ptr<hyperdisk::snapshot>();
    }

    return r->make_snapshot();
}

e::intrusive_ptr<hyperdisk::rolling_snapshot>
hyperdaemon :: datalayer :: make_rolling_snapshot(const regionid& ri)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (!r)
    {
        return e::intrusive_ptr<hyperdisk::rolling_snapshot>();
    }

    return r->make_rolling_snapshot();
}

void
hyperdaemon :: datalayer :: trickle(const regionid& ri)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (r)
    {
        r->flush();
    }
}

hyperdisk::returncode
hyperdaemon :: datalayer :: get(const regionid& ri,
                                const e::buffer& key,
                                std::vector<e::buffer>* value,
                                uint64_t* version)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (!r)
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->get(key, value, version);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: put(const regionid& ri,
                                const e::buffer& key,
                                const std::vector<e::buffer>& value,
                                uint64_t version)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (!r)
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->put(key, value, version);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: del(const regionid& ri,
                                const e::buffer& key)
{
    e::intrusive_ptr<hyperdisk::disk> r = get_region(ri);

    if (!r)
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->del(key);
}

e::intrusive_ptr<hyperdisk::disk>
hyperdaemon :: datalayer :: get_region(const regionid& ri)
{
    po6::threads::rwlock::rdhold hold(&m_lock);
    disk_map_t::iterator i;
    i = m_disks.find(ri);

    if (i == m_disks.end())
    {
        return e::intrusive_ptr<hyperdisk::disk>();
    }
    else
    {
        return i->second;
    }
}

void
hyperdaemon :: datalayer :: flush_loop()
{
    LOG(WARNING) << "Flush thread started.";
    uint64_t count = 0;

    while (!m_shutdown)
    {
        std::set<e::intrusive_ptr<hyperdisk::disk> > to_flush;

        { // Hold the lock only in this scope.
            po6::threads::rwlock::rdhold hold(&m_lock);

            for (disk_map_t::iterator i = m_disks.begin();
                    i != m_disks.end(); ++i)
            {
                to_flush.insert(i->second);
            }
        }

        for (std::set<e::intrusive_ptr<hyperdisk::disk> >::iterator i = to_flush.begin();
                    i != to_flush.end(); ++i)
        {
            if (count % 10 == 0)
            {
                (*i)->preallocate();
            }

            (*i)->flush();
        }

        e::sleep_ms(0, 100);
        ++count;
    }
}

void
hyperdaemon :: datalayer :: create_disk(const regionid& ri, uint16_t num_columns)
{
    LOG(INFO) << "Creating " << ri << " with " << num_columns << " columns.";
    std::ostringstream ostr;
    ostr << ri;
    po6::pathname path = po6::join(m_base, po6::pathname(ostr.str()));
    disk_ptr d = new hyperdisk::disk(path, num_columns);
    po6::threads::rwlock::wrhold hold(&m_lock);
    m_disks.insert(std::make_pair(ri, d));
}

void
hyperdaemon :: datalayer :: drop_disk(const regionid& ri)
{
    po6::threads::rwlock::wrhold hold(&m_lock);
    disk_map_t::iterator i;
    i = m_disks.find(ri);

    if (i != m_disks.end())
    {
        LOG(INFO) << "Dropping " << ri << ".";
        i->second->drop();
        m_disks.erase(i);
    }
}
