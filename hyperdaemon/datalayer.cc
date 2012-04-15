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
#include <algorithm>
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
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/runtimeconfig.h"

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
    , m_optimistic_io_thread(std::tr1::bind(&datalayer::optimistic_io_thread, this))
    , m_flush_threads()
    , m_disks()
    , m_preallocate_rr()
    , m_last_preallocation(0)
    , m_optimistic_rr()
    , m_last_dose_of_optimism(0)
    , m_flushed_recently(false)
{
    m_optimistic_io_thread.start();

    for (size_t i = 0; i < FLUSH_THREADS; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread>
            t(new po6::threads::thread(std::tr1::bind(&datalayer::flush_thread, this)));
        t->start();
        m_flush_threads.push_back(t);
    }
}

hyperdaemon :: datalayer :: ~datalayer() throw ()
{
    if (!m_shutdown)
    {
        shutdown();
    }

    m_optimistic_io_thread.join();

    for (size_t i = 0; i < m_flush_threads.size(); ++i)
    {
        m_flush_threads[i]->join();
    }
}

void
hyperdaemon :: datalayer :: prepare(const configuration& newconfig, const instance& us)
{
    std::set<regionid> regions = newconfig.regions_for(us);
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);
    std::map<uint16_t, regionid>::iterator t;

    // Make sure that inbound state exists for each in-progress transfer to us.
    for (t = in_transfers.begin(); t != in_transfers.end(); ++t)
    {
        regions.insert(t->second);
    }

    for (std::set<regionid>::const_iterator r = regions.begin();
            r != regions.end(); ++r)
    {
        if (!m_disks.contains(*r))
        {
            create_disk(*r, newconfig.disk_hasher(r->get_subspace()),
                        newconfig.dimensions(r->get_space()));
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
    std::set<regionid> regions = newconfig.regions_for(us);
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);
    std::map<uint16_t, regionid>::iterator t;

    // Make sure that inbound state exists for each in-progress transfer to us.
    for (t = in_transfers.begin(); t != in_transfers.end(); ++t)
    {
        regions.insert(t->second);
    }

    for (disk_map_t::iterator d = m_disks.begin(); d != m_disks.end(); d.next())
    {
        if (regions.find(d.key()) == regions.end())
        {
            drop_disk(d.key());
        }
    }
}

void
hyperdaemon :: datalayer :: shutdown()
{
    m_shutdown = true;
}

e::intrusive_ptr<hyperdisk::snapshot>
hyperdaemon :: datalayer :: make_snapshot(const regionid& ri,
                                          const hyperspacehashing::search& terms)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return e::intrusive_ptr<hyperdisk::snapshot>();
    }

    return r->make_snapshot(terms);
}

e::intrusive_ptr<hyperdisk::rolling_snapshot>
hyperdaemon :: datalayer :: make_rolling_snapshot(const regionid& ri)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return e::intrusive_ptr<hyperdisk::rolling_snapshot>();
    }

    return r->make_rolling_snapshot();
}

hyperdisk::returncode
hyperdaemon :: datalayer :: get(const regionid& ri,
                                const e::slice& key,
                                std::vector<e::slice>* value,
                                uint64_t* version,
                                hyperdisk::reference* ref)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->get(key, value, version, ref);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: put(const regionid& ri,
                                std::tr1::shared_ptr<e::buffer> backing,
                                const e::slice& key,
                                const std::vector<e::slice>& value,
                                uint64_t version)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->put(backing, key, value, version);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: del(const regionid& ri,
                                std::tr1::shared_ptr<e::buffer> backing,
                                const e::slice& key)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->del(backing, key);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: flush(const regionid& ri,
                                  size_t n)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->flush(n);
}

typedef std::map<hyperdex::regionid, e::intrusive_ptr<hyperdisk::disk> > disk_map_t;
typedef std::queue<hyperdex::regionid> disk_queue_t;

void
hyperdaemon :: datalayer :: optimistic_io_thread()
{
    LOG(WARNING) << "Started optimistic-I/O thread.";

    while (!m_shutdown)
    {
        // Ensure that all regions are in the preallocation and optimistic-I/O
        // queues.
        for (disk_map_t::iterator d = m_disks.begin(); d != m_disks.end(); d.next())
        {
            if (std::find(m_preallocate_rr.begin(), m_preallocate_rr.end(), d.key())
                    == m_preallocate_rr.end())
            {
                m_preallocate_rr.push_back(d.key());
            }

            if (std::find(m_optimistic_rr.begin(), m_optimistic_rr.end(), d.key())
                    == m_optimistic_rr.end())
            {
                m_optimistic_rr.push_back(d.key());
            }
        }

        // We rate-limit the number of preallocations we do each second.
        uint64_t nanos_since_last_prealloc = e::time() - m_last_preallocation;
        uint64_t preallocation_interval = 1000000000. / PREALLOCATIONS_PER_SECOND;

        if (nanos_since_last_prealloc / preallocation_interval >= 1)
        {
            for (size_t i = 0; i < m_preallocate_rr.size(); ++i)
            {
                e::intrusive_ptr<hyperdisk::disk> d;

                if (m_disks.lookup(m_preallocate_rr.front(), &d))
                {
                    m_preallocate_rr.push_back(m_preallocate_rr.front());
                    hyperdisk::returncode ret = d->preallocate();

                    if (ret == hyperdisk::SUCCESS)
                    {
                        break;
                    }
                    else if (ret == hyperdisk::DIDNOTHING)
                    {
                    }
                    else
                    {
                        PLOG(WARNING) << "Disk preallocation failed";
                    }
                }

                m_preallocate_rr.pop_front();
            }

            m_last_preallocation = e::time();
        }

        // We rate-limit the number of optimistic splits we do each second.
        uint64_t nanos_since_last_optimism = e::time() - m_last_dose_of_optimism;
        uint64_t optimism_interval = 1000000000. / OPTIMISM_BURSTS_PER_SECOND;

        if (nanos_since_last_optimism / optimism_interval >= 1)
        {
            for (size_t i = 0; i < m_optimistic_rr.size(); ++i)
            {
                e::intrusive_ptr<hyperdisk::disk> d;

                if (m_disks.lookup(m_optimistic_rr.front(), &d))
                {
                    m_optimistic_rr.push_back(m_optimistic_rr.front());
                    hyperdisk::returncode ret = d->do_optimistic_io();

                    if (ret == hyperdisk::SUCCESS)
                    {
                        break;
                    }
                    else if (ret == hyperdisk::DIDNOTHING)
                    {
                    }
                    else
                    {
                        PLOG(WARNING) << "Optimistic disk I/O failed";
                    }
                }

                m_optimistic_rr.pop_front();
            }

            m_last_dose_of_optimism = e::time();
        }

        (void) __sync_and_and_fetch(&m_flushed_recently, false);

        do
        {
            e::sleep_ms(0, 10);
        } while (!m_shutdown && !__sync_and_and_fetch(&m_flushed_recently, true));
    }
}

void
hyperdaemon :: datalayer :: flush_thread()
{
    LOG(WARNING) << "Started data-flush thread.";

    while (!m_shutdown)
    {
        bool sleep = true;

        for (disk_map_t::iterator d = m_disks.begin();
                d != m_disks.end(); d.next())
        {
            hyperdisk::returncode ret = d.value()->flush(10000);

            if (ret == hyperdisk::SUCCESS)
            {
                sleep = false;
            }
            else if (ret == hyperdisk::DIDNOTHING)
            {
            }
            else if (ret == hyperdisk::DATAFULL || ret == hyperdisk::SEARCHFULL)
            {
                hyperdisk::returncode ioret;
                ioret = d.value()->do_mandatory_io();

                if (ioret != hyperdisk::SUCCESS && ioret != hyperdisk::DIDNOTHING)
                {
                    PLOG(ERROR) << "Disk I/O returned " << ioret;
                }
            }
            else
            {
                PLOG(ERROR) << "Disk flush returned " << ret;
            }
        }

        if (sleep)
        {
            e::sleep_ms(0, 10);
        }
        else
        {
            (void) __sync_or_and_fetch(&m_flushed_recently, true);
        }
    }
}

void
hyperdaemon :: datalayer :: create_disk(const regionid& ri,
                                        const hyperspacehashing::mask::hasher& hasher,
                                        uint16_t num_columns)
{
    std::ostringstream ostr;
    ostr << ri;
    po6::pathname path(ostr.str());
    disk_ptr d;

    try
    {
        // XXX fail this region.
        d = hyperdisk::disk::create(path, hasher, num_columns);
    }
    catch (po6::error& e)
    {
        PLOG(ERROR) << "Could not create disk " << ri;
        return;
    }

    if (m_disks.insert(ri, d))
    {
        LOG(INFO) << "Created disk " << ri << " with " << num_columns << " columns";
    }
    else
    {
        LOG(ERROR) << "Could not create disk " << ri << " because it already exists";
    }
}

void
hyperdaemon :: datalayer :: drop_disk(const regionid& ri)
{
    if (m_disks.remove(ri))
    {
        LOG(INFO) << "Dropped disk " << ri;
    }
    else
    {
        LOG(ERROR) << "Could not drop disk " << ri << " because it already exists";
    }
}
