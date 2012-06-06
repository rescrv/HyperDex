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
#include <fstream>

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

// util
#include <util/atomicfile.h>

// HyperDex
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/configuration_parser.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/runtimeconfig.h"

using hyperdex::regionid;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::configuration;
using hyperdex::coordinatorlink;
using hyperdex::configuration_parser;

typedef e::intrusive_ptr<hyperdisk::disk> disk_ptr;
typedef std::map<hyperdex::regionid, disk_ptr> disk_map_t;

const char* hyperdaemon :: datalayer :: STATE_FILE_NAME = "datalayer_state.hd";
const int hyperdaemon :: datalayer :: STATE_FILE_VER = 1;

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
    , m_quiesce(false)
    , m_quiesce_state_id("")
{
    m_optimistic_io_thread.start();

    for (size_t i = 0; i < FLUSH_THREADS; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread>
            t(new po6::threads::thread(std::tr1::bind(&datalayer::flush_thread, this)));
        t->start();
        m_flush_threads.push_back(t);
    }
    
    // Load state from shutdown.
    // XXX handle errors
    load_state();
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

bool
hyperdaemon :: datalayer :: dump_state(const configuration& config, const instance& us)
{
    // Dump state information.
    std::ostringstream s;
    s << "version " << STATE_FILE_VER << std::endl;

    s << "us";
    s << " " << us.address;
    s << " " << us.inbound_port;
    s << " " << us.inbound_version;
    s << " " << us.outbound_port;
    s << " " << us.outbound_version; 
    s << std::endl;
    
    s << "config";
    s << " " << config.config_text();

    // Rewrite the state file atomically.
    if (!util::atomicfile::rewrite(m_base.get(), STATE_FILE_NAME, s.str().c_str()))
    {
        PLOG(ERROR) << "Could not write datalayer state to a file " 
                    << po6::join(m_base.get(), STATE_FILE_NAME);
        return false;
    }
    
    return true;
}

bool
hyperdaemon :: datalayer :: load_state()
{
    po6::pathname state_fname = po6::join(m_base.get(), STATE_FILE_NAME);
    std::ifstream f; 
    f.open(state_fname.get());
    if (!f)
    {
        PLOG(INFO) << "Could not open datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }

    // File version.
    std::string v;
    f >> v;
    if (f.fail() || "version" != v)
    {
        PLOG(ERROR) << "Expecting 'version' token in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }

    int32_t vn = -1;
    f >> vn;
    if (f.fail() || STATE_FILE_VER != vn)
    {
        PLOG(ERROR) << "Invalid version in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }
    
    // Instance describing self at the time of quiesce.
    std::string u;
    f >> u;
    if (f.fail() || "us" != u)
    {
        PLOG(ERROR) << "Expecting 'us' token in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }
    
    po6::net::ipaddr address;
    in_port_t inbound_port = -1;
    uint16_t inbound_version = -1;
    in_port_t outbound_port = -1;
    uint16_t outbound_version = -1;    
    f >> address >> inbound_port >> inbound_version >> outbound_port >> outbound_version;
    if (f.fail())
    {
        PLOG(ERROR) << "Expecting instance in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }
    
    instance us(address, inbound_port, inbound_version, outbound_port, outbound_version);

    // Config at the time of quiesce.
    std::string c;
    f >> c;
    if (f.fail() || "config" != c)
    {
        PLOG(ERROR) << "Expecting 'config' token in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }
    
    char space = (char)f.get();
    if (f.fail() || ' ' != space)
    {
        PLOG(ERROR) << "Expecting space in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }

    std::stringstream buffer;
    buffer << f.rdbuf();
    if (f.fail())
    {
        PLOG(ERROR) << "Expecting config in datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }
    
    std::string configtext = buffer.str();

    configuration_parser cp;
    configuration_parser::error e;
    e = cp.parse(configtext);

    if (e != configuration_parser::CP_SUCCESS)
    {
        PLOG(ERROR) << "Unable to parse config from datalayer state file " << state_fname << "; starting with fresh state";
        return false;
    }

    configuration config = cp.generate();

    // Re-open the disks that are needed according to config at the time of quiesce.
    std::set<regionid> regions = config.regions_for(us);

    for (std::set<regionid>::const_iterator r = regions.begin();
            r != regions.end(); ++r)
    {
        if (!m_disks.contains(*r))
        {
            // Re-open a disk quiesced on shutdown.
            // XXX handle errors
            open_disk(*r, config.disk_hasher(r->get_subspace()),
                        config.dimensions(r->get_space()),
                        config.quiesce_state_id());
        }
    }

    LOG(INFO) << "Datalayer state restored from quiesced state " << config.quiesce_state_id()
              << " (loaded from file " << state_fname << ")";
    return true;
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
            // Disk not present yet, create a new one.
            // XXX handle errors
            create_disk(*r, newconfig.disk_hasher(r->get_subspace()),
                        newconfig.dimensions(r->get_space()));
        }
    }
}

void
hyperdaemon :: datalayer :: reconfigure(const configuration& newconfig, const instance& us)
{
    // Quiesce (will quiesce multiple times if requested so).
    if (newconfig.quiesce())
    {
        m_quiesce = newconfig.quiesce();
        m_quiesce_state_id = newconfig.quiesce_state_id();
        
        // XXX - pull this out to the main loop, so we get the result and can fail the host?
        // Quiesce the disks.
        for (disk_map_t::iterator d = m_disks.begin(); d != m_disks.end(); d.next())
        {
            bool res = false;
            try
            {
                res = d.value()->quiesce(m_quiesce_state_id);
            }
            catch (po6::error& e)
            {
                res = false;
            }
            
            if (!res)
            {
                // XXX fail this region
                PLOG(ERROR) << "Could not quiesce disk " << d.key();
            }
        }

        if (!dump_state(newconfig, us))
        {
            // XXX fail entire host?
            PLOG(ERROR) << "Could not save datalayer state.";
        }
    }
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
                                  size_t n,
                                  bool nonblocking)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->flush(n, nonblocking);
}

hyperdisk::returncode
hyperdaemon :: datalayer :: do_mandatory_io(const regionid& ri)
{
    e::intrusive_ptr<hyperdisk::disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdisk::MISSINGDISK;
    }

    return r->do_mandatory_io();
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
            hyperdisk::returncode ret = d.value()->flush(10000, true);

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
        d = hyperdisk::disk::create(path, hasher, num_columns);
    }
    catch (po6::error& e)
    {
        // XXX fail this region.
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
hyperdaemon :: datalayer :: open_disk(const regionid& ri,
                                      const hyperspacehashing::mask::hasher& hasher,
                                      uint16_t num_columns,
                                      const std::string& quiesce_state_id)
{
    std::ostringstream ostr;
    ostr << ri;
    po6::pathname path(ostr.str());
    disk_ptr d;

    try
    {
        d = hyperdisk::disk::open(path, hasher, num_columns, quiesce_state_id);
        if (!d)
        {
            // XXX fail this region.
            PLOG(ERROR) << "Could not open disk " << ri;
            return;
        }
    }
    catch (po6::error& e)
    {
        // XXX fail this region.
        PLOG(ERROR) << "Could not open disk " << ri;
        return;
    }

    if (m_disks.insert(ri, d))
    {
        LOG(INFO) << "Opened disk " << ri << " with " << num_columns << " columns";
    }
    else
    {
        LOG(ERROR) << "Could not add opened disk " << ri << " because it already exists";
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
