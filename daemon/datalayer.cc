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

// C++
#include <sstream>

// Google Log
#include <glog/logging.h>

// HyperDex
#include "disk/disk.h"
#include "daemon/datalayer.h"
#include "hyperdex/hyperdex/configuration.h"

using hyperdex::datalayer;

datalayer :: datalayer(const po6::pathname& base)
    : m_base(base)
    , m_disks()
{
}

datalayer :: ~datalayer() throw ()
{
}

hyperdex::reconfigure_returncode
datalayer :: prepare(const configuration&,
                     const configuration& new_config,
                     const instance& us)
{
    bool fail = false;
    std::set<regionid> regions = new_config.regions_for(us);
    std::map<uint16_t, regionid> in_transfers = new_config.transfers_to(us);
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
            schema* sc = new_config.get_schema(r->get_space());
            assert(sc);
            disk_returncode rc;

            switch ((rc = create_disk(*r, sc)))
            {
                case DISK_SUCCESS:
                    break;
                case DISK_NOT_FOUND:
                case DISK_CORRUPT:
                case DISK_FATAL_ERROR:
                case DISK_WRONGARITY:
                case DISK_DATAFULL:
                case DISK_SEARCHFULL:
                case DISK_SYNCFAILED:
                case DISK_DROPFAILED:
                case DISK_MISSINGDISK:
                case DISK_SPLITFAILED:
                case DISK_DIDNOTHING:
                default:
                    LOG(ERROR) << "could not cleanup disk " << *r << " because " << rc;
                    fail = true;
                    break;
            }
        }
    }

    return fail ? RECONFIGURE_FAIL : RECONFIGURE_SUCCESS;
}

hyperdex::reconfigure_returncode
datalayer :: reconfigure(const configuration&,
                         const configuration&,
                         const instance&)
{
    return RECONFIGURE_SUCCESS;
}

hyperdex::reconfigure_returncode
datalayer :: cleanup(const configuration&,
                     const configuration& new_config,
                     const instance& us)
{
    bool warning = false;
    std::set<regionid> regions = new_config.regions_for(us);

    for (disk_map_t::iterator d = m_disks.begin(); d!= m_disks.end(); d.next())
    {
        if (regions.find(d.key()) == regions.end())
        {
            disk_returncode rc;

            switch ((rc = drop_disk(d.key())))
            {
                case DISK_SUCCESS:
                    break;
                case DISK_NOT_FOUND:
                case DISK_CORRUPT:
                case DISK_FATAL_ERROR:
                case DISK_WRONGARITY:
                case DISK_DATAFULL:
                case DISK_SEARCHFULL:
                case DISK_SYNCFAILED:
                case DISK_DROPFAILED:
                case DISK_MISSINGDISK:
                case DISK_SPLITFAILED:
                case DISK_DIDNOTHING:
                default:
                    LOG(ERROR) << "could not cleanup disk " << d.key() << " because " << rc;
                    warning = true;
                    break;
            }
        }
    }

    return warning ? RECONFIGURE_WARNING : RECONFIGURE_SUCCESS;
}

void
datalayer :: close()
{
    for (disk_map_t::iterator d = m_disks.begin(); d != m_disks.end(); d.next())
    {
        d.value()->close();
    }
}

hyperdex::disk_returncode
datalayer :: get(const hyperdex::regionid& ri, const e::slice& key,
                 std::vector<e::slice>* value, uint64_t* version,
                 disk_reference* ref)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    return r->get(key, /*XXX*/0, value, version, ref);
}

hyperdex::disk_returncode
datalayer :: put(const regionid& ri, const e::slice& key,
                 const std::vector<e::slice>& value, uint64_t version)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    std::vector<uint64_t> value_hashes(value.size(), 0);
    return r->put(key, /*XXX*/0, value, value_hashes, version);
}

hyperdex::disk_returncode
datalayer :: del(const regionid& ri, const e::slice& key)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    return r->del(key, /*XXX*/0);
}

#if 0
hyperdex::disk_returncode
datalayer :: make_search_snapshot(const hyperdex::regionid& ri,
                                  const hyperspacehashing::search& terms,
                                  search_snapshot* snap)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    return r->make_search_snapshot(terms, snap);
}

hyperdex::disk_returncode
datalayer :: make_transfer_iterator(const hyperdex::regionid& ri,
                                    transfer_iterator* snap)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    return r->make_transfer_iterator(snap);
}
#endif

hyperdex::disk_returncode
datalayer :: create_disk(const regionid& ri, schema* sc)
{
    try
    {
        std::ostringstream ostr;
        ostr << ri;
        po6::pathname path(ostr.str());
        path = po6::join(m_base, path);
        disk_ptr d(new disk(path, sc->attrs_sz));
        disk_returncode rc = d->open();

        while (!m_disks.insert(ri, d))
        {
            m_disks.remove(ri);
        }

        return rc;
    }
    catch (po6::error& e)
    {
        errno = e;
        PLOG(ERROR) << "could not create disk for " << ri;
        return DISK_FATAL_ERROR;
    }
}

hyperdex::disk_returncode
datalayer :: drop_disk(const regionid& ri)
{
    std::tr1::shared_ptr<disk> r;

    if (!m_disks.lookup(ri, &r))
    {
        return hyperdex::DISK_MISSINGDISK;
    }

    return r->destroy();
}
