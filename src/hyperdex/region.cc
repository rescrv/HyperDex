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

// POSIX
#include <sys/stat.h>
#include <sys/types.h>

// Google Log
#include <glog/logging.h>

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/city.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/region.h>

// XXX Implement splitting.

hyperdex :: region :: region(const regionid& ri, const po6::pathname& base, uint16_t nc)
    : m_ref(0)
    , m_numcolumns(nc)
    , m_point_mask(get_point_for(UINT64_MAX))
    , m_log()
    , m_rwlock()
    , m_disks()
    , m_base()
{
    // The base directory for this region.
    std::ostringstream ostr;
    ostr << ri;
    m_base = po6::join(base, po6::pathname(ostr.str()));

    if (mkdir(m_base.get(), S_IRWXU) < 0 && errno != EEXIST)
    {
        LOG(INFO) << "Could not create region " << ri << " because mkdir failed";
        throw po6::error(errno);
    }

    e::guard rmdir_guard = e::makeguard(rmdir, m_base.get());

    // Create a starting disk which holds everything.
    regionid starting(regionid(ri.get_subspace(), 0));
    e::intrusive_ptr<disk> newdisk = create_disk(starting);
    e::guard disk_guard = e::makeobjguard(*newdisk, &disk::drop);
    m_disks.push_back(std::make_pair(starting, newdisk));
    disk_guard.dismiss();
    rmdir_guard.dismiss();
}

hyperdex :: result_t
hyperdex :: region :: get(const e::buffer& key,
                          std::vector<e::buffer>* value,
                          uint64_t* version)
{
    log::iterator it(m_log.iterate());
    bool found = false;
    *version = 0;

    // Scan the in-memory WAL.
    for (; it.valid(); it.next())
    {
        if (it.key() == key)
        {
            if (it.op() == PUT)
            {
                *value = it.value();
                *version = it.version();
            }
            else if (it.op() == DEL)
            {
                *version = 0;
            }

            found = true;
        }
    }

    // We know that the log is a suffix of the linear ordering of all updates to
    // this region.  If we found something in the log, it is *guaranteed* to be
    // more recent than anything on-disk, so we just return.  In the uncommon
    // case this means that we'll not have to touch memory-mapped files.  We
    // have to iterate this part of the log anyway so it's not a big deal to do
    // part of it before touching disk.
    if (found)
    {
        return *version == 0 ? NOTFOUND : SUCCESS;
    }

    std::vector<e::buffer> tmp_value;
    uint64_t tmp_version = 0;
    uint64_t key_hash = CityHash64(key);
    uint64_t key_point = get_point_for(key_hash);
    po6::threads::rwlock::rdhold hold(&m_rwlock);

    for (disk_vector::iterator i = m_disks.begin(); i != m_disks.end(); ++i)
    {
        uint64_t pmask = prefixmask(i->first.prefix);

        if ((i->first.mask & m_point_mask & pmask)
                != (key_point & pmask))
        {
            continue;
        }

        result_t res = i->second->get(key, key_hash, &tmp_value, &tmp_version);

        if (res == SUCCESS)
        {
            if (tmp_version > *version)
            {
                value->swap(tmp_value);
                *version = tmp_version;
            }

            break;
        }
        else if (res != NOTFOUND)
        {
            return res;
        }
    }


    found = false;

    // Scan the in-memory WAL again.
    for (; it.valid(); it.next())
    {
        if (it.key() == key)
        {
            if (it.op() == PUT)
            {
                *value = it.value();
                *version = it.version();
            }
            else if (it.op() == DEL)
            {
                *version = 0;
            }

            found = true;
        }
    }

    if (*version > 0 || found)
    {
        return SUCCESS;
    }
    else
    {
        return NOTFOUND;
    }
}

hyperdex :: result_t
hyperdex :: region :: put(const e::buffer& key,
                          const std::vector<e::buffer>& value,
                          uint64_t version)
{
    uint64_t key_hash = CityHash64(key);
    std::vector<uint64_t> value_hashes;
    get_value_hashes(value, &value_hashes);
    uint64_t point = get_point_for(key_hash, value_hashes);

    if (value.size() + 1 != m_numcolumns)
    {
        return INVALID;
    }
    else if (m_log.append(point, key, key_hash, value, value_hashes, version))
    {
        return SUCCESS;
    }
    else
    {
        LOG(INFO) << "Could not append to in-memory WAL.";
        return ERROR;
    }
}

hyperdex :: result_t
hyperdex :: region :: del(const e::buffer& key)
{
    uint64_t key_hash = CityHash64(key);
    uint64_t point = get_point_for(key_hash);

    if (m_log.append(point, key, key_hash))
    {
        return SUCCESS;
    }
    else
    {
        LOG(INFO) << "Could not append to in-memory WAL.";
        return ERROR;
    }
}

size_t
hyperdex :: region :: flush()
{
    using std::tr1::placeholders::_1;
    using std::tr1::placeholders::_2;
    using std::tr1::placeholders::_3;
    using std::tr1::placeholders::_4;
    using std::tr1::placeholders::_5;
    using std::tr1::placeholders::_6;
    using std::tr1::placeholders::_7;
    return m_log.flush(std::tr1::bind(&region::flush_one, this, _1, _2, _3, _4, _5, _6, _7));
}

void
hyperdex :: region :: async()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);

    for (disk_vector::iterator i = m_disks.begin(); i != m_disks.end(); ++i)
    {
        i->second->async();
    }
}

void
hyperdex :: region :: sync()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);

    for (disk_vector::iterator i = m_disks.begin(); i != m_disks.end(); ++i)
    {
        i->second->sync();
    }
}

void
hyperdex :: region :: drop()
{
    for (disk_vector::iterator d = m_disks.begin(); d != m_disks.end(); ++d)
    {
        d->second->drop();
    }

    if (rmdir(m_base.get()) < 0)
    {
        PLOG(INFO) << "Could not remove region directory.";
    }
}

e::intrusive_ptr<hyperdex::region::snapshot>
hyperdex :: region :: make_snapshot()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);
    return inner_make_snapshot();
}

e::intrusive_ptr<hyperdex::region::rolling_snapshot>
hyperdex :: region :: make_rolling_snapshot()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);
    log::iterator it(m_log.iterate());
    e::intrusive_ptr<snapshot> snap(inner_make_snapshot());
    return new rolling_snapshot(it, snap);
}

e::intrusive_ptr<hyperdex::disk>
hyperdex :: region :: create_disk(const regionid& ri)
{
    std::ostringstream ostr;
    ostr << ri;
    po6::pathname path = po6::join(m_base, po6::pathname(ostr.str()));
    e::intrusive_ptr<disk> newdisk = disk::create(path);
    return newdisk;
}

void
hyperdex :: region :: get_value_hashes(const std::vector<e::buffer>& value,
                                       std::vector<uint64_t>* value_hashes)
{
    for (size_t i = 0; i < value.size(); ++i)
    {
        value_hashes->push_back(CityHash64(value[i]));
    }
}

uint64_t
hyperdex :: region :: get_point_for(uint64_t key_hash)
{
    std::vector<uint64_t> points;
    points.push_back(key_hash);

    for (size_t i = 1; i < m_numcolumns; ++i)
    {
        points.push_back(0);
    }

    return interlace(points);
}

uint64_t
hyperdex :: region :: get_point_for(uint64_t key_hash, const std::vector<uint64_t>& value_hashes)
{
    std::vector<uint64_t> points;
    points.push_back(key_hash);

    for (size_t i = 0; i < value_hashes.size(); ++i)
    {
        points.push_back(value_hashes[i]);
    }

    return interlace(points);
}

bool
hyperdex :: region :: flush_one(op_t op, uint64_t point, const e::buffer& key,
                                uint64_t key_hash,
                                const std::vector<e::buffer>& value,
                                const std::vector<uint64_t>& value_hashes,
                                uint64_t version)
{
    // Delete from every disk
    po6::threads::rwlock::wrhold hold(&m_rwlock);

    for (disk_vector::iterator i = m_disks.begin(); i != m_disks.end(); ++i)
    {
        // Use m_point_mask because we want every disk which could have this
        // key.
        uint64_t pmask = prefixmask(i->first.prefix) & m_point_mask;

        if ((i->first.mask & pmask) != (point & pmask))
        {
            continue;
        }

        result_t res = i->second->del(key, key_hash);

        switch (res)
        {
            case SUCCESS:
            case NOTFOUND:
                break;
            case DISKFULL:
                return false;
            case INVALID:
            case ERROR:
            default:
                // XXX FAIL DISK
                LOG(INFO) << "Disk has failed.";
        }
    }

    // Put to one disk
    if (op == PUT)
    {
        for (disk_vector::iterator i = m_disks.begin(); i != m_disks.end(); ++i)
        {
            uint64_t pmask = prefixmask(i->first.prefix);

            if ((i->first.mask & pmask) != (point & pmask))
            {
                continue;
            }

            result_t res = i->second->put(key, key_hash, value, value_hashes, version);

            switch (res)
            {
                case SUCCESS:
                    break;
                case DISKFULL:
                    return false;
                case NOTFOUND:
                    LOG(INFO) << "PUT returned NOTFOUND? Rediculous.";
                case INVALID:
                case ERROR:
                default:
                    // XXX FAIL DISK
                    LOG(INFO) << "Disk has failed.";
            }
        }
    }

    return true;
}

e::intrusive_ptr<hyperdex::region::snapshot>
hyperdex :: region :: inner_make_snapshot()
{
    std::vector<e::intrusive_ptr<disk::snapshot> > snaps;

    for (disk_vector::iterator d = m_disks.begin(); d != m_disks.end(); ++d)
    {
        snaps.push_back(d->second->make_snapshot());
    }

    e::intrusive_ptr<snapshot> ret(new snapshot(&snaps));
    return ret;
}

hyperdex :: region :: snapshot :: snapshot(std::vector<e::intrusive_ptr<disk::snapshot> >* ss)
    : m_snaps()
    , m_ref(0)
{
    m_snaps.swap(*ss);
}

bool
hyperdex :: region :: snapshot :: valid()
{
    while (!m_snaps.empty())
    {
        if (m_snaps.back()->valid())
        {
            return true;
        }
        else
        {
            m_snaps.pop_back();
        }
    }

    return false;
}

void
hyperdex :: region :: snapshot :: next()
{
    if (!m_snaps.empty())
    {
        m_snaps.back()->next();
    }
}

uint32_t
hyperdex :: region :: snapshot :: secondary_point()
{
    if (!m_snaps.empty())
    {
        return m_snaps.back()->secondary_point();
    }
    else
    {
        return uint64_t();
    }
}

hyperdex::op_t
hyperdex :: region :: snapshot :: op()
{
    if (!m_snaps.empty())
    {
        return PUT;
    }
    else
    {
        return op_t();
    }
}

uint64_t
hyperdex :: region :: snapshot :: version()
{
    if (!m_snaps.empty())
    {
        return m_snaps.back()->version();
    }
    else
    {
        return uint64_t();
    }
}

e::buffer
hyperdex :: region :: snapshot :: key()
{
    if (!m_snaps.empty())
    {
        return m_snaps.back()->key();
    }
    else
    {
        return e::buffer();
    }
}

std::vector<e::buffer>
hyperdex :: region :: snapshot :: value()
{
    if (!m_snaps.empty())
    {
        return m_snaps.back()->value();
    }
    else
    {
        return std::vector<e::buffer>();
    }
}

hyperdex :: region :: rolling_snapshot :: rolling_snapshot(const log::iterator& iter,
                                                           e::intrusive_ptr<snapshot> snap)
    : m_iter(iter)
    , m_snap(snap)
    , m_ref(0)
{
    valid();
}

bool
hyperdex :: region :: rolling_snapshot :: valid()
{
    return m_snap->valid() || m_iter.valid();
}

void
hyperdex :: region :: rolling_snapshot :: next()
{
    if (m_snap->valid())
    {
        m_snap->next();
    }
    else if (m_iter.valid())
    {
        m_iter.next();
    }
}

hyperdex::op_t
hyperdex :: region :: rolling_snapshot :: op()
{
    if (m_snap->valid())
    {
        return m_snap->op();
    }
    else if (m_iter.valid())
    {
        return m_iter.op();
    }
    else
    {
        return op_t();
    }
}

uint64_t
hyperdex :: region :: rolling_snapshot :: version()
{
    if (m_snap->valid())
    {
        return m_snap->version();
    }
    else if (m_iter.valid())
    {
        return m_iter.version();
    }
    else
    {
        return uint64_t();
    }
}

e::buffer
hyperdex :: region :: rolling_snapshot :: key()
{
    if (m_snap->valid())
    {
        return m_snap->key();
    }
    else if (m_iter.valid())
    {
        return m_iter.key();
    }
    else
    {
        return e::buffer();
    }
}

std::vector<e::buffer>
hyperdex :: region :: rolling_snapshot :: value()
{
    if (m_snap->valid())
    {
        return m_snap->value();
    }
    else if (m_iter.valid())
    {
        return m_iter.value();
    }
    else
    {
        return std::vector<e::buffer>();
    }
}
