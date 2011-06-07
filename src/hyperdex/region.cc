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

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/city.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/region.h>

hyperdex :: region :: region(const regionid& ri, const po6::pathname& directory, uint16_t nc)
    : m_ref(0)
    , m_numcolumns(nc)
    , m_point_mask(get_point_for(UINT64_MAX))
    , m_log()
    , m_rwlock()
    , m_disks()
{
    std::ostringstream ostr;
    ostr << ri;
    // Create one disk to start.  It will split as necessary.
    // XXX Implement splitting.
    po6::pathname path = join(directory, ostr.str());
    e::intrusive_ptr<disk> d = new disk(join(directory, ostr.str()));
    m_disks.push_back(std::make_pair(ri, d));
}

hyperdex :: result_t
hyperdex :: region :: get(const e::buffer& key,
                          std::vector<e::buffer>* value,
                          uint64_t* version)
{
    log::iterator it(m_log.iterate());
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
        }
    }

    // We know that the log is a suffix of the linear ordering of all updates to
    // this region.  If we found something in the log, it is *guaranteed* to be
    // more recent than anything on-disk, so we just return.  In the uncommon
    // case this means that we'll not have to touch memory-mapped files.  We
    // have to iterate this part of the log anyway so it's not a big deal to do
    // part of it before touching disk.
    if (*version > 0)
    {
        return SUCCESS;
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

    // Scan the in-memory WAL again.
    for (; it.valid(); it.next())
    {
        if (it.key() == key)
        {
            *value = it.value();
            *version = it.version();
        }
    }

    if (*version > 0)
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
hyperdex :: region :: flush_one(op_t op, uint64_t point, const e::buffer& key,
                                uint64_t key_hash,
                                const std::vector<e::buffer>& value,
                                const std::vector<uint64_t>& value_hashes,
                                uint64_t version)
{
    // XXX to prevent excessive failures, we should probably add some logic here
    // to block until a split occurs to resolve disk space issues.

    // Delete from every disk
    po6::threads::rwlock::rdhold hold(&m_rwlock);

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

        if (res != SUCCESS && res != NOTFOUND)
        {
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

            if (res != SUCCESS)
            {
                // XXX FAIL DISK
                LOG(INFO) << "Disk has failed.";
            }
        }
    }
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

    for (size_t i = 0; i < m_numcolumns; ++i)
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
