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
#include <cstdio>

// POSIX
#include <sys/stat.h>
#include <sys/types.h>

// Google CityHash
#include <city.h>

// e
#include <e/guard.h>

// HyperDisk
#include "../include/hyperdisk/disk.h"
#include "log.h"
#include "shard.h"
#include "shard_snapshot.h"

// XXX Remove this.
// HyperDex
#include <hyperdex/hyperspace.h>

hyperdisk :: disk :: disk(const po6::pathname& directory, uint16_t arity)
    : m_ref(0)
    , m_numcolumns(arity)
    , m_point_mask(get_point_for(UINT64_MAX))
    , m_log()
    , m_rwlock()
    , m_shards()
    , m_base(directory)
    , m_needs_more_space()
{
    if (mkdir(m_base.get(), S_IRWXU) < 0 && errno != EEXIST)
    {
        throw po6::error(errno);
    }

    // Create a starting disk which holds everything.
    hyperdex::regionid starting(hyperdex::regionid(0, 0, 0, 0));
    create_shard(starting);
}

// XXX Double check the logic of GET to make sure it is indeed linearizable.
hyperdisk::returncode
hyperdisk :: disk :: get(const e::buffer& key,
                          std::vector<e::buffer>* value,
                          uint64_t* version)
{
    hyperdisk::log_iterator it(m_log.get());
    bool found = false;
    *version = 0;

    // Scan the in-memory WAL.
    for (; it.valid(); it.next())
    {
        if (it.key() == key)
        {
            if (it.has_value())
            {
                *value = it.value();
                *version = it.version();
            }
            else
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

    for (shard_collection::iterator i = m_shards.begin(); i != m_shards.end(); ++i)
    {
        uint64_t pmask = hyperdex::prefixmask(i->first.prefix);

        if ((i->first.mask & m_point_mask & pmask)
                != (key_point & pmask))
        {
            continue;
        }

        returncode res = i->second->get(key, key_hash, &tmp_value, &tmp_version);

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
            if (it.has_value())
            {
                *value = it.value();
                *version = it.version();
            }
            else
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

hyperdisk::returncode
hyperdisk :: disk :: put(const e::buffer& key,
                         const std::vector<e::buffer>& value,
                         uint64_t version)
{
    uint64_t key_hash = CityHash64(key);
    std::vector<uint64_t> value_hashes;
    get_value_hashes(value, &value_hashes);
    uint64_t point = get_point_for(key_hash, value_hashes);

    if (value.size() + 1 != m_numcolumns)
    {
        return WRONGARITY;
    }
    else
    {
        m_log->append(point, key, key_hash, value, value_hashes, version);
        return SUCCESS;
    }
}

hyperdisk::returncode
hyperdisk :: disk :: del(const e::buffer& key)
{
    uint64_t key_hash = CityHash64(key);
    uint64_t point = get_point_for(key_hash);

    m_log->append(point, key, key_hash);
    return SUCCESS;
}

bool
hyperdisk :: disk :: flush()
{
    using std::tr1::placeholders::_1;
    using std::tr1::placeholders::_2;
    using std::tr1::placeholders::_3;
    using std::tr1::placeholders::_4;
    using std::tr1::placeholders::_5;
    using std::tr1::placeholders::_6;
    using std::tr1::placeholders::_7;
    size_t flushed = m_log->flush(std::tr1::bind(&disk::flush_one, this, _1, _2, _3, _4, _5, _6, _7));
    bool split = false;

    for (std::set<hyperdex::regionid>::iterator i = m_needs_more_space.begin();
            i != m_needs_more_space.end(); ++i)
    {
        shard_collection::iterator di = m_shards.find(*i);

        if (di == m_shards.end())
        {
            continue;
        }

        split = true;
        e::intrusive_ptr<hyperdisk::shard> d = di->second;

        if (d->needs_cleaning())
        {
            clean_shard(*i);
        }
        else
        {
            split_shard(*i);
        }
    }

    m_needs_more_space.clear();
    return split || flushed > 0;
}

hyperdisk::returncode
hyperdisk :: disk :: async()
{
    returncode ret = SUCCESS;
    po6::threads::rwlock::rdhold hold(&m_rwlock);

    for (shard_collection::iterator i = m_shards.begin(); i != m_shards.end(); ++i)
    {
        if (i->second->async() != SUCCESS)
        {
            ret = SYNCFAILED;
        }
    }

    return ret;
}

hyperdisk::returncode
hyperdisk :: disk :: sync()
{
    returncode ret = SUCCESS;
    po6::threads::rwlock::rdhold hold(&m_rwlock);

    for (shard_collection::iterator i = m_shards.begin(); i != m_shards.end(); ++i)
    {
        if (i->second->sync() != SUCCESS)
        {
            ret = SYNCFAILED;
        }
    }

    return ret;
}

hyperdisk::returncode
hyperdisk :: disk :: drop()
{
    returncode ret = SUCCESS;

    for (shard_collection::iterator d = m_shards.begin(); d != m_shards.end(); ++d)
    {
        if (d->second->drop() != SUCCESS)
        {
            ret = DROPFAILED;
        }
    }

    if (rmdir(m_base.get()) < 0)
    {
        ret = DROPFAILED;
    }

    return ret;
}

e::intrusive_ptr<hyperdisk::disk::snapshot>
hyperdisk :: disk :: make_snapshot()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);
    return inner_make_snapshot();
}

e::intrusive_ptr<hyperdisk::disk::rolling_snapshot>
hyperdisk :: disk :: make_rolling_snapshot()
{
    po6::threads::rwlock::rdhold hold(&m_rwlock);
    std::auto_ptr<hyperdisk::log_iterator> it(new log_iterator(m_log.get()));
    e::intrusive_ptr<snapshot> snap(inner_make_snapshot());
    return new rolling_snapshot(it, snap);
}

e::intrusive_ptr<hyperdisk::shard>
hyperdisk :: disk :: create_shard(const hyperdex::regionid& ri)
{
    std::ostringstream ostr;
    ostr << ri;
    po6::pathname path = po6::join(m_base, po6::pathname(ostr.str()));
    e::intrusive_ptr<hyperdisk::shard> newdisk = hyperdisk::shard::create(path);
    e::guard disk_guard = e::makeobjguard(*newdisk, &hyperdisk::shard::drop);
    m_shards.insert(std::make_pair(ri, newdisk));
    disk_guard.dismiss();
    return newdisk;
}

void
hyperdisk :: disk :: drop_shard(const hyperdex::regionid& ri)
{
    shard_collection::iterator di = m_shards.find(ri);

    if (di == m_shards.end())
    {
        return;
    }

    e::intrusive_ptr<hyperdisk::shard> d = di->second;
    m_shards.erase(di);
    di->second->drop();
}

void
hyperdisk :: disk :: get_value_hashes(const std::vector<e::buffer>& value,
                                       std::vector<uint64_t>* value_hashes)
{
    for (size_t i = 0; i < value.size(); ++i)
    {
        value_hashes->push_back(CityHash64(value[i]));
    }
}

uint64_t
hyperdisk :: disk :: get_point_for(uint64_t key_hash)
{
    std::vector<uint64_t> points;
    points.push_back(key_hash);

    for (size_t i = 1; i < m_numcolumns; ++i)
    {
        points.push_back(0);
    }

    return hyperdex::interlace(points);
}

uint64_t
hyperdisk :: disk :: get_point_for(uint64_t key_hash, const std::vector<uint64_t>& value_hashes)
{
    std::vector<uint64_t> points;
    points.push_back(key_hash);

    for (size_t i = 0; i < value_hashes.size(); ++i)
    {
        points.push_back(value_hashes[i]);
    }

    return hyperdex::interlace(points);
}

bool
hyperdisk :: disk :: flush_one(bool has_value, uint64_t point, const e::buffer& key,
                               uint64_t key_hash,
                               const std::vector<e::buffer>& value,
                               const std::vector<uint64_t>& value_hashes,
                               uint64_t version)
{
    // Delete from every disk
    po6::threads::rwlock::wrhold hold(&m_rwlock);

    for (shard_collection::iterator i = m_shards.begin(); i != m_shards.end(); ++i)
    {
        // Use m_point_mask because we want every disk which could have this
        // key.
        uint64_t pmask = hyperdex::prefixmask(i->first.prefix) & m_point_mask;

        if ((i->first.mask & pmask) != (point & pmask))
        {
            continue;
        }

        returncode res = i->second->del(key, key_hash);

        switch (res)
        {
            case SUCCESS:
            case NOTFOUND:
                break;
            case DATAFULL:
                m_needs_more_space.insert(i->first);
                return false;
            case HASHFULL:
            case SEARCHFULL:
            case MISSINGDISK:
            case WRONGARITY:
            case SYNCFAILED:
            case DROPFAILED:
            default:
                assert(!"Programming error.");
        }
    }

    // Put to one disk
    if (has_value)
    {
        for (shard_collection::iterator i = m_shards.begin(); i != m_shards.end(); ++i)
        {
            uint64_t pmask = hyperdex::prefixmask(i->first.prefix);

            if ((i->first.mask & pmask) != (point & pmask))
            {
                continue;
            }

            returncode res = i->second->put(key, key_hash, value, value_hashes, version);

            switch (res)
            {
                case SUCCESS:
                    break;
                case DATAFULL:
                case HASHFULL:
                case SEARCHFULL:
                    m_needs_more_space.insert(i->first);
                    return false;
                case MISSINGDISK:
                case NOTFOUND:
                case WRONGARITY:
                case SYNCFAILED:
                case DROPFAILED:
                default:
                    assert(!"Programming error.");
            }
        }
    }

    return true;
}

e::intrusive_ptr<hyperdisk::disk::snapshot>
hyperdisk :: disk :: inner_make_snapshot()
{
    std::vector<e::intrusive_ptr<hyperdisk::shard_snapshot> > snaps;

    for (shard_collection::iterator d = m_shards.begin(); d != m_shards.end(); ++d)
    {
        snaps.push_back(d->second->make_snapshot());
    }

    e::intrusive_ptr<snapshot> ret(new snapshot(&snaps));
    return ret;
}

hyperdisk::returncode
hyperdisk :: disk :: clean_shard(const hyperdex::regionid& ri)
{
    shard_collection::iterator di = m_shards.find(ri);

    if (di == m_shards.end())
    {
        return MISSINGDISK;
    }

    e::intrusive_ptr<hyperdisk::shard_snapshot> snap = di->second->make_snapshot();
    po6::pathname path = po6::join(m_base, "tmp");
    e::intrusive_ptr<hyperdisk::shard> newdisk = hyperdisk::shard::create(path);
    e::guard disk_guard = e::makeobjguard(*newdisk, &hyperdisk::shard::drop);

    for (; snap->valid(); snap->next())
    {
        e::buffer key(snap->key());
        std::vector<e::buffer> value(snap->value());
        uint64_t key_hash = CityHash64(key);
        std::vector<uint64_t> value_hashes;
        get_value_hashes(value, &value_hashes);
        newdisk->put(key, key_hash, value, value_hashes, snap->version());
    }

    if (rename(path.get(), di->second->filename().get()) < 0)
    {
        return DROPFAILED;
    }

    disk_guard.dismiss();
    newdisk->filename(di->second->filename());
    po6::threads::rwlock::wrhold hold(&m_rwlock);
    m_shards[ri] = newdisk;
    return SUCCESS;
}

// XXX Split disk will only create more space if there is enough variation among
// values to ensure that some are spun off into other disks.  This means that we
// need to somehow split disks without splitting regions if the data becomes so
// large (or so constructively worst-case) that it needs all 64 bits of the
// region mask to identify disks.

hyperdisk::returncode
hyperdisk :: disk :: split_shard(const hyperdex::regionid& ri)
{
    if (ri.prefix >= 64)
    {
        assert(!"Programming error."); // XXX
    }

    shard_collection::iterator di = m_shards.find(ri);

    if (di == m_shards.end())
    {
        return MISSINGDISK;
    }

    uint64_t new_bit = 1;
    new_bit = new_bit << (64 - ri.prefix - 1);
    e::intrusive_ptr<hyperdisk::shard_snapshot> snap = di->second->make_snapshot();
    hyperdex::regionid lower_reg(ri.get_subspace(), ri.prefix + 1, ri.mask);
    hyperdex::regionid upper_reg(ri.get_subspace(), ri.prefix + 1, ri.mask | new_bit);
    e::intrusive_ptr<hyperdisk::shard> lower;
    e::intrusive_ptr<hyperdisk::shard> upper;

    {
        po6::threads::rwlock::wrhold hold(&m_rwlock);
        lower = create_shard(lower_reg);
        upper = create_shard(upper_reg);
    }

    e::guard lower_disk_guard = e::makeobjguard(*this, &disk::drop_shard, lower_reg);
    e::guard upper_disk_guard = e::makeobjguard(*this, &disk::drop_shard, upper_reg);

    if (!lower || !upper)
    {
        return MISSINGDISK; // XXX This is not the real reason for failure.
    }

    uint64_t prefix = hyperdex::prefixmask(ri.prefix + 1);
    std::vector<bool> which_dims(m_numcolumns, true);

    for (; snap->valid(); snap->next())
    {
        e::buffer key(snap->key());
        std::vector<e::buffer> value(snap->value());
        uint64_t key_hash = CityHash64(key);
        std::vector<uint64_t> value_hashes;
        get_value_hashes(value, &value_hashes);
        uint64_t point = get_point_for(key_hash, value_hashes);
#ifndef NDEBUG
        bool set = false;
#endif

        if ((prefix & point) == lower_reg.mask)
        {
            assert(!set);
            lower->put(key, key_hash, value, value_hashes, snap->version());
#ifndef NDEBUG
            set = true;
#endif
        }

        if ((prefix & point) == upper_reg.mask)
        {
            assert(!set);
            upper->put(key, key_hash, value, value_hashes, snap->version());
#ifndef NDEBUG
            set = true;
#endif
        }
    }

    po6::pathname old_fn = di->second->filename();
    po6::threads::rwlock::wrhold hold(&m_rwlock);
    drop_shard(ri);
    lower_disk_guard.dismiss();
    upper_disk_guard.dismiss();
    return SUCCESS;
}

hyperdisk :: disk :: snapshot :: snapshot(std::vector<e::intrusive_ptr<hyperdisk::shard_snapshot> >* ss)
    : m_snaps()
    , m_ref(0)
{
    m_snaps.swap(*ss);
}

bool
hyperdisk :: disk :: snapshot :: valid()
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
hyperdisk :: disk :: snapshot :: next()
{
    if (!m_snaps.empty())
    {
        m_snaps.back()->next();
    }
}

uint32_t
hyperdisk :: disk :: snapshot :: secondary_point()
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

bool
hyperdisk :: disk :: snapshot :: has_value()
{
    if (!m_snaps.empty())
    {
        return true;
    }
    else
    {
        return false;
    }
}

uint64_t
hyperdisk :: disk :: snapshot :: version()
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
hyperdisk :: disk :: snapshot :: key()
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
hyperdisk :: disk :: snapshot :: value()
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

hyperdisk :: disk :: rolling_snapshot :: rolling_snapshot(std::auto_ptr<hyperdisk::log_iterator> iter,
                                                          e::intrusive_ptr<snapshot> snap)
    : m_iter(iter)
    , m_snap(snap)
    , m_ref(0)
{
    valid();
}

bool
hyperdisk :: disk :: rolling_snapshot :: valid()
{
    return m_snap->valid() || m_iter->valid();
}

void
hyperdisk :: disk :: rolling_snapshot :: next()
{
    if (m_snap->valid())
    {
        m_snap->next();
    }
    else if (m_iter->valid())
    {
        m_iter->next();
    }
}

bool
hyperdisk :: disk :: rolling_snapshot :: has_value()
{
    if (m_snap->valid())
    {
        return m_snap->has_value();
    }
    else if (m_iter->valid())
    {
        return m_iter->has_value();
    }
    else
    {
        return false;
    }
}

uint64_t
hyperdisk :: disk :: rolling_snapshot :: version()
{
    if (m_snap->valid())
    {
        return m_snap->version();
    }
    else if (m_iter->valid())
    {
        return m_iter->version();
    }
    else
    {
        return uint64_t();
    }
}

e::buffer
hyperdisk :: disk :: rolling_snapshot :: key()
{
    if (m_snap->valid())
    {
        return m_snap->key();
    }
    else if (m_iter->valid())
    {
        return m_iter->key();
    }
    else
    {
        return e::buffer();
    }
}

std::vector<e::buffer>
hyperdisk :: disk :: rolling_snapshot :: value()
{
    if (m_snap->valid())
    {
        return m_snap->value();
    }
    else if (m_iter->valid())
    {
        return m_iter->value();
    }
    else
    {
        return std::vector<e::buffer>();
    }
}
