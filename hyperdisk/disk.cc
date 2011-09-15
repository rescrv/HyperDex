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

// e
#include <e/guard.h>

// HyperspaceHashing
#include <hyperspacehashing/mask.h>

// HyperDisk
#include "hyperdisk/disk.h"
#include "log_entry.h"
#include "shard.h"
#include "shard_snapshot.h"
#include "shard_vector.h"

using hyperspacehashing::mask::coordinate;

// LOCKING:  IF YOU DO ANYTHING WITH THIS CODE, READ THIS FIRST!
//
// At any given time, only one thread should be mutating shards.  In this
// context a mutation to a shard may be either a PUT/DEL, or
// cleaning/splitting/joining the shard. The m_shard_mutate mutex is used to
// enforce this constraint.
//
// Certain mutations require changing the shard_vector (e.g., to replace a shard
// with its equivalent that has had dead space collected).  These mutations
// conflict with reading from the shards (e.g. for a GET).  To that end, the
// m_shards_lock is a reader-writer lock which provides this synchronization
// between the readers and single mutator.  We know that there is a single
// mutator because of the above reasoning.  It is safe for the single mutator to
// grab a reference to m_shards while holding m_shard_mutate without grabbing a
// read lock on m_shards_lock.  The mutator must grab a write lock when changing
// the shards.
//
// Note that synchronization around m_shards revolves around the
// reference-counted *pointer* to a shard_vector, and not the shard_vector
// itself.  Methods which access the shard_vector are responsible for ensuring
// proper synchronization.  GET does this by allowing races in shard_vector
// accesses, but using the WAL to detect them.  PUT/DEL do this by writing to
// the WAL.  Trickle does this by using locking when exchanging the
// shard_vectors.

e::intrusive_ptr<hyperdisk::disk>
hyperdisk :: disk :: create(const po6::pathname& directory,
                            const hyperspacehashing::mask::hasher& hasher,
                            uint16_t arity)
{
    return new disk(directory, hasher, arity);
}

hyperdisk::returncode
hyperdisk :: disk :: get(const e::buffer& key,
                         std::vector<e::buffer>* value,
                         uint64_t* version)
{
    coordinate coord = m_hasher.hash(key);
    returncode shard_res = NOTFOUND;
    e::intrusive_ptr<shard_vector> shards;
    e::locking_iterable_fifo<log_entry>::iterator it = m_log.iterate();

    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }

    for (size_t i = 0; i < shards->size(); ++i)
    {
        if (!shards->get_coordinate(i).primary_intersects(coord))
        {
            continue;
        }

        shard_res = shards->get_shard(i)->get(coord.primary_hash, key, value, version);

        if (shard_res == SUCCESS)
        {
            break;
        }
    }

    bool found = false;
    returncode wal_res = NOTFOUND;

    for (; it.valid(); it.next())
    {
        if (it->coord.primary_intersects(coord) && it->key == key)
        {
            if (it->coord.secondary_mask == UINT32_MAX)
            {
                assert(it->coord.primary_mask == UINT32_MAX);
                assert(it->coord.secondary_mask == UINT32_MAX);
                *value = it->value;
                *version = it->version;
                wal_res = SUCCESS;
            }
            else
            {
                assert(it->coord.primary_mask == UINT32_MAX);
                assert(it->coord.secondary_mask == 0);
                wal_res = NOTFOUND;
            }

            found = true;
        }
    }

    if (found)
    {
        return wal_res;
    }

    return shard_res;
}

hyperdisk::returncode
hyperdisk :: disk :: put(const e::buffer& key,
                         const std::vector<e::buffer>& value,
                         uint64_t version)
{
    if (value.size() + 1 != m_arity)
    {
        return WRONGARITY;
    }

    coordinate coord = m_hasher.hash(key, value);
    m_log.append(log_entry(coord, key, value, version));
    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: del(const e::buffer& key)
{
    coordinate coord = m_hasher.hash(key);
    m_log.append(log_entry(coord, key));
    return SUCCESS;
}

e::intrusive_ptr<hyperdisk::snapshot>
hyperdisk :: disk :: make_snapshot()
{
    assert(!"Not implemented."); // XXX
}

e::intrusive_ptr<hyperdisk::rolling_snapshot>
hyperdisk :: disk :: make_rolling_snapshot()
{
    assert(!"Not implemented."); // XXX
}

hyperdisk::returncode
hyperdisk :: disk :: drop()
{
    po6::threads::mutex::hold a(&m_shards_mutate);
    po6::threads::mutex::hold b(&m_shards_lock);
    po6::threads::mutex::hold c(&m_spare_shards_lock);
    returncode ret = SUCCESS;
    e::intrusive_ptr<shard_vector> shards = m_shards;

    while (!m_spare_shards.empty())
    {
        if (unlinkat(m_base.get(), m_spare_shards.front().first.get(), 0) < 0)
        {
            ret = DROPFAILED;
        }

        m_spare_shards.pop();
    }

    for (size_t i = 0; i < shards->size(); ++i)
    {
        if (drop_shard(shards->get_coordinate(i)) != SUCCESS)
        {
            ret = DROPFAILED;
        }
    }

    if (ret == SUCCESS)
    {
        if (rmdir(m_base_filename.get()) < 0)
        {
            ret = DROPFAILED;
        }
    }

    return ret;
}

// This operation will return SUCCESS as long as it knows that progress is being
// made.  It will return FLUSHNONE if there was nothing to do.
hyperdisk::returncode
hyperdisk :: disk :: flush(size_t num)
{
    if (!m_shards_mutate.trylock())
    {
        return SUCCESS;
    }

    bool flushed = false;
    returncode flush_status = SUCCESS;
    e::guard hold = e::makeobjguard(m_shards_mutate, &po6::threads::mutex::unlock);
    e::locking_iterable_fifo<log_entry>::iterator it = m_log.iterate();

    for (size_t nf = 0; nf < num && it.valid(); ++nf, it.next())
    {
        const coordinate& coord = it->coord;
        const e::buffer& key = it->key;
        bool del_needed = false;
        size_t del_num = 0;
        uint32_t del_offset = 0;

        for (size_t i = 0; !del_needed && i < m_shards->size(); ++i)
        {
            if (!m_shards->get_coordinate(i).primary_intersects(coord))
            {
                continue;
            }

            returncode ret;
            ret = m_shards->get_shard(i)->get(coord.primary_hash, key);

            if (ret == SUCCESS)
            {
                del_needed = true;
                del_num = i;
            }
            else if (ret == NOTFOUND)
            {
            }
            else
            {
                assert(false);
            }
        }

        bool put_succeeded = false;
        size_t put_num = 0;
        uint32_t put_offset = 0;

        if (coord.secondary_mask == UINT32_MAX)
        {
            const std::vector<e::buffer>& value = it->value;
            const uint64_t version = it->version;

            for (ssize_t i = m_shards->size() - 1; !put_succeeded && i >= 0; --i)
            {
                if (!m_shards->get_coordinate(i).intersects(coord))
                {
                    continue;
                }

                returncode ret;
                ret = m_shards->get_shard(i)->put(coord.primary_hash, coord.secondary_hash,
                                                  key, value, version, &put_offset);

                if (ret == SUCCESS)
                {
                    put_succeeded = true;
                    put_num = i;
                }
                else if (ret == DATAFULL || ret == SEARCHFULL)
                {
                    m_needs_io = i;
                    flush_status = ret;
                    break;
                }
                else
                {
                    assert(false);
                }
            }

            if (!put_succeeded)
            {
                break;
            }
        }

        if (del_needed && del_num != put_num)
        {
            switch (m_shards->get_shard(del_num)->del(coord.primary_hash, key, &del_offset))
            {
                case SUCCESS:
                    break;
                case NOTFOUND:
                case DATAFULL:
                case WRONGARITY:
                case SEARCHFULL:
                case SYNCFAILED:
                case DROPFAILED:
                case MISSINGDISK:
                case SPLITFAILED:
                case FLUSHNONE:
                default:
                    assert(!"Programming error.");
            }
        }

        flushed = true;
    }

    m_log.advance_to(it);

    if (flush_status != SUCCESS)
    {
        return flush_status;
    }

    if (flushed)
    {
        return SUCCESS;
    }
    else
    {
        return FLUSHNONE;
    }
}

hyperdisk::returncode
hyperdisk :: disk :: do_mandatory_io()
{
    po6::threads::mutex::hold hold(&m_shards_mutate);

    if (m_needs_io != static_cast<size_t>(-1))
    {
        assert(m_needs_io <= m_shards->size());
        size_t needs_io = m_needs_io;
        m_needs_io = -1;
        return deal_with_full_shard(needs_io);
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: preallocate()
{
    {
        po6::threads::mutex::hold hold(&m_spare_shards_lock);

        if (m_spare_shards.size() >= 16)
        {
            return SUCCESS;
        }
    }

    e::intrusive_ptr<shard_vector> shards;

    {
        po6::threads::mutex::hold hold(&m_shards_lock);
        shards = m_shards;
    }

    ssize_t needed_shards = 0;

    for (size_t i = 0; i < shards->size(); ++i)
    {
        shard* s = shards->get_shard(i);
        int stale = s->stale_space();
        int used = s->used_space();

        // There is no describable reason for picking these except that you can
        // be pretty sure that enough shards will exist to do splits.  That
        // being said, this will waste space when shards are mostly full.  Feel
        // free to tune this using logic and reason and submit a patch.

        if (used >= 75)
        {
            if (stale >= 10)
            {
                needed_shards += 1;
            }
            else
            {
                needed_shards += static_cast<double>((100 - used) * 4) / 25.;
            }
        }
    }

    bool need_shard = false;

    {
        po6::threads::mutex::hold hold(&m_spare_shards_lock);
        need_shard = needed_shards - static_cast<ssize_t>(m_spare_shards.size()) > 0;
    }

    if (need_shard)
    {
        std::ostringstream ostr;

        {
            po6::threads::mutex::hold hold(&m_spare_shards_lock);
            ostr << "spare-" << m_spare_shard_counter;
            ++m_spare_shard_counter;
        }

        po6::pathname sparepath(ostr.str());
        e::intrusive_ptr<hyperdisk::shard> spareshard = hyperdisk::shard::create(m_base, sparepath);

        {
            po6::threads::mutex::hold hold(&m_spare_shards_lock);
            m_spare_shards.push(std::make_pair(sparepath, spareshard));
        }
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: async()
{
    e::intrusive_ptr<shard_vector> shards;
    returncode ret = SUCCESS;

    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }

    for (size_t i = 0; i < shards->size(); ++i)
    {
        if (shards->get_shard(i)->async() != SUCCESS)
        {
            ret = SYNCFAILED;
        }
    }

    return ret;
}

hyperdisk::returncode
hyperdisk :: disk :: sync()
{
    e::intrusive_ptr<shard_vector> shards;
    returncode ret = SUCCESS;

    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }

    for (size_t i = 0; i < shards->size(); ++i)
    {
        if (shards->get_shard(i)->sync() != SUCCESS)
        {
            ret = SYNCFAILED;
        }
    }

    return ret;
}

hyperdisk :: disk :: disk(const po6::pathname& directory,
                          const hyperspacehashing::mask::hasher& hasher,
                          const uint16_t arity)
    : m_ref(0)
    , m_arity(arity)
    , m_hasher(hasher)
    , m_shards_mutate()
    , m_shards_lock()
    , m_shards()
    , m_log()
    , m_base()
    , m_base_filename(directory)
    , m_spare_shards_lock()
    , m_spare_shards()
    , m_spare_shard_counter(0)
    , m_needs_io(-1)
{
    if (mkdir(directory.get(), S_IRWXU) < 0 && errno != EEXIST)
    {
        throw po6::error(errno);
    }

    m_base = open(directory.get(), O_RDONLY);

    if (m_base.get() < 0)
    {
        throw po6::error(errno);
    }

    // Create a starting disk which holds everything.
    po6::threads::mutex::hold a(&m_shards_mutate);
    po6::threads::mutex::hold b(&m_shards_lock);
    coordinate start(0, 0, 0, 0);
    e::intrusive_ptr<shard> s = create_shard(start);
    m_shards = new shard_vector(start, s);
}

hyperdisk :: disk :: ~disk() throw ()
{
}

po6::pathname
hyperdisk :: disk :: shard_filename(const coordinate& c)
{
    std::ostringstream ostr;
    ostr << std::hex << std::setfill('0') << std::setw(16) << c.primary_mask;
    ostr << "-" << std::setw(16) << c.primary_hash;
    ostr << "-" << std::setw(16) << c.secondary_mask;
    ostr << "-" << std::setw(16) << c.secondary_hash;
    return po6::pathname(ostr.str());
}

po6::pathname
hyperdisk :: disk :: shard_tmp_filename(const coordinate& c)
{
    std::ostringstream ostr;
    ostr << std::hex << std::setfill('0') << std::setw(16) << c.primary_mask;
    ostr << "-" << std::setw(16) << c.primary_hash;
    ostr << "-" << std::setw(16) << c.secondary_mask;
    ostr << "-" << std::setw(16) << c.secondary_hash;
    ostr << "-tmp";
    return po6::pathname(ostr.str());
}

e::intrusive_ptr<hyperdisk::shard>
hyperdisk :: disk :: create_shard(const coordinate& c)
{
    po6::pathname spareshard_fn;
    e::intrusive_ptr<hyperdisk::shard> spareshard;

    {
        po6::threads::mutex::hold hold(&m_spare_shards_lock);

        if (!m_spare_shards.empty())
        {
            std::pair<po6::pathname, e::intrusive_ptr<hyperdisk::shard> > p;
            p = m_spare_shards.front();
            spareshard_fn = p.first;
            spareshard = p.second;
            m_spare_shards.pop();
        }
    }

    po6::pathname path = shard_filename(c);

    if (spareshard)
    {
        if (renameat(m_base.get(), spareshard_fn.get(),
                     m_base.get(), path.get()) < 0)
        {
            throw po6::error(errno);
        }

        return spareshard;
    }
    else
    {
        e::intrusive_ptr<hyperdisk::shard> newshard = hyperdisk::shard::create(m_base, path);
        return newshard;
    }
}

e::intrusive_ptr<hyperdisk::shard>
hyperdisk :: disk :: create_tmp_shard(const coordinate& c)
{
    po6::pathname spareshard_fn;
    e::intrusive_ptr<hyperdisk::shard> spareshard;

    {
        po6::threads::mutex::hold hold(&m_spare_shards_lock);

        if (!m_spare_shards.empty())
        {
            std::pair<po6::pathname, e::intrusive_ptr<hyperdisk::shard> > p;
            p = m_spare_shards.front();
            spareshard_fn = p.first;
            spareshard = p.second;
            m_spare_shards.pop();
        }
    }

    po6::pathname path = shard_tmp_filename(c);

    if (spareshard)
    {
        if (renameat(m_base.get(), spareshard_fn.get(),
                     m_base.get(), path.get()) < 0)
        {
            throw po6::error(errno);
        }

        return spareshard;
    }
    else
    {
        e::intrusive_ptr<hyperdisk::shard> newshard = hyperdisk::shard::create(m_base, path);
        return newshard;
    }
}

hyperdisk::returncode
hyperdisk :: disk :: drop_shard(const coordinate& c)
{
    // What would we do with the error?  It's just going to leave dirty data,
    // but if we can cleanly save state, then it doesn't matter.
    if (unlinkat(m_base.get(), shard_filename(c).get(), 0) < 0)
    {
        return DROPFAILED;
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: drop_tmp_shard(const coordinate& c)
{
    // What would we do with the error?  It's just going to leave dirty data,
    // but if we can cleanly save state, then it doesn't matter.
    if (unlinkat(m_base.get(), shard_tmp_filename(c).get(), 0) < 0)
    {
        return DROPFAILED;
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: deal_with_full_shard(size_t shard_num)
{
    coordinate c = m_shards->get_coordinate(shard_num);
    shard* s = m_shards->get_shard(shard_num);

    if (s->stale_space() >= 30)
    {
        // Just clean up the shard.
        return clean_shard(shard_num);
    }
    else if (c.primary_mask == UINT32_MAX || c.secondary_mask == UINT32_MAX)
    {
        // XXX NOCOMMIT;
        assert(!"Not implemented");
        return SPLITFAILED;
    }
    else
    {
        // Split the shard 4-ways.
        return split_shard(shard_num);
    }
}

hyperdisk::returncode
hyperdisk :: disk :: clean_shard(size_t shard_num)
{
    coordinate c = m_shards->get_coordinate(shard_num);
    shard* s = m_shards->get_shard(shard_num);
    e::intrusive_ptr<hyperdisk::shard> newshard = create_tmp_shard(c);
    e::guard disk_guard = e::makeobjguard(*this, &hyperdisk::disk::drop_tmp_shard, c);
    s->copy_to(c, newshard);
    e::intrusive_ptr<shard_vector> newshard_vector;
    newshard_vector = m_shards->replace(shard_num, newshard);

    if (renameat(m_base.get(), shard_tmp_filename(c).get(),
                 m_base.get(), shard_filename(c).get()) < 0)
    {
        return DROPFAILED;
    }

    disk_guard.dismiss();
    po6::threads::mutex::hold hold(&m_shards_lock);
    m_shards = newshard_vector;
    return SUCCESS;
}

static int
which_to_split(uint32_t mask, const int* zeros, const int* ones)
{
    int32_t diff = INT32_MAX;
    int32_t pos = 0;
    diff = (diff < 0) ? (-diff) : diff;

    for (int i = 1; i < 32; ++i)
    {
        if (!(mask & (1 << i)))
        {
            int tmpdiff = ones[i] - zeros[i];
            tmpdiff = (tmpdiff < 0) ? (-tmpdiff) : tmpdiff;

            if (tmpdiff < diff)
            {
                pos = i;
                diff = tmpdiff;
            }
        }
    }

    return pos;
}

hyperdisk::returncode
hyperdisk :: disk :: split_shard(size_t shard_num)
{
    coordinate c = m_shards->get_coordinate(shard_num);
    shard* s = m_shards->get_shard(shard_num);
    e::intrusive_ptr<hyperdisk::shard_snapshot> snap = s->make_snapshot();

    // Find which bit of the secondary hash is the best to split over.
    int zeros[32];
    int ones[32];
    memset(zeros, 0, sizeof(zeros));
    memset(ones, 0, sizeof(ones));

    for (; snap->valid(); snap->next())
    {
        for (uint64_t i = 1, j = 0; i < UINT32_MAX; i <<= 1, ++j)
        {
            if (c.secondary_mask & i)
            {
                continue;
            }

            if (snap->secondary_hash() & i)
            {
                ++ones[j];
            }
            else
            {
                ++zeros[j];
            }
        }
    }

    int secondary_split = which_to_split(c.secondary_mask, zeros, ones);
    uint32_t secondary_bit = 1 << secondary_split;
    snap = s->make_snapshot();

    // Determine the splits for the two shards resulting from the split above.
    int zeros_lower[32];
    int zeros_upper[32];
    int ones_lower[32];
    int ones_upper[32];
    memset(zeros_lower, 0, sizeof(zeros_lower));
    memset(zeros_upper, 0, sizeof(zeros_upper));
    memset(ones_lower, 0, sizeof(ones_lower));
    memset(ones_upper, 0, sizeof(ones_upper));

    for (; snap->valid(); snap->next())
    {
        for (uint64_t i = 1, j = 0; i < UINT32_MAX; i <<= 1, ++j)
        {
            if (c.primary_mask & i)
            {
                continue;
            }

            if (snap->secondary_hash() & secondary_bit)
            {
                if (snap->primary_hash() & i)
                {
                    ++ones_upper[j];
                }
                else
                {
                    ++zeros_upper[j];
                }
            }
            else
            {
                if (snap->primary_hash() & i)
                {
                    ++ones_lower[j];
                }
                else
                {
                    ++zeros_lower[j];
                }
            }
        }
    }

    int primary_lower_split = which_to_split(c.primary_mask, zeros_lower, ones_lower);
    uint32_t primary_lower_bit = 1 << primary_lower_split;
    int primary_upper_split = which_to_split(c.primary_mask, zeros_upper, ones_upper);
    uint32_t primary_upper_bit = 1 << primary_upper_split;

    // Create four new shards, and scatter the data between them.
    coordinate zero_zero_coord(c.primary_mask | primary_lower_bit, c.primary_hash,
                               c.secondary_mask | secondary_bit, c.secondary_hash);
    coordinate zero_one_coord(c.primary_mask | primary_upper_bit, c.primary_hash,
                              c.secondary_mask | secondary_bit, c.secondary_hash | secondary_bit);
    coordinate one_zero_coord(c.primary_mask | primary_lower_bit, c.primary_hash | primary_lower_bit,
                              c.secondary_mask | secondary_bit, c.secondary_hash);
    coordinate one_one_coord(c.primary_mask | primary_upper_bit, c.primary_hash | primary_upper_bit,
                              c.secondary_mask | secondary_bit, c.secondary_hash | secondary_bit);

    try
    {
        e::intrusive_ptr<hyperdisk::shard> zero_zero = create_shard(zero_zero_coord);
        e::guard zzg = e::makeobjguard(*this, &hyperdisk::disk::drop_shard, zero_zero_coord);
        s->copy_to(zero_zero_coord, zero_zero);

        e::intrusive_ptr<hyperdisk::shard> zero_one = create_shard(zero_one_coord);
        e::guard zog = e::makeobjguard(*this, &hyperdisk::disk::drop_shard, zero_one_coord);
        s->copy_to(zero_one_coord, zero_one);

        e::intrusive_ptr<hyperdisk::shard> one_zero = create_shard(one_zero_coord);
        e::guard ozg = e::makeobjguard(*this, &hyperdisk::disk::drop_shard, one_zero_coord);
        s->copy_to(one_zero_coord, one_zero);

        e::intrusive_ptr<hyperdisk::shard> one_one = create_shard(one_one_coord);
        e::guard oog = e::makeobjguard(*this, &hyperdisk::disk::drop_shard, one_one_coord);
        s->copy_to(one_one_coord, one_one);

        e::intrusive_ptr<shard_vector> newshard_vector;
        newshard_vector = m_shards->replace(shard_num,
                                            zero_zero_coord, zero_zero,
                                            zero_one_coord, zero_one,
                                            one_zero_coord, one_zero,
                                            one_one_coord, one_one);

        {
            po6::threads::mutex::hold hold(&m_shards_lock);
            m_shards = newshard_vector;
        }

        zzg.dismiss();
        zog.dismiss();
        ozg.dismiss();
        oog.dismiss();
        return drop_shard(c);
    }
    catch (std::exception& e)
    {
        return SPLITFAILED;
    }
}
