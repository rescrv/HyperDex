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
#include <cmath>

// POSIX
#include <sys/stat.h>
#include <sys/types.h>

// C++
#include <iomanip>
#include <sstream>
#include <fstream>

// e
#include <e/guard.h>

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/mask.h"

// HyperDisk
#include "hyperdisk/hyperdisk/disk.h"
#include "hyperdisk/log_entry.h"
#include "hyperdisk/offset_update.h"
#include "hyperdisk/shard.h"
#include "hyperdisk/shard_snapshot.h"
#include "hyperdisk/shard_vector.h"

// util
#include <util/atomicfile.h>


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

const int hyperdisk :: disk :: STATE_FILE_VER = 1;
const char* hyperdisk :: disk :: STATE_FILE_NAME = "disk_state.hd";

e::intrusive_ptr<hyperdisk::disk>
hyperdisk :: disk :: create(const po6::pathname& directory,
                            const hyperspacehashing::mask::hasher& hasher,
                            uint16_t arity)
{
    // Create a blank disk.
    return new disk(directory, hasher, arity);
}

e::intrusive_ptr<hyperdisk::disk>
hyperdisk :: disk :: open(const po6::pathname& directory,
                          const hyperspacehashing::mask::hasher& hasher,
                          uint16_t arity,
                          const std::string& quiesce_state_id)
{
    // Open quiesced disk.
    return new disk(directory, hasher, arity, true, quiesce_state_id);
}

bool
hyperdisk :: disk :: quiesce(const std::string& quiesce_state_id)
{
    // Flush all data to O/S buffers.
    bool flushed = false;
    while (!flushed)
    {
        returncode rc = flush(-1, false);

        switch (rc)
        {
            case DIDNOTHING:
                // All data is flushed, move on.
                flushed = true;
                break;
            case SUCCESS:
                // Some data flushed, try again.
                continue;
            case DATAFULL:
            case SEARCHFULL:
                // Split the shards and try agian.
                do_mandatory_io();
                continue;
            case NOTFOUND:
            case WRONGARITY:
            case SYNCFAILED:
            case DROPFAILED:
            case MISSINGDISK:
            case SPLITFAILED:
            default:
                return false;
        }
    }
    
    // Flush O/S buffers to disk.
    returncode rc = sync();
    if (SUCCESS != rc)
    {
        return false;
    }
    
    // Persist the state into a file.
    return dump_state(quiesce_state_id);
}

bool
hyperdisk :: disk :: dump_state(const std::string& quiesce_state_id)
{
    // Dump state information.
    e::intrusive_ptr<shard_vector> shards;
    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }
    
    std::ostringstream s;
    s << "version " << STATE_FILE_VER << std::endl;
    s << "state_id " << quiesce_state_id << std::endl;
    for (size_t i = 0; i < shards->size(); ++i)
    {
        coordinate c = shards->get_coordinate(i);
        uint32_t o = shards->get_offset(i);
        s << "shard";
        s << " " << c.primary_mask;
        s << " " << c.primary_hash;
        s << " " << c.secondary_lower_mask;
        s << " " << c.secondary_lower_hash;
        s << " " << c.secondary_upper_mask;
        s << " " << c.secondary_upper_hash;
        s << " " << o;
        s << std::endl;
    }
    
    // Rewrite the state file atomically.
    return util::atomicfile::rewrite(m_base_filename.get(), STATE_FILE_NAME, s.str().c_str());
}

bool
hyperdisk :: disk :: load_state(const std::string& quiesce_state_id)
{
    po6::pathname config_name = po6::join(m_base_filename, STATE_FILE_NAME);
    std::ifstream f; 
    f.open(config_name.get());
    if (!f)
    {
        return false;
    }

    // Is state file right version?
    std::string v;
    f >> v;
    if (f.fail() || "version" != v)
    {
        return false;
    }

    int32_t vn = -1;
    f >> vn;
    if (f.fail() || STATE_FILE_VER != vn)
    {
        return false;
    }
    
    // Does the state file match the quiesced state we are loading? 
    std::string s;
    f >> s;
    if (f.fail() || "state_id" != s)
    {
        return false;
    }

    std::string sid;
    f >> sid;
    if (f.fail() || quiesce_state_id != sid)
    {
        return false;
    }

    // Restore the shards.
    std::vector<std::pair<coordinate, e::intrusive_ptr<shard> > > shards;
    while (!f.eof())
    {
        // Line header.
        std::string h;
        f >> h;
        if (f.eof() && "" == h)
        {
            // White space at the end of file, done processing.
            break;
        }
        if (f.fail() || "shard" != h)
        {
            return false;
        }
        
        // Coordinate.
        uint64_t ct[6] = {-1, -1, -1, -1, -1 ,-1};
        for (int i=0; i<6; i++)
        {
            f >> ct[i];
        }
        if (f.fail())
        {
            return false;
        }

        // Offset.
        uint32_t o = -1;
        f >> o;
        if (f.fail())
        {
            return false;
        }

        // Reopen the shard.
        coordinate c(ct[0], ct[1], ct[2], ct[3], ct[4], ct[5]);
        po6::pathname path = shard_filename(c);
        // XXX need to compare offset inside open 
        e::intrusive_ptr<shard> sh = hyperdisk::shard::open(m_base, path);
        
        shards.push_back(std::make_pair(c, sh));
    }

    // Re-install the reopened shards into the disk.
    po6::threads::mutex::hold a(&m_shards_mutate);
    po6::threads::mutex::hold b(&m_shards_lock);
    m_shards = new shard_vector(1, &shards);
 
    return true;
}

hyperdisk::returncode
hyperdisk :: disk :: get(const e::slice& key,
                         std::vector<e::slice>* value,
                         uint64_t* version,
                         reference* backing)
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
            backing->set(shards->get_shard(i));
            break;
        }
    }

    bool found = false;
    returncode wal_res = NOTFOUND;

    for (; it.valid(); it.next())
    {
        if (it->coord.primary_intersects(coord) && it->key == key)
        {
            if (it->is_put)
            {
                *value = it->value;
                *version = it->version;
                wal_res = SUCCESS;
            }
            else
            {
                wal_res = NOTFOUND;
            }

            found = true;
            backing->set(it);
        }
    }

    if (found)
    {
        return wal_res;
    }

    return shard_res;
}

hyperdisk::returncode
hyperdisk :: disk :: put(std::tr1::shared_ptr<e::buffer> backing,
                         const e::slice& key,
                         const std::vector<e::slice>& value,
                         uint64_t version)
{
    if (value.size() + 1 != m_arity)
    {
        return WRONGARITY;
    }

    coordinate coord = m_hasher.hash(key, value);
    m_log.append(log_entry(coord, backing, key, value, version));
    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: disk :: del(std::tr1::shared_ptr<e::buffer> backing,
                         const e::slice& key)
{
    coordinate coord = m_hasher.hash(key);
    m_log.append(log_entry(coord, backing, key));
    return SUCCESS;
}

e::intrusive_ptr<hyperdisk::snapshot>
hyperdisk :: disk :: make_snapshot(const hyperspacehashing::search& terms)
{
    hyperspacehashing::mask::coordinate coord(m_hasher.hash(terms));
    e::intrusive_ptr<shard_vector> shards;
    e::locking_iterable_fifo<offset_update>::iterator it = m_offsets.iterate();

    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }

    std::vector<uint32_t> offsets(shards->size());

    for (size_t i = 0; i < shards->size(); ++i)
    {
        offsets[i] = shards->get_offset(i);
    }

    for (; it.valid() && it->shard_generation <= shards->generation(); it.next())
    {
        if (it->shard_generation == shards->generation())
        {
            offsets[it->shard_num] = it->new_offset;
        }
    }

    std::vector<hyperdisk::shard_snapshot> snaps;
    snaps.reserve(shards->size());

    for (size_t i = 0; i < shards->size(); ++i)
    {
        if (coord.intersects(shards->get_coordinate(i)))
        {
            snaps.push_back(shard_snapshot(offsets[i], shards->get_shard(i)));
        }
    }

    e::intrusive_ptr<hyperdisk::snapshot> ret;
    ret = new snapshot(coord, shards, &snaps);
    return ret;
}

e::intrusive_ptr<hyperdisk::rolling_snapshot>
hyperdisk :: disk :: make_rolling_snapshot()
{
    hyperspacehashing::search terms(m_arity);
    e::locking_iterable_fifo<log_entry>::iterator iter(m_log.iterate());
    e::intrusive_ptr<snapshot> snap = make_snapshot(terms);
    e::intrusive_ptr<rolling_snapshot> ret = new rolling_snapshot(iter, snap);
    return ret;
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
    return SUCCESS;
}

// This operation will return SUCCESS as long as it knows that progress is being
// made.  It will return DIDNOTHING if there was nothing to do.
hyperdisk::returncode
hyperdisk :: disk :: flush(ssize_t num, bool nonblocking)
{
    if (nonblocking)
    {
        if (!m_shards_mutate.trylock())
        {
            return SUCCESS;
        }
    }
    else
    {
        m_shards_mutate.lock();
    }

    e::guard hold = e::makeobjguard(m_shards_mutate, &po6::threads::mutex::unlock);

    bool flushed = false;
    returncode flush_status = SUCCESS;
    hold.use_variable();
    e::locking_iterable_fifo<log_entry>::iterator it = m_log.iterate();

    // num == -1 means flush all
    for (ssize_t nf = 0; (nf < num || num < 0) && it.valid(); ++nf, it.next())
    {
        const coordinate& coord = it->coord;
        const e::slice& key = it->key;
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
                abort();
            }
        }

        bool put_performed = false;
        size_t put_num = 0;
        uint32_t put_offset = 0;

        if (it->is_put)
        {
            const std::vector<e::slice>& value = it->value;
            const uint64_t version = it->version;

            // This must start at the last position and work downward so that
            // the last arg to "shard_vector->replace" will be considered first.
            for (ssize_t i = m_shards->size() - 1; !put_performed && i >= 0; --i)
            {
                if (!m_shards->get_coordinate(i).intersects(coord))
                {
                    continue;
                }

                returncode ret;
                ret = m_shards->get_shard(i)->put(coord, key, value,
                                                  version, &put_offset);

                if (ret == SUCCESS)
                {
                    put_performed = true;
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
                    abort();
                }
            }

            if (!put_performed)
            {
                break;
            }
        }

        if (del_needed && (!put_performed || del_num != put_num))
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
                case DIDNOTHING:
                default:
                    abort();
            }
        }

        flushed = true;

        // Here we prepare two offset_updates that we can push onto the offsets
        // log.  We then make the offset changes to the shard_vector, and then
        // finish by removing the items we put on the log.
        std::vector<offset_update> updates;

        if (del_needed && (!put_performed || del_num != put_num))
        {
            updates.push_back(offset_update());
            updates.back().shard_generation = m_shards->generation();
            updates.back().shard_num = del_num;
            updates.back().new_offset = del_offset;
        }

        if (put_performed)
        {
            updates.push_back(offset_update());
            updates.back().shard_generation = m_shards->generation();
            updates.back().shard_num = put_num;
            updates.back().new_offset = put_offset;
        }

        // Log our intentions.
        m_offsets.batch_append(updates);

        // Do our updates.
        for (size_t i = 0; i < updates.size(); ++i)
        {
            assert(updates[i].shard_generation == m_shards->generation());
            assert(updates[i].new_offset > m_shards->get_offset(updates[i].shard_num));
            m_shards->set_offset(updates[i].shard_num, updates[i].new_offset);
        }

        // Remove our updates from the log.
        for (size_t i = 0; i < updates.size(); ++i)
        {
            assert(m_offsets.oldest() == updates[i]);
            m_offsets.remove_oldest();
        }
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
        return DIDNOTHING;
    }
    return SUCCESS;
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
hyperdisk :: disk :: do_optimistic_io()
{
    e::intrusive_ptr<shard_vector> shards;

    {
        po6::threads::mutex::hold b(&m_shards_lock);
        shards = m_shards;
    }

    size_t most_loaded = 0;
    int most_loaded_amt = 0;
    uint64_t used = 0;
    uint64_t capacity = 0;

    for (size_t i = 0; i < shards->size(); ++i)
    {
        int loaded = shards->get_shard(i)->used_space();

        if (loaded > most_loaded_amt)
        {
            most_loaded = i;
            most_loaded_amt = loaded;
        }

        used += loaded;
        capacity += 100;
    }

    if (capacity)
    {
        double flip = static_cast<double>(rand_r(&m_seed)) / static_cast<double>(RAND_MAX);
        double thresh = 1 / pow(1.01, 100 - used);
        po6::threads::mutex::hold holdm(&m_shards_mutate);

        if (shards == m_shards && flip < thresh && most_loaded_amt >= 75)
        {
            return deal_with_full_shard(most_loaded);
        }
    }

    return DIDNOTHING;
}

hyperdisk::returncode
hyperdisk :: disk :: preallocate()
{
    {
        po6::threads::mutex::hold hold(&m_spare_shards_lock);

        if (m_spare_shards.size() >= 16)
        {
            return DIDNOTHING;
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

        if (used < 25)
        {
        }
        else if (used < 50)
        {
            needed_shards += 1;
        }
        else
        {
            if (stale >= 30)
            {
                needed_shards += 1;
            }
            else
            {
                needed_shards += 4;
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

    return need_shard ? SUCCESS : DIDNOTHING;
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
                          const uint16_t arity,
                          bool load_quiesced_state,
                          const std::string& quiesce_state_id)
    : m_ref(0)
    , m_arity(arity)
    , m_hasher(hasher)
    , m_shards_mutate()
    , m_shards_lock()
    , m_shards()
    , m_log()
    , m_offsets()
    , m_base()
    , m_base_filename(directory)
    , m_spare_shards_lock()
    , m_spare_shards()
    , m_spare_shard_counter(0)
    , m_needs_io(-1)
    , m_seed(0)
{
    if (mkdir(directory.get(), S_IRWXU) < 0 && errno != EEXIST)
    {
        throw po6::error(errno);
    }

    m_base = ::open(directory.get(), O_RDONLY);

    if (m_base.get() < 0)
    {
        throw po6::error(errno);
    }
    
    // Create vs reload.
    if (!load_quiesced_state)
    {
        // Create a starting disk which holds everything.
        po6::threads::mutex::hold a(&m_shards_mutate);
        po6::threads::mutex::hold b(&m_shards_lock);
        coordinate start;
        e::intrusive_ptr<shard> s = create_shard(start);
        m_shards = new shard_vector(start, s);
    }
    else
    {
        // Reopen quiesced disk.
        // XXX handle errors
        load_state(quiesce_state_id);
    }
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
    ostr << "-" << std::setw(16) << c.secondary_upper_mask;
    ostr << "-" << std::setw(16) << c.secondary_upper_hash;
    ostr << "-" << std::setw(16) << c.secondary_lower_mask;
    ostr << "-" << std::setw(16) << c.secondary_lower_hash;
    return po6::pathname(ostr.str());
}

po6::pathname
hyperdisk :: disk :: shard_tmp_filename(const coordinate& c)
{
    std::ostringstream ostr;
    ostr << std::hex << std::setfill('0') << std::setw(16) << c.primary_mask;
    ostr << "-" << std::setw(16) << c.primary_hash;
    ostr << "-" << std::setw(16) << c.secondary_upper_mask;
    ostr << "-" << std::setw(16) << c.secondary_upper_hash;
    ostr << "-" << std::setw(16) << c.secondary_lower_mask;
    ostr << "-" << std::setw(16) << c.secondary_lower_hash;
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
    // XXX Handle the case where the masks have been maxed.
    else if (c.primary_mask == UINT64_MAX ||
             c.secondary_lower_mask == UINT64_MAX ||
             c.secondary_upper_mask == UINT64_MAX)
    {
        abort();
        return DIDNOTHING;
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
which_to_split(uint64_t mask, const int* zeros, const int* ones)
{
    int64_t diff = INT32_MAX;
    int64_t pos = 63;

    for (int i = 0; i < 64; ++i)
    {
        if (!(mask & (1ULL << i)))
        {
            int tmpdiff = ones[i] - zeros[i];
            tmpdiff = (tmpdiff < 0) ? (-tmpdiff) : tmpdiff;

            if (tmpdiff * 1.25 < diff)
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
    hyperdisk::shard_snapshot snap = s->make_snapshot();

    // Find which bit of the secondary hash is the best to split over.
    int zeros[64];
    int ones[64];
    memset(zeros, 0, sizeof(zeros));
    memset(ones, 0, sizeof(ones));

    for (; snap.valid(); snap.next())
    {
        for (int j = 0; j < 64; ++j)
        {
            uint64_t bit = 1ULL << j;

            if (c.secondary_lower_mask & bit)
            {
                continue;
            }

            if (snap.coordinate().secondary_lower_hash & bit)
            {
                ++ones[j];
            }
            else
            {
                ++zeros[j];
            }
        }
    }

    int secondary_split = which_to_split(c.secondary_lower_mask, zeros, ones);
    uint64_t secondary_bit = 1ULL << secondary_split;
    snap = s->make_snapshot();

    // Determine the splits for the two shards resulting from the split above.
    int zeros_lower[64];
    int zeros_upper[64];
    int ones_lower[64];
    int ones_upper[64];
    memset(zeros_lower, 0, sizeof(zeros_lower));
    memset(zeros_upper, 0, sizeof(zeros_upper));
    memset(ones_lower, 0, sizeof(ones_lower));
    memset(ones_upper, 0, sizeof(ones_upper));

    for (; snap.valid(); snap.next())
    {
        for (int j = 0; j < 64; ++j)
        {
            uint64_t bit = 1ULL << j;

            if (c.primary_mask & bit)
            {
                continue;
            }

            if (snap.coordinate().secondary_lower_hash & secondary_bit)
            {
                if (snap.coordinate().primary_hash & bit)
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
                if (snap.coordinate().primary_hash & bit)
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
    uint64_t primary_lower_bit = 1ULL << primary_lower_split;
    int primary_upper_split = which_to_split(c.primary_mask, zeros_upper, ones_upper);
    uint64_t primary_upper_bit = 1ULL << primary_upper_split;

    // Create four new shards, and scatter the data between them.
    coordinate zero_zero_coord(c.primary_mask | primary_lower_bit, c.primary_hash,
                               c.secondary_lower_mask | secondary_bit, c.secondary_lower_hash,
                               c.secondary_upper_mask, c.secondary_upper_hash);
    coordinate zero_one_coord(c.primary_mask | primary_upper_bit, c.primary_hash,
                              c.secondary_lower_mask | secondary_bit, c.secondary_lower_hash | secondary_bit,
                              c.secondary_upper_mask, c.secondary_upper_hash);
    coordinate one_zero_coord(c.primary_mask | primary_lower_bit, c.primary_hash | primary_lower_bit,
                              c.secondary_lower_mask | secondary_bit, c.secondary_lower_hash,
                              c.secondary_upper_mask, c.secondary_upper_hash);
    coordinate one_one_coord(c.primary_mask | primary_upper_bit, c.primary_hash | primary_upper_bit,
                             c.secondary_lower_mask | secondary_bit, c.secondary_lower_hash | secondary_bit,
                             c.secondary_upper_mask, c.secondary_upper_hash);

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
        // Those with a zero bit for the secondary hash must come last, so that
        // they will be picked up first.  This is necessary to make objects with
        // no searchable attribute work properly.
        newshard_vector = m_shards->replace(shard_num,
                                            zero_one_coord, zero_one,
                                            one_one_coord, one_one,
                                            zero_zero_coord, zero_zero,
                                            one_zero_coord, one_zero);

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
