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
#include <endian.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/io/fd.h>

// HyperDex
#include <hyperdex/disk.h>
#include <hyperdex/hyperspace.h>

e::intrusive_ptr<hyperdex::disk>
hyperdex :: disk :: create(const po6::pathname& filename)
{
    // Open a temporary file in fd with path tmp.
    po6::io::fd fd;
    po6::pathname tmp = filename;

    if (!po6::mkstemp(&fd, &tmp))
    {
        throw po6::error(errno);
    }

    // Make sure that tmp is unlinked under all normal circumstances.
    e::guard g_unlink = e::makeguard(unlink, tmp.get());

    // Truncate it to the correct size.
    if (ftruncate(fd.get(), TOTAL_FILE_SIZE) < 0)
    {
        throw po6::error(errno);
    }

    // Create the disk object.
    e::intrusive_ptr<disk> ret = new disk(&fd, filename);

    // Move the filename.
    if (rename(tmp.get(), filename.get()) < 0)
    {
        LOG(INFO) << "Could not rename disk.";
        throw po6::error(errno);
    }

    g_unlink.dismiss();
    return ret;
}

hyperdex :: result_t
hyperdex :: disk :: get(const e::buffer& key,
                        uint64_t key_hash,
                        std::vector<e::buffer>* value,
                        uint64_t* version)
{
    size_t entry;

    if (!find_bucket(key, key_hash, &entry))
    {
        return NOTFOUND;
    }

    const uint32_t offset = hashtable_offset(entry);

    if (offset == 0 || offset == UINT32_MAX)
    {
        return NOTFOUND;
    }

    // Load the information.
    *version = data_version(offset);
    // const size_t key_size = data_key_size(offset);
    // data_key(offset, &key);
    // ^ Skipped because find_bucket ensures that the key matches.
    data_value(offset, key.size(), value);
    return SUCCESS;
}

hyperdex :: result_t
hyperdex :: disk :: put(const e::buffer& key,
                        uint64_t key_hash,
                        const std::vector<e::buffer>& value,
                        const std::vector<uint64_t>& value_hashes,
                        uint64_t version)
{
    size_t required = data_size(key, value);

    if (required + m_offset > TOTAL_FILE_SIZE)
    {
        LOG(INFO) << "PUT failed because disk data segment is full.";
        return DISKFULL;
    }

    if (m_search == SEARCH_INDEX_ENTRIES - 1)
    {
        LOG(INFO) << "PUT failed because disk search index is full.";
        return DISKFULL;
    }

    // Find the bucket.
    size_t entry;
    bool overwrite = find_bucket(key, key_hash, &entry);

    if (entry == HASH_TABLE_SIZE)
    {
        LOG(INFO) << "PUT failed because disk hash table is full.";
        return DISKFULL;
    }

    // Values to pack.
    uint64_t pack_version = htobe64(version);
    uint32_t pack_key_size = htobe32(key.size());
    uint16_t pack_value_arity = htobe16(value.size());

    // Pack the values on disk.
    uint32_t curr_offset = m_offset;
    memmove(m_base + curr_offset, &pack_version, sizeof(pack_version));
    curr_offset += sizeof(pack_version);
    memmove(m_base + curr_offset, &pack_key_size, sizeof(pack_key_size));
    curr_offset += sizeof(pack_key_size);
    memmove(m_base + curr_offset, key.get(), key.size());
    curr_offset += key.size();
    memmove(m_base + curr_offset, &pack_value_arity, sizeof(pack_value_arity));
    curr_offset += sizeof(pack_value_arity);

    for (size_t i = 0; i < value.size(); ++i)
    {
        uint32_t size = htobe32(value[i].size());
        memmove(m_base + curr_offset, &size, sizeof(size));
        curr_offset += sizeof(size);
        memmove(m_base + curr_offset, value[i].get(), value[i].size());
        curr_offset += value[i].size();
    }


    // Invalidate anything pointing to the old version.
    if (overwrite)
    {
        invalidate_search_index(hashtable_offset(entry), m_offset);
    }

    // Do an mfence to make sure that everything we've written is visible.
    __sync_synchronize();

    // Insert into the search index.
    searchindex_hash(m_search, hyperspace::secondary_point(value_hashes));
    searchindex_invalid(m_search, 0);
    searchindex_offset(m_search, m_offset);

    // Insert into the hash table.
    hashtable_hash(entry, hyperspace::primary_point(key_hash));
    hashtable_offset(entry, m_offset);

    // Update the offsets
    ++m_search;
    m_offset = (curr_offset + 7) & ~7; // Keep everything 8-byte aligned.
    return SUCCESS;
}

hyperdex :: result_t
hyperdex :: disk :: del(const e::buffer& key,
                        uint64_t key_hash)
{
    size_t entry;

    if (!find_bucket(key, key_hash, &entry))
    {
        return NOTFOUND;
    }

    if (entry == HASH_TABLE_ENTRIES)
    {
        return NOTFOUND;
    }

    if (m_offset + sizeof(uint64_t) > TOTAL_FILE_SIZE)
    {
        LOG(INFO) << "DEL failed because disk data segment is full.";
        return DISKFULL;
    }

    const uint32_t offset = hashtable_offset(entry);
    assert(offset != 0 && offset != UINT32_MAX);
    invalidate_search_index(offset, m_offset);
    m_offset += sizeof(uint64_t);
    hashtable_offset(entry, UINT32_MAX);
    return SUCCESS;
}

void
hyperdex :: disk :: sync()
{
    if (msync(m_base, TOTAL_FILE_SIZE, MS_SYNC) < 0)
    {
        PLOG(INFO) << "Could not sync disk";
    }
}

bool
hyperdex :: disk :: needs_cleaning() const
{
    size_t i;
    size_t freeable = 0;
    uint32_t prev = searchindex_offset(0);

    for (i = 1; i < SEARCH_INDEX_ENTRIES; ++i)
    {
        uint32_t cur = searchindex_offset(i);

        if (cur == 0)
        {
            break;
        }

        if (searchindex_invalid(i - 1) > 0)
        {
            freeable += cur - prev;
        }

        prev = cur;
    }

    if (i == SEARCH_INDEX_ENTRIES && searchindex_invalid(SEARCH_INDEX_ENTRIES - 1) > 0)
    {
        freeable += m_offset - prev;
    }

    return freeable > (DATA_SEGMENT_SIZE >> 1);
}

void
hyperdex :: disk :: async()
{
    if (msync(m_base, TOTAL_FILE_SIZE, MS_ASYNC) < 0)
    {
        PLOG(INFO) << "Could not sync disk";
    }
}

void
hyperdex :: disk :: drop()
{
    if (unlink(m_filename.get()) < 0)
    {
        PLOG(WARNING) << "Could not drop disk \"" << m_filename << "\"";
    }
}

e::intrusive_ptr<hyperdex::disk::snapshot>
hyperdex :: disk :: make_snapshot()
{
    e::intrusive_ptr<disk> d = this;
    assert(m_ref >= 2);
    e::intrusive_ptr<snapshot> ret = new snapshot(d);
    return ret;
}

hyperdex :: disk :: disk(po6::io::fd* fd, const po6::pathname& fn)
    : m_ref(0)
    , m_base(NULL)
    , m_offset(INDEX_SEGMENT_SIZE)
    , m_search(0)
    , m_filename(fn)
{
    m_base = static_cast<char*>(mmap(NULL, TOTAL_FILE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd->get(), 0));

    if (m_base == MAP_FAILED)
    {
        throw po6::error(errno);
    }
}

hyperdex :: disk :: ~disk()
                    throw ()
{
    try
    {
        if (munmap(m_base, TOTAL_FILE_SIZE) < 0)
        {
            PLOG(WARNING) << "Could not munmap disk";
        }
    }
    catch (...)
    {
    }
}

size_t
hyperdex :: disk :: data_size(const e::buffer& key,
                              const std::vector<e::buffer>& value) const
{
    size_t hypothetical_size = sizeof(uint64_t) + sizeof(uint32_t)
                             + sizeof(uint16_t) + key.size()
                             + sizeof(uint32_t) * value.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        hypothetical_size += value[i].size();
    }

    return hypothetical_size;
}

uint64_t
hyperdex :: disk :: data_version(uint32_t offset) const
{
    assert(((offset + 7) & ~7) == offset);
    return be64toh(*reinterpret_cast<uint64_t*>(m_base + offset));
}

size_t
hyperdex :: disk :: data_key_size(uint32_t offset) const
{
    assert(((offset + 7) & ~7) == offset);
    return be32toh(*reinterpret_cast<uint32_t*>(m_base + offset + sizeof(uint64_t)));
}

void
hyperdex :: disk :: data_key(uint32_t offset,
                             size_t keysize,
                             e::buffer* key) const
{
    assert(((offset + 7) & ~7) == offset);
    uint32_t cur_offset = offset + sizeof(uint64_t) + sizeof(uint32_t);
    *key = e::buffer(m_base + cur_offset, keysize);
}

void
hyperdex :: disk :: data_value(uint32_t offset,
                               size_t keysize,
                               std::vector<e::buffer>* value) const
{
    assert(((offset + 7) & ~7) == offset);
    uint32_t cur_offset = offset + sizeof(uint64_t) + sizeof(uint32_t) + keysize;
    uint16_t num_dims;
    memmove(&num_dims, m_base + cur_offset, sizeof(uint16_t));
    num_dims = be16toh(num_dims);
    cur_offset += sizeof(uint16_t);
    value->clear();

    for (uint16_t i = 0; i < num_dims; ++i)
    {
        uint32_t size;
        memmove(&size, m_base + cur_offset, sizeof(size));
        size = be32toh(size);
        cur_offset += sizeof(size);
        value->push_back(e::buffer());
        e::buffer buf(m_base + cur_offset, size);
        value->back().swap(buf);
        cur_offset += size;
    }
}

bool
hyperdex :: disk :: find_bucket(const e::buffer& key,
                                uint64_t key_hash,
                                size_t* entry)
{
    // The first dead bucket.
    size_t dead = HASH_TABLE_ENTRIES;

    // The bucket/hash we want to use.
    *entry = key_hash % HASH_TABLE_ENTRIES;
    uint32_t primary_point = hyperspace::primary_point(key_hash);

    for (size_t off = 0; off < HASH_TABLE_ENTRIES; ++off)
    {
        size_t bucket = (*entry + off) % HASH_TABLE_ENTRIES;
        uint32_t hash = hashtable_hash(bucket);
        uint32_t offset = hashtable_offset(bucket);

        if (hash == primary_point && offset == UINT32_MAX)
        {
            dead = bucket;
        }
        else if (hash == primary_point)
        {
            const size_t key_size = data_key_size(offset);

            if (key_size == key.size() && memcmp(m_base + data_key_offset(offset), key.get(), key_size) == 0)
            {
                *entry = bucket;
                return true;
            }
        }

        if (offset == 0)
        {
            if (dead == HASH_TABLE_ENTRIES)
            {
                *entry = bucket;
            }
            else
            {
                *entry = dead;
            }

            return false;
        }

        if (offset == UINT32_MAX && dead == HASH_TABLE_ENTRIES)
        {
            dead = bucket;
        }
    }

    *entry = dead;
    return false;
}

void
hyperdex :: disk :: invalidate_search_index(uint32_t to_invalidate, uint32_t invalidate_with)
{
    int64_t low = 0;
    int64_t high = SEARCH_INDEX_ENTRIES - 1;

    while (low <= high)
    {
        int64_t mid = low + ((high - low) / 2);
        const uint32_t mid_offset = searchindex_offset(mid);

        if (mid_offset == 0 || mid_offset > to_invalidate)
        {
            high = mid - 1;
        }
        else if (mid_offset < to_invalidate)
        {
            low = mid + 1;
        }
        else if (mid_offset == to_invalidate)
        {
            searchindex_invalid(mid, invalidate_with);
            return;
        }
    }
}

bool
hyperdex :: disk :: snapshot :: valid()
{
    uint32_t offset = 0;
    uint32_t invalid = 0;

    while (m_entry < SEARCH_INDEX_ENTRIES)
    {
        offset = m_disk->searchindex_offset(m_entry);
        invalid = m_disk->searchindex_invalid(m_entry);

        // If the m_valid flag is set; the offset is within the subsection of
        // data we may observe; and the data was never invalidated, or was
        // invalidated after we scanned it, then we may return true;
        if (m_valid && offset > 0 && offset < m_limit &&
                (invalid == 0 || invalid >= m_limit))
        {
            return true;
        }

        // If offset is 0, then we know that there are no more entries further
        // on.  Stop iterating.  If offset is >= m_limit, we know that the
        // operation (and all succeeding it) happened after the snapshot.
        if (offset == 0 || offset >= m_limit)
        {
            m_valid = false;
            break;
        }

        ++m_entry;
        m_valid = true;
    }

    return m_entry < SEARCH_INDEX_ENTRIES && m_valid;
}

void
hyperdex :: disk :: snapshot :: next()
{
    m_valid = false;
}

uint32_t
hyperdex :: disk :: snapshot :: secondary_point()
{
    return m_disk->searchindex_hash(m_entry);
}

uint64_t
hyperdex :: disk :: snapshot :: version()
{
    uint32_t offset = m_disk->searchindex_offset(m_entry);

    if (!offset)
    {
        return uint64_t();
    }

    return m_disk->data_version(offset);
}

e::buffer
hyperdex :: disk :: snapshot :: key()
{
    uint32_t offset = m_disk->searchindex_offset(m_entry);

    if (!offset)
    {
        return e::buffer();
    }

    e::buffer k;
    size_t key_size = m_disk->data_key_size(offset);
    m_disk->data_key(offset, key_size, &k);
    return k;
}

std::vector<e::buffer>
hyperdex :: disk :: snapshot :: value()
{
    uint32_t offset = m_disk->searchindex_offset(m_entry);

    if (!offset)
    {
        return std::vector<e::buffer>();
    }

    std::vector<e::buffer> v;
    size_t key_size = m_disk->data_key_size(offset);
    m_disk->data_value(offset, key_size, &v);
    return v;
}

hyperdex :: disk :: snapshot :: snapshot(e::intrusive_ptr<disk> d)
    : m_ref(0)
    , m_valid(true)
    , m_disk(d)
    , m_limit(d->m_offset)
    , m_entry(0)
{
}
