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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// po6
#include <po6/io/fd.h>

// BitTwiddling
#include "bithacks.h"

// HyperDisk
#include "shard.h"
#include "shard_constants.h"
#include "shard_snapshot.h"

e::intrusive_ptr<hyperdisk::shard>
hyperdisk :: shard :: create(const po6::pathname& filename)
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
    if (ftruncate(fd.get(), FILE_SIZE) < 0)
    {
        throw po6::error(errno);
    }

    // Create the shard object.
    e::intrusive_ptr<shard> ret = new shard(&fd, filename);

    // Move the filename.
    if (rename(tmp.get(), filename.get()) < 0)
    {
        throw po6::error(errno);
    }

    g_unlink.dismiss();
    return ret;
}

hyperdisk::returncode
hyperdisk :: shard :: get(const e::buffer& key,
                          uint64_t key_hash,
                          std::vector<e::buffer>* value,
                          uint64_t* version)
{
    size_t entry;

    if (!find_bucket(key, key_hash, &entry))
    {
        return NOTFOUND;
    }

    const uint64_t hash_entry = m_hash_table[entry];
    const uint32_t offset = hash_entry & OFFSETMASK;

    if (offset == 0 || offset == OFFSETMASK)
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

hyperdisk::returncode
hyperdisk :: shard :: put(const e::buffer& key,
                          uint64_t key_hash,
                          const std::vector<e::buffer>& value,
                          const std::vector<uint64_t>& value_hashes,
                          uint64_t version)
{
    size_t required = data_size(key, value);

    if (required + m_data_offset > FILE_SIZE)
    {
        return DATAFULL;
    }

    if (m_search_offset == SEARCH_INDEX_ENTRIES - 1)
    {
        return SEARCHFULL;
    }

    // Find the bucket.
    size_t entry;
    bool overwrite = find_bucket(key, key_hash, &entry);

    if (entry == HASH_TABLE_SIZE)
    {
        return HASHFULL;
    }

    // Values to pack.
    uint32_t key_size = key.size();
    uint16_t value_arity = value.size();

    // Pack the values on disk.
    uint32_t curr_offset = m_data_offset;
    memmove(m_data + curr_offset, &version, sizeof(version));
    curr_offset += sizeof(version);
    memmove(m_data + curr_offset, &key_size, sizeof(key_size));
    curr_offset += sizeof(key_size);
    memmove(m_data + curr_offset, key.get(), key.size());
    curr_offset += key.size();
    memmove(m_data + curr_offset, &value_arity, sizeof(value_arity));
    curr_offset += sizeof(value_arity);

    for (size_t i = 0; i < value.size(); ++i)
    {
        uint32_t size = value[i].size();
        memmove(m_data + curr_offset, &size, sizeof(size));
        curr_offset += sizeof(size);
        memmove(m_data + curr_offset, value[i].get(), value[i].size());
        curr_offset += value[i].size();
    }

    // Invalidate anything pointing to the old version.
    if (overwrite)
    {
        const uint64_t hash_entry = m_hash_table[entry];
        const uint32_t invalidated_offset = hash_entry & OFFSETMASK;
        invalidate_search_index(invalidated_offset, m_data_offset);
    }

    // Insert into the search index.
    const uint64_t hashcode = (lower_interlace(value_hashes) << 32)
                            | (key_hash & 0xffffffffULL);
    const uint64_t dataoffset = m_data_offset;
    m_search_index[m_search_offset * 2] = hashcode;
    m_search_index[m_search_offset * 2 + 1] = dataoffset;

    // Insert into the hash table.
    uint64_t new_hash_entry = key_hash;
    new_hash_entry <<= 25;
    new_hash_entry |= m_data_offset;
    m_hash_table[entry] = new_hash_entry;

    // Update the offsets
    ++m_search_offset;
    m_data_offset = (curr_offset + 7) & ~7; // Keep everything 8-byte aligned.
    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: shard :: del(const e::buffer& key,
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

    if (m_data_offset + sizeof(uint64_t) > FILE_SIZE)
    {
        return DATAFULL;
    }

    const uint32_t offset = m_hash_table[entry] & OFFSETMASK;
    assert(offset != 0 && offset != OFFSETMASK);
    invalidate_search_index(offset, m_data_offset);
    m_data_offset += sizeof(uint64_t);
    m_hash_table[entry] |= OFFSETMASK;
    return SUCCESS;
}

int
hyperdisk :: shard :: stale_space() const
{
    size_t stale = 0;
    uint32_t prev = m_search_index[1] & 0xffffffffUL;
    uint32_t cur;
    size_t i;

    for (i = 0; i < SEARCH_INDEX_ENTRIES; ++i)
    {
        cur = m_search_index[2 * i + 1] & 0xffffffffUL;

        if (cur == 0)
        {
            break;
        }

        if (m_search_index[2 * i + 1] >> 32 > 0)
        {
            stale += cur - prev;
        }

        prev = cur;
    }

    if (i == SEARCH_INDEX_ENTRIES && m_search_index[2 * i + 1] >> 32 > 0)
    {
        stale += cur - prev;
    }

    return static_cast<double>(100 * stale) / DATA_SEGMENT_SIZE;
}

int
hyperdisk :: shard :: free_space() const
{
    return static_cast<double>(100 * (m_data_offset - INDEX_SEGMENT_SIZE))
           / DATA_SEGMENT_SIZE;
}

hyperdisk::returncode
hyperdisk :: shard :: async()
{
    if (msync(m_data, FILE_SIZE, MS_ASYNC) < 0)
    {
        return SYNCFAILED;
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: shard :: sync()
{
    if (msync(m_data, FILE_SIZE, MS_SYNC) < 0)
    {
        return SYNCFAILED;
    }

    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: shard :: drop()
{
    if (unlink(m_filename.get()) < 0)
    {
        return DROPFAILED;
    }

    return SUCCESS;
}

e::intrusive_ptr<hyperdisk::shard_snapshot>
hyperdisk :: shard :: make_snapshot()
{
    e::intrusive_ptr<shard> d = this;
    assert(m_ref >= 2);
    e::intrusive_ptr<shard_snapshot> ret = new shard_snapshot(d);
    return ret;
}

hyperdisk :: shard :: shard(po6::io::fd* fd, const po6::pathname& fn)
    : m_ref(0)
    , m_hash_table(NULL)
    , m_search_index(NULL)
    , m_data(NULL)
    , m_data_offset(INDEX_SEGMENT_SIZE)
    , m_search_offset(0)
    , m_filename(fn)
{
    m_data = static_cast<char*>(mmap(NULL, FILE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd->get(), 0));

    if (m_data == MAP_FAILED)
    {
        throw po6::error(errno);
    }

    m_hash_table = reinterpret_cast<uint64_t*>(m_data);
    m_search_index = reinterpret_cast<uint64_t*>(m_data + HASH_TABLE_SIZE);
}

hyperdisk :: shard :: ~shard()
                    throw ()
{
    munmap(m_data, FILE_SIZE);
}

size_t
hyperdisk :: shard :: data_size(const e::buffer& key,
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
hyperdisk :: shard :: data_version(uint32_t offset) const
{
    assert(((offset + 7) & ~7) == offset);
    return *reinterpret_cast<uint64_t*>(m_data + offset);
}

size_t
hyperdisk :: shard :: data_key_size(uint32_t offset) const
{
    assert(((offset + 7) & ~7) == offset);
    return *reinterpret_cast<uint32_t*>(m_data + offset + sizeof(uint64_t));
}

void
hyperdisk :: shard :: data_key(uint32_t offset,
                             size_t keysize,
                             e::buffer* key) const
{
    assert(((offset + 7) & ~7) == offset);
    uint32_t cur_offset = offset + sizeof(uint64_t) + sizeof(uint32_t);
    *key = e::buffer(m_data + cur_offset, keysize);
}

void
hyperdisk :: shard :: data_value(uint32_t offset,
                               size_t keysize,
                               std::vector<e::buffer>* value) const
{
    assert(((offset + 7) & ~7) == offset);
    uint32_t cur_offset = offset + sizeof(uint64_t) + sizeof(uint32_t) + keysize;
    uint16_t num_dims;
    memmove(&num_dims, m_data + cur_offset, sizeof(uint16_t));
    cur_offset += sizeof(uint16_t);
    value->clear();

    for (uint16_t i = 0; i < num_dims; ++i)
    {
        uint32_t size;
        memmove(&size, m_data + cur_offset, sizeof(size));
        cur_offset += sizeof(size);
        value->push_back(e::buffer());
        e::buffer buf(m_data + cur_offset, size);
        value->back().swap(buf);
        cur_offset += size;
    }
}

bool
hyperdisk :: shard :: find_bucket(const e::buffer& key,
                                uint64_t key_hash,
                                size_t* entry)
{
    // The first dead bucket.
    size_t dead = HASH_TABLE_ENTRIES;

    // The bucket/hash we want to use.
    *entry = HASH_INTO_TABLE(key_hash);
    key_hash <<= 25;

    for (size_t off = 0; off < HASH_TABLE_ENTRIES; ++off)
    {
        size_t bucket = HASH_INTO_TABLE(*entry + off);
        const uint64_t hash_entry = m_hash_table[bucket];
        const uint64_t hash = hash_entry & ~uint64_t(OFFSETMASK);
        const uint32_t offset = hash_entry & OFFSETMASK;

        if (key_hash == hash && offset == OFFSETMASK)
        {
            dead = bucket;
        }
        else if (key_hash == hash)
        {
            const size_t key_size = data_key_size(offset);

            if (key_size == key.size() && memcmp(m_data + data_key_offset(offset), key.get(), key_size) == 0)
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

        if (offset == OFFSETMASK && dead == HASH_TABLE_ENTRIES)
        {
            dead = bucket;
        }
    }

    *entry = dead;
    return false;
}

void
hyperdisk :: shard :: invalidate_search_index(uint32_t to_invalidate, uint32_t invalidate_with)
{
    int64_t low = 0;
    int64_t high = SEARCH_INDEX_ENTRIES - 1;

    while (low <= high)
    {
        int64_t mid = low + ((high - low) / 2);
        const uint32_t mid_offset = m_search_index[mid * 2 + 1] & 0xffffffffUL;

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
            uint64_t new_entry = invalidate_with;
            new_entry <<= 32;
            new_entry |= to_invalidate;
            m_search_index[mid * 2 + 1] |= new_entry;
            return;
        }
    }
}
