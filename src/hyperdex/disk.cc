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

// XXX It might be a good idea to add bounds checking in here to prevent
// corrupted files from causing us to access out-of-bounds memory.

// Where possible, we use pointers instead of memmove, but if alignment is not
// guaranteed, memmove is preferred.

#define HASH_TABLE_ENTRIES 262144
#define HASH_TABLE_SIZE (HASH_TABLE_ENTRIES * 8)
#define SEARCH_INDEX_ENTRIES (HASH_TABLE_ENTRIES * 4)
#define SEARCH_INDEX_SIZE (SEARCH_INDEX_ENTRIES * 12)
#define INDEX_SEGMENT_SIZE (HASH_TABLE_SIZE + SEARCH_INDEX_SIZE)
#define DATA_SEGMENT_SIZE 1073741824
#define TOTAL_FILE_SIZE (INDEX_SEGMENT_SIZE + DATA_SEGMENT_SIZE)
#define INIT_BLOCK_SIZE 16384
#define INIT_ITERATIONS (TOTAL_FILE_SIZE / INIT_BLOCK_SIZE)

hyperdex :: disk :: disk(const char* filename)
    : m_base(NULL)
    , m_offset(INDEX_SEGMENT_SIZE)
    , m_search(0)
{
    po6::io::fd fd(open(filename, O_RDWR, S_IRUSR|S_IWUSR));

    if (fd.get() < 0)
    {
        throw po6::error(errno);
    }

    struct stat buf;

    if (fstat(fd.get(), &buf) < 0)
    {
        throw po6::error(errno);
    }

    if (buf.st_size < TOTAL_FILE_SIZE)
    {
        throw std::runtime_error("File is too small.");
    }

    m_base = static_cast<char*>(mmap(NULL, TOTAL_FILE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd.get(), 0));

    if (m_base == MAP_FAILED
        || madvise(m_base, HASH_TABLE_SIZE, MADV_WILLNEED) < 0
        || madvise(m_base + HASH_TABLE_SIZE, SEARCH_INDEX_SIZE, MADV_SEQUENTIAL) < 0
        || madvise(m_base + INDEX_SEGMENT_SIZE, DATA_SEGMENT_SIZE, MADV_RANDOM) < 0)
    {
        int saved = errno;

        if (m_base != MAP_FAILED)
        {
            if (munmap(m_base, TOTAL_FILE_SIZE) < 0)
            {
                PLOG(WARNING) << "Could not mmap disk";
            }
        }

        throw po6::error(saved);
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

hyperdex :: result_t
hyperdex :: disk :: get(const e::buffer& key,
                        uint64_t key_hash,
                        std::vector<e::buffer>* value,
                        uint64_t* version)
{
    uint32_t* hash;
    uint32_t* offset;

    if (!find_bucket_for_key(key, key_hash, &hash, &offset))
    {
        return NOTFOUND;
    }

    if (!offset || *offset == 0 || *offset == UINT32_MAX)
    {
        return NOTFOUND;
    }

    // Load the version.
    uint32_t curr_offset = *offset;
    *version = *reinterpret_cast<uint64_t*>(m_base + curr_offset);
    // Fast-forward past the key (checked by find_bucket_for_key).
    curr_offset += sizeof(*version) + sizeof(uint32_t) + key.size();
    // Load the value.
    value->clear();
    uint16_t num_values;
    memmove(&num_values, m_base + curr_offset, sizeof(num_values));
    curr_offset += sizeof(num_values);

    for (uint16_t i = 0; i < num_values; ++i)
    {
        uint32_t size;
        memmove(&size, m_base + curr_offset, sizeof(size));
        curr_offset += sizeof(size);
        value->push_back(e::buffer());
        e::buffer buf(m_base + curr_offset, size);
        value->back().swap(buf);
        curr_offset += size;
    }

    return SUCCESS;
}

hyperdex :: result_t
hyperdex :: disk :: put(const e::buffer& key,
                        uint64_t key_hash,
                        const std::vector<e::buffer>& value,
                        const std::vector<uint64_t>& value_hashes,
                        uint64_t version)
{
    // Figure out if the PUT will fit in memory.
    uint64_t hypothetical_size = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint16_t) + key.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        hypothetical_size += sizeof(uint32_t) + value[i].size();
    }

    if (hypothetical_size + m_offset > TOTAL_FILE_SIZE)
    {
        LOG(INFO) << "Put failed because disk is full.";
        return ERROR;
    }

    if (m_search == SEARCH_INDEX_ENTRIES - 1)
    {
        LOG(INFO) << "Put failed because disk is full.";
        return ERROR;
    }

    // Find the bucket.
    uint32_t* hash;
    uint32_t* offset;

    if (!find_bucket_for_key(key, key_hash, &hash, &offset))
    {
        LOG(INFO) << "Put failed because index is full.";
        return ERROR;
    }

    // We have our index position.
    uint32_t curr_offset = m_offset;
    *reinterpret_cast<uint64_t*>(m_base + curr_offset) = version;
    curr_offset += sizeof(version);
    uint32_t key_size = key.size();
    *reinterpret_cast<uint32_t*>(m_base + curr_offset) = key_size;
    curr_offset += sizeof(key_size);
    memmove(m_base + curr_offset, key.get(), key_size);
    curr_offset += key_size;

    for (size_t i = 0; i < value.size(); ++i)
    {
        uint32_t size = value[i].size();
        memmove(m_base + curr_offset, &size, sizeof(size));
        curr_offset += sizeof(size);
        memmove(m_base + curr_offset, value[i].get(), size);
        curr_offset += size;
    }

    // Invalidate anything point to the old version.  Update the offset to point
    // to the new version.  Update the search index.
    invalidate_search_index(*offset);
    *reinterpret_cast<uint32_t*>(m_base + m_search * 12) =
        0xffffffff & interlace(value_hashes);
    *reinterpret_cast<uint32_t*>(m_base + m_search * 12 + sizeof(uint32_t)) = m_offset;
    *reinterpret_cast<uint32_t*>(m_base + m_search * 12 + sizeof(uint32_t) * 2) = 0;
    ++m_search;
    *offset = m_offset;
    m_offset = curr_offset;
    return SUCCESS;
}

hyperdex :: result_t
hyperdex :: disk :: del(const e::buffer& key,
                        uint64_t key_hash)
{
    // Find the bucket.
    uint32_t* hash;
    uint32_t* offset;

    if (!find_bucket_for_key(key, key_hash, &hash, &offset))
    {
        LOG(INFO) << "Put failed because index is full.";
        return ERROR;
    }

    if (*offset != 0 && *offset != UINT32_MAX)
    {
        invalidate_search_index(*offset);
        *offset = UINT32_MAX;
        m_offset += sizeof(uint64_t);
        return SUCCESS;
    }

    return NOTFOUND;
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
hyperdex :: disk :: find_bucket_for_key(const e::buffer& key,
                                        uint64_t key_hash,
                                        uint32_t** hash,
                                        uint32_t** offset)
{
    // First "dead" bucket.
    uint32_t* dead_hash = NULL;
    uint32_t* dead_offset = NULL;

    // Figure out the bucket.
    uint64_t bucket = key_hash % HASH_TABLE_ENTRIES;
    uint32_t short_key_hash = 0xffffffff & key_hash;

    for (uint64_t entry = 0; entry < HASH_TABLE_ENTRIES; ++entry)
    {
        uint32_t* tmp_hash = reinterpret_cast<uint32_t*>(m_base + (bucket + entry % HASH_TABLE_ENTRIES) * 8);
        uint32_t* tmp_offset = reinterpret_cast<uint32_t*>(m_base + (bucket + entry % HASH_TABLE_ENTRIES) * 8 + 4);

        // If we've found an empty bucket.
        if (*tmp_offset == 0)
        {
            *hash = tmp_hash;
            *offset = tmp_offset;
            return true;
        }

        // If we've found our first dead bucket.
        if (*tmp_offset == UINT32_MAX && dead_hash == NULL)
        {
            dead_hash = tmp_hash;
            dead_offset = tmp_offset;
            continue;
        }

        if (*tmp_hash == short_key_hash)
        {
            uint64_t curr_offset = *tmp_offset + sizeof(uint64_t);
            uint32_t key_size = *reinterpret_cast<uint32_t*>(m_base + curr_offset);
            curr_offset += sizeof(key_size);

            // If the key doesn't match because of a hash collision.
            if (key_size != key.size() || memcmp(m_base + curr_offset, key.get(), key_size) != 0)
            {
                continue;
            }

            *hash = tmp_hash;
            *offset = tmp_offset;
            return true;
        }
    }

    *hash = dead_hash;
    *offset = dead_offset;
    return hash != NULL;
}

void
hyperdex :: disk :: invalidate_search_index(uint32_t to_invalidate)
{
    for (uint64_t entry = 0; entry < SEARCH_INDEX_ENTRIES; ++entry)
    {
        uint32_t* offset = reinterpret_cast<uint32_t*>(m_base + entry * 12 + sizeof(uint32_t));
        uint32_t* invalidator = reinterpret_cast<uint32_t*>(m_base + entry * 12 + sizeof(uint32_t));

        if (*offset == 0)
        {
            return;
        }
        else if (*offset == to_invalidate)
        {
            *invalidator = m_offset;
        }
    }
}

void
hyperdex :: zero_fill(const char* filename)
{
    po6::io::fd fd(open(filename, O_RDWR|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR));

    if (fd.get() < 0)
    {
        throw po6::error(errno);
    }

    char buf[INIT_BLOCK_SIZE];
    memset(buf, 0, sizeof(buf));

    for (uint64_t i = 0; i < INIT_ITERATIONS; ++i)
    {
        fd.xwrite(buf, sizeof(buf));
    }

    if (fsync(fd.get()) < 0)
    {
        throw po6::error(errno);
    }
}
