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

// STL
#include <memory>

// Google Log
#include <glog/logging.h>

// e
#include <e/buffer.h>
#include <e/endian.h>

// HyperDex
#include "disk/disk.h"
#include "disk/disk_returncode.h"
#include "disk/search_tree.h"

using hyperdex::disk;
using hyperdex::disk_returncode;

bool tripped = false;

disk :: disk(const po6::pathname& prefix)
    : m_prefix(prefix)
    , m_log()
    , m_key_idx()
{
}

disk :: ~disk() throw ()
{
}

disk_returncode
disk :: open()
{
    size_t sz = strlen(m_prefix.get());
    std::vector<char> buf(sz + 5/*strlen('.log') + 1*/);
    memmove(&buf.front(), m_prefix.get(), sz);
    buf[sz + 0] = '.';
    buf[sz + 1] = 'l';
    buf[sz + 2] = 'o';
    buf[sz + 3] = 'g';
    buf[sz + 4] = '\0';

    switch (m_log.open(&buf.front()))
    {
        case append_only_log::SUCCESS:
            break;
        case append_only_log::NOT_FOUND:
        case append_only_log::CLOSED:
        case append_only_log::TOO_BIG:
        case append_only_log::OPEN_FAIL:
        case append_only_log::READ_FAIL:
        case append_only_log::WRITE_FAIL:
        case append_only_log::SYNC_FAIL:
        case append_only_log::CLOSE_FAIL:
        case append_only_log::CORRUPT_STATE:
        case append_only_log::IDS_EXHAUSTED:
        default:
            return DISK_FATAL_ERROR;
    }

    return DISK_SUCCESS;
}

disk_returncode
disk :: close()
{
    m_log.close();
    return DISK_SUCCESS;
}

disk_returncode
disk :: destroy()
{
    abort(); // XXX
}

disk_returncode
disk :: get(const e::slice& key, uint64_t key_hash,
            std::vector<e::slice>* value,
            uint64_t* version,
            disk_reference* ref)
{
    bool consistent_read = false;

    while (!consistent_read)
    {
        // We assume we'll make a consistent read, i.e., it will be free from
        // race conditions.  If the cuckoo_index refers to log ids that return
        // not found, then we are doing a racy read.  We need to retry in that
        // case.
        consistent_read = true;
        std::vector<uint64_t> ids;
        cuckoo_returncode rc = m_key_idx.lookup(key_hash, &ids);

        switch (rc)
        {
            case CUCKOO_SUCCESS:
                break;
            case CUCKOO_NOT_FOUND:
                return DISK_NOT_FOUND;
            case CUCKOO_FULL:
            default:
                abort();
        }

        std::sort(ids.begin(), ids.end());

        for (ssize_t i = ids.size() - 1; i >= 0; --i)
        {
            std::auto_ptr<e::buffer> data;
            append_only_log::returncode lrc = m_log.lookup(ids[i], 0, 0, &data);

            switch (lrc)
            {
                case append_only_log::SUCCESS:
                    break;
                case append_only_log::NOT_FOUND:
                    consistent_read = false;
                    continue;
                case append_only_log::CLOSED:
                case append_only_log::TOO_BIG:
                case append_only_log::OPEN_FAIL:
                case append_only_log::READ_FAIL:
                case append_only_log::WRITE_FAIL:
                case append_only_log::SYNC_FAIL:
                case append_only_log::CLOSE_FAIL:
                case append_only_log::CORRUPT_STATE:
                case append_only_log::IDS_EXHAUSTED:
                default:
                    LOG(ERROR) << "the append only log at " << m_prefix
                               << " returned " << lrc << "; operations will fail";
                    return DISK_FATAL_ERROR;
            }

            e::slice log_key;
            std::vector<e::slice> log_val;
            uint64_t log_ver;
            disk_returncode drc = parse_log_entry(data.get(), &log_key, &log_val, &log_ver);

            switch (drc)
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
                    LOG(ERROR) << "could not parse log entry at " << m_prefix
                               << " because the parser failed with " << drc << "; this op will fail";
                    return drc;
            }

            if (log_key == key)
            {
                ref->data = data;
                *value = log_val;
                *version = log_ver;
                return DISK_SUCCESS;
            }
        }
    }

    return DISK_NOT_FOUND;
}

// We don't need to do the funny "consistent_read" thing for writes as we assume
// that the caller provides mutual exclusion between writes to the same key.

disk_returncode
disk :: put(const e::slice& key, uint64_t key_hash,
            const std::vector<e::slice>& value, uint64_t version)
{
    std::vector<uint64_t> ids;
    cuckoo_returncode rc = m_key_idx.lookup(key_hash, &ids);

    switch (rc)
    {
        case CUCKOO_SUCCESS:
            break;
        case CUCKOO_NOT_FOUND:
            break;
        case CUCKOO_FULL:
        default:
            abort();
    }

    uint64_t overwrite = 0;
    std::sort(ids.begin(), ids.end());

    for (ssize_t i = ids.size() - 1; i >= 0; --i)
    {
        std::auto_ptr<e::buffer> data;
        append_only_log::returncode lrc = m_log.lookup(ids[i], 0, 0, &data);

        switch (lrc)
        {
            case append_only_log::SUCCESS:
                break;
            case append_only_log::NOT_FOUND:
                continue;
            case append_only_log::CLOSED:
            case append_only_log::TOO_BIG:
            case append_only_log::OPEN_FAIL:
            case append_only_log::READ_FAIL:
            case append_only_log::WRITE_FAIL:
            case append_only_log::SYNC_FAIL:
            case append_only_log::CLOSE_FAIL:
            case append_only_log::CORRUPT_STATE:
            case append_only_log::IDS_EXHAUSTED:
            default:
                LOG(ERROR) << "the append only log at " << m_prefix
                           << " returned " << lrc << "; operations will fail";
                return DISK_FATAL_ERROR;
        }

        e::slice log_key;
        std::vector<e::slice> log_val;
        uint64_t log_ver;
        disk_returncode drc = parse_log_entry(data.get(), &log_key, &log_val, &log_ver);

        switch (drc)
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
                LOG(ERROR) << "could not parse log entry at " << m_prefix
                           << " because the parser failed with " << drc << "; this op will fail";
                return DISK_FATAL_ERROR;
        }

        if (log_key == key)
        {
            overwrite = ids[i];
            break;
        }
    }

    // Pack the buffer to write to disk
    size_t sz = sizeof(uint64_t)
              + sizeof(uint16_t)
              + sizeof(uint32_t)
              + key.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        sz += sizeof(uint32_t) + value[i].size();
    }

    std::vector<uint8_t> buf(sz);
    uint8_t* ptr = &buf.front();
    ptr = e::pack64be(version, ptr);
    ptr = e::pack16be(value.size(), ptr);
    ptr = e::pack32be(key.size(), ptr);
    memmove(ptr, key.data(), key.size());
    ptr += key.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        ptr = e::pack32be(value[i].size(), ptr);
        memmove(ptr, value[i].data(), value[i].size());
        ptr += value[i].size();
    }

    // append to log
    uint64_t log_id;
    append_only_log::returncode lrc = m_log.append(&buf.front(), buf.size(), &log_id);

    switch (lrc)
    {
        case append_only_log::SUCCESS:
            break;
        case append_only_log::NOT_FOUND:
        case append_only_log::CLOSED:
        case append_only_log::TOO_BIG:
        case append_only_log::OPEN_FAIL:
        case append_only_log::READ_FAIL:
        case append_only_log::WRITE_FAIL:
        case append_only_log::SYNC_FAIL:
        case append_only_log::CLOSE_FAIL:
        case append_only_log::CORRUPT_STATE:
        case append_only_log::IDS_EXHAUSTED:
        default:
            LOG(ERROR) << "the append only log at " << m_prefix
                       << " returned " << lrc << "; operations will fail";
            return DISK_FATAL_ERROR;
    }

    cuckoo_returncode crc = m_key_idx.insert(key_hash, log_id);

    switch (crc)
    {
        case CUCKOO_SUCCESS:
            break;
        case CUCKOO_NOT_FOUND:
        case CUCKOO_FULL:
        default:
            LOG(ERROR) << "could not insert key " << key_hash << " to " << m_prefix
                       << " because the key index failed with " << crc << "; this op will fail";
            return DISK_FATAL_ERROR;
    }

    if (overwrite > 0)
    {
        crc = m_key_idx.remove(key_hash, overwrite);

        switch (crc)
        {
            case CUCKOO_SUCCESS:
                break;
            case CUCKOO_NOT_FOUND:
            case CUCKOO_FULL:
            default:
                LOG(ERROR) << "could not remove old version of key " << key_hash << " from " << m_prefix
                           << " because the remove operation failed with " << crc << "; this op will fail";
                return DISK_FATAL_ERROR;
        }
    }

    // XXX insert to search_tree
    return DISK_SUCCESS;
}

disk_returncode
disk :: del(const e::slice& key, uint64_t key_hash)
{
    std::vector<uint64_t> ids;
    cuckoo_returncode rc = m_key_idx.lookup(key_hash, &ids);

    switch (rc)
    {
        case CUCKOO_SUCCESS:
            break;
        case CUCKOO_NOT_FOUND:
            return DISK_NOT_FOUND;
        case CUCKOO_FULL:
        default:
            abort();
    }

    std::sort(ids.begin(), ids.end());

    for (ssize_t i = ids.size() - 1; i >= 0; --i)
    {
        std::auto_ptr<e::buffer> data;
        append_only_log::returncode lrc = m_log.lookup(ids[i], 0, 0, &data);

        switch (lrc)
        {
            case append_only_log::SUCCESS:
                break;
            case append_only_log::NOT_FOUND:
                continue;
            case append_only_log::CLOSED:
            case append_only_log::TOO_BIG:
            case append_only_log::OPEN_FAIL:
            case append_only_log::READ_FAIL:
            case append_only_log::WRITE_FAIL:
            case append_only_log::SYNC_FAIL:
            case append_only_log::CLOSE_FAIL:
            case append_only_log::CORRUPT_STATE:
            case append_only_log::IDS_EXHAUSTED:
            default:
                LOG(ERROR) << "the append only log at " << m_prefix
                           << " returned " << lrc << "; operations will fail";
                return DISK_FATAL_ERROR;
        }

        e::slice log_key;
        std::vector<e::slice> log_val;
        uint64_t log_ver;
        disk_returncode drc = parse_log_entry(data.get(), &log_key, &log_val, &log_ver);

        switch (drc)
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
                LOG(ERROR) << "could not parse log entry at " << m_prefix
                           << " because the parser failed with " << drc << "; this op will fail";
                return DISK_FATAL_ERROR;
        }

        if (log_key == key)
        {
            uint64_t remove_id;
            uint8_t buf[sizeof(uint8_t) + sizeof(uint64_t)];
            buf[0] = 0;
            e::pack64be(ids[i], buf + sizeof(uint8_t));
            lrc = m_log.append(buf, sizeof(buf), &remove_id);

            switch (lrc)
            {
                case append_only_log::SUCCESS:
                    break;
                case append_only_log::NOT_FOUND:
                case append_only_log::CLOSED:
                case append_only_log::TOO_BIG:
                case append_only_log::OPEN_FAIL:
                case append_only_log::READ_FAIL:
                case append_only_log::WRITE_FAIL:
                case append_only_log::SYNC_FAIL:
                case append_only_log::CLOSE_FAIL:
                case append_only_log::CORRUPT_STATE:
                case append_only_log::IDS_EXHAUSTED:
                default:
                    LOG(ERROR) << "the append only log at " << m_prefix
                               << " returned " << lrc << "; operations will fail";
                    return DISK_FATAL_ERROR;
            }

            cuckoo_returncode crc = m_key_idx.remove(key_hash, ids[i]);

            switch (crc)
            {
                case CUCKOO_SUCCESS:
                    break;
                case CUCKOO_NOT_FOUND:
                    LOG(WARNING) << "tried removing from the key index at " << m_prefix
                                 << " but the entry was already gone";
                    break;
                case CUCKOO_FULL:
                default:
                    LOG(ERROR) << "could not remove key " << key_hash << " from " << m_prefix
                               << " because the key index failed with " << crc << "; this op will fail";
                    return DISK_FATAL_ERROR;
            }

            // XXX remove from search_tree
            return DISK_SUCCESS;
        }
    }

    return DISK_NOT_FOUND;
}

disk_returncode
disk :: parse_log_entry(e::buffer* data,
                        e::slice* key,
                        std::vector<e::slice>* value,
                        uint64_t* version)
{
    uint16_t attrs;
    e::buffer::unpacker up = data->unpack_from(0);
    up = up >> *version >> attrs >> *key;
    value->clear();

    for (size_t i = 0; i < attrs; ++i)
    {
        e::slice s;
        up = up >> s;
        value->push_back(s);
    }

    if (up.error())
    {
        LOG(ERROR) << "data->hex() " << data->hex();
    }

    return up.error() ? DISK_CORRUPT : DISK_SUCCESS;
}
