// Copyright (c) 2012, Robert Escriva
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
#include <cassert>
#include <cstdio>

// STL
#include <vector>

// e
#include <e/atomic.h>
#include <e/endian.h>

// append only log
#include "append_only_log.h"
#include "append_only_log_block.h"
#include "append_only_log_constants.h"
#include "append_only_log_segment.h"
#include "append_only_log_segment_list.h"
#include "append_only_log_writable_segment.h"

#ifdef AOLDEBUG
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif

append_only_log :: append_only_log()
    : m_id(0)
    , m_removed(0)
    , m_path(4, '\0')
    , m_protect()
    , m_segment_number(0)
    , m_block_number(0)
    , m_block_offset(0)
    , m_pre_write_sync(1)
    , m_post_write_sync(1)
    , m_segments(new segment_list())
    , m_unfinished_segment()
    , m_unfinished_index()
    , m_unfinished_block(get_empty_block())
{
    m_path[0] = 'l';
    m_path[1] = 'o';
    m_path[2] = 'g';
    m_path[3] = '\0';
    e::atomic::store_64_release(&m_pre_write_sync, 1);
    e::atomic::store_64_release(&m_post_write_sync, 1);
}

append_only_log :: ~append_only_log() throw ()
{
}

#define ADVANCE_LINE \
    eol = static_cast<const char*>(memchr(ptr, '\n', end - ptr)); \
    if (!eol) \
    { \
        return CORRUPT_STATE; \
    } \
    memmove(tmp, ptr, eol - ptr); \
    tmp[eol - ptr] = '\0'; \
    ptr = eol + 1;

append_only_log::returncode
append_only_log :: open(const char* prefix)
{
    po6::threads::spinlock::hold hold(&m_protect);
    m_path.resize(strlen(prefix) + 1);
    memmove(&m_path.front(), prefix, m_path.size());
    std::vector<char> name;
    path_state(&name);
    po6::io::fd fd(::open(&name.front(), O_RDONLY));
    std::vector<char> buf;
    const char* state = "id 1\nremoved 0\nsegment 0 1\nblock 0\noffset 0\n";
    size_t state_sz = strlen(state);

    if (fd.get() >= 0)
    {
        char tmp[64];
        ssize_t sz;

        while ((sz = fd.xread(tmp, 64)) > 0)
        {
            size_t orig = buf.size();
            buf.resize(orig + sz);
            memmove(&buf[orig], tmp, sz);
        }

        if (sz < 0)
        {
            return READ_FAIL;
        }

        state = &buf.front();
        state_sz = buf.size();
    }

    char tmp[64];
    const char* ptr = state;
    const char* end = ptr + state_sz;
    const char* eol;

    ADVANCE_LINE

    if (sscanf(tmp, "id %lu", &m_id) != 1)
    {
        return CORRUPT_STATE;
    }

    ADVANCE_LINE

    if (sscanf(tmp, "removed %lu", &m_removed) != 1)
    {
        return CORRUPT_STATE;
    }

    ADVANCE_LINE
    m_segments = new segment_list();
    m_unfinished_segment = NULL;

    do
    {
        uint64_t segno;
        uint64_t lowerbound;

        if (sscanf(tmp, "segment %lu %lu", &segno, &lowerbound) != 2)
        {
            return CORRUPT_STATE;
        }

        if (m_unfinished_segment && !m_unfinished_segment->sync())
        {
            return SYNC_FAIL;
        }

        std::vector<char> path;
        path_segment(&path, segno);
        m_unfinished_segment = new writable_segment();

        if (!m_unfinished_segment->open(&path.front()))
        {
            return OPEN_FAIL;
        }

        m_segments = m_segments->add(lowerbound, m_unfinished_segment.get());
        m_segment_number = std::max(m_segment_number, segno);
        ADVANCE_LINE
    }
    while (strncmp(tmp, "segment ", 8) == 0);

    if (sscanf(tmp, "block %lu", &m_block_number) != 1)
    {
        return CORRUPT_STATE;
    }

    ADVANCE_LINE

    if (sscanf(tmp, "offset %lu", &m_block_offset) != 1)
    {
        return CORRUPT_STATE;
    }

    eol = static_cast<const char*>(memchr(ptr, '\n', end - ptr));

    if (eol)
    {
        return CORRUPT_STATE;
    }

    e::atomic::store_64_release(&m_pre_write_sync, m_id);
    e::atomic::store_64_release(&m_post_write_sync, m_id);
    m_unfinished_index = m_segments->get_segment(m_segment_number)->read_index();
    m_unfinished_block = new block();
    memmove(m_unfinished_block->data, m_segments->get_segment(m_segment_number)->read(m_block_number), BLOCK_SIZE);
    return SUCCESS;
}

append_only_log::returncode
append_only_log :: close()
{
    uint64_t id;

    {
        po6::threads::spinlock::hold hold(&m_protect);
        id = m_id;
        m_id = 0;
    }

    while (e::atomic::load_64_acquire(&m_post_write_sync) < id)
        ;

    std::vector<char> name(m_path);
    name.resize(name.size() + 6 /*strlen('.state')*/);
    memmove(&name.front() + m_path.size() - 1, ".state\0", 7);
    po6::io::fd fd(::open(&name.front(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));

    char buf[64];
    ssize_t sz;
    sz = sprintf(buf, "id %lu\n", id);

    if (fd.xwrite(buf, sz) != sz)
    {
        return WRITE_FAIL;
    }

    sz = sprintf(buf, "removed %lu\n", m_removed);

    if (fd.xwrite(buf, sz) != sz)
    {
        return WRITE_FAIL;
    }

    for (size_t i = 0; i <= m_segment_number; ++i)
    {
        m_segments->sync(i);
        sz = sprintf(buf, "segment %lu %lu\n", i, m_segments->get_lower_bound(i));

        if (fd.xwrite(buf, sz) != sz)
        {
            return WRITE_FAIL;
        }
    }

    sz = sprintf(buf, "block %lu\n", m_block_number);

    if (fd.xwrite(buf, sz) != sz)
    {
        return WRITE_FAIL;
    }

    sz = sprintf(buf, "offset %lu\n", m_block_offset);

    if (fd.xwrite(buf, sz) != sz)
    {
        return WRITE_FAIL;
    }

    if (!m_unfinished_segment->write_index(m_unfinished_index.get()))
    {
        return WRITE_FAIL;
    }

    if (!m_unfinished_segment->write(m_block_number, m_unfinished_block.get()))
    {
        return WRITE_FAIL;
    }

    if (!m_unfinished_segment->sync())
    {
        return SYNC_FAIL;
    }

    return SUCCESS;
}

append_only_log::returncode
append_only_log :: append(const uint8_t* data, size_t data_sz, uint64_t* id)
{
    DEBUG << "BEGIN WRITE" << std::endl;

    if (m_id == 0)
    {
        return CLOSED;
    }

    if (data_sz > MAX_WRITE_SIZE)
    {
        return TOO_BIG;
    }

    // We'll need this many blocks in the worst case.
    size_t upper_bound_blocks = (data_sz + (BLOCK_SIZE - ENTRY_HEADER_SIZE - 1))
                             / (BLOCK_SIZE - ENTRY_HEADER_SIZE);
    std::vector<block_ptr> blocks;
    blocks.reserve(upper_bound_blocks + 1);
    blocks.push_back(block_ptr());

    for (size_t i = 0; i < upper_bound_blocks; ++i)
    {
        blocks.push_back(get_empty_block());
    }

    // Read the offsets, and record our starting point
    uint64_t start_segment_number;
    uint64_t start_block_number;
    uint64_t start_block_offset;
    uint64_t end_segment_number;
    uint64_t end_block_number;
    uint64_t end_block_offset;
    e::intrusive_ptr<writable_segment> seg1;
    e::intrusive_ptr<writable_segment> seg2;
    e::intrusive_ptr<block> idx1;
    e::intrusive_ptr<block> idx2;

    {
        // Hold the lock in this scope
        po6::threads::spinlock::hold hold(&m_protect);
        // Get the ID for this write
        *id = m_id;
        ++m_id;
        DEBUG << "write assigned id " << id << std::endl;
        // Preserve the start segment/block/offset information
        start_segment_number = m_segment_number;
        start_block_number = m_block_number;
        start_block_offset = m_block_offset;
        // Capture the last unfinished block
        blocks[0] = m_unfinished_block;
        // Update segment/block/offset information

        for (size_t sz = 0; sz < data_sz; )
        {
            size_t space_left = BLOCK_SIZE - m_block_offset - ENTRY_HEADER_SIZE;

            // Last block
            if (sz + space_left >= data_sz)
            {
                m_block_offset += ENTRY_HEADER_SIZE + (data_sz - sz);
                sz = data_sz;
            }
            else
            {
                m_block_offset += ENTRY_HEADER_SIZE + space_left;
                sz += space_left;
            }

            if (m_block_offset + ENTRY_HEADER_SIZE > BLOCK_SIZE)
            {
                m_block_offset = 0;
                m_block_number += 1;
            }
        }

        m_segment_number += m_block_number / BLOCKS_PER_SEGMENT;
        m_block_number = m_block_number % BLOCKS_PER_SEGMENT;
        // Preserve the start segment/block/offset information
        end_segment_number = m_segment_number;
        end_block_number = m_block_number;
        end_block_offset = m_block_offset;
        // Set another block to be the next unfinished block
        size_t actual_blocks = (end_segment_number * BLOCKS_PER_SEGMENT + end_block_number) -
                               (start_segment_number * BLOCKS_PER_SEGMENT + start_block_number);
        assert(actual_blocks + 1 == blocks.size() ||
               actual_blocks + 2 == blocks.size());

        if (actual_blocks + 2 == blocks.size())
        {
            blocks.pop_back();
        }

        // If this is the first segment
        if (start_segment_number == 0 &&
            start_block_number == 0 &&
            start_block_offset == 0)
        {
            m_unfinished_segment = get_writable_segment(start_segment_number);
            m_unfinished_index = get_empty_block();
            memset(m_unfinished_index->data, 0, BLOCK_SIZE);
            e::pack64be(static_cast<int64_t>(*id) - 1, m_unfinished_index->data + sizeof(uint32_t));
            m_segments = m_segments->add(*id, m_unfinished_segment.get());
        }

        // Capture the first segment
        seg1 = m_unfinished_segment;
        idx1 = m_unfinished_index;

        // Open a new segment
        if (start_segment_number != end_segment_number)
        {
            m_unfinished_segment = get_writable_segment(end_segment_number);
            m_unfinished_index = get_empty_block();
            memset(m_unfinished_index->data, 0, BLOCK_SIZE);
            e::pack64be(static_cast<int64_t>(*id) - 1, m_unfinished_index->data + sizeof(uint32_t));
            m_segments = m_segments->add(*id, m_unfinished_segment.get());
        }

        // Capture the second segment
        seg2 = m_unfinished_segment;
        idx2 = m_unfinished_index;

        if (start_segment_number != end_segment_number ||
            start_block_number != end_block_number)
        {
            m_unfinished_block = blocks.back();
        }
    }

    // If you're using this 8.9 years later and doing 1M
    // writes-per-second-per-log I congratulate you.  Now deal with my
    // assumption!
    if (*id >= ID_UPPER_BOUND)
    {
        *id = ID_UPPER_BOUND;
        return IDS_EXHAUSTED;
    }

    // Check for error states
    if (!seg1 || !seg2)
    {
        return OPEN_FAIL;
    }

    size_t offset = start_block_offset;
    uint32_t chksum = 0xdeadbeefUL;
    uint16_t entry_sz = 0;
    uint8_t entry_type = blocks.size() > 1 ? TYPE_FIRST : TYPE_FULL;
    uint16_t id_upper = ((*id) >> 32) & UINT16_MAX;
    uint32_t id_lower = (*id) & UINT32_MAX;

    // For each block, copy
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        size_t sz = std::min(BLOCK_SIZE - offset - ENTRY_HEADER_SIZE, data_sz);
        assert(sz < UINT16_MAX);
        entry_sz = sz;
        DEBUG << "writing " << sz << " bytes to (seg,block)=(" << start_segment_number << "," << start_block_number << ") +" << i << " blocks at offset " << offset << std::endl;
        uint8_t* ptr = blocks[i]->data + offset;
        ptr = e::pack32be(chksum, ptr);
        ptr = e::pack16be(entry_sz, ptr);
        ptr = e::pack8be(entry_type, ptr);
        ptr = e::pack16be(id_upper, ptr);
        ptr = e::pack32be(id_lower, ptr);
        memmove(ptr, data, entry_sz);
        data += entry_sz;
        data_sz -= entry_sz;
        offset = 0;

        if (i + 1 >= blocks.size())
        {
            entry_type = TYPE_LAST;
        }
        else
        {
            entry_type = TYPE_MIDDLE;
        }
    }

    for (size_t i = 0; i < blocks.size(); ++i)
    {
        if ((i == 0 && start_block_offset == 0) ||
            (i + 1 == blocks.size() &&
             start_block_number != end_block_number &&
             end_block_offset != 0) ||
            (0 < i && i + 1 < blocks.size()))
        {
            block* b = idx1.get();
            size_t which = start_block_number + i;

            if (which >= BLOCKS_PER_SEGMENT)
            {
                b = idx2.get();
                which -= BLOCKS_PER_SEGMENT;
            }

            int64_t base;
            e::unpack64be(b->data + sizeof(uint32_t), &base);
            assert(*id - base < UINT32_MAX);
            uint32_t diff = *id - base;
            DEBUG << "store index " << base << " " << diff << " @ " << (sizeof(uint32_t) + sizeof(uint64_t) + which * sizeof(uint32_t)) << std::endl;
            e::pack32be(diff, b->data + sizeof(uint32_t) + sizeof(uint64_t) + which * sizeof(uint32_t));
        }
    }

    while (e::atomic::load_64_acquire(&m_pre_write_sync) < *id)
        ;
    e::atomic::increment_64_fullbarrier(&m_pre_write_sync, 1);

    returncode rc = SUCCESS;

    // For each block, write
    for (size_t i = 0; i < blocks.size() - 1; ++i)
    {
        writable_segment* ws = seg1.get();
        size_t which = start_block_number + i;

        if (which >= BLOCKS_PER_SEGMENT)
        {
            ws = seg2.get();
            which -= BLOCKS_PER_SEGMENT;
        }

        DEBUG << "writing block " << which << std::endl;
        if (!ws->write(which, blocks[i].get()) && rc == SUCCESS)
        {
            rc = WRITE_FAIL;
        }
    }

    // Need to write the index
    if (start_segment_number != end_segment_number)
    {
        if (!seg1->write_index(idx1.get()) && rc == SUCCESS)
        {
            rc = WRITE_FAIL;
        }
    }

    while (e::atomic::load_64_acquire(&m_post_write_sync) < *id)
        ;

    // Need to open a new segment
    if (start_segment_number != end_segment_number)
    {
        if (!seg1->sync() && rc == SUCCESS)
        {
            rc = SYNC_FAIL;
        }

        if (!seg1->close() && rc == SUCCESS)
        {
            rc = CLOSE_FAIL;
        }
    }

    e::atomic::increment_64_fullbarrier(&m_post_write_sync, 1);
    // Blocks will be implicitly cleaned up here by block_ptr.
    return rc;
}

append_only_log::returncode
append_only_log :: lookup(uint64_t id, size_t prefix, size_t suffix, std::auto_ptr<e::buffer>* data)
{
    DEBUG << "BEGIN LOOKUP " << id << std::endl;
    std::vector<uint8_t*> pieces;
    std::vector<e::intrusive_ptr<block> > blocks;
    std::vector<e::intrusive_ptr<segment> > segments;
    returncode rc = get_blocks(id, &pieces, &blocks, &segments, NULL);

    if (rc != SUCCESS)
    {
        return rc;
    }

    // Now that we have the blocks, we need to capture the data
    std::vector<uint8_t> backing;
    backing.resize(pieces.size() * (BLOCK_SIZE - ENTRY_HEADER_SIZE));
    uint8_t* write_ptr = &backing.front();
    rc = NOT_FOUND;

    // Read those blocks!
    for (size_t blocknum = 0; blocknum < pieces.size(); ++blocknum)
    {
        DEBUG << "read a block" << std::endl;
        const uint8_t* read_ptr = pieces[blocknum];
        uint32_t entry_chksum;
        uint16_t entry_sz;
        uint8_t entry_type;
        uint16_t entry_id_upper;
        uint32_t entry_id_lower;
        uint64_t entry_id;

        while (read_ptr <= pieces[blocknum] + BLOCK_SIZE - ENTRY_HEADER_SIZE)
        {
            read_ptr = e::unpack32be(read_ptr, &entry_chksum);
            read_ptr = e::unpack16be(read_ptr, &entry_sz);
            read_ptr = e::unpack8be(read_ptr, &entry_type);
            read_ptr = e::unpack16be(read_ptr, &entry_id_upper);
            read_ptr = e::unpack32be(read_ptr, &entry_id_lower);
            const uint8_t* copy_from = read_ptr;
            read_ptr += entry_sz;
            entry_id = entry_id_upper;
            entry_id <<= 32;
            entry_id |= entry_id_lower;

            if (entry_id < id)
            {
                continue;
            }

            if (entry_id > id || entry_id == 0)
            {
                blocknum = pieces.size();
                break;
            }

            if (entry_type == TYPE_REMOVED)
            {
                rc = NOT_FOUND;
                blocknum = pieces.size();
                break;
            }

            DEBUG << "found entry of size " << entry_sz << " at offset " << (read_ptr - entry_sz - ENTRY_HEADER_SIZE - pieces[blocknum]) << std::endl;
            DEBUG << "entry type " << (int) entry_type << std::endl;
            // XXX do something with the entry_chksum
            // XXX do something with the entry_type
            memmove(write_ptr, copy_from, entry_sz);
            write_ptr += entry_sz;
            rc = SUCCESS;
        }
    }

    size_t data_sz = write_ptr - &backing.front();
    size_t sz = prefix + data_sz + suffix;
    data->reset(e::buffer::create(sz));
    (*data)->resize(sz);
    memmove((*data)->data() + prefix, &backing.front(), data_sz);
    DEBUG << "returning " << data_sz << " bytes" << std::endl;
    return rc;
}

append_only_log::returncode
append_only_log :: remove(uint64_t id)
{
    DEBUG << "BEGIN REMOVE " << id << std::endl;
    uint64_t opid;
    std::vector<uint8_t*> pieces;
    std::vector<e::intrusive_ptr<block> > blocks;
    std::vector<e::intrusive_ptr<segment> > segments;
    returncode rc = get_blocks(id, &pieces, &blocks, &segments, &opid);

    if (rc != SUCCESS)
    {
        return rc;
    }

    rc = NOT_FOUND;

    // Scan those blocks!
    for (size_t blocknum = 0; blocknum < pieces.size(); ++blocknum)
    {
        DEBUG << "read a block" << std::endl;
        uint8_t* read_ptr = pieces[blocknum];
        uint32_t entry_chksum;
        uint16_t entry_sz;
        uint8_t entry_type;
        uint16_t entry_id_upper;
        uint32_t entry_id_lower;
        uint64_t entry_id;

        while (read_ptr <= pieces[blocknum] + BLOCK_SIZE - ENTRY_HEADER_SIZE)
        {
            read_ptr = const_cast<uint8_t*>(e::unpack32be(read_ptr, &entry_chksum));
            read_ptr = const_cast<uint8_t*>(e::unpack16be(read_ptr, &entry_sz));
            uint8_t* type_ptr = read_ptr;
            read_ptr = const_cast<uint8_t*>(e::unpack8be(read_ptr, &entry_type));
            read_ptr = const_cast<uint8_t*>(e::unpack16be(read_ptr, &entry_id_upper));
            read_ptr = const_cast<uint8_t*>(e::unpack32be(read_ptr, &entry_id_lower));
            read_ptr += entry_sz;
            entry_id = entry_id_upper;
            entry_id <<= 32;
            entry_id |= entry_id_lower;

            if (entry_id < id)
            {
                continue;
            }

            if (entry_id > id || entry_id == 0)
            {
                blocknum = pieces.size();
                break;
            }

            if (entry_type == TYPE_REMOVED)
            {
                rc = NOT_FOUND;
                continue;
            }

            DEBUG << "removing entry of size " << entry_sz << " at offset " << (read_ptr - entry_sz - ENTRY_HEADER_SIZE - pieces[blocknum]) << std::endl;
            *type_ptr = TYPE_REMOVED;
            // XXX do something with the entry_chksum
            rc = SUCCESS;
        }
    }

    // one to offset the id we added
    uint64_t increm = 1;

    if (rc == SUCCESS)
    {
        // one to offset the id we removed
        ++increm;
    }

    e::atomic::increment_64_fullbarrier(&m_removed, increm);
    e::atomic::increment_64_fullbarrier(&m_pre_write_sync, 1);
    e::atomic::increment_64_fullbarrier(&m_post_write_sync, 1);
    return rc;
}

append_only_log::returncode
append_only_log :: get_blocks(uint64_t getid,
                              std::vector<uint8_t*>* pieces,
                              std::vector<block_ptr>* blocks,
                              std::vector<segment_ptr>* segments,
                              uint64_t* opid)
{
    // See the latest possible position we could write to
    uint64_t highest_id;
    uint64_t highest_segment_number;
    uint64_t highest_block_number;
    uint64_t highest_block_offset;
    segment_list_ptr segments_list;
    block_ptr unfinished_index;
    block_ptr unfinished_block;

    // Figure out where we've written
    {
        // Hold the lock in this scope
        po6::threads::spinlock::hold hold(&m_protect);
        // Get the ID for this read
        highest_id = m_id;

        // If this is a mutating operation
        if (opid)
        {
            *opid = m_id;
            ++m_id;
        }

        // Preserve the start segment/block/offset information
        highest_segment_number = m_segment_number;
        highest_block_number = m_block_number;
        highest_block_offset = m_block_offset;
        // Grab the read structures
        segments_list = m_segments;
        unfinished_index = m_unfinished_index;
        unfinished_block = m_unfinished_block;
    }

    if (highest_id == 0)
    {
        return CLOSED;
    }

    // If it's not yet written
    if (highest_id <= getid || getid == 0)
    {
        return NOT_FOUND;
    }

    // Make sure writers have passed the ID we want to read
    while (e::atomic::load_64_acquire(&m_post_write_sync) < getid)
        ;

    // Make sure everyone else is up to the same page as us
    if (opid)
    {
        while (e::atomic::load_64_acquire(&m_post_write_sync) < getid)
            ;
        while (e::atomic::load_64_acquire(&m_post_write_sync) < getid)
            ;
    }

    uint64_t segno = 0;
    uint64_t segnum = 1;

    // Find the highest segment number such that the lower_bound on entries in
    // that segment is less than the id we are looking for.  We want to look for
    // LT rather than LE because it is possible that an item straddles two
    // segments when lower_bound == id.
    for (int64_t i = highest_segment_number; i >= 0; --i)
    {
        uint64_t lb = segments_list->get_lower_bound(i);

        if (lb == getid && i > 0)
        {
            ++segnum;
        }

        if (lb < getid)
        {
            segno = i;
            break;
        }
    }

    DEBUG << "segments " << segno << "+" << segnum << std::endl;
    DEBUG << "highest_id " << highest_id << std::endl;
    DEBUG << "highest_segment_number " << highest_segment_number << std::endl;
    DEBUG << "highest_block_number " << highest_block_number << std::endl;
    DEBUG << "highest_block_offset " << highest_block_offset << std::endl;

    // Read all relevant blocks
    for (size_t no = 0; no < segnum; ++no)
    {
        // Retrieve the index
        e::intrusive_ptr<block> index;
        segment* seg = segments_list->get_segment(segno + no);

        if (segno + no == highest_segment_number)
        {
            index = unfinished_index;
        }
        else
        {
            index = seg->read_index();
        }

        // Figure out a range of blocks to read
        uint64_t start_blockno = 0;
        uint64_t end_blockno = 0;
        const uint8_t* ptr = index->data + sizeof(uint32_t);
        int64_t base;
        uint32_t diff;
        ptr = e::unpack64be(ptr, &base);
        uint64_t j = 0;

        for (j = 0; j < BLOCKS_PER_SEGMENT; ++j)
        {
            ptr = e::unpack32be(ptr, &diff);
            //DEBUG << "retrieve index " << base << " " << diff << " @ " << (ptr - index->data) << std::endl;
            uint64_t lb = base + diff;
            //DEBUG << "base=" << base << " lb=" << lb << " id=" << id << " j=" << j << std::endl;
            //DEBUG << "adjust end_blockno from " << end_blockno << " to " << j << std::endl;
            end_blockno = j;

            if (lb > getid || (j > 0 && diff == 0))
            {
                //DEBUG << "stop adjusting" << std::endl;
                break;
            }
            else if (lb < getid)
            {
                //DEBUG << "adjust start_blockno from " << start_blockno << " to " << j << std::endl;
                start_blockno = j;
            }
            else
            {
                //DEBUG << "no adjustment" << std::endl;
            }
        }

        if (j == BLOCKS_PER_SEGMENT)
        {
            DEBUG << "ran off the end" << std::endl;
            end_blockno = BLOCKS_PER_SEGMENT;
        }

        DEBUG << "blockrange " << segno + segnum << " " << start_blockno << " " << end_blockno << std::endl;

        // Read each block in the range
        for (uint64_t k = start_blockno; k < end_blockno; ++k)
        {
            uint8_t* p;
            e::intrusive_ptr<block> b;

            if (segno + no == highest_segment_number &&
                k == highest_block_number)
            {
                DEBUG << "took block " << k << " from unfinished_block" << std::endl;
                p = unfinished_block->data;
                b = unfinished_block;
            }
            else
            {
                DEBUG << "took block " << k << " from segment" << std::endl;
                p = seg->read(k);
            }

            pieces->push_back(p);
            blocks->push_back(b);
        }
    }

    DEBUG << "blocks->size() " << blocks->size() << std::endl;
    return SUCCESS;
}

append_only_log::block_ptr
append_only_log :: get_empty_block()
{
    block_ptr b = new block();
    memset(b->data + BLOCK_SIZE - ENTRY_HEADER_SIZE, 0, ENTRY_HEADER_SIZE);
    return b;
}

append_only_log::writable_segment_ptr
append_only_log :: get_writable_segment(size_t segment_number)
{
    std::vector<char> buf;
    path_segment(&buf, segment_number);
    writable_segment_ptr ws = new writable_segment();

    if (ws->open(&buf.front()))
    {
        return ws;
    }
    else
    {
        return NULL;
    }
}

void
append_only_log :: path_state(std::vector<char>* out)
{
    out->resize(m_path.size() + 6 /*strlen('.state')*/);
    memmove(&out->front(), &m_path.front(), m_path.size());
    memmove(&out->front() + m_path.size() - 1, ".state\0", 7);
}

void
append_only_log :: path_segment(std::vector<char>* out, uint64_t segno)
{
    out->resize(20 /*len(2**64)*/ + 2 /*len('.\0')*/ + m_path.size(), '\0');
    sprintf(&out->front(), "%s.%lu", &m_path.front(), segno);
}
