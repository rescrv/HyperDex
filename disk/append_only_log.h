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

#ifndef append_only_log_h_
#define append_only_log_h_

// C
#include <cstdlib>
#include <stdint.h>

// STL
#include <memory>
#include <vector>

// po6
#include <po6/io/fd.h>
#include <po6/threads/spinlock.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

class append_only_log
{
    public:
        class reference
        {
            public:
                reference() : m_data() {}
                ~reference() throw () {}

            private:
                friend class append_only_log;
                friend class e::intrusive_ptr<reference>;

            private:
                std::vector<uint8_t> m_data;
        };

        enum returncode
        {
            SUCCESS,
            NOT_FOUND,
            CLOSED,
            TOO_BIG,
            OPEN_FAIL,
            READ_FAIL,
            WRITE_FAIL,
            SYNC_FAIL,
            CLOSE_FAIL,
            CORRUPT_STATE,
            IDS_EXHAUSTED
        };

    public:
        append_only_log();
        ~append_only_log() throw ();

    public:
        returncode open(const char* prefix);
        returncode close();
        returncode append(const uint8_t* data, size_t data_sz, uint64_t* id);
        returncode lookup(uint64_t id, size_t prefix, size_t suffix, std::auto_ptr<e::buffer>* data);
        returncode remove(uint64_t id);

    private:
        struct block;
        struct segment;
        struct segment_list;
        struct writable_segment;
        typedef e::intrusive_ptr<block> block_ptr;
        typedef e::intrusive_ptr<segment> segment_ptr;
        typedef e::intrusive_ptr<segment_list> segment_list_ptr;
        typedef e::intrusive_ptr<writable_segment> writable_segment_ptr;

    private:
        append_only_log(const append_only_log&);

    private:
        returncode get_blocks(uint64_t getid,
                              std::vector<uint8_t*>* data,
                              std::vector<block_ptr>* blocks,
                              std::vector<segment_ptr>* segments,
                              uint64_t* opid);
        block_ptr get_empty_block();
        writable_segment_ptr get_writable_segment(size_t segment_number);
        void path_state(std::vector<char>* out);
        void path_segment(std::vector<char>* out, uint64_t segno);

    private:
        append_only_log& operator = (const append_only_log&);

    private:
        uint64_t m_id;
        uint64_t m_removed;
        std::vector<char> m_path;
        po6::threads::spinlock m_protect;
        uint64_t m_segment_number;
        uint64_t m_block_number;
        uint64_t m_block_offset;
        volatile uint64_t m_pre_write_sync;
        volatile uint64_t m_post_write_sync;
        segment_list_ptr m_segments;
        writable_segment_ptr m_unfinished_segment;
        block_ptr m_unfinished_index;
        block_ptr m_unfinished_block;
};

#endif // append_only_log_h_
