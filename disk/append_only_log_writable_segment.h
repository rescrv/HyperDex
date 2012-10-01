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

#ifndef append_only_log_writable_segment_h_
#define append_only_log_writable_segment_h_

// POSIX
#include <sys/mman.h>

// append only log
#include "append_only_log.h"
#include "append_only_log_block.h"
#include "append_only_log_constants.h"

class append_only_log::writable_segment
{
    public:
        writable_segment();
        ~writable_segment() throw ();

    public:
        bool open(const char* filename);
        bool write(uint64_t which, block* b);
        bool write_index(block* b);
        bool sync();
        bool close();
        e::intrusive_ptr<segment> get_segment();

    private:
        friend class e::intrusive_ptr<writable_segment>;

    private:
        void inc();
        void dec();

    private:
        po6::io::fd m_fd;
        uint64_t m_ref;
};

inline
append_only_log :: writable_segment :: writable_segment()
    : m_fd()
    , m_ref(0)
{
}

inline
append_only_log :: writable_segment :: ~writable_segment() throw ()
{
}

inline bool
append_only_log :: writable_segment :: open(const char* filename)
{
    int fd = ::open(filename, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);

    if (fd < 0)
    {
        return false;
    }

    m_fd = fd;

    if (ftruncate(fd, SEGMENT_SIZE) < 0)
    {
        return false;
    }

    return true;
}

inline bool
append_only_log :: writable_segment :: write(uint64_t which, block* b)
{
    return pwrite(m_fd.get(), b->data, BLOCK_SIZE, (which + 1) * BLOCK_SIZE) == BLOCK_SIZE;
}

inline bool
append_only_log :: writable_segment :: write_index(block* b)
{
    return pwrite(m_fd.get(), b->data, BLOCK_SIZE, 0) == BLOCK_SIZE;
}

inline bool
append_only_log :: writable_segment :: sync()
{
    return ::fsync(m_fd.get()) == 0;
}

inline bool
append_only_log :: writable_segment :: close()
{
    return ::close(m_fd.get()) == 0;
}

inline e::intrusive_ptr<append_only_log::segment>
append_only_log :: writable_segment :: get_segment()
{
    e::intrusive_ptr<segment> s(new segment());
    void* base = mmap(NULL, SEGMENT_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, m_fd.get(), 0);

    if (base == MAP_FAILED)
    {
        return NULL;
    }

    s->set_mapping(base);
    return s;
}

inline void
append_only_log :: writable_segment :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

inline void
append_only_log :: writable_segment :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}

#endif // append_only_log_writable_segment_h_
