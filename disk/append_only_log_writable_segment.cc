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

// e
#include <e/atomic.h>

// append only log
#include "append_only_log_segment.h"
#include "append_only_log_writable_segment.h"

using hyperdex::append_only_log;

append_only_log :: writable_segment :: writable_segment()
    : m_fd()
    , m_ref(0)
{
}

append_only_log :: writable_segment :: ~writable_segment() throw ()
{
}

bool
append_only_log :: writable_segment :: open(const char* filename)
{
    int fd = ::open(filename, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);

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

bool
append_only_log :: writable_segment :: write(uint64_t which, block* b)
{
    return pwrite(m_fd.get(), b->data, BLOCK_SIZE, (which + 1) * BLOCK_SIZE) == BLOCK_SIZE;
}

bool
append_only_log :: writable_segment :: write_index(block* b)
{
    return pwrite(m_fd.get(), b->data, BLOCK_SIZE, 0) == BLOCK_SIZE;
}

bool
append_only_log :: writable_segment :: sync()
{
    return ::fsync(m_fd.get()) == 0;
}

bool
append_only_log :: writable_segment :: close()
{
    return ::close(m_fd.get()) == 0;
}

e::intrusive_ptr<append_only_log::segment>
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

void
append_only_log :: writable_segment :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
append_only_log :: writable_segment :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}
