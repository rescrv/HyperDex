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
#include "append_only_log_block.h"
#include "append_only_log_constants.h"
#include "append_only_log_segment.h"

append_only_log :: segment :: segment()
    : m_ref(0)
    , m_data(NULL)
{
}

append_only_log :: segment :: ~segment() throw ()
{
}

e::intrusive_ptr<append_only_log::block>
append_only_log :: segment :: read(uint64_t which)
{
    e::intrusive_ptr<block> b(new block());
    memmove(b->data, m_data + (which + 1) * BLOCK_SIZE, BLOCK_SIZE);
    return b;
}

e::intrusive_ptr<append_only_log::block>
append_only_log :: segment :: read_index()
{
    e::intrusive_ptr<block> b(new block());
    memmove(b->data, m_data, BLOCK_SIZE);
    return b;
}

void
append_only_log :: segment :: set_mapping(void* base)
{
    m_data = static_cast<uint8_t*>(base);
}

void
append_only_log :: segment :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
append_only_log :: segment :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}
