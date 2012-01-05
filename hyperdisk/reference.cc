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

// HyperDisk
#include "hyperdisk/hyperdisk/reference.h"
#include "hyperdisk/log_entry.h"
#include "hyperdisk/shard.h"

hyperdisk :: reference :: reference()
    : m_it()
    , m_shard()
{
}

hyperdisk :: reference :: reference(const reference& other)
    : m_it()
    , m_shard(other.m_shard)
{
    if (other.m_it.get())
    {
        m_it.reset(new e::locking_iterable_fifo<log_entry>::iterator(*other.m_it));
    }
}

hyperdisk :: reference :: ~reference() throw ()
{
}

void
hyperdisk :: reference :: set(const e::locking_iterable_fifo<log_entry>::iterator& it)
{
    m_it.reset(new e::locking_iterable_fifo<log_entry>::iterator(it));
}

void
hyperdisk :: reference :: set(const e::intrusive_ptr<shard>& shard)
{
    m_shard = shard;
}

hyperdisk::reference&
hyperdisk :: reference :: operator = (const reference& rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    if (rhs.m_it.get())
    {
        m_it.reset(new e::locking_iterable_fifo<log_entry>::iterator(*rhs.m_it));
    }
    else
    {
        m_it.reset();
    }

    m_shard = rhs.m_shard;
    return *this;
}
