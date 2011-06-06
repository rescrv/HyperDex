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

// C
#include <cassert>
#include <cstring>

// STL
#include <stdexcept>
#include <string>

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/log.h>

hyperdex :: log :: log()
    : m_seqno(1)
    , m_head_lock()
    , m_tail_lock()
    , m_head(NULL)
    , m_tail(NULL)
    , m_flush_lock()
{
    m_head = m_tail = new node();
    m_head->seqno = 0;
}

hyperdex :: log :: ~log() throw ()
{
    while (m_head)
    {
        m_head = m_head->next;
    }
}

bool
hyperdex :: log :: append(uint64_t point,
                          const e::buffer& key,
                          uint64_t key_hash,
                          const std::vector<e::buffer>& value,
                          const std::vector<uint64_t>& value_hashes,
                          uint64_t version)
{
    e::intrusive_ptr<node> n = new node(point, key, key_hash, value, value_hashes, version);
    return common_append(n);
}

bool
hyperdex :: log :: append(uint64_t point,
                          const e::buffer& key,
                          uint64_t key_hash)
{
    e::intrusive_ptr<node> n = new node(point, key, key_hash);
    return common_append(n);
}

size_t
hyperdex :: log :: flush(std::tr1::function<void (op_t op,
                                                  uint64_t point,
                                                  const e::buffer& key,
                                                  uint64_t key_hash,
                                                  const std::vector<e::buffer>& value,
                                                  const std::vector<uint64_t>& value_hashes,
                                                  uint64_t version)> save_one)
{
    po6::threads::mutex::hold hold(&m_flush_lock);
    m_head_lock.wrlock();
    e::intrusive_ptr<node> pos = m_head;
    m_head_lock.unlock();
    e::intrusive_ptr<node> end = new node();
    end->seqno = 0;
    common_append(end, false);
    size_t flushed = 0;

    while (pos != end)
    {
        if (pos->seqno > 0)
        {
            save_one(pos->op, pos->point, pos->key, pos->key_hash,
                     pos->value, pos->value_hashes, pos->version);
        }

        m_head_lock.wrlock();
        m_head = m_head->next;
        pos = m_head;
        m_head_lock.unlock();
        ++ flushed;
    }

    return flushed - 1;
}

e::intrusive_ptr<hyperdex::log::node>
hyperdex :: log :: get_head()
{
    po6::threads::rwlock::rdhold hold(&m_head_lock);
    return m_head;
}

bool
hyperdex :: log :: common_append(e::intrusive_ptr<node> n, bool seqno)
{
    po6::threads::mutex::hold hold(&m_tail_lock);

    if (seqno)
    {
        n->seqno = m_seqno;
        ++m_seqno;
    }

    m_tail->next = n;
    m_tail = n;
    return true;
}

hyperdex :: log :: iterator :: iterator(hyperdex::log* l)
    : m_l(l)
    , m_n(l->get_head())
    , m_seen(false)
{
}

hyperdex :: log :: iterator :: ~iterator() throw ()
{
    while (m_n)
    {
        e::intrusive_ptr<node> tmp = m_n;
        m_n = m_n->next;
    }
}

bool
hyperdex :: log :: iterator :: advance()
{
    // - m_n should always point to a node.
    // - m_seen is set to false when advancing to a node, and true when it is
    //   impossible to advance.

    // Find the earliest node for which we have not called the callback and
    // which also has a non-zero sequence number.
    while (m_n->next && (m_seen || m_n->seqno == 0))
    {
        m_seen = false;
        e::intrusive_ptr<node> tmp;
        tmp = m_n;
        m_n = m_n->next;
    }

    // If said node doesn't exist, return false
    if (m_seen || m_n->seqno == 0)
    {
        return false;
    }

    m_seen = true;
    return true;
}
