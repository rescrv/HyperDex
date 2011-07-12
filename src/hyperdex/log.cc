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
    : m_head_lock()
    , m_tail_lock()
    , m_head(NULL)
    , m_tail(NULL)
    , m_flush_lock()
{
    m_head = m_tail = new node();
    int ref = m_head->inc();
    assert(ref == 1);
}

hyperdex :: log :: ~log() throw ()
{
    release(m_head);
}

bool
hyperdex :: log :: append(uint64_t point,
                          const e::buffer& key,
                          uint64_t key_hash,
                          const std::vector<e::buffer>& value,
                          const std::vector<uint64_t>& value_hashes,
                          uint64_t version)
{
    std::auto_ptr<node> n(new node(point, key, key_hash, value, value_hashes, version));
    return common_append(n);
}

bool
hyperdex :: log :: append(uint64_t point,
                          const e::buffer& key,
                          uint64_t key_hash)
{
    std::auto_ptr<node> n(new node(point, key, key_hash));
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
    po6::threads::mutex::hold hold_flush(&m_flush_lock);
    iterator cleanup(iterate());
    iterator flushing(iterate());
    std::auto_ptr<node> n(new node());
    node* end = n.get();
    common_append(n, false);
    size_t flushed = 0;

    // XXX This flushes past "end".  This is a performance problem, not a
    // correctness problem (as anyone who iterates the log, and refers to where
    // it flushes to must be able to replay the log anyway).
    for (; flushing.valid(); flushing.next())
    {
        save_one(flushing.op(), flushing.point(), flushing.key(),
                 flushing.key_hash(), flushing.value(), flushing.value_hashes(),
                 flushing.version());
    }

    po6::threads::rwlock::wrhold hold_head(&m_head_lock);

    while (m_head != end)
    {
        step_list(&m_head);
    }

    return flushed;
}

hyperdex::log::node*
hyperdex :: log :: get_head()
{
    po6::threads::rwlock::rdhold hold(&m_head_lock);
    node* ret = m_head;

    if (ret)
    {
        ret->inc();
    }

    return m_head;
}

void
hyperdex :: log :: step_list(node** pos)
{
    node* cur = *pos;
    node* next = cur->next;

    if (next)
    {
        next->inc();
    }

    *pos = next;

    int ref = cur->dec();

    if (ref == 0)
    {
        if (next)
        {
            int ref = next->dec();
            assert(ref > 0);
        }

        cur->next = NULL;
        delete cur;
    }
}

void
hyperdex :: log :: release(node* pos)
{
    while (pos)
    {
        node* next = pos->next;

        if (next)
        {
            next->inc();
        }

        int ref = pos->dec();

        if (ref == 0)
        {
            if (next)
            {
                next->dec();
            }

            pos->next = NULL;
            delete pos;
            pos = next;
        }
        else
        {
            break;
        }
    }
}

bool
hyperdex :: log :: common_append(std::auto_ptr<node> n, bool real)
{
    po6::threads::mutex::hold hold(&m_tail_lock);
    n->real = real;
    int ref = n->inc();
    assert(ref == 1);
    m_tail->next = n.get();
    m_tail = n.get();
    n.release();
    return true;
}

hyperdex :: log :: iterator :: iterator(hyperdex::log* l)
    : m_l(l)
    , m_n(l->get_head())
    , m_valid(true)
{
    valid();
}

hyperdex :: log :: iterator :: iterator(const log::iterator& i)
    : m_l(i.m_l)
    , m_n(i.m_n)
    , m_valid(i.m_valid)
{
    if (m_n)
    {
        m_n->inc();
    }

    valid();
}

hyperdex :: log :: iterator :: ~iterator() throw ()
{
    m_l->release(m_n);
}

bool
hyperdex :: log :: iterator :: valid()
{
    while (m_n->next && (!m_valid || !m_n->real))
    {
        m_valid = true;
        m_l->step_list(&m_n);
    }

    return m_valid && m_n->real;
}

void
hyperdex :: log :: iterator :: next()
{
    m_valid = false;
    valid();
}
