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

// HyperDisk
#include <hyperdisk/log.h>

hyperdisk :: log :: log()
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

hyperdisk :: log :: ~log() throw ()
{
    release(m_head);
}

void
hyperdisk :: log :: append(uint64_t point,
                           const e::buffer& key,
                           uint64_t key_hash,
                           const std::vector<e::buffer>& value,
                           const std::vector<uint64_t>& value_hashes,
                           uint64_t version)
{
    std::auto_ptr<node> n(new node(point, key, key_hash, value, value_hashes, version));
    common_append(n);
}

void
hyperdisk :: log :: append(uint64_t point,
                           const e::buffer& key,
                           uint64_t key_hash)
{
    std::auto_ptr<node> n(new node(point, key, key_hash));
    common_append(n);
}

size_t
hyperdisk :: log :: flush(std::tr1::function<bool (bool has_value,
                                                   uint64_t point,
                                                   const e::buffer& key,
                                                   uint64_t key_hash,
                                                   const std::vector<e::buffer>& value,
                                                   const std::vector<uint64_t>& value_hashes,
                                                   uint64_t version)> save_one)
{
    po6::threads::mutex::hold hold_flush(&m_flush_lock);
    std::auto_ptr<node> end_(new node());
    node* end = end_.get();
    common_append(end_, false);
    size_t flushed = 0;

    node* head;

    // Get a reference to the head.
    {
        po6::threads::mutex::hold hold_head(&m_head_lock);
        head = m_head;
    }

    while (head != end)
    {
        if (head->real)
        {
            if (!save_one(head->has_value, head->point, head->key,
                          head->key_hash, head->value, head->value_hashes,
                          head->version))
            {
                return flushed;
            }

            ++flushed;
        }

        po6::threads::mutex::hold hold_head(&m_head_lock);
        step_list(&m_head);
        head = m_head;
    }

    return flushed;
}

hyperdisk::log::node*
hyperdisk :: log :: get_head()
{
    po6::threads::mutex::hold hold(&m_head_lock);
    node* ret = m_head;

    if (ret)
    {
        int ref = ret->inc();
        assert(ref >= 2);
    }

    return ret;
}

void
hyperdisk :: log :: step_list(node** pos)
{
    node* cur = *pos;
    node* next = cur->next;
    bool incr = false;
    int ref;

    if (next)
    {
        ref = next->inc();
        assert(ref >= 2);
        incr = true;
    }

    *pos = next;

    ref = cur->dec();
    assert(ref >= 0);

    if (ref == 0)
    {
        __sync_synchronize();

        if (cur->next && incr)
        {
            ref = cur->next->dec();
            assert(ref >= 1);
        }

        cur->next = NULL;
        delete cur;
    }
}

void
hyperdisk :: log :: release(node* pos)
{
// XXX This way is super slow compared to what we could do.  On the other hand,
// it's much easier to maintain.  This implementation requires iterating
// iterating the whole log, which we shouldn't have to do; on the other hand,
// the typical usage pattern of an iterator is to iterate until the end anyway,
// so this is not a big concern).
    while (pos && pos->next)
    {
        step_list(&pos);
    }

    po6::threads::mutex::hold hold(&m_tail_lock);

    while (pos)
    {
        step_list(&pos);
    }
}

void
hyperdisk :: log :: common_append(std::auto_ptr<node> n, bool real)
{
    po6::threads::mutex::hold hold(&m_tail_lock);
    n->real = real;
    int ref = n->inc();
    assert(ref == 1);
    m_tail->next = n.get();
    m_tail = n.get();
    n.release();
}

hyperdisk :: log :: iterator :: iterator(hyperdisk::log* l)
    : m_l(l)
    , m_n(l->get_head())
    , m_valid(true)
{
    valid();
}

hyperdisk :: log :: iterator :: iterator(const log::iterator& i)
    : m_l(i.m_l)
    , m_n(i.m_n)
    , m_valid(i.m_valid)
{
    if (m_n)
    {
        int ref = m_n->inc();
        assert(ref >= 2);
    }

    valid();
}

hyperdisk :: log :: iterator :: ~iterator() throw ()
{
    m_l->release(m_n);
}

bool
hyperdisk :: log :: iterator :: valid()
{
    while (m_n->next && (!m_valid || !m_n->real))
    {
        m_valid = true;
        m_l->step_list(&m_n);
    }

    return m_valid && m_n->real;
}

void
hyperdisk :: log :: iterator :: next()
{
    m_valid = false;
    valid();
}
