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

// HyperDex
#include <hyperdex/log.h>

hyperdex :: log :: log(std::tr1::shared_ptr<hyperdex::log::drain> d)
    : m_drain(d)
    , m_seqno(1)
    , m_head_lock()
    , m_tail_lock()
    , m_head(NULL)
    , m_tail(NULL)
    , m_flush_lock()
    , m_shutdown(false)
{
    m_head = m_tail = new node();
    int ref = m_head->inc();
    assert(ref == 1);
}

hyperdex :: log :: ~log()
{
    assert(m_shutdown);
    assert(m_head == m_tail);
    assert(m_head != NULL);

    delete m_head;
}

bool
hyperdex :: log :: append(std::auto_ptr<record> rec)
{
    if (m_shutdown)
    {
        return false;
    }

    m_tail_lock.lock();
    node* n = new node(m_seqno, rec);
    int ref = n->inc();
    assert(ref == 1);
    m_tail->next = n;
    m_tail = n;
    ++ m_seqno;
    m_tail_lock.unlock();
    return true;
}

void
hyperdex :: log :: flush()
{
    m_flush_lock.lock();
    m_head_lock.wrlock();
    node* pos = m_head;
    m_head_lock.unlock();
    node* end = append_empty();

    while (pos != end)
    {
        if (pos->seqno > 0 && m_drain)
        {
            m_drain->one(pos->seqno, pos->rec.get());
        }

        m_head_lock.wrlock();
        m_head = pos->next;
        m_head_lock.unlock();
        pos = step_list(pos);
    }

    m_flush_lock.unlock();
}

hyperdex :: log :: node*
hyperdex :: log :: append_empty()
{
    m_tail_lock.lock();
    node* n = new node();
    int ref = n->inc();
    assert(ref == 1);
    m_tail->next = n;
    m_tail = n;
    m_tail_lock.unlock();
    return n;
}

hyperdex :: log :: node*
hyperdex :: log :: get_head()
{
    m_head_lock.rdlock();
    node* ret = m_head;

    if (ret)
    {
        ret->inc();
    }

    m_head_lock.unlock();
    return ret;
}

hyperdex :: log :: node*
hyperdex :: log :: step_list(node* pos)
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
    }

    return next;
}

hyperdex :: log :: record :: record()
{
}

hyperdex :: log :: record :: ~record()
{
}

hyperdex :: log :: iterator :: iterator(hyperdex::log* l)
    : m_l(l)
    , m_n(l->get_head())
    , m_called(false)
{
    if (l->is_shutdown())
    {
        throw std::runtime_error("Cannot iterate over a shutdown log.");
    }
}

hyperdex :: log :: iterator :: ~iterator()
{
    while (m_n)
    {
        m_n = m_l->step_list(m_n);
    }
}

bool
hyperdex :: log :: iterator :: step()
{
    // Invariants which hold at the beginning of step:
    //  - The current node has been passed to the callback if and only if
    //    m_called.
    //  - m_n points to a valid list node.

    // Find the earliest node for which we have not called the callback and
    // which also has a non-zero sequence number.
    while (m_n->next && (m_called || m_n->seqno == 0))
    {
        m_called = false;
        m_n = m_l->step_list(m_n);
    }

    // If said node doesn't exist, return false
    if (m_called || m_n->seqno == 0)
    {
        return false;
    }

    // Otherwise call the iterator callback
    this->callback(m_n->seqno, m_n->rec.get());
    m_called = true;
    return true;
}

void
hyperdex :: log :: iterator :: callback(uint64_t, record*)
{
}

hyperdex :: log :: drain :: drain()
{
}

hyperdex :: log :: drain :: ~drain()
{
}

void
hyperdex :: log :: drain :: one(uint64_t, hyperdex::log::record*)
{
}
