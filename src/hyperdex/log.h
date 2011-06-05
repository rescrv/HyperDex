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

#ifndef hyperdex_log_h_
#define hyperdex_log_h_

// C
#include <stdint.h>

// STL
#include <tr1/memory>

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/op_t.h>

namespace hyperdex
{

class log
{
    private:
        class node;

    public:
        class iterator
        {
            public:
                iterator(const iterator&);
                ~iterator() throw ();

            public:
                bool advance();

            public:
                op_t op() const { return m_n->op; }
                const e::buffer& key() const { return m_n->key; }
                const std::vector<e::buffer>& value() const { return m_n->value; }
                uint64_t version() const { return m_n->version; }

            private:
                friend class log;

            private:
                iterator(log* l);

            private:
                iterator& operator = (const iterator&);

            private:
                log* m_l;
                e::intrusive_ptr<node> m_n;
                bool m_seen;
        };

    public:
        log();
        ~log() throw ();

    public:
        iterator iterate() { return iterator(this); }
        bool append(uint64_t point, uint64_t point_mask, uint64_t version,
                    const e::buffer& key, const std::vector<e::buffer>& value);
        bool append(uint64_t point, uint64_t point_mask, const e::buffer& key);

    public:
        //void flush();

    public:
        //bool is_flushed() const throw () { return (m_head == m_tail); }

    private:
        friend class iterator;

    private:
        class node
        {
            public:
                node()
                    : seqno(0)
                    , next(NULL)
                    , op(DEL)
                    , point()
                    , point_mask()
                    , version()
                    , key()
                    , value()
                    , m_ref(0)
                {
                }

                node(uint64_t p,
                     uint64_t pm,
                     uint64_t ver,
                     const e::buffer& k,
                     const std::vector<e::buffer>& val)
                    : seqno(0)
                    , next(NULL)
                    , op(PUT)
                    , point(p)
                    , point_mask(pm)
                    , version(ver)
                    , key(k)
                    , value(val)
                    , m_ref(0)
                {
                }

                node(uint64_t p,
                     uint64_t pm,
                     const e::buffer& k)
                    : seqno(0)
                    , next(NULL)
                    , op(DEL)
                    , point(p)
                    , point_mask(pm)
                    , version()
                    , key(k)
                    , value()
                    , m_ref(0)
                {
                }

                ~node() throw ()
                {
                }

            public:
                uint64_t seqno;
                e::intrusive_ptr<node> next;
                op_t op;
                uint64_t point;
                uint64_t point_mask;
                uint64_t version;
                e::buffer key;
                std::vector<e::buffer> value;

            private:
                friend class e::intrusive_ptr<node>;

            private:
                node(const node&);

            private:
                node& operator = (const node&);

            private:
                size_t m_ref;
        };

    private:
        log(const log&);
        log& operator = (const log&);

    private:
        bool common_append(e::intrusive_ptr<node> n);
        e::intrusive_ptr<node> get_head();

    private:
        uint64_t m_seqno;
        po6::threads::rwlock m_head_lock;
        po6::threads::mutex m_tail_lock;
        e::intrusive_ptr<node> m_head;
        e::intrusive_ptr<node> m_tail;
        po6::threads::mutex m_flush_lock;
};

} // namespace hyperdex

#endif // hyperdex_log_h_
