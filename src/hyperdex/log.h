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

namespace hyperdex
{

class log
{
    protected:
        class node;

    public:
        class record
        {
            public:
                record();
                virtual ~record();

            private:
                record(const record&);
                record& operator = (const record&);
        };

        class iterator
        {
            public:
                iterator(log* l);
                virtual ~iterator();

            public:
                bool step();

            private:
                iterator(const iterator&);
                iterator& operator = (const iterator&);

            private:
                virtual void callback(uint64_t seqno, record* rec);

            private:
                log* m_l;
                node* m_n;
                bool m_called;
        };

        class drain
        {
            public:
                drain();
                virtual ~drain();

            public:
                virtual void one(uint64_t seqno, record* rec);

            private:
                drain(const iterator&);
                drain& operator = (const iterator&);
        };

    public:
        log(std::tr1::shared_ptr<hyperdex::log::drain> d = std::tr1::shared_ptr<hyperdex::log::drain>());
        ~log();

    public:
        bool append(std::auto_ptr<record> rec);
        void flush();
        void shutdown() { m_shutdown = true; }

    public:
        bool is_shutdown() const throw () { return m_shutdown; }
        bool is_flushed() const throw () { return (m_head == m_tail); }

    protected:
        node* append_empty();
        node* get_head();
        node* step_list(node* pos);

    protected:
        class node
        {
            public:
                node()
                    : seqno(0), rec(), next(NULL), refcount(0) {}
                node(uint64_t _seqno, std::auto_ptr<record> _rec)
                    : seqno(_seqno), rec(_rec), next(NULL), refcount(0) {}
                ~node() {}

            public:
                const uint64_t seqno;
                const std::auto_ptr<record> rec;
                node* next;

            // Reference counting operations.
            // The value after the operation is returned.
            public:
                int inc() { return __sync_add_and_fetch(&refcount, 1); }
                int dec() { return __sync_add_and_fetch(&refcount, -1); }

            private:
                node(const node&);
                node& operator = (const node&);
                int refcount;
        };

    private:
        friend class iterator;

    private:
        log(const log&);
        log& operator = (const log&);

    private:
        std::tr1::shared_ptr<hyperdex::log::drain> m_drain;
        uint64_t m_seqno;
        po6::threads::rwlock m_head_lock;
        po6::threads::mutex m_tail_lock;
        node* m_head;
        node* m_tail;
        po6::threads::mutex m_flush_lock;
        bool m_shutdown;
};

} // namespace hyperdex

#endif // hyperdex_log_h_
