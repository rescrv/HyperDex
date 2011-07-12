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
#include <tr1/functional>
#include <tr1/memory>

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>

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
                bool valid();
                void next();

            public:
                op_t op() const { return m_n->op; }
                uint64_t point() const { return m_n->point; }
                const e::buffer& key() const { return m_n->key; }
                uint64_t key_hash() const { return m_n->key_hash; }
                const std::vector<e::buffer>& value() const { return m_n->value; }
                const std::vector<uint64_t>& value_hashes() const { return m_n->value_hashes; }
                uint64_t version() const { return m_n->version; }

            private:
                friend class log;

            private:
                iterator(log* l);

            private:
                void skip_dead_nodes();

            private:
                iterator& operator = (const iterator&);

            private:
                log* m_l;
                node* m_n;
                bool m_valid;
        };

    public:
        log();
        ~log() throw ();

    public:
        iterator iterate() { return iterator(this); }
        bool append(uint64_t point, const e::buffer& key, uint64_t key_hash,
                    const std::vector<e::buffer>& value,
                    const std::vector<uint64_t>& value_hashes, uint64_t version);
        bool append(uint64_t point, const e::buffer& key, uint64_t key_hash);

    public:
        size_t flush(std::tr1::function<void (op_t op,
                                              uint64_t point,
                                              const e::buffer& key,
                                              uint64_t key_hash,
                                              const std::vector<e::buffer>& value,
                                              const std::vector<uint64_t>& value_hashes,
                                              uint64_t version)> save_one);

    private:
        friend class iterator;

    private:
        class node
        {
            public:
                node()
                    : real(false)
                    , next(NULL)
                    , op(DEL)
                    , point()
                    , key()
                    , key_hash(0)
                    , value()
                    , value_hashes()
                    , version()
                    , m_ref(0)
                {
                }

                node(uint64_t p, const e::buffer& k,
                     uint64_t kh, const std::vector<e::buffer>& val,
                     const std::vector<uint64_t>& valh, uint64_t ver)
                    : real(false)
                    , next(NULL)
                    , op(PUT)
                    , point(p)
                    , key(k)
                    , key_hash(kh)
                    , value(val)
                    , value_hashes(valh)
                    , version(ver)
                    , m_ref(0)
                {
                }

                node(uint64_t p, const e::buffer& k, uint64_t kh)
                    : real(false)
                    , next(NULL)
                    , op(DEL)
                    , point(p)
                    , key(k)
                    , key_hash(kh)
                    , value()
                    , value_hashes()
                    , version()
                    , m_ref(0)
                {
                }

                ~node() throw ()
                {
                }

            public:
                bool real;
                node* next;
                op_t op;
                uint64_t point;
                e::buffer key;
                uint64_t key_hash;
                std::vector<e::buffer> value;
                std::vector<uint64_t> value_hashes;
                uint64_t version;

            public:
                int inc() { return __sync_add_and_fetch(&m_ref, 1); }
                int dec() { return __sync_sub_and_fetch(&m_ref, 1); }

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
        bool common_append(std::auto_ptr<node> n, bool real = true);
        node* get_head();
        void step_list(node** pos);
        void release(node* pos);

    private:
        po6::threads::rwlock m_head_lock;
        po6::threads::mutex m_tail_lock;
        node* m_head;
        node* m_tail;
        po6::threads::mutex m_flush_lock;
};

} // namespace hyperdex

#endif // hyperdex_log_h_
