// Copyright (c) 2012, Cornell University
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

// POSIX
#include <sys/mman.h>

// e
#include <e/atomic.h>
#include <e/guard.h>

// HyperDex
#include "disk/cuckoo_index.h"
#include "disk/cuckoo_table.h"

using hyperdex::cuckoo_index;
using hyperdex::cuckoo_returncode;
using hyperdex::cuckoo_table;

class cuckoo_index::table_list
{
    public:
        table_list();
        ~table_list() throw ();

    public:
        e::intrusive_ptr<table_list> expand(size_t table_no,
                                            e::intrusive_ptr<table_base> new_table_base1,
                                            e::intrusive_ptr<table_base> new_table_base2,
                                            uint64_t lower_bound_key,
                                            uint64_t lower_bound_val);
        table_info* info(size_t sz);
        size_t size();

    private:
        friend class e::intrusive_ptr<table_list>;

    private:
        table_list(size_t i);
        table_list(const table_list&);

    private:
        void inc();
        void dec();

    private:
        table_list& operator = (const table_list&);

    private:
        size_t m_ref;
        size_t m_sz;
        table_info* m_tables;
};

class cuckoo_index::table_info
{
    public:
        table_info();
        ~table_info() throw ();

    public:
        uint64_t _lock;
        uint64_t _lower_bound_key;
        uint64_t _lower_bound_val;
        e::intrusive_ptr<table_base> _base;
};

class cuckoo_index::table_base
{
    public:
        table_base();
        ~table_base() throw ();

    public:
        void* base;
        cuckoo_table table;

    private:
        friend class e::intrusive_ptr<table_base>;

    private:
        table_base(const table_base&);

    private:
        void inc();
        void dec();

    private:
        table_base& operator = (const table_base&);

    private:
        size_t m_ref;
};

cuckoo_index :: cuckoo_index()
    : m_tables()
    , m_tables_lock()
    , m_table_locks(256)
{
    po6::threads::spinlock::hold hold(&m_tables_lock);
    m_tables = new table_list();
}

cuckoo_index :: ~cuckoo_index() throw ()
{
}

cuckoo_returncode
cuckoo_index :: insert(uint64_t key, uint64_t val)
{
    while (true)
    {
        table_list_ptr t;

        {
            po6::threads::spinlock::hold hold(&m_tables_lock);
            t = m_tables;
        }

        size_t table_no = 0;
        assert(t->size() > 0);

        for (table_no = 0; table_no < t->size(); ++table_no)
        {
            if (key < t->info(table_no)->_lower_bound_key ||
                (key == t->info(table_no)->_lower_bound_key && val < t->info(table_no)->_lower_bound_val))
            {
                break;
            }
        }

        --table_no;
        e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->_lock);

        {
            po6::threads::spinlock::hold hold2(&m_tables_lock);

            if (t != m_tables)
            {
                continue;
            }
        }

        switch (t->info(table_no)->_base->table.insert(key, val))
        {
            case CUCKOO_SUCCESS:
                return CUCKOO_SUCCESS;
            case CUCKOO_FULL:
                break;
            case CUCKOO_NOT_FOUND:
            default:
                abort();
        }

        e::intrusive_ptr<table_base> new_table_base1 = new table_base();
        e::intrusive_ptr<table_base> new_table_base2 = new table_base();
        uint64_t lower_bound_key;
        uint64_t lower_bound_val;
        t->info(table_no)->_base->table.split(&new_table_base1->table,
                                              &new_table_base2->table,
                                              &lower_bound_key,
                                              &lower_bound_val);

        {
            po6::threads::spinlock::hold hold2(&m_tables_lock);
            m_tables = m_tables->expand(table_no,
                                        new_table_base1,
                                        new_table_base2,
                                        lower_bound_key,
                                        lower_bound_val);
        }

        return CUCKOO_SUCCESS;
    }
}

cuckoo_returncode
cuckoo_index :: lookup(uint64_t key, std::vector<uint64_t>* vals)
{
    vals->clear();
    table_list_ptr t;

    {
        po6::threads::spinlock::hold hold(&m_tables_lock);
        t = m_tables;
    }

    size_t table_no = 0;
    assert(t->size() > 0);

    for (table_no = 0; table_no < t->size(); ++table_no)
    {
        if (key < t->info(table_no)->_lower_bound_key)
        {
            --table_no;
            break;
        }
        else if (key == t->info(table_no)->_lower_bound_key)
        {
            break;
        }
    }

    if (table_no == t->size())
    {
        --table_no;
    }

    while (table_no < t->size() && t->info(table_no)->_lower_bound_key <= key)
    {
        e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->_lock);

        switch (t->info(table_no)->_base->table.lookup(key, vals))
        {
            case CUCKOO_SUCCESS:
            case CUCKOO_NOT_FOUND:
                break;
            case CUCKOO_FULL:
            default:
                abort();
        }

        ++table_no;
    }

    return vals->empty() ? CUCKOO_NOT_FOUND : CUCKOO_SUCCESS;
}

cuckoo_returncode
cuckoo_index :: remove(uint64_t key, uint64_t val)
{
    while (true)
    {
        table_list_ptr t;

        {
            po6::threads::spinlock::hold hold(&m_tables_lock);
            t = m_tables;
        }

        size_t table_no = 0;
        assert(t->size() > 0);

        for (table_no = 0; table_no < t->size(); ++table_no)
        {
            if (key < t->info(table_no)->_lower_bound_key ||
                (key == t->info(table_no)->_lower_bound_key && val < t->info(table_no)->_lower_bound_val))
            {
                break;
            }
        }

        --table_no;
        e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->_lock);

        {
            // Need to double check that the original still holds
            po6::threads::spinlock::hold hold2(&m_tables_lock);

            if (t != m_tables)
            {
                continue;
            }
        }

        return t->info(table_no)->_base->table.remove(key, val);
    }
}

cuckoo_index :: table_list :: table_list()
    : m_ref(0)
    , m_sz(1)
    , m_tables(new table_info[1])
{
    m_tables[0]._base = new table_base();
}

cuckoo_index :: table_list :: ~table_list() throw ()
{
    delete[] m_tables;
}

e::intrusive_ptr<cuckoo_index::table_list>
cuckoo_index :: table_list :: expand(size_t table_no,
                                     e::intrusive_ptr<table_base> new_table_base1,
                                     e::intrusive_ptr<table_base> new_table_base2,
                                     uint64_t lower_bound_key,
                                     uint64_t lower_bound_val)
{
    assert(table_no < m_sz);
    e::intrusive_ptr<table_list> t = new table_list(m_sz + 1);
    size_t i = 0;
    size_t j = 0;

    while (i < t->m_sz)
    {
        assert(j < m_sz);
        assert(i < t->m_sz);

        if (i == table_no)
        {
            t->m_tables[i + 0] = m_tables[j];
            t->m_tables[i + 1] = m_tables[j];
            t->m_tables[i + 0]._base = new_table_base1;
            t->m_tables[i + 1]._base = new_table_base2;
            t->m_tables[i + 1]._lower_bound_key = lower_bound_key;
            t->m_tables[i + 1]._lower_bound_val = lower_bound_val;
            ++i;
        }
        else
        {
            t->m_tables[i] = m_tables[j];
        }

        ++i;
        ++j;
    }

    return t;
}

cuckoo_index::table_info*
cuckoo_index :: table_list :: info(size_t i)
{
    assert(i < m_sz);
    return m_tables + i;
}

size_t
cuckoo_index :: table_list :: size()
{
    return m_sz;
}

void
cuckoo_index :: table_list :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
cuckoo_index :: table_list :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}

cuckoo_index :: table_list :: table_list(size_t i)
    : m_ref(0)
    , m_sz(i)
    , m_tables(new table_info[i])
{
}

cuckoo_index :: table_info :: table_info()
    : _lock(0)
    , _lower_bound_key(0)
    , _lower_bound_val(0)
    , _base()
{
}

cuckoo_index :: table_info :: ~table_info() throw ()
{
}

cuckoo_index :: table_base :: table_base()
    : base(mmap(NULL, 65536ULL*2ULL*64ULL, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1 , 0))
    , table(base)
    , m_ref(0)
{
    if (base == MAP_FAILED)
    {
        throw std::bad_alloc();
    }

    memset(base, 0, 65536ULL*2ULL*64ULL);
}

cuckoo_index :: table_base :: ~table_base() throw ()
{
    munmap(base, 65536ULL*2ULL*64ULL);
}

void
cuckoo_index :: table_base :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
cuckoo_index :: table_base :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}
