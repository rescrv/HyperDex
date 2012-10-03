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
                                            e::intrusive_ptr<table_info> newtable1,
                                            e::intrusive_ptr<table_info> newtable2);
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
        e::intrusive_ptr<table_info>* m_tables;
};

class cuckoo_index::table_info
{
    public:
        table_info();
        ~table_info() throw ();

    public:
        void* base;
        uint64_t lock;
        uint64_t lower_bound;
        cuckoo_table table;

    private:
        friend class e::intrusive_ptr<table_info>;

    private:
        table_info(const table_info&);

    private:
        void inc();
        void dec();

    private:
        table_info& operator = (const table_info&);

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
cuckoo_index :: insert(uint64_t key, uint64_t old_val, uint64_t new_val)
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
            if (key < t->info(table_no)->lower_bound)
            {
                break;
            }
        }

        --table_no;
        e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->lock);

        {
            po6::threads::spinlock::hold hold2(&m_tables_lock);

            if (t != m_tables)
            {
                continue;
            }
        }

        switch (t->info(table_no)->table.insert(key, old_val, new_val))
        {
            case SUCCESS:
                return SUCCESS;
            case FULL:
                break;
            case NOT_FOUND:
            default:
                abort();
        }

        e::intrusive_ptr<table_info> newtable1 = new table_info();
        e::intrusive_ptr<table_info> newtable2 = new table_info();
        newtable1->lock = 0;
        newtable2->lock = 0;
        newtable1->lower_bound = t->info(table_no)->lower_bound;
        t->info(table_no)->table.split(&newtable1->table, &newtable2->table, &newtable2->lower_bound);

        {
            po6::threads::spinlock::hold hold2(&m_tables_lock);
            m_tables = m_tables->expand(table_no, newtable1, newtable2);
        }
    }
}

cuckoo_returncode
cuckoo_index :: lookup(uint64_t key, std::vector<uint64_t>* vals)
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
        if (key < t->info(table_no)->lower_bound)
        {
            break;
        }
    }

    --table_no;
    e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->lock);
    return t->info(table_no)->table.lookup(key, vals);
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
            if (key < t->info(table_no)->lower_bound)
            {
                break;
            }
        }

        --table_no;
        e::striped_lock<po6::threads::spinlock>::hold hold(&m_table_locks, t->info(table_no)->lock);

        {
            po6::threads::spinlock::hold hold2(&m_tables_lock);

            if (t != m_tables)
            {
                continue;
            }
        }

        return t->info(table_no)->table.remove(key, val);
    }
}

cuckoo_index :: table_list :: table_list()
    : m_ref(0)
    , m_sz(1)
    , m_tables(new e::intrusive_ptr<table_info>[1])
{
    m_tables[0] = new table_info();
}

cuckoo_index :: table_list :: ~table_list() throw ()
{
    delete[] m_tables;
}

e::intrusive_ptr<cuckoo_index::table_list>
cuckoo_index :: table_list :: expand(size_t table_no,
                                     e::intrusive_ptr<table_info> newtable1,
                                     e::intrusive_ptr<table_info> newtable2)
{
    assert(table_no < m_sz);
    e::intrusive_ptr<table_list> t = new table_list(m_sz + 1);
    size_t i = 0;
    size_t j = 0;

    while (i < t->m_sz)
    {
        if (i == table_no)
        {
            t->m_tables[i + 0] = newtable1;
            t->m_tables[i + 1] = newtable2;
            i += 2;
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
    return m_tables[i].get();
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
    , m_tables(new e::intrusive_ptr<table_info>[i])
{
}

cuckoo_index :: table_info :: table_info()
    : base(mmap(NULL, 65536ULL*2ULL*64ULL, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1 , 0))
    , lock(0)
    , lower_bound(0)
    , table(base)
    , m_ref(0)
{
    if (base == MAP_FAILED)
    {
        throw std::bad_alloc();
    }
}

cuckoo_index :: table_info :: ~table_info() throw ()
{
}

void
cuckoo_index :: table_info :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
cuckoo_index :: table_info :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}
