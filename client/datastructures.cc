// Copyright (c) 2013, Cornell University
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

#define __STDC_LIMIT_MACROS

// C
#include <cstring>

// POSIX
#include <errno.h>

// STL
#include <algorithm>
#include <list>
#include <memory>
#include <string>

// e
#include <e/endian.h>

// HyperDex
#include <hyperdex/datastructures.h>
#include "common/datatype_info.h"
#include "common/macros.h"
#include "visibility.h"

using hyperdex::datatype_info;

class hyperdex_ds_arena
{
    public:
        hyperdex_ds_arena();
        ~hyperdex_ds_arena() throw ();

    public:
        void* allocate(size_t bytes);
        hyperdex_ds_list* allocate_list();
        hyperdex_ds_set* allocate_set();
        hyperdex_ds_map* allocate_map();

    private:
        std::list<void*> m_voids;
        std::list<hyperdex_ds_list*> m_lists;
        std::list<hyperdex_ds_set*> m_sets;
        std::list<hyperdex_ds_map*> m_maps;
};

class hyperdex_ds_list
{
    public:
        hyperdex_ds_list();
        ~hyperdex_ds_list() throw ();

    public:
        hyperdatatype type;
        std::string elems;
};

class hyperdex_ds_set
{
    public:
        hyperdex_ds_set();
        ~hyperdex_ds_set() throw ();

    public:
        hyperdatatype type;
        std::string elems;
};

class hyperdex_ds_map
{
    public:
        hyperdex_ds_map();
        ~hyperdex_ds_map() throw ();

    public:
        hyperdatatype type;
        std::string pairs;
        enum { EXPECT_KEY, EXPECT_VAL } state;
};

hyperdex_ds_arena :: hyperdex_ds_arena()
    : m_voids()
    , m_lists()
    , m_sets()
    , m_maps()
{
}

hyperdex_ds_arena :: ~hyperdex_ds_arena() throw ()
{
    for (std::list<void*>::iterator it = m_voids.begin();
            it != m_voids.end(); ++it)
    {
        if (*it)
        {
            free(*it);
        }
    }

    for (std::list<hyperdex_ds_list*>::iterator it = m_lists.begin();
            it != m_lists.end(); ++it)
    {
        if (*it)
        {
            delete *it;
        }
    }

    for (std::list<hyperdex_ds_set*>::iterator it = m_sets.begin();
            it != m_sets.end(); ++it)
    {
        if (*it)
        {
            delete *it;
        }
    }

    for (std::list<hyperdex_ds_map*>::iterator it = m_maps.begin();
            it != m_maps.end(); ++it)
    {
        if (*it)
        {
            delete *it;
        }
    }
}

void*
hyperdex_ds_arena :: allocate(size_t bytes)
{
    try
    {
        m_voids.push_back(NULL);
        m_voids.back() = malloc(bytes);

        if (m_voids.back() == NULL)
        {
            m_voids.pop_back();
            return NULL;
        }

        return m_voids.back();
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        abort();
    }
}

hyperdex_ds_list*
hyperdex_ds_arena :: allocate_list()
{
    try
    {
        std::auto_ptr<hyperdex_ds_list> list(new hyperdex_ds_list());
        m_lists.push_back(list.get());
        return list.release();
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        abort();
    }
}

hyperdex_ds_set*
hyperdex_ds_arena :: allocate_set()
{
    try
    {
        std::auto_ptr<hyperdex_ds_set> set(new hyperdex_ds_set());
        m_sets.push_back(set.get());
        return set.release();
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        abort();
    }
}

hyperdex_ds_map*
hyperdex_ds_arena :: allocate_map()
{
    try
    {
        std::auto_ptr<hyperdex_ds_map> map(new hyperdex_ds_map());
        m_maps.push_back(map.get());
        return map.release();
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        abort();
    }
}

hyperdex_ds_list :: hyperdex_ds_list()
    : type(HYPERDATATYPE_LIST_GENERIC)
    , elems()
{
}

hyperdex_ds_list :: ~hyperdex_ds_list() throw ()
{
}

hyperdex_ds_set :: hyperdex_ds_set()
    : type(HYPERDATATYPE_SET_GENERIC)
    , elems()
{
}

hyperdex_ds_set :: ~hyperdex_ds_set() throw ()
{
}

hyperdex_ds_map :: hyperdex_ds_map()
    : type(HYPERDATATYPE_MAP_GENERIC)
    , pairs()
    , state(EXPECT_KEY)
{
}

hyperdex_ds_map :: ~hyperdex_ds_map() throw ()
{
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////// Public API //////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

extern "C"
{

///////////////////////////////////// Arena ////////////////////////////////////

HYPERDEX_API struct hyperdex_ds_arena*
hyperdex_ds_arena_create()
{
    return new (std::nothrow) hyperdex_ds_arena();
}

HYPERDEX_API void
hyperdex_ds_arena_destroy(struct hyperdex_ds_arena* arena)
{
    if (arena)
    {
        delete arena;
    }
}

HYPERDEX_API void*
hyperdex_ds_malloc(struct hyperdex_ds_arena* arena, size_t sz)
{
    return reinterpret_cast<void*>(arena->allocate(sz));
}

//////////////////////////////// Client Structs ////////////////////////////////

HYPERDEX_API struct hyperdex_client_attribute*
hyperdex_ds_allocate_attribute(struct hyperdex_ds_arena* arena, size_t sz)
{
    size_t bytes = sizeof(struct hyperdex_client_attribute) * sz;
    return reinterpret_cast<struct hyperdex_client_attribute*>(arena->allocate(bytes));
}

HYPERDEX_API struct hyperdex_client_attribute_check*
hyperdex_ds_allocate_attribute_check(struct hyperdex_ds_arena* arena, size_t sz)
{
    size_t bytes = sizeof(struct hyperdex_client_attribute_check) * sz;
    return reinterpret_cast<struct hyperdex_client_attribute_check*>(arena->allocate(bytes));
}

HYPERDEX_API struct hyperdex_client_map_attribute*
hyperdex_ds_allocate_map_attribute(struct hyperdex_ds_arena* arena, size_t sz)
{
    size_t bytes = sizeof(struct hyperdex_client_map_attribute) * sz;
    return reinterpret_cast<struct hyperdex_client_map_attribute*>(arena->allocate(bytes));
}

//////////////////////////// pack/unpack ints/floats ///////////////////////////

HYPERDEX_API void
hyperdex_ds_pack_int(int64_t num, char* buf)
{
    e::pack64le(num, buf);
}

HYPERDEX_API int
hyperdex_ds_unpack_int(const char* value, size_t value_sz, int64_t* num)
{
    if (value_sz == 0)
    {
        *num = 0;
        return 0;
    }
    else if (value_sz != sizeof(int64_t))
    {
        return -1;
    }

    e::unpack64le(value, num);
    return 0;
}

HYPERDEX_API void
hyperdex_ds_pack_float(double num, char* buf)
{
    e::packdoublele(num, buf);
}

HYPERDEX_API int
hyperdex_ds_unpack_float(const char* value, size_t value_sz, double* num)
{
    if (value_sz == 0)
    {
        *num = 0;
        return 0;
    }
    else if (value_sz != sizeof(double))
    {
        return -1;
    }

    e::unpackdoublele(value, num);
    return 0;
}

HYPERDEX_API int
hyperdex_ds_copy_string(struct hyperdex_ds_arena* arena,
                        const char* str, size_t str_sz,
                        enum hyperdex_ds_returncode* status,
                        const char** value, size_t* value_sz)
{
    char* x = reinterpret_cast<char*>(arena->allocate(str_sz));

    if (!x)
    {
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }

    memmove(x, str, str_sz);
    *value = x;
    *value_sz = str_sz;
    *status = HYPERDEX_DS_SUCCESS;
    return 0;
}

HYPERDEX_API int
hyperdex_ds_copy_int(struct hyperdex_ds_arena* arena, int64_t num,
                     enum hyperdex_ds_returncode* status,
                     const char** value, size_t* value_sz)
{
    char* x = reinterpret_cast<char*>(arena->allocate(sizeof(int64_t)));

    if (!x)
    {
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }

    e::pack64le(num, x);
    *value = x;
    *value_sz = sizeof(int64_t);
    *status = HYPERDEX_DS_SUCCESS;
    return 0;
}

HYPERDEX_API int
hyperdex_ds_copy_float(struct hyperdex_ds_arena* arena, double num,
                       enum hyperdex_ds_returncode* status,
                       const char** value, size_t* value_sz)
{
    char* x = reinterpret_cast<char*>(arena->allocate(sizeof(double)));

    if (!x)
    {
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }

    e::packdoublele(num, x);
    *value = x;
    *value_sz = sizeof(double);
    *status = HYPERDEX_DS_SUCCESS;
    return 0;
}

////////////////////////////////// pack lists //////////////////////////////////

HYPERDEX_API struct hyperdex_ds_list*
hyperdex_ds_allocate_list(struct hyperdex_ds_arena* arena)
{
    return arena->allocate_list();
}

HYPERDEX_API int
hyperdex_ds_list_append_string(struct hyperdex_ds_list* list,
                               const char* str, size_t str_sz,
                               enum hyperdex_ds_returncode* status)
{
    try
    {
        if (list->type == HYPERDATATYPE_LIST_GENERIC)
        {
            list->type = HYPERDATATYPE_LIST_STRING;
        }
        else if (list->type != HYPERDATATYPE_LIST_STRING)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        if (str_sz >= UINT32_MAX)
        {
            *status = HYPERDEX_DS_STRING_TOO_LONG;
            return -1;
        }

        char buf[sizeof(uint32_t)];
        e::pack32le(str_sz, buf);
        list->elems += std::string(buf, sizeof(uint32_t));
        list->elems += std::string(str, str_sz);
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_list_append_int(struct hyperdex_ds_list* list,
                            int64_t num,
                            enum hyperdex_ds_returncode* status)
{
    try
    {
        if (list->type == HYPERDATATYPE_LIST_GENERIC)
        {
            list->type = HYPERDATATYPE_LIST_INT64;
        }
        else if (list->type != HYPERDATATYPE_LIST_INT64)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(int64_t)];
        e::pack64le(num, buf);
        list->elems += std::string(buf, buf + sizeof(int64_t));
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_list_append_float(struct hyperdex_ds_list* list,
                              double num,
                              enum hyperdex_ds_returncode* status)
{
    try
    {
        if (list->type == HYPERDATATYPE_LIST_GENERIC)
        {
            list->type = HYPERDATATYPE_LIST_FLOAT;
        }
        else if (list->type != HYPERDATATYPE_LIST_FLOAT)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(double)];
        e::packdoublele(num, buf);
        list->elems += std::string(buf, buf + sizeof(double));
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_list_finalize(struct hyperdex_ds_list* list,
                          enum hyperdex_ds_returncode* status,
                          const char** value, size_t* value_sz,
                          enum hyperdatatype* datatype)
{
    *status = HYPERDEX_DS_SUCCESS;
    *value = list->elems.data();
    *value_sz = list->elems.size();
    *datatype = list->type;
    return 0;
}

/////////////////////////////////// pack sets //////////////////////////////////

HYPERDEX_API struct hyperdex_ds_set*
hyperdex_ds_allocate_set(struct hyperdex_ds_arena* arena)
{
    return arena->allocate_set();
}

HYPERDEX_API int
hyperdex_ds_set_insert_string(struct hyperdex_ds_set* set,
                              const char* str, size_t str_sz,
                              enum hyperdex_ds_returncode* status)
{
    try
    {
        if (set->type == HYPERDATATYPE_SET_GENERIC)
        {
            set->type = HYPERDATATYPE_SET_STRING;
        }
        else if (set->type != HYPERDATATYPE_SET_STRING)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        if (str_sz >= UINT32_MAX)
        {
            *status = HYPERDEX_DS_STRING_TOO_LONG;
            return -1;
        }

        char buf[sizeof(uint32_t)];
        e::pack32le(str_sz, buf);
        set->elems += std::string(buf, sizeof(uint32_t));
        set->elems += std::string(str, str_sz);
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_set_insert_int(struct hyperdex_ds_set* set, int64_t num,
                           enum hyperdex_ds_returncode* status)
{
    try
    {
        if (set->type == HYPERDATATYPE_SET_GENERIC)
        {
            set->type = HYPERDATATYPE_SET_INT64;
        }
        else if (set->type != HYPERDATATYPE_SET_INT64)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(int64_t)];
        e::pack64le(num, buf);
        set->elems += std::string(buf, buf + sizeof(int64_t));
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_set_insert_float(struct hyperdex_ds_set* set, double num,
                             enum hyperdex_ds_returncode* status)
{
    try
    {
        if (set->type == HYPERDATATYPE_SET_GENERIC)
        {
            set->type = HYPERDATATYPE_SET_FLOAT;
        }
        else if (set->type != HYPERDATATYPE_SET_FLOAT)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(double)];
        e::packdoublele(num, buf);
        set->elems += std::string(buf, buf + sizeof(double));
        *status = HYPERDEX_DS_SUCCESS;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_set_finalize(struct hyperdex_ds_set* set,
                         enum hyperdex_ds_returncode* status,
                         const char** value, size_t* value_sz,
                         enum hyperdatatype* datatype)
{
    if (set->type == HYPERDATATYPE_SET_GENERIC)
    {
        assert(set->elems.empty());
        *status = HYPERDEX_DS_SUCCESS;
        *value = set->elems.data();
        *value_sz = set->elems.size();
        *datatype = set->type;
        return 0;
    }

    datatype_info* di = datatype_info::lookup(CONTAINER_ELEM(set->type));
    assert(di);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(set->elems.data());
    const uint8_t* end = ptr + set->elems.size();
    e::slice elem;
    std::vector<e::slice> elems;

    while (ptr < end)
    {
        bool stepped = di->step(&ptr, end, &elem);
        assert(stepped);
        elems.push_back(elem);
    }

    assert(ptr == end);
    std::sort(elems.begin(), elems.end(), di->compare_less());
    std::vector<char> tmp(set->elems.size());
    uint8_t* wr = reinterpret_cast<uint8_t*>(&tmp[0]);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        wr = di->write(elems[i], wr);
    }

    set->elems = std::string(tmp.begin(), tmp.end());
    *status = HYPERDEX_DS_SUCCESS;
    *value = set->elems.data();
    *value_sz = set->elems.size();
    *datatype = set->type;
    return 0;
}

/////////////////////////////////// pack maps //////////////////////////////////

HYPERDEX_API struct hyperdex_ds_map*
hyperdex_ds_allocate_map(struct hyperdex_ds_arena* arena)
{
    return arena->allocate_map();
}

HYPERDEX_API int
hyperdex_ds_map_insert_key_string(struct hyperdex_ds_map* map,
                                  const char* str, size_t str_sz,
                                  enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_KEY)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_KEY(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = HYPERDATATYPE_MAP_STRING_KEYONLY;
        }
        else if (CONTAINER_KEY(map->type) != HYPERDATATYPE_STRING)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        if (str_sz >= UINT32_MAX)
        {
            *status = HYPERDEX_DS_STRING_TOO_LONG;
            return -1;
        }

        char buf[sizeof(uint32_t)];
        e::pack32le(str_sz, buf);
        map->pairs += std::string(buf, sizeof(uint32_t));
        map->pairs += std::string(str, str_sz);
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_VAL;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_map_insert_val_string(struct hyperdex_ds_map* map,
                                  const char* str, size_t str_sz,
                                  enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_VAL)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_VAL(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = RESTRICT_CONTAINER(map->type, HYPERDATATYPE_STRING);
        }
        else if (CONTAINER_VAL(map->type) != HYPERDATATYPE_STRING)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        if (str_sz >= UINT32_MAX)
        {
            *status = HYPERDEX_DS_STRING_TOO_LONG;
            return -1;
        }

        char buf[sizeof(uint32_t)];
        e::pack32le(str_sz, buf);
        map->pairs += std::string(buf, sizeof(uint32_t));
        map->pairs += std::string(str, str_sz);
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_KEY;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_map_insert_key_int(struct hyperdex_ds_map* map, int64_t num,
                               enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_KEY)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_KEY(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = HYPERDATATYPE_MAP_INT64_KEYONLY;
        }
        else if (CONTAINER_KEY(map->type) != HYPERDATATYPE_INT64)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(int64_t)];
        e::pack64le(num, buf);
        map->pairs += std::string(buf, buf + sizeof(int64_t));
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_VAL;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_map_insert_val_int(struct hyperdex_ds_map* map, int64_t num,
                               enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_VAL)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_VAL(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = RESTRICT_CONTAINER(map->type, HYPERDATATYPE_INT64);
        }
        else if (CONTAINER_VAL(map->type) != HYPERDATATYPE_INT64)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(int64_t)];
        e::pack64le(num, buf);
        map->pairs += std::string(buf, buf + sizeof(int64_t));
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_KEY;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_map_insert_key_float(struct hyperdex_ds_map* map, double num,
                                 enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_KEY)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_KEY(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = HYPERDATATYPE_MAP_FLOAT_KEYONLY;
        }
        else if (CONTAINER_KEY(map->type) != HYPERDATATYPE_FLOAT)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(double)];
        e::packdoublele(num, buf);
        map->pairs += std::string(buf, buf + sizeof(double));
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_VAL;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

HYPERDEX_API int
hyperdex_ds_map_insert_val_float(struct hyperdex_ds_map* map, double num,
                                 enum hyperdex_ds_returncode* status)
{
    if (map->state != hyperdex_ds_map::EXPECT_VAL)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }

    try
    {
        if (CONTAINER_VAL(map->type) == HYPERDATATYPE_GENERIC)
        {
            map->type = RESTRICT_CONTAINER(map->type, HYPERDATATYPE_FLOAT);
        }
        else if (CONTAINER_VAL(map->type) != HYPERDATATYPE_FLOAT)
        {
            *status = HYPERDEX_DS_MIXED_TYPES;
            return -1;
        }

        char buf[sizeof(double)];
        e::packdoublele(num, buf);
        map->pairs += std::string(buf, buf + sizeof(double));
        *status = HYPERDEX_DS_SUCCESS;
        map->state = hyperdex_ds_map::EXPECT_KEY;
        return 0;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_DS_NOMEM;
        return -1;
    }
    catch (...)
    {
        abort();
    }
}

namespace
{

class map_comparator
{
    public:
        map_comparator(datatype_info* di) : m_di(di) {}
        map_comparator(const map_comparator& other) : m_di(other.m_di) {}

    public:
        map_comparator& operator = (const map_comparator& rhs)
        { m_di = rhs.m_di; return *this; }
        bool operator () (const std::pair<e::slice, e::slice>& lhs,
                          const std::pair<e::slice, e::slice>& rhs)
        { return m_di->compare_less()(lhs.first, rhs.first); }

    private:
        datatype_info* m_di;
};

} // namespace

HYPERDEX_API int
hyperdex_ds_map_finalize(struct hyperdex_ds_map* map,
                         enum hyperdex_ds_returncode* status,
                         const char** value, size_t* value_sz,
                         enum hyperdatatype* datatype)
{
    if (map->state != hyperdex_ds_map::EXPECT_KEY)
    {
        *status = HYPERDEX_DS_WRONG_STATE;
        return -1;
    }
    if (map->type == HYPERDATATYPE_MAP_GENERIC)
    {
        assert(map->pairs.empty());
        *status = HYPERDEX_DS_SUCCESS;
        *value = map->pairs.data();
        *value_sz = map->pairs.size();
        *datatype = map->type;
        return 0;
    }

    datatype_info* dik = datatype_info::lookup(CONTAINER_KEY(map->type));
    datatype_info* div = datatype_info::lookup(CONTAINER_VAL(map->type));
    assert(dik);
    assert(div);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(map->pairs.data());
    const uint8_t* end = ptr + map->pairs.size();
    e::slice key;
    e::slice val;
    std::vector<std::pair<e::slice, e::slice> > pairs;

    while (ptr < end)
    {
        bool stepped;
        stepped = dik->step(&ptr, end, &key);
        assert(stepped);
        stepped = div->step(&ptr, end, &val);
        assert(stepped);
        pairs.push_back(std::make_pair(key, val));
    }

    assert(ptr == end);
    std::sort(pairs.begin(), pairs.end(), map_comparator(dik));
    std::vector<char> tmp(map->pairs.size());
    uint8_t* wr = reinterpret_cast<uint8_t*>(&tmp[0]);

    for (size_t i = 0; i < pairs.size(); ++i)
    {
        wr = dik->write(pairs[i].first, wr);
        wr = div->write(pairs[i].second, wr);
    }

    map->pairs = std::string(tmp.begin(), tmp.end());
    *status = HYPERDEX_DS_SUCCESS;
    *value = map->pairs.data();
    *value_sz = map->pairs.size();
    *datatype = map->type;
    return 0;
}

HYPERDEX_API void
hyperdex_ds_iterator_init(struct hyperdex_ds_iterator* iter,
                          enum hyperdatatype datatype,
                          const char* value,
                          size_t value_sz)
{
    iter->datatype = datatype;
    iter->value = value;
    iter->value_end = value + value_sz;
    iter->progress = value;
}

HYPERDEX_API int
hyperdex_ds_iterate_list_string_next(struct hyperdex_ds_iterator* iter,
                                     const char** str, size_t* str_sz)
{
    if (iter->datatype != HYPERDATATYPE_LIST_STRING)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_STRING);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        *str = reinterpret_cast<const char*>(elem.data());
        *str_sz = elem.size();
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_list_int_next(struct hyperdex_ds_iterator* iter, int64_t* num)
{
    if (iter->datatype != HYPERDATATYPE_LIST_INT64)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_INT64);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem) ||
            elem.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpack64le(elem.data(), num);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_list_float_next(struct hyperdex_ds_iterator* iter, double* num)
{
    if (iter->datatype != HYPERDATATYPE_LIST_FLOAT)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem) ||
            elem.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpackdoublele(elem.data(), num);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_set_string_next(struct hyperdex_ds_iterator* iter,
                                    const char** str, size_t* str_sz)
{
    if (iter->datatype != HYPERDATATYPE_SET_STRING)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_STRING);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        *str = reinterpret_cast<const char*>(elem.data());
        *str_sz = elem.size();
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_set_int_next(struct hyperdex_ds_iterator* iter, int64_t* num)
{
    if (iter->datatype != HYPERDATATYPE_SET_INT64)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_INT64);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem) ||
            elem.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpack64le(elem.data(), num);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_set_float_next(struct hyperdex_ds_iterator* iter, double* num)
{
    if (iter->datatype != HYPERDATATYPE_SET_FLOAT)
    {
        return -1;
    }

    datatype_info* di = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    e::slice elem;

    if (iter->progress < iter->value_end)
    {
        if (!di->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                      reinterpret_cast<const uint8_t*>(iter->value_end), &elem) ||
            elem.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpackdoublele(elem.data(), num);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_string_string_next(struct hyperdex_ds_iterator* iter,
                                           const char** key, size_t* key_sz,
                                           const char** val, size_t* val_sz)
{
    if (iter->datatype != HYPERDATATYPE_MAP_STRING_STRING)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_STRING);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_STRING);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        *key = reinterpret_cast<const char*>(mkey.data());
        *key_sz = mkey.size();
        *val = reinterpret_cast<const char*>(mval.data());
        *val_sz = mval.size();
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_string_int_next(struct hyperdex_ds_iterator* iter,
                                        const char** key, size_t* key_sz,
                                        int64_t* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_STRING_INT64)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_STRING);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_INT64);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mval.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        *key = reinterpret_cast<const char*>(mkey.data());
        *key_sz = mkey.size();
        e::unpack64le(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_string_float_next(struct hyperdex_ds_iterator* iter,
                                          const char** key, size_t* key_sz,
                                          double* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_STRING_FLOAT)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_STRING);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mval.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        *key = reinterpret_cast<const char*>(mkey.data());
        *key_sz = mkey.size();
        e::unpackdoublele(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_int_string_next(struct hyperdex_ds_iterator* iter,
                                        int64_t* key, const char** val, size_t* val_sz)
{
    if (iter->datatype != HYPERDATATYPE_MAP_INT64_STRING)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_INT64);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_STRING);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpack64le(mkey.data(), key);
        *val = reinterpret_cast<const char*>(mval.data());
        *val_sz = mval.size();
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_int_int_next(struct hyperdex_ds_iterator* iter,
                                     int64_t* key, int64_t* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_INT64_INT64)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_INT64);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_INT64);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(int64_t) ||
            mval.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpack64le(mkey.data(), key);
        e::unpack64le(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_int_float_next(struct hyperdex_ds_iterator* iter,
                                       int64_t* key, double* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_INT64_FLOAT)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_INT64);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(int64_t) ||
            mval.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpack64le(mkey.data(), key);
        e::unpackdoublele(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_float_string_next(struct hyperdex_ds_iterator* iter,
                                          double* key, const char** val, size_t* val_sz)
{
    if (iter->datatype != HYPERDATATYPE_MAP_FLOAT_STRING)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_STRING);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpackdoublele(mkey.data(), key);
        *val = reinterpret_cast<const char*>(mval.data());
        *val_sz = mval.size();
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_float_int_next(struct hyperdex_ds_iterator* iter,
                                       double* key, int64_t* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_FLOAT_INT64)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_INT64);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(double) ||
            mval.size() != sizeof(int64_t))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpackdoublele(mkey.data(), key);
        e::unpack64le(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

HYPERDEX_API int
hyperdex_ds_iterate_map_float_float_next(struct hyperdex_ds_iterator* iter,
                                         double* key, double* val)
{
    if (iter->datatype != HYPERDATATYPE_MAP_FLOAT_FLOAT)
    {
        return -1;
    }

    datatype_info* dik = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    datatype_info* div = datatype_info::lookup(HYPERDATATYPE_FLOAT);
    e::slice mkey;
    e::slice mval;

    if (iter->progress < iter->value_end)
    {
        if (!dik->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mkey) ||
            !div->step(reinterpret_cast<const uint8_t**>(&iter->progress),
                       reinterpret_cast<const uint8_t*>(iter->value_end), &mval) ||
            mkey.size() != sizeof(double) ||
            mval.size() != sizeof(double))
        {
            iter->progress = iter->value_end + 1;
            return -1;
        }

        e::unpackdoublele(mkey.data(), key);
        e::unpackdoublele(mval.data(), val);
        return 1;
    }

    return iter->progress == iter->value_end ? 0 : -1;
}

} // extern "C"

HYPERDEX_API std::ostream&
operator << (std::ostream& lhs, hyperdex_ds_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(HYPERDEX_DS_SUCCESS);
        STRINGIFY(HYPERDEX_DS_NOMEM);
        STRINGIFY(HYPERDEX_DS_MIXED_TYPES);
        STRINGIFY(HYPERDEX_DS_WRONG_STATE);
        STRINGIFY(HYPERDEX_DS_STRING_TOO_LONG);
        default:
            lhs << "unknown hyperdex_ds_returncode";
            break;
    }

    return lhs;
}
