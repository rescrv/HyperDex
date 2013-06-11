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

// C
#include <cassert>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <cstdio>

// STL
#include <list>
#include <string>
#include <vector>

// HyperDex
#include "common/attribute.h"
#include "common/datatypes.h"
#include "common/hyperspace.h"
#include "common/schema.h"
#include "client/hyperspace_builder.h"
#include "client/hyperspace_builder_internal.h"
#include "client/partition.h"
#include "client/parse_space_y.h"

#define BUFFER_SIZE 1024

using hyperdex::attribute;
using hyperdex::datatype_info;

namespace
{

class hypersubspace
{
    public:
        hypersubspace();
        ~hypersubspace() throw ();

    public:
        std::vector<const char*> attrs;
        std::vector<const char*> sindices;
};

hypersubspace :: hypersubspace()
    : attrs()
    , sindices()
{
}

hypersubspace :: ~hypersubspace() throw ()
{
}

}

class hyperspace
{
    public:
        hyperspace();
        ~hyperspace() throw ();

    public:
        // copy the string and return a pointer to the copy
        const char* internalize(const char*);
        bool has_attr(const char* name);
        hyperdatatype attr_type(const char* name);

    public:
        void* scanner;
        const char* error;
        char buffer[BUFFER_SIZE];
        std::list<std::string> strings;

        // parsed components
        const char* name;
        attribute key;
        std::vector<attribute> attributes;
        std::vector<const char*> pindices;
        std::vector<hypersubspace> subspaces;
        uint64_t fault_tolerance;
        uint64_t partitions;

    private:
        hyperspace(const hyperspace&);
        hyperspace& operator = (const hyperspace&);
};

hyperspace :: hyperspace()
    : scanner(NULL)
    , error(NULL)
    , strings()
    , name(NULL)
    , key()
    , attributes()
    , pindices()
    , subspaces()
    , fault_tolerance(2)
    , partitions(256)
{
    memset(buffer, 0, 1024);
}

hyperspace :: ~hyperspace() throw ()
{
}

bool
hyperspace :: has_attr(const char* attr)
{
    for (size_t i = 0; i < attributes.size(); ++i)
    {
        if (strcmp(attributes[i].name, attr) == 0)
        {
            return true;
        }
    }

    return false;
}

hyperdatatype
hyperspace :: attr_type(const char* attr)
{
    for (size_t i = 0; i < attributes.size(); ++i)
    {
        if (strcmp(attributes[i].name, attr) == 0)
        {
            return attributes[i].type;
        }
    }

    abort();
}

const char*
hyperspace :: internalize(const char* str)
{
    strings.push_back(std::string(str));
    return strings.back().c_str();
}

static bool
is_identifier(const char* str)
{
    size_t i = 0;

    for (i = 0; str[i]; ++i)
    {
        if (!(i > 0 && isdigit(str[i])) &&
            !(isalpha(str[i])) &&
            str[i] != '_')
        {
            return false;
        }
    }

    return i > 0;
}

static bool
is_key_datatype(hyperdatatype type)
{
    datatype_info* di = datatype_info::lookup(type);
    return di && di->hashable();
}

static bool
is_concrete_datatype(hyperdatatype type)
{
    return datatype_info::lookup(type) != NULL;
}

extern "C"
{

extern int
yylex_init(void* scanner);

extern int
yylex_destroy(void* scanner);

extern struct yy_buffer_state*
yy_scan_string(const char *bytes, void* scanner);

extern void
yy_switch_to_buffer(struct yy_buffer_state* new_buffer, void* scanner);

extern void
yyset_lineno(int line_number, void* scanner);

extern int
yyparse(struct hyperspace* space, void* scanner);

struct hyperspace*
hyperspace_create()
{
    struct hyperspace* space = new (std::nothrow) hyperspace();

    if (!space)
    {
        return NULL;
    }

    return space;
}

struct hyperspace*
hyperspace_parse(const char* desc)
{
    struct hyperspace* space = hyperspace_create();
    struct yy_buffer_state* buf = NULL;

    if (!space)
    {
        return NULL;
    }

    if (yylex_init(&space->scanner) < 0)
    {
        space->error = "yylex_init failed";
        return space;
    }

    buf = yy_scan_string(desc, space->scanner);

    if (!buf)
    {
        space->error = "yy_scan_string failed";
        yylex_destroy(space->scanner);
        return space;
    }

    yy_switch_to_buffer(buf, space->scanner);
    yyset_lineno(1, space->scanner);

    if (yyparse(space, space->scanner) < 0)
    {
        space->error = "yyparse failed";
        yylex_destroy(space->scanner);
        return space;
    }

    if (yylex_destroy(space->scanner) < 0)
    {
        space->error = "yylex_destroy failed";
        return space;
    }

    space->scanner = NULL;
    return space;
}

void
hyperspace_destroy(struct hyperspace* space)
{
    delete space;
}

const char*
hyperspace_error(struct hyperspace* space)
{
    if (!space)
    {
        return "failed to allocate memory";
    }

    return space->error;
}

enum hyperspace_returncode
hyperspace_set_name(struct hyperspace* space, const char* name)
{
    if (!is_identifier(name))
    {
        snprintf(space->buffer, BUFFER_SIZE, "\"%s\" is not a valid name for a space", name);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_INVALID_NAME;
    }

    space->name = space->internalize(name);
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_set_key(struct hyperspace* space,
                   const char* attr,
                   enum hyperdatatype datatype)
{
    if (!is_identifier(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "\"%s\" is not a valid name for the key", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_INVALID_NAME;
    }

    if (!is_key_datatype(datatype))
    {
        snprintf(space->buffer, BUFFER_SIZE, "\"%d\" is not a valid type for the key", datatype);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_INVALID_TYPE;
    }

    if (space->has_attr(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot call the key \"%s\" because there is already an attribute by that name", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_DUPLICATE;
    }

    space->key = attribute(space->internalize(attr), datatype);
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_add_attribute(struct hyperspace* space,
                         const char* attr,
                         enum hyperdatatype datatype)
{
    if (!is_identifier(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "\"%s\" is not a valid name for an attribute", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_INVALID_NAME;
    }

    if (!is_concrete_datatype(datatype))
    {
        snprintf(space->buffer, BUFFER_SIZE, "\"%d\" is not a valid type for an attribute", datatype);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_INVALID_TYPE;
    }

    if (space->has_attr(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create attribute \"%s\" because there is already an attribute by that name", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_DUPLICATE;
    }

    if (strcmp(space->key.name, attr) == 0)
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create attribute \"%s\" because the key is called \"%s\"", attr, attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_DUPLICATE;
    }

    space->attributes.push_back(attribute(space->internalize(attr), datatype));
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_primary_index(struct hyperspace* space, const char* attr)
{
    if (strcmp(space->key.name, attr) == 0)
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create primary index on \"%s\" because it is the key", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_IS_KEY;
    }

    if (!space->has_attr(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create primary index on \"%s\" because there is no attribute by that name", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_UNKNOWN_ATTR;
    }

    if (!datatype_info::lookup(space->attr_type(attr))->indexable())
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create primary index on \"%s\" because the type is not indexable", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_UNINDEXABLE;
    }

    space->pindices.push_back(space->internalize(attr));
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_add_subspace(struct hyperspace* space)
{
    space->subspaces.push_back(hypersubspace());
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_add_subspace_attribute(struct hyperspace* space, const char* attr)
{
    if (space->subspaces.empty())
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot add attribute to subspace, because there is no subspace");
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_NO_SUBSPACE;
    }

    if (!space->has_attr(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot add attribute \"%s\" to subspace because there is no attribute by that name", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_UNKNOWN_ATTR;
    }

    if (!datatype_info::lookup(space->attr_type(attr))->hashable())
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot add attribute \"%s\" to subspace because the type is not hashable", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_UNINDEXABLE;
    }

    for (size_t i = 0; i < space->subspaces.back().attrs.size(); ++i)
    {
        if (strcmp(space->subspaces.back().attrs[i], attr) == 0)
        {
            snprintf(space->buffer, BUFFER_SIZE, "cannot add attribute \"%s\" to subspace because it is already a subspace attribute", attr);
            space->buffer[BUFFER_SIZE - 1] = '\0';
            space->error = space->buffer;
            return HYPERSPACE_DUPLICATE;
        }
    }

    space->subspaces.back().attrs.push_back(space->internalize(attr));
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_add_secondary_index(struct hyperspace* space, const char* attr)
{
    if (space->subspaces.empty())
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot add attribute to subspace, because there is no subspace");
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_NO_SUBSPACE;
    }

    if (strcmp(space->key.name, attr) == 0)
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create secondary index on \"%s\" because it is the key", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_IS_KEY;
    }

    if (!space->has_attr(attr))
    {
        snprintf(space->buffer, BUFFER_SIZE, "cannot create secondary index on \"%s\" because there is no attribute by that name", attr);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_UNKNOWN_ATTR;
    }

    for (size_t i = 0; i < space->subspaces.back().sindices.size(); ++i)
    {
        if (strcmp(space->subspaces.back().sindices[i], attr) == 0)
        {
            snprintf(space->buffer, BUFFER_SIZE, "cannot create secondary index on \"%s\" because it is already indexed", attr);
            space->buffer[BUFFER_SIZE - 1] = '\0';
            space->error = space->buffer;
            return HYPERSPACE_DUPLICATE;
        }
    }

    space->subspaces.back().sindices.push_back(space->internalize(attr));
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_set_fault_tolerance(struct hyperspace* space, uint64_t num)
{
    space->fault_tolerance = num;
    return HYPERSPACE_SUCCESS;
}

enum hyperspace_returncode
hyperspace_set_number_of_partitions(struct hyperspace* space, uint64_t num)
{
    if (num < 1)
    {
        snprintf(space->buffer, BUFFER_SIZE, "the number of partitions must be positive, not %ld", num);
        space->buffer[BUFFER_SIZE - 1] = '\0';
        space->error = space->buffer;
        return HYPERSPACE_NO_SUBSPACE;
    }

    space->partitions = num;
    return HYPERSPACE_SUCCESS;
}

char*
hyperspace_buffer(struct hyperspace* space)
{
    return space->buffer;
}

size_t
hyperspace_buffer_sz(struct hyperspace*)
{
    return BUFFER_SIZE;
}

void
hyperspace_set_error(struct hyperspace* space, const char* msg)
{
    space->error = msg;
}

} // extern "C"

bool
hyperdex :: space_to_space(struct hyperspace* in, hyperdex::space* out)
{
    assert(!hyperspace_error(in));
    std::vector<attribute> attrs;
    attrs.push_back(in->key);

    for (size_t i = 0; i < in->attributes.size(); ++i)
    {
        attrs.push_back(in->attributes[i]);
    }

    schema sc;
    sc.attrs_sz = attrs.size();
    sc.attrs = &attrs.front();
    space sp(in->name, sc);
    sp.subspaces.push_back(subspace());
    sp.subspaces.back().attrs.push_back(0);

    for (size_t i = 0; i < in->pindices.size(); ++i)
    {
        uint16_t attr = sc.lookup_attr(in->pindices[i]);
        assert(attr < sc.attrs_sz);
        sp.subspaces.back().indices.push_back(attr);
    }

    for (size_t i = 0; i < in->subspaces.size(); ++i)
    {
        if (in->subspaces[i].attrs.empty())
        {
            continue;
        }

        sp.subspaces.push_back(subspace());

        for (size_t j = 0; j < in->subspaces[i].attrs.size(); ++j)
        {
            uint16_t attr = sc.lookup_attr(in->subspaces[i].attrs[j]);
            assert(attr < sc.attrs_sz);
            sp.subspaces.back().attrs.push_back(attr);
        }

        for (size_t j = 0; j < in->subspaces[i].sindices.size(); ++j)
        {
            uint16_t attr = sc.lookup_attr(in->subspaces[i].sindices[j]);
            assert(attr < sc.attrs_sz);
            sp.subspaces.back().indices.push_back(attr);
        }
    }

    sp.fault_tolerance = in->fault_tolerance;

    if (!sp.validate())
    {
        return false;
    }

    for (size_t i = 0; i < sp.subspaces.size(); ++i)
    {
        partition(sp.subspaces[i].attrs.size(), in->partitions, &sp.subspaces[i].regions);
    }

    *out = sp;
    return true;
}
