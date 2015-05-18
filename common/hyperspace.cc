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

// HyperDex
#include "common/hyperspace.h"
#include "common/serialization.h"

using hyperdex::space;
using hyperdex::subspace;
using hyperdex::region;
using hyperdex::replica;

space :: space()
    : id()
    , name("")
    , fault_tolerance()
    , predecessor_width(1)
    , sc()
    , subspaces()
    , indices()
    , m_c_strs()
    , m_attrs()
{
}

space :: space(const char* new_name, const hyperdex::schema& _sc)
    : id()
    , name(new_name)
    , fault_tolerance()
    , predecessor_width(1)
    , sc(_sc)
    , subspaces()
    , indices()
    , m_c_strs()
    , m_attrs()
{
    reestablish_backing();
}

space :: space(const space& other)
    : id(other.id)
    , name(other.name)
    , fault_tolerance(other.fault_tolerance)
    , predecessor_width(other.predecessor_width)
    , sc(other.sc)
    , subspaces(other.subspaces)
    , indices(other.indices)
    , m_c_strs()
    , m_attrs()
{
    reestablish_backing();
}

space :: ~space() throw ()
{
}

bool
space :: validate() const
{
    for (size_t i = 0; i < sc.attrs_sz; ++i)
    {
        for (size_t j = i + 1; j < sc.attrs_sz; ++j)
        {
            if (strcmp(sc.attrs[i].name, sc.attrs[j].name) == 0)
            {
                return false;
            }
        }
    }

    for (size_t i = 0; i < subspaces.size(); ++i)
    {
        // XXX if subspaces[i].regions does not fill volume, then fail

        for (size_t j = 0; j < subspaces[i].regions.size(); ++j)
        {
            if (subspaces[i].attrs.size() != subspaces[i].regions[j].lower_coord.size() ||
                subspaces[i].attrs.size() != subspaces[i].regions[j].upper_coord.size())
            {
                return false;
            }
        }

        for (size_t j = 0; j < subspaces[i].attrs.size(); ++j)
        {
            if (subspaces[i].attrs[j] >= sc.attrs_sz)
            {
                return false;
            }

            for (size_t k = j + 1; k < subspaces[i].attrs.size(); ++k)
            {
                if (subspaces[i].attrs[j] == subspaces[i].attrs[k])
                {
                    return false;
                }
            }
        }
    }

    for (size_t i = 0; i < indices.size(); ++i)
    {
        if (indices[i].attr >= sc.attrs_sz)
        {
            return false;
        }
    }

    return true;
}

space&
space :: operator = (const space& rhs)
{
    id = rhs.id;
    name = rhs.name;
    fault_tolerance = rhs.fault_tolerance;
    sc = rhs.sc;
    subspaces = rhs.subspaces;
    indices = rhs.indices;
    reestablish_backing();
    return *this;
}

void
space :: reestablish_backing()
{
    // Compute the size of the c_strs backing
    size_t sz = strlen(name) + 1;

    for (size_t i = 0; i < sc.attrs_sz; ++i)
    {
        sz += strlen(sc.attrs[i].name) + 1;
    }

    for (size_t i = 0; i < indices.size(); ++i)
    {
        sz += indices[i].extra.size();
    }

    // Create the two new backings
    e::array_ptr<char> old_c_strs = m_c_strs;
    e::array_ptr<attribute> old_attrs = m_attrs;
    m_c_strs = new char[sz];
    m_attrs = new attribute[sc.attrs_sz];
    char* ptr = m_c_strs.get();
    sz = strlen(name) + 1;
    memmove(ptr, name, sz);
    name = ptr;
    ptr += sz;

    for (size_t i = 0; i < sc.attrs_sz; ++i)
    {
        m_attrs[i].type = sc.attrs[i].type;
        sz = strlen(sc.attrs[i].name) + 1;
        memmove(ptr, sc.attrs[i].name, sz);
        m_attrs[i].name = ptr;
        ptr += sz;
    }

    sc.attrs = m_attrs.get();

    for (size_t i = 0; i < indices.size(); ++i)
    {
        memmove(ptr, indices[i].extra.data(), indices[i].extra.size());
        indices[i].extra = e::slice(ptr, indices[i].extra.size());
        ptr += indices[i].extra.size();
    }
}

e::packer
hyperdex :: operator << (e::packer pa, const space& s)
{
    e::slice name;
    uint16_t num_subspaces = s.subspaces.size();
    uint16_t num_indices = s.indices.size();
    name = e::slice(s.name, strlen(s.name));
    pa = pa << s.id.get() << name << s.fault_tolerance << s.sc.attrs_sz
            << num_subspaces << num_indices;

    for (size_t i = 0; i < s.sc.attrs_sz; ++i)
    {
        name = e::slice(s.sc.attrs[i].name, strlen(s.sc.attrs[i].name));
        pa = pa << name << static_cast<uint16_t>(s.sc.attrs[i].type);
    }

    for (size_t i = 0; i < num_subspaces; ++i)
    {
        pa = pa << s.subspaces[i];
    }

    for (size_t i = 0; i < num_indices; ++i)
    {
        pa = pa << s.indices[i];
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, space& s)
{
    e::slice name;
    std::vector<std::string> strs;
    std::vector<attribute> attrs;
    uint16_t num_subspaces;
    uint16_t num_indices;
    up = up >> s.id >> name >> s.fault_tolerance >> s.sc.attrs_sz
            >> num_subspaces >> num_indices;
    strs.push_back(std::string(name.cdata(), name.size()));
    s.name = strs.back().c_str();

    // Unpack all attributes
    for (size_t i = 0; !up.error() && i < s.sc.attrs_sz; ++i)
    {
        e::slice attr;
        hyperdatatype type;
        up = up >> attr >> type;
        strs.push_back(std::string(attr.cdata(), attr.size()));
        attrs.push_back(attribute(strs.back().c_str(), type));

        if (type == HYPERDATATYPE_MACAROON_SECRET)
        {
            s.sc.authorization = true;
        }
    }

    s.sc.attrs = &attrs.front();
    s.sc.attrs_sz = attrs.size();

    // Unpack subspaces
    s.subspaces.resize(num_subspaces);

    for (size_t i = 0; !up.error() && i < num_subspaces; ++i)
    {
        up = up >> s.subspaces[i];
    }

    // Unpack indices
    s.indices.resize(num_indices);

    for (size_t i = 0; !up.error() && i < num_indices; ++i)
    {
        up = up >> s.indices[i];
    }

    s.reestablish_backing();
    return up;
}

size_t
hyperdex :: pack_size(const space& s)
{
    size_t sz = sizeof(uint64_t) /* id */
              + sizeof(uint32_t) + strlen(s.name) /* name */
              + sizeof(uint64_t) /* fault_tolerance */
              + sizeof(uint16_t) /* sc.attrs_sz */
              + sizeof(uint16_t) /* num subspaces */
              + sizeof(uint16_t); /* num indices */

    for (size_t i = 0; i < s.sc.attrs_sz; ++i)
    {
        sz += sizeof(uint32_t) + strlen(s.sc.attrs[i].name)
            + sizeof(uint16_t);
    }

    for (size_t i = 0; i < s.subspaces.size(); ++i)
    {
        sz += pack_size(s.subspaces[i]);
    }

    for (size_t i = 0; i < s.indices.size(); ++i)
    {
        sz += pack_size(s.indices[i]);
    }

    return sz;
}

subspace :: subspace()
    : id()
    , attrs()
    , regions()
{
}

subspace :: subspace(const subspace& other)
    : id(other.id)
    , attrs(other.attrs)
    , regions(other.regions)
{
}

subspace :: ~subspace() throw ()
{
}

subspace&
subspace :: operator = (const subspace& rhs)
{
    id = rhs.id;
    attrs = rhs.attrs;
    regions = rhs.regions;
    return *this;
}

e::packer
hyperdex :: operator << (e::packer pa, const subspace& s)
{
    uint16_t num_attrs = s.attrs.size();
    uint32_t num_regions = s.regions.size();
    pa = pa << s.id.get() << num_attrs << num_regions;

    for (size_t i = 0; i < num_attrs; ++i)
    {
        pa = pa << s.attrs[i];
    }

    for (size_t i = 0; i < num_regions; ++i)
    {
        pa = pa << s.regions[i];
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, subspace& s)
{
    uint64_t id;
    uint16_t num_attrs;
    uint32_t num_regions;
    up = up >> id >> num_attrs >> num_regions;
    s.id = subspace_id(id);
    s.attrs.clear();
    s.regions.resize(num_regions);

    for (size_t i = 0; !up.error() && i < num_attrs; ++i)
    {
        uint16_t attr;
        up = up >> attr;
        s.attrs.push_back(attr);
    }

    for (size_t i = 0; !up.error() && i < num_regions; ++i)
    {
        up = up >> s.regions[i];
    }

    return up;
}

size_t
hyperdex :: pack_size(const subspace& s)
{
    size_t sz = sizeof(uint64_t) /* id */
              + sizeof(uint16_t) /* num_attrs */
              + sizeof(uint32_t) /* num_regions */
              + sizeof(uint16_t) * s.attrs.size();

    for (size_t i = 0; i < s.regions.size(); ++i)
    {
        sz += pack_size(s.regions[i]);
    }

    return sz;
}

region :: region()
    : id()
    , lower_coord()
    , upper_coord()
    , replicas()
{
}

region :: region(const region& other)
    : id(other.id)
    , lower_coord(other.lower_coord)
    , upper_coord(other.upper_coord)
    , replicas(other.replicas)
{
}

region :: ~region() throw ()
{
}

region&
region :: operator = (const region& rhs)
{
    id = rhs.id;
    lower_coord = rhs.lower_coord;
    upper_coord = rhs.upper_coord;
    replicas = rhs.replicas;
    return *this;
}

e::packer
hyperdex :: operator << (e::packer pa, const region& r)
{
    uint16_t num_hashes = r.lower_coord.size();
    uint8_t num_replicas = r.replicas.size();
    pa = pa << r.id.get() << num_hashes << num_replicas;

    for (size_t i = 0; i < num_hashes; ++i)
    {
        pa = pa << r.lower_coord[i] << r.upper_coord[i];
    }

    for (size_t i = 0; i < num_replicas; ++i)
    {
        pa = pa << r.replicas[i];
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, region& r)
{
    uint64_t id;
    uint16_t num_hashes;
    uint8_t num_replicas;
    up = up >> id >> num_hashes >> num_replicas;
    r.id = region_id(id);
    r.lower_coord.resize(num_hashes);
    r.upper_coord.resize(num_hashes);
    r.replicas.resize(num_replicas);

    for (size_t i = 0; !up.error() && i < num_hashes; ++i)
    {
        up = up >> r.lower_coord[i] >> r.upper_coord[i];
    }

    for (size_t i = 0; !up.error() && i < num_replicas; ++i)
    {
        up = up >> r.replicas[i];
    }

    return up;
}

size_t
hyperdex :: pack_size(const region& r)
{
    size_t sz = sizeof(uint64_t) /* id */
              + sizeof(uint16_t) /* num_hashes */
              + sizeof(uint8_t) /* num_replicas */
              + 2 * sizeof(uint64_t) * r.lower_coord.size();

    for (size_t i = 0; i < r.replicas.size(); ++i)
    {
        sz += pack_size(r.replicas[i]);
    }

    return sz;
}

replica :: replica()
    : si()
    , vsi()
{
}

replica :: replica(const hyperdex::server_id& s,
                   const hyperdex::virtual_server_id& v)
    : si(s)
    , vsi(v)
{
}

replica :: replica(const replica& other)
    : si(other.si)
    , vsi(other.vsi)
{
}

replica :: ~replica() throw ()
{
}

replica&
replica :: operator = (const replica& rhs)
{
    si = rhs.si;
    vsi = rhs.vsi;
    return *this;
}

e::packer
hyperdex :: operator << (e::packer pa, const replica& r)
{
    return pa << r.si.get() << r.vsi.get();
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, replica& r)
{
    uint64_t si;
    uint64_t vsi;
    up = up >> si >> vsi;
    r.si = server_id(si);
    r.vsi = virtual_server_id(vsi);
    return up;
}

size_t
hyperdex :: pack_size(const replica&)
{
    return sizeof(uint64_t) + sizeof(uint64_t);
}
