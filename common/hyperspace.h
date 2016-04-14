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

#ifndef hyperdex_common_hyperspace_h_
#define hyperdex_common_hyperspace_h_

// STL
#include <vector>

// e
#include <e/array_ptr.h>
#include <e/buffer.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/index.h"
#include "common/schema.h"

BEGIN_HYPERDEX_NAMESPACE
class space;
class subspace;
class region;
class replica;

class space
{
public:
	space();
	space(const char *name, const schema &sc);
	space(const space &);
	~space() throw ();

public:
	bool validate() const;
	void reestablish_backing();

public:
	space &operator = (const space &);

public:
	space_id id;
	const char *name;
	uint64_t fault_tolerance;
	uint64_t predecessor_width;
	hyperdex::schema sc;
	std::vector<subspace> subspaces;
	std::vector<index> indices;

	const attribute &get_attribute(uint16_t index) const;

private:
	friend e::packer operator << (e::packer, const space &s);
	friend e::unpacker operator >> (e::unpacker, space &s);
	friend size_t pack_size(const space &);

private:
	e::array_ptr<char> m_c_strs;
	e::array_ptr<attribute> m_attrs;

};

inline const attribute &space::get_attribute(uint16_t index) const
{
	return m_attrs[index];
}

e::packer
operator << (e::packer, const space &s);
e::unpacker
operator >> (e::unpacker, space &s);
size_t
pack_size(const space &s);

class subspace
{
public:
	subspace();
	subspace(const subspace &);
	~subspace() throw ();

public:
	subspace &operator = (const subspace &);

public:
	subspace_id id;
	std::vector<uint16_t> attrs;
	std::vector<region> regions;
};

e::packer
operator << (e::packer, const subspace &s);
e::unpacker
operator >> (e::unpacker, subspace &s);
size_t
pack_size(const subspace &s);

class region
{
public:
	region();
	region(const region &);
	~region() throw ();

public:
	region &operator = (const region &);

public:
	region_id id;
	std::vector<uint64_t> lower_coord;
	std::vector<uint64_t> upper_coord;
	std::vector<replica> replicas;
};

e::packer
operator << (e::packer, const region &r);
e::unpacker
operator >> (e::unpacker, region &r);
size_t
pack_size(const region &r);

class replica
{
public:
	replica();
	replica(const server_id &si,
	        const virtual_server_id &vsi);
	replica(const replica &);
	~replica() throw ();

public:
	replica &operator = (const replica &);

public:
	server_id si;
	virtual_server_id vsi;
};

e::packer
operator << (e::packer, const replica &r);
e::unpacker
operator >> (e::unpacker, replica &r);
size_t
pack_size(const replica &r);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_hyperspace_h_
