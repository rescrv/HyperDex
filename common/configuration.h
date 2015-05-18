// Copyright (c) 2011-2012, Cornell University
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

#ifndef hyperdex_common_configuration_h_
#define hyperdex_common_configuration_h_

// STL
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/array_ptr.h>
#include <e/slice.h>

// HyperDex
#include "namespace.h"
#include "common/attribute.h"
#include "common/attribute_check.h"
#include "common/hyperspace.h"
#include "common/ids.h"
#include "common/schema.h"
#include "common/server.h"
#include "common/transfer.h"

BEGIN_HYPERDEX_NAMESPACE

class configuration
{
    public:
        configuration();
        configuration(const configuration& other);
        ~configuration() throw ();

    // configuration metadata
    public:
        uint64_t cluster() const;
        uint64_t version() const;
        bool read_only() const;

    // membership metadata
    public:
        void get_all_addresses(std::vector<std::pair<server_id, po6::net::location> >* addrs) const;
        bool exists(const server_id& id) const;
        po6::net::location get_address(const server_id& id) const;
        server::state_t get_state(const server_id& id) const;
        region_id get_region_id(const virtual_server_id& id) const;
        server_id get_server_id(const virtual_server_id& id) const;

    // hyperspace metadata
    public:
        const schema* get_schema(const char* space) const;
        const schema* get_schema(const region_id& ri) const;
        const subspace* get_subspace(const region_id& ri) const;
        virtual_server_id get_virtual(const region_id& ri, const server_id& si) const;
        subspace_id subspace_of(const region_id& ri) const;
        subspace_id subspace_prev(const subspace_id& ss) const;
        subspace_id subspace_next(const subspace_id& ss) const;
        virtual_server_id head_of_region(const region_id& ri) const;
        virtual_server_id tail_of_region(const region_id& ri) const;
        virtual_server_id next_in_region(const virtual_server_id& vsi) const;
        void point_leaders(const server_id& s, std::vector<region_id>* servers) const;
        void key_regions(const server_id& s, std::vector<region_id>* servers) const;
        bool is_point_leader(const virtual_server_id& e) const;
        virtual_server_id point_leader(const char* space, const e::slice& key) const;
        // point leader for this key in the same space as ri
        virtual_server_id point_leader(const region_id& ri, const e::slice& key) const;
        // lhs and rhs are in adjacent subspaces such that lhs sends CHAIN_PUT
        // to rhs and rhs sends CHAIN_ACK to lhs
        bool subspace_adjacent(const virtual_server_id& lhs, const virtual_server_id& rhs) const;
        // mapped regions -- regions mapped for server "us"
        void mapped_regions(const server_id& s, std::vector<region_id>* servers) const;

    // index metadata
    public:
        const index* get_index(const index_id& ii) const;
        void all_indices(const server_id& s, std::vector<std::pair<region_id, index_id> >* indices) const;

    // transfers
    public:
        bool is_server_involved_in_transfer(const server_id& si, const region_id& ri) const;
        bool is_server_blocked_by_live_transfer(const server_id& si, const region_id& ri) const;
        bool is_transfer_live(const transfer_id& tid) const;
        void transfers_in(const server_id& s, std::vector<transfer>* transfers) const;
        void transfers_out(const server_id& s, std::vector<transfer>* transfers) const;
        void transfers_in_regions(const server_id& s, std::vector<region_id>* transfers) const;
        void transfers_out_regions(const server_id& s, std::vector<region_id>* transfers) const;

    // hashing functions
    public:
        void lookup_region(const subspace_id& subspace,
                           const std::vector<uint64_t>& hashes,
                           region_id* region) const;
        void lookup_search(const char* space,
                           const std::vector<attribute_check>& chks,
                           std::vector<virtual_server_id>* servers) const;

    public:
        std::string dump() const;

        // List all spaces separated by \n
        std::string list_spaces() const;

        // List all suspaces of a space
        // Attributes will be separated by a comma
        // Spaces will be separated by a new line
        std::string list_subspaces(const char* space) const;

        // List all indexes of a space
        // Indices will be seperate by a new line
        // The output for each index is:
        // <id>:<attribute>
        std::string list_indices(const char* space) const;

    public:
        configuration& operator = (const configuration& rhs);

    private:
        void refill_cache();
        friend size_t pack_size(const configuration&);
        friend e::packer operator << (e::packer, const configuration& s);
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        typedef std::pair<uint64_t, uint64_t> pair_uint64_t;
        typedef std::pair<uint64_t, schema*> uint64_schema_t;
        typedef std::pair<uint64_t, subspace*> uint64_subspace_t;
        typedef std::pair<uint64_t, po6::net::location> uint64_location_t;

    private:
        uint64_t m_cluster;
        uint64_t m_version;
        uint64_t m_flags;
        std::vector<server> m_servers;
        std::vector<pair_uint64_t> m_region_ids_by_virtual;
        std::vector<pair_uint64_t> m_server_ids_by_virtual;
        std::vector<uint64_schema_t> m_schemas_by_region;
        std::vector<uint64_subspace_t> m_subspaces_by_region;
        std::vector<pair_uint64_t> m_subspace_ids_by_region;
        std::vector<pair_uint64_t> m_subspace_ids_for_prev;
        std::vector<pair_uint64_t> m_subspace_ids_for_next;
        std::vector<pair_uint64_t> m_heads_by_region;
        std::vector<pair_uint64_t> m_tails_by_region;
        std::vector<pair_uint64_t> m_next_by_virtual;
        std::vector<uint64_t> m_point_leaders_by_virtual;
        std::vector<space> m_spaces;
        std::vector<transfer> m_transfers;
};

e::packer
operator << (e::packer, const configuration& c);
e::unpacker
operator >> (e::unpacker, configuration& c);
size_t
pack_size(const configuration&);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_configuration_h_
