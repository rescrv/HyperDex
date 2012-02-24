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

#ifndef hyperdex_configuration_h_
#define hyperdex_configuration_h_

// STL
#include <map>
#include <set>
#include <tr1/functional>

// po6
#include <po6/net/location.h>

// HyperspaceHashing
#include <hyperspacehashing/mask.h>
#include <hyperspacehashing/prefix.h>
#include <hyperspacehashing/search.h>

// HyperDex
#include <hyperdex/ids.h>
#include <hyperdex/instance.h>

namespace hyperdex
{

class configuration
{
    public:
        const static uint32_t CLIENTSPACE;
        const static uint32_t TRANSFERSPACE;

    public:
        configuration();
        ~configuration() throw ();

    public:
        bool add_line(const std::string& line);

    // Data-layout (not hashing)
    public:
        size_t dimensions(const spaceid& s) const;
        std::vector<std::string> dimension_names(const spaceid& s) const;
        spaceid space(const char* spacename) const;
        size_t subspaces(const spaceid& s) const;

    // Entity/instance
    public:
        // The entity that corresponds to instance i in region r.
        entityid entityfor(const instance& i, const regionid& r) const;
        // Returns the correct instance or NULLINSTANCE
        instance instancefor(const entityid& e) const;
        // Sets the port versions to the match the given IP/ports, or 0 if there
        // is no instance with the given IP/ports
        void instance_versions(instance* i) const;
        // The set of regions to which an instance is assigned
        std::set<regionid> regions_for(const instance& i) const;
        // Return the containing entity (number is preserved).
        // entityid() is returned if no containing entity is found.
        entityid sloppy_lookup(const entityid& e) const;
        // Is the instance a member of the region's chain?
        bool in_region(const instance& i, const regionid& r) const;
        bool is_client(const entityid& e) const;
        bool is_point_leader(const entityid& e) const;

    // Chain
    public:
        bool chain_adjacent(const entityid& f, const entityid& s) const;
        bool chain_has_next(const entityid& e) const;
        bool chain_has_prev(const entityid& e) const;
        bool is_head(const entityid& e) const { return !chain_has_prev(e); }
        bool is_tail(const entityid& e) const { return !chain_has_next(e); }
        entityid chain_next(const entityid& e) const;
        entityid chain_prev(const entityid& e) const;
        entityid headof(const regionid& r) const;
        entityid tailof(const regionid& r) const;

    // Hashing
    public:
        hyperspacehashing::mask::hasher disk_hasher(const subspaceid& subspace) const;
        hyperspacehashing::prefix::hasher repl_hasher(const subspaceid& subspace) const;
        bool point_leader_entity(const spaceid& space, const e::slice& key,
                                 hyperdex::entityid* ent, hyperdex::instance* inst) const;
        std::map<entityid, instance> search_entities(const spaceid& space,
                                                     const hyperspacehashing::search& s) const;
        std::map<entityid, instance> search_entities(const subspaceid& subspace,
                                                     const hyperspacehashing::search& s) const;

    // Transfers
    public:
        std::map<uint16_t, regionid> transfers_from(const instance& i) const;
        std::map<uint16_t, regionid> transfers_to(const instance& i) const;

    private:
        std::map<entityid, instance> _search_entities(std::map<entityid, instance>::const_iterator start,
                                                      std::map<entityid, instance>::const_iterator end,
                                                      const hyperspacehashing::search& s) const;

    private:
        std::map<std::string, instance> m_hosts;
        std::map<std::string, spaceid> m_space_assignment;

        // Map a spaceid to the strings labeling the dimensions.
        std::map<spaceid, std::vector<std::string> > m_spaces;
        // Map a subspaceid to the bitmask of the dimensions of the space which
        // are used as dimensions in the subspace.
        std::map<subspaceid, std::vector<bool> > m_subspaces;
        // A set of regions.  Elements of this set are guaranteed to be
        // non-overlapping.
        std::set<regionid> m_regions;
        // Map an entity id onto the hyperdex instance.
        std::map<entityid, instance> m_entities;
        // Track currently specified transfers.
        std::map<uint16_t, regionid> m_transfers;
        // Hash-calculating objects that work for the replication layer.
        std::map<subspaceid, hyperspacehashing::prefix::hasher> m_repl_hashers;
        // Hash-calculating objects that work for the disk layer.
        std::map<subspaceid, hyperspacehashing::mask::hasher> m_disk_hashers;
};

inline std::ostream&
operator << (std::ostream& lhs, const instance& rhs)
{
    lhs << "instance(" << rhs.address << ", " << rhs.inbound_version
        << ", " << rhs.outbound_version << ")";
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_configuration_h_
