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
#include <hyperspacehashing/equality_wildcard.h>
#include <hyperspacehashing/mask.h>
#include <hyperspacehashing/prefix.h>

// HyperDex
#include <hyperdex/ids.h>
#include <hyperdex/instance.h>

namespace hyperdex
{

class configuration
{
    public:
        const static spaceid NULLSPACE;
        const static instance NULLINSTANCE;
        const static uint32_t CLIENTSPACE;

    public:
        configuration();

    public:
        bool add_line(const std::string& line);

    public:
        spaceid lookup_spaceid(const std::string& space) const;
        std::vector<std::string> lookup_space_dimensions(spaceid space) const;

    public:
        entityid headof(const regionid& r) const;
        entityid tailof(const regionid& r) const;
        instance instancefor(const entityid& e) const;

    // Hashing-related functions.
    public:
        bool point_leader_entity(const spaceid& space, const e::buffer& key,
                                 hyperdex::entityid* ent, hyperdex::instance* inst);
        std::map<entityid, instance> search_entities(const spaceid& space,
                                                     const hyperspacehashing::equality_wildcard& wc) const;
        std::map<entityid, instance> search_entities(const subspaceid& subspace,
                                                     const hyperspacehashing::equality_wildcard& wc) const;

    // Everything public below this line is deprecated.
    // XXX Remove stuff below this line in favor of easier APIs.
        instance lookup(const entityid& e) const { return instancefor(e); }
    public:
        // Return the number of subspaces within the space.
        bool subspaces(const spaceid& s, size_t* sz) const;

        // Returns the number of dimensions (including the key) which describe a
        // point in the space.
        bool dimensionality(const spaceid& s, size_t* sz) const;

        // Return the number of dimensions  which describe a point within a
        // region.  The number of dimensions which describe the point within a
        // region are defined to be those dimension from the parent space which
        // were specified at creation time.  Example:
        //
        // Config:
        //      space   0x0 kv  key value1  value2
        //      subspace    kv  0   key
        //      subspace    kv  1   value1  value2
        //
        // Calls:
        //      dimensionality(subspace(0x0, 0)) == 1
        //      dimensionality(subspace(0x0, 1)) == 2
        bool dimensionality(const subspaceid& r, size_t* sz) const;

        // Return a bitmask indicating which dimensions of a space are used to
        // create the subspace.  If the subspace doesn't exist, an empty vector
        // is returned.
        bool dimensions(const subspaceid& ss, std::vector<bool>* dims) const;

        // Return the set of transfers in progress to a particular host.
        std::map<uint16_t, regionid> transfers_to(const instance& i) const;
        std::map<uint16_t, regionid> transfers_from(const instance& i) const;

        // Perhaps these should be a little less transparent.
        const std::map<spaceid, std::vector<std::string> >& spaces() const { return m_spaces; }
        const std::map<entityid, instance>& entity_mapping() const { return m_entities; }
        const std::map<std::string, spaceid>& space_assignment() const { return m_space_assignment; }
        std::map<regionid, size_t> regions() const;
        std::set<instance> hosts() const;

    // Below this line is not deprecated.
    private:
        std::map<entityid, instance> _search_entities(std::map<entityid, instance>::const_iterator start,
                                                      std::map<entityid, instance>::const_iterator end,
                                                      const hyperspacehashing::equality_wildcard& terms) const;

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
    lhs << "instance(" << rhs.inbound << ", " << rhs.inbound_version
        << rhs.outbound << ", " << rhs.outbound_version << ")";
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_configuration_h_
