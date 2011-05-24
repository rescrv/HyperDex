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
#include <tr1/functional>

// po6
#include <po6/net/location.h>

// HyperDex
#include <hyperdex/datalayer.h>
#include <hyperdex/ids.h>
#include <hyperdex/instance.h>

namespace hyperdex
{

class configuration
{
    public:
        configuration();

    public:
        bool add_line(const std::string& line);

    public:
        const std::map<entityid, instance>& entity_mapping() const { return m_entities; }
        std::map<regionid, size_t> regions() const;

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
