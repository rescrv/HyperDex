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

#ifndef hyperdex_configuration_parser_h_
#define hyperdex_configuration_parser_h_

// HyperDex
#include "hyperdex.h"
#include "hyperdex/hyperdex/attribute.h"
#include "hyperdex/hyperdex/configuration.h"

namespace hyperdex
{

class configuration_parser
{
    public:
        enum error
        {
            CP_SUCCESS,
            CP_DUPE_ATTR,
            CP_DUPE_HOST,
            CP_DUPE_SPACE,
            CP_DUPE_SUBSPACE,
            CP_DUPE_REGION,
            CP_DUPE_ENTITY,
            CP_DUPE_XFER,
            CP_DUPE_VERSION,
            CP_DUPE_QUIESCE_STATE_ID,
            CP_MISSING_SPACE,
            CP_MISSING_SUBSPACE,
            CP_MISSING_REGION,
            CP_OUTOFORDER_SUBSPACE,
            CP_EXCESS_DATA,
            CP_ATTR_NOT_SEARCHABLE,
            CP_MISSING,
            CP_F_FAILURES,
            CP_UNKNOWN_ATTR,
            CP_UNKNOWN_CMD,
            CP_UNKNOWN_HOST,
            CP_UNKNOWN_TYPE,
            CP_BAD_BOOL,
            CP_BAD_IP,
            CP_BAD_UINT64,
            CP_BAD_UINT32,
            CP_BAD_UINT16,
            CP_BAD_UINT8,
            CP_BAD_ATTR_CHOICE,
            CP_BAD_TYPE,
            EOE
        };

    public:
        configuration_parser();
        ~configuration_parser() throw ();

    public:
        configuration generate();
        error parse(const std::string& config);

    private:
        error parse_version(char* start,
                            char* const eol);
        error parse_host(char* start,
                         char* const eol);
        error parse_space(char* start,
                          char* const eol);
        error parse_subspace(char* start,
                             char* const eol);
        error parse_region(char* start,
                           char* const eol);
        error parse_transfer(char* start,
                             char* const eol);
        error parse_quiesce(char* start,
                            char* const eol);
        error parse_shutdown(char* start,
                             char* const eol);
        error extract_bool(char* start,
                           char* end,
                           bool* t);
        error extract_datatype(char* start,
                               char* end,
                               hyperdatatype* t);
        error extract_ip(char* start,
                         char* end,
                         po6::net::ipaddr* ip);
        error extract_uint64_t(char* start,
                               char* end,
                               uint64_t* num);
        error extract_uint32_t(char* start,
                               char* end,
                               uint32_t* num);
        error extract_uint16_t(char* start,
                               char* end,
                               uint16_t* num);
        error extract_uint8_t(char* start,
                              char* end,
                              uint8_t* num);
        std::vector<hyperspacehashing::hash_t>
            attrs_to_hashfuncs(const subspaceid& ssi,
                               const std::vector<bool>& attrs);

    private:
        std::string m_config_text;
        uint64_t m_version;
        std::map<uint64_t, instance> m_hosts;
        std::map<std::string, spaceid> m_space_assignment;
        std::map<spaceid, std::vector<attribute> > m_spaces;
        std::set<subspaceid> m_subspaces;
        std::map<subspaceid, std::vector<bool> > m_repl_attrs;
        std::map<subspaceid, std::vector<bool> > m_disk_attrs;
        std::set<regionid> m_regions;
        std::map<entityid, instance> m_entities;
        std::map<std::pair<instance, uint16_t>, hyperdex::regionid> m_transfers;
        bool m_quiesce;
        std::string m_quiesce_state_id;
        bool m_shutdown;
};

} // namespace hyperdex

#endif // hyperdex_configuration_parser_h_
