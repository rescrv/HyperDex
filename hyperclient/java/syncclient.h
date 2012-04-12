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

#ifndef hyperclient_syncclient_h_
#define hyperclient_syncclient_h_

// HyperClient
#include "../hyperclient.h"

typedef hyperclient_returncode ReturnCode;

class HyperClient
{
    public:
        HyperClient(const char* coordinator, in_port_t port);
        ~HyperClient() throw ();

    public:
        ReturnCode get(const std::string& space,
                       const std::string& key,
                       std::map<std::string, std::string>* svalues,
                       std::map<std::string, uint64_t>* nvalues);
        ReturnCode put(const std::string& space,
                       const std::string& key,
                       const std::map<std::string, std::string>& svalues,
                       const std::map<std::string, uint64_t>& nvalues);
        ReturnCode del(const std::string& space,
                       const std::string& key);
        ReturnCode range_search(const std::string& space,
                                const std::string& attr,
                                uint64_t lower,
                                uint64_t upper,
                                std::vector<std::map<std::string,
                                            std::string> >* sresults,
                                std::vector<std::map<std::string,
                                            uint64_t> >* nresults);

    private:
        hyperclient m_client;
};

#endif // hyperclient_syncclient_h_
