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

#ifndef hyperclient_sync_client_h_
#define hyperclient_sync_client_h_

// STL
#include <map>
#include <string>
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>

// HyperClient
#include <hyperclient/returncode.h>

namespace hyperclient
{

class search_results;

class sync_client
{
    public:
        static sync_client* create(po6::net::location coordinator);

    public:
        virtual ~sync_client() throw ();

    public:
        virtual returncode connect() = 0;

    public:
        virtual returncode get(const std::string& space, const e::buffer& key,
                               std::vector<e::buffer>* value) = 0;
        virtual returncode put(const std::string& space, const e::buffer& key,
                               const std::vector<e::buffer>& value) = 0;
        virtual returncode del(const std::string& space, const e::buffer& key) = 0;
        virtual returncode update(const std::string& space, const e::buffer& key,
                                  const std::map<std::string, e::buffer>& value) = 0;
        virtual search_results* search(const std::string& space,
                                       const std::map<std::string, e::buffer>& params,
                                       returncode* ret) = 0;
};

class search_results
{
    public:
        ~search_results() throw ();

    public:
        bool valid();
        returncode next();
        const e::buffer& key();
        const std::vector<e::buffer>& value();

    protected:
        search_results();
};

} // namespace hyperclient

#endif // hyperclient_sync_client_h_
