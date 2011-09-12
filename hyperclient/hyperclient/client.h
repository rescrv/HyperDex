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

#ifndef hyperclient_client_h_
#define hyperclient_client_h_

// STL
#include <tr1/functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>

// HyperClient
#include <hyperclient/returncode.h>
#include <hyperclient/result.h>

namespace hyperclient
{

class client
{
    public:
        client(po6::net::location coordinator);
        ~client() throw ();

    public:
        returncode connect();

    public:
        void get(const std::string& space, const e::buffer& key,
                 std::tr1::function<void (returncode, const std::vector<e::buffer>&)> callback);
        void put(const std::string& space, const e::buffer& key,
                 const std::vector<e::buffer>& value,
                 std::tr1::function<void (returncode)> callback);
        void del(const std::string& space, const e::buffer& key,
                 std::tr1::function<void (returncode)> callback);
        void update(const std::string& space, const e::buffer& key,
                    const std::map<std::string, e::buffer>& value,
                    std::tr1::function<void (returncode)> callback);
        void search(const std::string& space,
                    const std::map<std::string, e::buffer>& params,
                    std::tr1::function<void (returncode,
                                             const e::buffer&,
                                             const std::vector<e::buffer>&)> callback);
        void search(const std::string& space,
                    const std::map<std::string, e::buffer>& params,
                    std::tr1::function<void (returncode,
                                             const e::buffer&,
                                             const std::vector<e::buffer>&)> callback,
                    uint16_t subspace_hint);
        size_t outstanding();
        returncode flush(int timeout);
        returncode flush_one(int timeout);

    // Used for SWIG bindings where we don't want to write C callbacks.
    public:
        void get(const std::string& space, const e::buffer& key, get_result& r);
        void put(const std::string& space, const e::buffer& key,
                 const std::vector<e::buffer>& value, result& r);
        void del(const std::string& space, const e::buffer& key, result& r);
        void update(const std::string& space, const e::buffer& key,
                    const std::map<std::string, e::buffer>& value, result& r);

    private:
        friend class pending_search;

    private:
        void inner_mutate(result& r, returncode ret);
        void inner_get(get_result& r, returncode ret, const std::vector<e::buffer>& value);

    private:
        struct priv;
        std::auto_ptr<priv> p;
};

} // namespace hyperclient

#endif // hyperclient_client_h_
