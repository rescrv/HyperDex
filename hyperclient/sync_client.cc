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

// STL
#include <memory>

#include <glog/logging.h>

// HyperClient
#include <hyperclient/async_client.h>
#include <hyperclient/sync_client.h>

namespace hyperclient
{

class sync_client_impl : public sync_client
{
    public:
        sync_client_impl(po6::net::location coordinator);
        virtual ~sync_client_impl() throw ();

    public:
        virtual returncode connect();

    public:
        virtual returncode get(const std::string& space, const e::buffer& key,
                               std::vector<e::buffer>* value);
        virtual returncode put(const std::string& space, const e::buffer& key,
                               const std::vector<e::buffer>& value);
        virtual returncode del(const std::string& space, const e::buffer& key);
        virtual returncode update(const std::string& space, const e::buffer& key,
                                  const std::map<std::string, e::buffer>& value);
        virtual search_results* search(const std::string& space,
                                       const std::map<std::string, e::buffer>& params,
                                       returncode* ret);

    public:
        void inner(returncode* ret1, returncode ret2)
        { *ret1 = ret2; }
        void inner_w_value(returncode* ret1, std::vector<e::buffer>* value1,
                           returncode ret2, const std::vector<e::buffer>& value2)
        { *ret1 = ret2; *value1 = value2; }

    private:
        friend class search_results;

    private:
        std::auto_ptr<async_client> m_acl;
};

} // hyperclient

hyperclient::sync_client*
hyperclient :: sync_client :: create(po6::net::location coordinator)
{
    return new sync_client_impl(coordinator);
}

hyperclient :: sync_client :: ~sync_client() throw()
{
}

hyperclient :: sync_client_impl :: sync_client_impl(po6::net::location coordinator)
    : m_acl(async_client::create(coordinator))
{
}

hyperclient :: sync_client_impl :: ~sync_client_impl() throw ()
{
}

hyperclient::returncode
hyperclient :: sync_client_impl :: connect()
{
    return m_acl->connect();
}

hyperclient::returncode
hyperclient :: sync_client_impl :: get(const std::string& space,
                                       const e::buffer& key,
                                       std::vector<e::buffer>* value)
{
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    using std::tr1::placeholders::_2;
    hyperclient::returncode ret;
    m_acl->get(space, key, bind(&sync_client_impl::inner_w_value, this, &ret, value, _1, _2));
    m_acl->flush();
    return ret;
}

hyperclient::returncode
hyperclient :: sync_client_impl :: put(const std::string& space,
                                       const e::buffer& key,
                                       const std::vector<e::buffer>& value)
{
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    hyperclient::returncode ret;
    m_acl->put(space, key, value, bind(&sync_client_impl::inner, this, &ret, _1));
    m_acl->flush();
    return ret;
}

hyperclient::returncode
hyperclient :: sync_client_impl :: del(const std::string& space,
                                       const e::buffer& key)
{
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    hyperclient::returncode ret;
    m_acl->del(space, key, bind(&sync_client_impl::inner, this, &ret, _1));
    m_acl->flush();
    return ret;
}

hyperclient::returncode
hyperclient :: sync_client_impl :: update(const std::string& space,
                                          const e::buffer& key,
                                          const std::map<std::string, e::buffer>& value)
{
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    hyperclient::returncode ret;
    m_acl->update(space, key, value, bind(&sync_client_impl::inner, this, &ret, _1));
    m_acl->flush();
    return ret;
}

hyperclient::search_results*
hyperclient :: sync_client_impl :: search(const std::string& space,
                                          const std::map<std::string, e::buffer>& params,
                                          returncode* ret)
{
    assert(false);
}
