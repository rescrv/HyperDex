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

#ifndef hyperclient_javaclient_h_
#define hyperclient_javaclient_h_

// HyperClient
#include "../hyperclient.h"

typedef hyperclient_attribute* phyperclient_attribute;

class HyperClient
{
    public:
        HyperClient(const char* coordinator, in_port_t port);
        ~HyperClient() throw ();

    public:
        static std::string read_attr_name(hyperclient_attribute *ha);
        static int read_value(hyperclient_attribute *ha, char *value, int value_sz);

        static void set_attribute(hyperclient_attribute *ha,
                           char *attr,
                           char *value, int value_sz,
                           hyperdatatype type);

        static hyperclient_attribute *get_attribute(hyperclient_attribute *ha, int i);

        hyperclient_returncode get(const std::string& space,
                       const std::string& key,
                       hyperclient_attribute **attrs, int *attrs_sz);

        hyperclient_returncode put(const std::string& space,
                       const std::string& key,
                       const hyperclient_attribute *attrs, int attrs_sz);

    private:
        hyperclient m_client;
        static int read(const char *memb, int memb_sz, char *ret, int ret_sz);
};

#endif // hyperclient_javaclient_h_
