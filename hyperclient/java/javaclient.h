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

typedef hyperclient_attribute* hyperclient_attribute_asterisk;

class HyperClient
{
    public:
        HyperClient(const char* coordinator, in_port_t port);
        ~HyperClient() throw ();

    public:
        static std::string read_attr_name(hyperclient_attribute *ha);
        static void read_attr_value(hyperclient_attribute *ha,
                                      char *value, size_t value_sz,
                                      size_t pos);

        int64_t loop(int *i_rc);

        static hyperclient_attribute *alloc_attrs(size_t attrs_sz);
        static void destroy_attrs(hyperclient_attribute *attrs, size_t attrs_sz);

        // Returns 1 on success. ha->attr will point to allocated memory
        // Returns 0 on failure. ha->attr will be NULL
        static int write_attr_name(hyperclient_attribute *ha,
                                   const char *attr, size_t attr_sz,
                                   hyperdatatype type);

        // Returns 1 on success. ha->value will point to allocated memory
        //                       ha->value_sz will hold the size of this memory
        // Returns 0 on failure. ha->value will be NULL
        //                       ha->value_sz will be 0
        //
        // If ha->value is already non-NULL, then we're appending to it. 
        static int write_attr_value(hyperclient_attribute *ha,
                                    const char *value, size_t value_sz);

        static hyperclient_attribute *get_attr(hyperclient_attribute *ha, size_t i);

        int64_t get(const std::string& space,
                       const std::string& key,
                       int *i_rc,
                       hyperclient_attribute **attrs, size_t *attrs_sz);

        int64_t put(const std::string& space,
                       const std::string& key,
                       const hyperclient_attribute *attrs, size_t attrs_sz, int *i_rc);

    private:
        hyperclient *m_client;
        static void destroy_attr_value(hyperclient_attribute *ha);
};

#endif // hyperclient_javaclient_h_
