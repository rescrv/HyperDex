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

        static std::string read_map_attr_name(hyperclient_map_attribute *hma);

        static std::string read_range_query_attr_name(hyperclient_range_query *rq);

        int64_t loop(hyperclient_returncode *rc);

        static hyperclient_attribute *alloc_attrs(size_t attrs_sz);
        static void destroy_attrs(hyperclient_attribute *attrs, size_t attrs_sz);

        static hyperclient_map_attribute *alloc_map_attrs(size_t attrs_sz);
        static void destroy_map_attrs(hyperclient_map_attribute *attrs,
                                                        size_t attrs_sz);

        static hyperclient_range_query *alloc_range_queries(size_t rqs_sz);
        static void destroy_range_queries(hyperclient_range_query *rqs,
                                                        size_t rqs_sz);

        // Returns 1 on success. ha->attr will point to allocated memory
        // Returns 0 on failure. ha->attr will be NULL
        static int write_attr_name(hyperclient_attribute *ha,
                                   const char *attr, size_t attr_sz,
                                   hyperdatatype type);
        // Returns 1 on success. hma->attr will point to allocated memory
        // Returns 0 on failure. hma->attr will be NULL
        static int write_map_attr_name(hyperclient_map_attribute *hma,
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
        // Returns 1 on success. hma->map_key will point to allocated memory
        //                       hma->map_key_sz will hold the size of this memory
        // Returns 0 on failure. hma->map_key will be NULL
        //                       hma->map_key_sz will be 0
        //
        // If hma->value is already non-NULL, then we're appending to it. 
        static int write_map_attr_map_key(hyperclient_map_attribute *hma,
                                          const char *map_key, size_t map_key_sz);

        // Returns 1 on success. hma->value will point to allocated memory
        //                       hma->value_sz will hold the size of this memory
        // Returns 0 on failure. hma->value will be NULL
        //                       hma->value_sz will be 0
        //
        // If hma->value is already non-NULL, then we're appending to it. 
        static int write_map_attr_value(hyperclient_map_attribute *hma,
                                        const char *value, size_t value_sz);


        static int write_range_query(hyperclient_range_query *rq,
                                     const char *attr, size_t attr_sz,
                                     int64_t lower,
                                     int64_t upper);

        static hyperclient_attribute *get_attr(hyperclient_attribute *ha, size_t i);
        static hyperclient_map_attribute *get_map_attr(
                                            hyperclient_map_attribute *hma,
                                            size_t i);
        static hyperclient_range_query *get_range_query(
                                            hyperclient_range_query *rqs,
                                            size_t i);

        hyperclient *get_hyperclient();

    private:
        hyperclient *m_client;
};

#endif // hyperclient_javaclient_h_
