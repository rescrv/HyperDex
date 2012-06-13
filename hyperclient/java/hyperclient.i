/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

%module hyperclient

%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%include "std_pair.i"

%include "enums.swg"
%javaconst(1);

%{
#include "hyperclient/java/syncclient.h"
#include <utility>
%}

typedef uint16_t in_port_t;

%pragma(java) jniclasscode=
%{
    static
    {
        System.loadLibrary("hyperclient-java");
    }
%}

enum ReturnCode
{
    SUCCESS      = 8448,
    NOTFOUND     = 8449,
    SEARCHDONE   = 8450,
    CMPFAIL      = 8451,
    UNKNOWNSPACE = 8512,
    COORDFAIL    = 8513,
    SERVERERROR  = 8514,
    CONNECTFAIL  = 8515,
    DISCONNECT   = 8516,
    RECONFIGURE  = 8517,
    LOGICERROR   = 8518,
    TIMEOUT      = 8519,
    UNKNOWNATTR  = 8520,
    DUPEATTR     = 8521,
    SEEERRNO     = 8522,
    NONEPENDING  = 8523,
    DONTUSEKEY   = 8524,
    WRONGTYPE    = 8525,
    EXCEPTION    = 8574,
    ZERO         = 8575
};

/* Taken from hyperdex.h */
enum hyperdatatype
{
    HYPERDATATYPE_STRING    = 8960,
    HYPERDATATYPE_INT64     = 8961,

    HYPERDATATYPE_LIST_GENERIC = 8976,
    HYPERDATATYPE_LIST_STRING  = 8977,
    HYPERDATATYPE_LIST_INT64   = 8978,

    HYPERDATATYPE_SET_GENERIC = 8992,
    HYPERDATATYPE_SET_STRING  = 8993,
    HYPERDATATYPE_SET_INT64   = 8994,

    HYPERDATATYPE_MAP_GENERIC       = 9008,
    HYPERDATATYPE_MAP_STRING_STRING = 9009,
    HYPERDATATYPE_MAP_STRING_INT64  = 9010,
    HYPERDATATYPE_MAP_INT64_STRING  = 9011,
    HYPERDATATYPE_MAP_INT64_INT64   = 9012,

    HYPERDATATYPE_GARBAGE   = 9087
};

class HyperClient
{
    public:
        HyperClient(const char* coordinator, in_port_t port);
        ~HyperClient() throw ();

    public:
        ReturnCode get(const std::string& space,
                       const std::string& key,
                       std::map<std::string, std::string>* STR_OUT,
                       std::map<std::string, uint64_t>* NUM_OUT);

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
                                std::vector<std::map<std::string, std::string> >* STR_OUT,
                                std::vector<std::map<std::string, uint64_t> >* NUM_OUT);

        ReturnCode search(const std::string& space,
                          const std::map<std::string, pair<std::string, hyperdatatype> >& eq_attr,
                          const std::map<std::string, pair<uint64_t, uint64_t> >& rn_attr,
                          std::vector<std::map<std::string, std::string> >* sresults,
                          std::vector<std::map<std::string, uint64_t> >* nresults);

    private:
        hyperclient m_client;
};

namespace std
{
    %template(ssmap) map<string, string>;
    %template(snmap) map<string, unsigned long long>;
    %template(ssearchresult) vector<map<string, string> >;
    %template(nsearchresult) vector<map<string, unsigned long long> >;
    %template(val_type) pair<string, hyperdatatype>;
    %template(range) pair<unsigned long long, unsigned long long>;
    %template(eq_pmap) map<string, pair<string, hyperdatatype> >;
    %template(rn_pmap) map<string, pair<unsigned long long, unsigned long long> >;
}

