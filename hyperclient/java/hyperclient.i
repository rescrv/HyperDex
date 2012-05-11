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

%include "enums.swg"
%javaconst(1);

%{
#include "hyperclient/java/javaclient.h"
%}

typedef uint16_t in_port_t;

%pragma(java) jniclasscode=
%{
    static
    {
        System.loadLibrary("hyperclient-java");
    }
%}

/*
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
*/

%include "HyperClient.i"

%include "hyperclient/java/javaclient.h"

namespace std
{
    %template(Attributes) map<string, Attribute*>;
    %template(SSMap) map<string, string>;
    %template(SIMap) map<string, long long>;
    %template(ISMap) map<long long, string>;
    %template(IIMap) map<long long, long long>;
    %template(SearchResult) vector<map<string, Attribute*> >;
}
