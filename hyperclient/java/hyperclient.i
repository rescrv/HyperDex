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

%include "std_string.i"
%include "stdint.i"

%include "enums.swg"
%javaconst(1);

%{
#include <limits.h>
#include <cstdlib>
#include "hyperclient/hyperclient.h"
typedef hyperclient_attribute* hyperclient_attribute_asterisk;
typedef hyperclient_range_query* hyperclient_range_query_asterisk;
%}

typedef uint16_t in_port_t;
typedef hyperclient_attribute* hyperclient_attribute_asterisk;
typedef hyperclient_range_query* hyperclient_range_query_asterisk;

%pragma(java) jniclasscode=
%{
    static
    {
        System.loadLibrary("hyperclient-java");
    }
%}

%include "cpointer.i"

// Using typedef hyperclient_attribute_asterisk instead of hyperclient_attribute*
// as the latter confuses SWIG when trying to define SWIG pointer handling macros
// on something that is already a pointer.
// hyperclient_attribute_asterisk - c++ typedef for hyperclient_attribute* type
// hyperclient_attribute_ptr - the java name I chose to represent the resulting:
//                             hyperclient_attribute_asterisk*
//                             = hyperclient_attribute** 
//                             To wit, in java hyperclient_attribute is already
//                             a pointer, so the name hyperclient_attribute_ptr is
//                             essentially a pointer to this java-transparent pointer.  
%pointer_functions(hyperclient_attribute_asterisk,hyperclient_attribute_ptr);

// Likewise for hyperclient_range_query*
%pointer_functions(hyperclient_range_query_asterisk,hyperclient_range_query_ptr);

// A couple more c++ pointer handling macros I will need
//
%pointer_functions(size_t, size_t_ptr);
%pointer_functions(hyperclient_returncode, rc_ptr);
%pointer_functions(uint64_t, uint64_t_ptr);

%include "proxies/hyperclient_attribute.i"
%include "proxies/hyperclient_map_attribute.i"
%include "proxies/hyperclient_range_query.i"
%include "proxies/HyperClient.i"

%apply (char *STRING, int LENGTH) { (const char *key, size_t key_sz) }
%apply (char *STRING, int LENGTH) { (const char *attr, size_t attr_sz) }
%apply (char *STRING, int LENGTH) { (char *name, size_t name_sz) }
%apply (char *STRING, int LENGTH) { (char *value, size_t value_sz) }
%apply (char *STRING, int LENGTH) { (const char *map_key, size_t map_key_sz) }
%apply (char *STRING, int LENGTH) { (const char *value, size_t value_sz) }


// Pertaining to the include of hyperdex.h and hyperclient/hyperclient.h below:

// Ignore everything
%ignore "";

// Un-ignore a couple of enums
%rename("%s") "hyperclient_returncode";
%rename("%s") "hyperdatatype";
%rename("%s", %$isenumitem) "";

// Un-ignore some classes I want to proxy in java
%rename("%s") "hyperclient_attribute";
%rename("%s") "hyperclient_map_attribute";
%rename("%s") "hyperclient_range_query";
%rename("%s",%$isvariable) "";

// Un-ignore the only needed C function
%rename("%s") "hyperclient_destroy_attrs";

%rename("HyperClient",%$isclass) "hyperclient";
%rename("%s", %$ismember) "";

%include "hyperdex.h"
%include "hyperclient/hyperclient.h"
