/* Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_hyperspace_builder_h_
#define hyperdex_hyperspace_builder_h_

/* C */
#include <stdint.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperspace;

/* hyperspace_returncode occupies [8576, 8704) */
enum hyperspace_returncode
{
    HYPERSPACE_SUCCESS       = 8576,
    HYPERSPACE_INVALID_NAME  = 8577,
    HYPERSPACE_INVALID_TYPE  = 8578,
    HYPERSPACE_DUPLICATE     = 8579,
    HYPERSPACE_IS_KEY        = 8580,
    HYPERSPACE_UNKNOWN_ATTR  = 8581,
    HYPERSPACE_NO_SUBSPACE   = 8582,
    HYPERSPACE_OUT_OF_BOUNDS = 8583,
    HYPERSPACE_UNINDEXABLE   = 8584,

    HYPERSPACE_GARBAGE       = 8703
};

struct hyperspace*
hyperspace_create();
struct hyperspace*
hyperspace_parse(const char* desc);
void
hyperspace_destroy(struct hyperspace* space);

const char*
hyperspace_error(struct hyperspace* space);

enum hyperspace_returncode
hyperspace_set_name(struct hyperspace* space, const char* name);

enum hyperspace_returncode
hyperspace_set_key(struct hyperspace* space,
                   const char* attr,
                   enum hyperdatatype datatype);

enum hyperspace_returncode
hyperspace_add_attribute(struct hyperspace* space,
                         const char* attr,
                         enum hyperdatatype datatype);

enum hyperspace_returncode
hyperspace_add_subspace(struct hyperspace* space);

enum hyperspace_returncode
hyperspace_add_subspace_attribute(struct hyperspace* space, const char* attr);

enum hyperspace_returncode
hyperspace_add_index(struct hyperspace* space, const char* attr);

enum hyperspace_returncode
hyperspace_set_fault_tolerance(struct hyperspace* space, uint64_t num);

enum hyperspace_returncode
hyperspace_set_number_of_partitions(struct hyperspace* space, uint64_t num);

enum hyperspace_returncode
hyperspace_use_authorization(struct hyperspace* space);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_hyperspace_builder_h_ */
