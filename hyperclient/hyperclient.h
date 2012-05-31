/* Copyright (c) 2011-2012, Cornell University
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

#ifndef hyperclient_h_
#define hyperclient_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

/* POSIX */
#include <netinet/in.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
// C++
#include <iostream>

// STL
#include <map>
#include <memory>
#include <queue>
#include <vector>

// po6
#include <po6/io/fd.h>
#include <po6/net/location.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>

// Forward declarations
namespace e
{
class bitfield;
} // namespace e
class busybee_st;
namespace hyperdex
{
class attribute;
class configuration;
class coordinatorlink;
class instance;
} // namespace hyperdex

extern "C"
{
#endif /* __cplusplus */

struct hyperclient;

struct hyperclient_attribute
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
};

struct hyperclient_map_attribute
{
    const char* attr; /* NULL-terminated */
    const char* map_key;
    size_t map_key_sz;
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
};

struct hyperclient_range_query
{
    const char* attr;
    uint64_t lower;
    uint64_t upper;
};

/* HyperClient returncode occupies [8448, 8576) */
enum hyperclient_returncode
{
    HYPERCLIENT_SUCCESS      = 8448,
    HYPERCLIENT_NOTFOUND     = 8449,
    HYPERCLIENT_SEARCHDONE   = 8450,
    HYPERCLIENT_CMPFAIL      = 8451,
    HYPERCLIENT_READONLY     = 8452,

    /* Error conditions */
    HYPERCLIENT_UNKNOWNSPACE = 8512,
    HYPERCLIENT_COORDFAIL    = 8513,
    HYPERCLIENT_SERVERERROR  = 8514,
    HYPERCLIENT_POLLFAILED   = 8515,
    HYPERCLIENT_OVERFLOW     = 8516,
    HYPERCLIENT_RECONFIGURE  = 8517,
    HYPERCLIENT_TIMEOUT      = 8519,
    HYPERCLIENT_UNKNOWNATTR  = 8520,
    HYPERCLIENT_DUPEATTR     = 8521,
    HYPERCLIENT_NONEPENDING  = 8523,
    HYPERCLIENT_DONTUSEKEY   = 8524,
    HYPERCLIENT_WRONGTYPE    = 8525,
    HYPERCLIENT_NOMEM        = 8526,

    /* This should never happen.  It indicates a bug */
    HYPERCLIENT_EXCEPTION    = 8574,
    HYPERCLIENT_ZERO         = 8575,

    /* Enums may be initialized to these to trigger assertion failures in the
     * java code.  Initialize hyperclient_returncode statuses to these if you
     * suspect there is a bug.
     */
    HYPERCLIENT_A            = 8576,
    HYPERCLIENT_B            = 8577
};

struct hyperclient*
hyperclient_create(const char* coordinator, in_port_t port);
void
hyperclient_destroy(struct hyperclient* client);

/* All values return a 64-bit integer, which uniquely identifies the request
 * until its completion.  Positive values indicate valid identifiers.  Negative
 * values indicate that the request fails immediately for the reason stored in
 * status.
 *
 * Each call to "hyperclient_loop" will return the identifier corresponding to
 * the request which made progress.  get/put/del requests are completed
 * immediately after the identifier returns.  search requests continue until the
 * status indicates that the search is done.
 */

/* Retrieve the secondary attributes corresponding to "key" in "space".
 *
 * Allocated memory will be returned in *attrs.  This memory *MUST* be freed
 * using hyperclient_attribute_free.
 *
 * - space, key must point to memory that exists for the duration of this call
 * - client, status, attrs, attrs_sz must point to memory that exists until the
 *   request is considered complete
 */
int64_t
hyperclient_get(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, enum hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz);

/* Store the secondary attributes under "key" in "space".
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error.
 *
 * All attributes not specified by the put are left as-is (if the key already
 * exists), or set to "" (if the key doesn't yet exist).
 *
 * - space, key, attrs must point to memory that exists for the duration of this
 *   call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_put(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, enum hyperclient_returncode* status);

/* Perform a put if the specified conditional attributes match.
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in the
 * combined array of condattrs and attrs.
 *
 * All specified conditional attributes must match those in the object
 *
 * If no object exists under this key, the conditional put will fail.
 *
 * All attribute values not specified by the conditional put are left as-is.
 *
 * - space, key, condattrs, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_condput(struct hyperclient* client, const char* space, const char* key,
		    size_t key_sz, const struct hyperclient_attribute* condattrs,
		    size_t condattrs_sz, const struct hyperclient_attribute* attrs,
		    size_t attrs_sz, enum hyperclient_returncode* status);

/* Delete the object under "key".
 *
 * - space, key point to memory that exists for the duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_del(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, enum hyperclient_returncode* status);

/* Atomically add the values given to the existing attribute values
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_add(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically subtract the values given from the existing attribute values
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_sub(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically multiply the existing attribute values by the values given
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_mul(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically divide the existing attribute values by the values given
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_div(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically take the modulus of the existing attribute values by the values given
 *
 * This uses C-style modulus for negative numbers.
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_mod(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically and the existing attribute values with the values given
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_and(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically or the existing attribute values with the values given
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_or(struct hyperclient* client, const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status);

/* Atomically xor the existing attribute values with the values given
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be numeric fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_atomic_xor(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically prepend the values given to the existing attribute values
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be string fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_string_prepend(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

/* Atomically append the values given to the existing attribute values
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be string fields.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_string_append(struct hyperclient* client, const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);

/* Atomically push the values given to the head existing attribute lists
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be string fields.  Each will be pushed to
 * the list in the order specified.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_list_lpush(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

/* Atomically push the values given to the tail existing attribute lists
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error in attrs.
 *
 * If no object exists under this key, the operation will fail.
 *
 * The specified attributes need to be string fields.  Each will be pushed to
 * the list in the order specified.
 *
 * All attribute values not specified by the operation are left as-is.
 *
 * - space, key, attrs must point to memory that exists for the
 *   duration of this call
 * - client, status must point to memory that exists until the request is
 *   considered complete
 */
int64_t
hyperclient_list_rpush(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_set_add(struct hyperclient* client, const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    enum hyperclient_returncode* status);

int64_t
hyperclient_set_remove(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_set_intersect(struct hyperclient* client, const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);

int64_t
hyperclient_set_union(struct hyperclient* client, const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status);

int64_t
hyperclient_map_add(struct hyperclient* client, const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                    enum hyperclient_returncode* status);

int64_t
hyperclient_map_remove(struct hyperclient* client, const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_add(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_sub(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_mul(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_div(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_mod(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_and(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_or(struct hyperclient* client, const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_xor(struct hyperclient* client, const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_string_prepend(struct hyperclient* client, const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);

int64_t
hyperclient_map_string_append(struct hyperclient* client, const char* space,
                              const char* key, size_t key_sz,
                              const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                              enum hyperclient_returncode* status);

/* Perform a search for objects which match "eq" and "rn".
 *
 * Each time hyperclient_loop returns the identifier generated by a call to
 * hyperclient_search the memory pointed to by status, attrs, and attrs_sz will
 * be overwritten.  When hyperclient_loop returns and the status is
 * HYPERCLIENT_SEARCHDONE, the search is completely finished.
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error.  If the
 * attr's index >= eq_sz, it is an index into rn.
 *
 * If an error is encountered early in the search such that no hosts have been
 * contacted for the search, -1 will be returned, and *status will be set to the
 * error.
 */
int64_t
hyperclient_search(struct hyperclient* client, const char* space,
                   const struct hyperclient_attribute* eq, size_t eq_sz,
                   const struct hyperclient_range_query* rn, size_t rn_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz);

/* Handle I/O until at least one event is complete (either a key-op finishes, or
 * a search returns one item).
 *
 * Errors which impact a subset of pending operations are passed through the
 * "status" parameter passed in when the operation started.  Errors which impact
 * all pending operations (e.g., a failure to connect to the coordinator) are
 * passed through the "status" parameter to loop.
 */
int64_t
hyperclient_loop(struct hyperclient* client, int timeout,
                 enum hyperclient_returncode* status);

/* Free an array of hyperclient_attribute objects.  This typically corresponds
 * to the value returned by the get call.
 *
 * The layout of this memory is implementation defined, and may not have been
 * allocated using ``malloc``.  It is most certainly an error to free it using
 * anything other than this call.
 */
void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t attrs_sz);

#ifdef __cplusplus
}

// Each of these public methods corresponds to a C call above.
//
// Methods which return memory *MUST* be freed with the C API calls above.
// This is so that one implementation can service both the C and C++ APIs
// without a performance penalty for either.  A simple "delete" or "free" will
// likely blow up.
class hyperclient
{
    public:
        hyperclient(const char* coordinator, in_port_t port);
        ~hyperclient() throw ();

    public:
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status,
                    struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t put(const char* space, const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    hyperclient_returncode* status);
        int64_t condput(const char* space, const char* key, size_t key_sz,
			const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
			const struct hyperclient_attribute* attrs, size_t attrs_sz,
			hyperclient_returncode* status);
        int64_t del(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status);
        int64_t atomic_add(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_sub(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_mul(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_div(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_mod(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_and(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t atomic_or(const char* space, const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);
        int64_t atomic_xor(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t string_prepend(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t string_append(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_attribute* attrs, size_t attrs_sz,
                              enum hyperclient_returncode* status);
        int64_t list_lpush(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t list_rpush(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t set_add(const char* space, const char* key, size_t key_sz,
                        const struct hyperclient_attribute* attrs, size_t attrs_sz,
                        enum hyperclient_returncode* status);
        int64_t set_remove(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t set_intersect(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_attribute* attrs, size_t attrs_sz,
                              enum hyperclient_returncode* status);
        int64_t set_union(const char* space, const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);
        int64_t map_add(const char* space, const char* key, size_t key_sz,
                        const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                        enum hyperclient_returncode* status);
        int64_t map_remove(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);
        int64_t map_atomic_add(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_sub(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_mul(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_div(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_mod(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_and(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_atomic_or(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                              enum hyperclient_returncode* status);
        int64_t map_atomic_xor(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);
        int64_t map_string_prepend(const char* space, const char* key, size_t key_sz,
                                   const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                   enum hyperclient_returncode* status);
        int64_t map_string_append(const char* space, const char* key, size_t key_sz,
                                  const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                  enum hyperclient_returncode* status);
        int64_t search(const char* space,
                       const struct hyperclient_attribute* eq, size_t eq_sz,
                       const struct hyperclient_range_query* rn, size_t rn_sz,
                       enum hyperclient_returncode* status,
                       struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t loop(int timeout, hyperclient_returncode* status);

    private:
        class completedop;
        class pending;
        class pending_get;
        class pending_search;
        class pending_statusonly;
        typedef std::map<int64_t, e::intrusive_ptr<pending> > incomplete_map_t;

    private:
        int64_t maintain_coord_connection(hyperclient_returncode* status);
        int64_t add_keyop(const char* space,
                          const char* key,
                          size_t key_sz,
                          std::auto_ptr<e::buffer> msg,
                          e::intrusive_ptr<pending> op);
        int64_t send(e::intrusive_ptr<pending> op,
                     std::auto_ptr<e::buffer> msg);
        int64_t pack_attrs(const char* space,
                           e::buffer::packer* p,
                           const struct hyperclient_attribute* attrs,
                           size_t attrs_sz,
                           hyperclient_returncode* status);
        size_t pack_attrs_sz(const struct hyperclient_attribute* attrs,
                             size_t attrs_sz);
        void killall(const po6::net::location& loc, hyperclient_returncode status);
        int64_t attributes_to_microops(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                                       int action, const char* space,
                                       const char* key, size_t key_sz,
                                       const struct hyperclient_attribute* attrs,
                                       size_t attrs_sz,
                                       hyperclient_returncode* status);
        int64_t map_attributes_to_microops(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                                           int action, const char* space,
                                           const char* key, size_t key_sz,
                                           const struct hyperclient_map_attribute* attrs,
                                           size_t attrs_sz,
                                           hyperclient_returncode* status);
        int validate_attr(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                          const std::vector<hyperdex::attribute>& dimensions,
                          e::bitfield* dimensions_seen,
                          const char* attr,
                          hyperdatatype* type,
                          hyperclient_returncode* status);

    private:
        const std::auto_ptr<hyperdex::coordinatorlink> m_coord;
        const std::auto_ptr<hyperdex::configuration> m_config;
        const std::auto_ptr<busybee_st> m_busybee;
        incomplete_map_t m_incomplete;
        std::queue<completedop> m_complete;
        int64_t m_server_nonce;
        int64_t m_client_id;
        int m_old_coord_fd;
        bool m_have_seen_config;
};

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs);

#endif /* __cplusplus */

#endif /* hyperclient_h_ */
