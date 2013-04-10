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

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
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
    enum hyperdatatype map_key_datatype;
    const char* value;
    size_t value_sz;
    enum hyperdatatype value_datatype;
};

struct hyperclient_attribute_check
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
    enum hyperpredicate predicate;
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
    HYPERCLIENT_BADCONFIG    = 8527,
    HYPERCLIENT_BADSPACE     = 8528,
    HYPERCLIENT_DUPLICATE    = 8529,
    HYPERCLIENT_INTERRUPTED  = 8530,
    HYPERCLIENT_CLUSTER_JUMP = 8531,
    HYPERCLIENT_COORD_LOGGED = 8532,

    /* This should never happen.  It indicates a bug */
    HYPERCLIENT_INTERNAL     = 8573,
    HYPERCLIENT_EXCEPTION    = 8574,
    HYPERCLIENT_GARBAGE      = 8575
};

struct hyperclient*
hyperclient_create(const char* coordinator, uint16_t port);
void
hyperclient_destroy(struct hyperclient* client);

enum hyperclient_returncode
hyperclient_add_space(struct hyperclient* client, const char* description);

enum hyperclient_returncode
hyperclient_rm_space(struct hyperclient* client, const char* space);

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

int64_t
hyperclient_put_if_not_exist(struct hyperclient* client, const char* space, const char* key,
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
hyperclient_cond_put(struct hyperclient* client, const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     const struct hyperclient_attribute* attrs, size_t attrs_sz,
                     enum hyperclient_returncode* status);

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
                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz);

/* Perform a search, and build a string describing the costs of the search.
 */
int64_t
hyperclient_search_describe(struct hyperclient* client, const char* space,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            enum hyperclient_returncode* status, const char** description);

/* Perform a search for objects which match "eq" and "rn", sorting the results
 * and limiting the number of objects returned.
 *
 * Each time hyperclient_loop returns the identifier generated by a call to
 * hyperclient_search the memory pointed to by status, attrs, and attrs_sz will
 * be overwritten.  When hyperclient_loop returns and the status is
 * HYPERCLIENT_SEARCHDONE, the search is completely finished.
 *
 * If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
 * abs(returned value) - 1 == the attribute which caused the error.  If the
 * attr's index >= eq_sz, it is an index into rn.  If the attr's index == eq_sz
 * + rn_sz, the sort_by attribute caused the error.
 *
 * If an error is encountered early in the search such that no hosts have been
 * contacted for the search, -1 will be returned, and *status will be set to the
 * error.
 */
int64_t
hyperclient_sorted_search(struct hyperclient* client, const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          const char* sort_by, uint64_t limit, int maximize,
                          enum hyperclient_returncode* status,
                          struct hyperclient_attribute** attrs, size_t* attrs_sz);

/* Delete objects which mach "eq" and "rn".
 *
 * The remote servers will perform a search as if this were a call to
 * ``hyperclient_search``, but instead of returning the search results, it will
 * issue a ``del`` operation.  Once all searches complete, the call will return
 * to the client.
 *
 * This is a best effort call.  It may miss objects that are updated
 * concurrently with the search and it will not tolerate failures.
 */
int64_t
hyperclient_group_del(struct hyperclient* client, const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status);

int64_t
hyperclient_count(struct hyperclient* client, const char* space,
                  const struct hyperclient_attribute_check* checks, size_t checks_sz,
                  enum hyperclient_returncode* status, uint64_t* result);

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

/* Retrieve the datatype for the attribute "name" in the space "space".
 *
 * This will return a valid attribute, or return HYPERDATATYPE_GARBAGE if either
 * "space" or "name" do not exist.  If it returns an error, "status" will
 * indicate the problem.
 */
enum hyperdatatype
hyperclient_attribute_type(struct hyperclient* client,
                           const char* space, const char* name,
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
} // extern "C"

// STL
#include <map>
#include <memory>
#include <queue>
#include <vector>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// Forward declarations
class busybee_st;
namespace hyperdex
{
class attribute_check;
class configuration;
class coordinator_link;
class funcall;
class mapper;
class schema;
class server_id;
class tool_wrapper;
class virtual_server_id;
} //namespace hyperdex

// Each of these public methods corresponds to a C call above.
//
// Methods which return memory *MUST* be freed with the C API calls above.
// This is so that one implementation can service both the C and C++ APIs
// without a performance penalty for either.  A simple "delete" or "free" will
// blow up.
class hyperclient
{
    public:
        hyperclient(const char* coordinator, uint16_t port);
        ~hyperclient() throw ();

    public:
        hyperclient_returncode add_space(const char* description);
        hyperclient_returncode rm_space(const char* space);

    public:
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status,
                    struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t put(const char* space, const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    hyperclient_returncode* status);
        int64_t put_if_not_exist(const char* space, const char* key, size_t key_sz,
                                 const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                 hyperclient_returncode* status);
        int64_t cond_put(const char* space, const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
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
                       const struct hyperclient_attribute_check* checks, size_t checks_sz,
                       enum hyperclient_returncode* status,
                       struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t search_describe(const char* space,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                enum hyperclient_returncode* status, const char** description);
        int64_t sorted_search(const char* space,
                              const struct hyperclient_attribute_check* checks, size_t checks_sz,
                              const char* sort_by,
                              uint64_t limit,
                              bool maximize,
                              enum hyperclient_returncode* status,
                              struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t group_del(const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          enum hyperclient_returncode* status);
        int64_t count(const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status, uint64_t* result);
        int64_t loop(int timeout, hyperclient_returncode* status);
        // Introspect things
        hyperdatatype attribute_type(const char* space, const char* name,
                                     enum hyperclient_returncode* status);

    private:
        class complete;
        class description;
        class pending;
        class pending_count;
        class pending_get;
        class pending_group_del;
        class pending_search;
        class pending_search_description;
        class pending_sorted_search;
        class pending_statusonly;
        class refcount;
        typedef std::map<int64_t, e::intrusive_ptr<pending> > incomplete_map_t;
        friend class hyperdex::tool_wrapper;

    // these are the only private things that tool_wrapper should touch
    private:
        hyperclient_returncode initialize_cluster(uint64_t cluster, const char* path);
        hyperclient_returncode show_config(std::ostream& out);
        hyperclient_returncode kill(uint64_t server_id);
        hyperclient_returncode initiate_transfer(uint64_t region_id, uint64_t server_id);

    private:
        int64_t maintain_coord_connection(hyperclient_returncode* status);
        int64_t perform_funcall1(const struct hyperclient_keyop_info* opinfo,
                                 const char* space, const char* key, size_t key_sz,
                                 const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                 const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                 hyperclient_returncode* status);
        int64_t perform_funcall2(const struct hyperclient_keyop_info* opinfo,
                                 const char* space, const char* key, size_t key_sz,
                                 const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                 const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                 hyperclient_returncode* status);
        int64_t prepare_searchop(const char* space,
                                 const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                 hyperclient_returncode* status,
                                 std::vector<hyperdex::attribute_check>* chks,
                                 std::vector<hyperdex::virtual_server_id>* servers);
        int64_t prepare_searchop(const char* space,
                                 const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                 const char* aux_attr,
                                 hyperclient_returncode* status,
                                 std::vector<hyperdex::attribute_check>* chks,
                                 std::vector<hyperdex::virtual_server_id>* servers,
                                 uint16_t* aux_attrno,
                                 hyperdatatype* aux_attrtype);
        size_t prepare_checks(const hyperdex::schema* sc,
                              const hyperclient_attribute_check* checks, size_t checks_sz,
                              hyperclient_returncode* status,
                              std::vector<hyperdex::attribute_check>* chks);
        size_t prepare_ops(const hyperdex::schema* sc,
                           const hyperclient_keyop_info* opinfo,
                           const hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<hyperdex::funcall>* ops);
        size_t prepare_ops(const hyperdex::schema* sc,
                           const hyperclient_keyop_info* opinfo,
                           const hyperclient_map_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<hyperdex::funcall>* ops);
        int64_t add_keyop(const char* space,
                          const char* key,
                          size_t key_sz,
                          std::auto_ptr<e::buffer> msg,
                          e::intrusive_ptr<pending> op);
        int64_t send(e::intrusive_ptr<pending> op,
                     std::auto_ptr<e::buffer> msg);
        void killall(const hyperdex::server_id& id, hyperclient_returncode status);

    private:
        const std::auto_ptr<hyperdex::configuration> m_config;
        const std::auto_ptr<hyperdex::mapper> m_busybee_mapper;
        const std::auto_ptr<busybee_st> m_busybee;
        const std::auto_ptr<hyperdex::coordinator_link> m_coord;
        incomplete_map_t m_incomplete;
        std::queue<int64_t> m_complete_succeeded;
#ifdef _MSC_VER
        std::queue<std::shared_ptr<complete>> m_complete_failed;
#else
        std::queue<complete> m_complete_failed;
#endif
        int64_t m_server_nonce;
        int64_t m_client_id;
        bool m_have_seen_config;
};

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs);

#endif /* __cplusplus */
#endif /* hyperclient_h_ */
