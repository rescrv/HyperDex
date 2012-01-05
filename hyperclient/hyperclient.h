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

#ifndef hyperclient_h_
#define hyperclient_h_

// C
#include <stdint.h>
#include <stdlib.h>

// POSIX
#include <netinet/in.h>

#ifdef __cplusplus
// STL
#include <map>
#include <memory>
#include <queue>
#include <stack>
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// Forward declarations
namespace hyperdex
{
class configuration;
class coordinatorlink;
class instance;
} // namespace hyperdex

extern "C"
{
#endif // __cplusplus

struct hyperclient;

struct hyperclient_attribute
{
    const char* attr; // NULL-terminated
    const char* value;
    size_t value_sz;
};

struct hyperclient_range_query
{
    const char* attr;
    size_t attr_sz;
    uint64_t lower;
    uint64_t upper;
};

// HyperClient returncode occupies [8448, 8576)
enum hyperclient_returncode
{
    HYPERCLIENT_SUCCESS      = 8448,
    HYPERCLIENT_NOTFOUND     = 8449,
    HYPERCLIENT_UNKNOWNSPACE = 8450,
    HYPERCLIENT_COORDFAIL    = 8451,
    HYPERCLIENT_SERVERERROR  = 8452,
    HYPERCLIENT_CONNECTFAIL  = 8453,
    HYPERCLIENT_DISCONNECT   = 8454,
    HYPERCLIENT_RECONFIGURE  = 8455,
    HYPERCLIENT_LOGICERROR   = 8456,
    HYPERCLIENT_TIMEOUT      = 8457,
    HYPERCLIENT_UNKNOWNATTR  = 8458,
    HYPERCLIENT_DUPEATTR     = 8459,
    HYPERCLIENT_SEEERRNO     = 8460,
    HYPERCLIENT_NONEPENDING  = 8461,

    /* This should never happen.  It indicates a bug */
    HYPERCLIENT_EXCEPTION    = 8574,
    HYPERCLIENT_ZERO         = 8575
};

struct hyperclient_search_result
{
    struct hyperclient_search_result* next;
    enum hyperclient_returncode status;
    const char* key;
    size_t key_sz;
    struct hyperclient_attribute* attrs;
    size_t attrs_sz;
};

struct hyperclient*
hyperclient_create(const char* coordinator, in_port_t port);
void
hyperclient_destroy(struct hyperclient* client);

// The user of this API is responsible for freeing objects returned through a **
// argument, or * return value.  This must be done by using the appropriate
// typename_destroy() calls.
//
// All values return a 64-bit integer, which uniquely identifies the request
// until its completion.  Positive values indicate valid identifiers.  Negative
// values indicate that the request fails immediately for the reason stored in
// status.
//
// Each call to "hyperclient_loop" will return the identifier corresponding to
// the request which made progress.  get/put/del requests are completed
// immediately after the identifier returns.  search requests continue until the
// status indicates that the search is done.

// Retrieve the secondary attributes corresponding to "key" in "space".
int64_t
hyperclient_get(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, enum hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz);

// Store the secondary attributes under "key" in "space".
// If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
// abs(returned value) - 1 == the attribute which caused the error.
//
// All attributes not specified by the put are left as-is (if the key already
// exists), or set to "" (if the key doesn't yet exist).
int64_t
hyperclient_put(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, enum hyperclient_returncode* status);

// Delete the object under "key".
int64_t
hyperclient_del(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, enum hyperclient_returncode* status);

// Perform a search for objects which match "eq" and "rn".
// Each call to loop will overwrite "status", "key", "key_sz", "attrs", and
// "attrs_sz".
// If this returns a value < 0 and *status == HYPERCLIENT_UNKNOWNATTR, then
// abs(returned value) - 1 == the attribute which caused the error.  If the
// attr's index >= eq_sz, it is an index into rn.
int64_t
hyperclient_search(struct hyperclient* client, const char* space,
                   const struct hyperclient_attribute* eq, size_t eq_sz,
                   const struct hyperclient_range_query* rn, size_t rn_sz,
                   struct hyperclient_search_result** results);

// Handle I/O until at least one event is complete (either a key-op finishes, or
// a search returns one item).
//
// Errors which impact a subset of pending operations are passed through the
// "status" parameter passed in when the operation started.  Errors which impact
// all pending operations (e.g., a failure to connect to the coordinator) are
// passed through the "status" parameter to loop.
int64_t
hyperclient_loop(struct hyperclient* client, int timeout,
                 enum hyperclient_returncode* status);

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t attrs_sz);

#ifdef __cplusplus
}

// Each of these public methods corresponds to a C call above.
class hyperclient
{
    public:
        hyperclient(const po6::net::location& coordinator);
        ~hyperclient() throw ();

    public:
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status,
                    struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t put(const char* space, const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    hyperclient_returncode* status);
        int64_t del(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status);
        int64_t search(const char* space,
                       const struct hyperclient_attribute* eq, size_t eq_sz,
                       const struct hyperclient_range_query* rn, size_t rn_sz,
                       hyperclient_search_result** results);
        int64_t loop(int timeout, hyperclient_returncode* status);

    private:
        class channel;
        class failedop;
        class pending;
        class pending_get;
        class pending_statusonly;
        typedef std::map<hyperdex::instance, e::intrusive_ptr<channel> > instances_map_t;
        typedef std::deque<e::intrusive_ptr<pending> > requests_list_t;

    private:
        int64_t add_keyop(const char* space,
                          const char* key,
                          size_t key_sz,
                          std::auto_ptr<e::buffer> msg,
                          e::intrusive_ptr<pending> op);
        int64_t pack_attrs(const char* space,
                           e::buffer::packer p,
                           const struct hyperclient_attribute* attrs,
                           size_t attrs_sz,
                           hyperclient_returncode* status);
        size_t pack_attrs_sz(const struct hyperclient_attribute* attrs,
                             size_t attrs_sz);
        int64_t send(e::intrusive_ptr<channel> chan,
                     e::intrusive_ptr<pending> op,
                     std::auto_ptr<e::buffer> msg);
        int64_t try_coord_connect(hyperclient_returncode* status);
        void killall(int fd, hyperclient_returncode status);
        e::intrusive_ptr<channel> get_channel(hyperdex::instance inst,
                                              hyperclient_returncode* status);
        e::intrusive_ptr<channel> channel_from_fd(int fd);

    private:
        const std::auto_ptr<hyperdex::coordinatorlink> m_coord;
        const std::auto_ptr<hyperdex::configuration> m_config;
        std::vector<e::intrusive_ptr<channel> > m_fds;
        instances_map_t m_instances;
        requests_list_t m_requests;
        int64_t m_requestid;
        std::queue<failedop> m_failed;
        bool m_configured;
};

#endif // __cplusplus

#endif // hyperclient_h_
