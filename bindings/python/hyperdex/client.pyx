# Copyright (c) 2011-2014, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

try: import simplejson as json
except ImportError: import json
import datetime
import calendar
import time

from cpython cimport bool


cdef extern from "stdint.h":

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t

cdef extern from "stdlib.h":

    void* malloc(size_t size)
    void free(void* ptr)


cdef extern from "hyperdex.h":

    cdef enum hyperdatatype:
        HYPERDATATYPE_GENERIC            = 9216
        HYPERDATATYPE_STRING             = 9217
        HYPERDATATYPE_INT64              = 9218
        HYPERDATATYPE_FLOAT              = 9219
        HYPERDATATYPE_DOCUMENT           = 9223
        HYPERDATATYPE_LIST_GENERIC       = 9280
        HYPERDATATYPE_LIST_STRING        = 9281
        HYPERDATATYPE_LIST_INT64         = 9282
        HYPERDATATYPE_LIST_FLOAT         = 9283
        HYPERDATATYPE_SET_GENERIC        = 9344
        HYPERDATATYPE_SET_STRING         = 9345
        HYPERDATATYPE_SET_INT64          = 9346
        HYPERDATATYPE_SET_FLOAT          = 9347
        HYPERDATATYPE_MAP_GENERIC        = 9408
        HYPERDATATYPE_MAP_STRING_KEYONLY = 9416
        HYPERDATATYPE_MAP_STRING_STRING  = 9417
        HYPERDATATYPE_MAP_STRING_INT64   = 9418
        HYPERDATATYPE_MAP_STRING_FLOAT   = 9419
        HYPERDATATYPE_MAP_INT64_KEYONLY  = 9424
        HYPERDATATYPE_MAP_INT64_STRING   = 9425
        HYPERDATATYPE_MAP_INT64_INT64    = 9426
        HYPERDATATYPE_MAP_INT64_FLOAT    = 9427
        HYPERDATATYPE_MAP_FLOAT_KEYONLY  = 9432
        HYPERDATATYPE_MAP_FLOAT_STRING   = 9433
        HYPERDATATYPE_MAP_FLOAT_INT64    = 9434
        HYPERDATATYPE_MAP_FLOAT_FLOAT    = 9435
        HYPERDATATYPE_TIMESTAMP_GENERIC  = 9472
        HYPERDATATYPE_TIMESTAMP_SECOND   = 9473
        HYPERDATATYPE_TIMESTAMP_MINUTE   = 9474
        HYPERDATATYPE_TIMESTAMP_HOUR     = 9475
        HYPERDATATYPE_TIMESTAMP_DAY      = 9476
        HYPERDATATYPE_TIMESTAMP_WEEK     = 9477
        HYPERDATATYPE_TIMESTAMP_MONTH    = 9478
        HYPERDATATYPE_MACAROON_SECRET    = 9664
        HYPERDATATYPE_GARBAGE            = 9727

    cdef enum hyperpredicate:
        HYPERPREDICATE_FAIL          = 9728
        HYPERPREDICATE_EQUALS        = 9729
        HYPERPREDICATE_LESS_THAN     = 9738
        HYPERPREDICATE_LESS_EQUAL    = 9730
        HYPERPREDICATE_GREATER_EQUAL = 9731
        HYPERPREDICATE_GREATER_THAN  = 9739
        HYPERPREDICATE_REGEX         = 9733
        HYPERPREDICATE_LENGTH_EQUALS        = 9734
        HYPERPREDICATE_LENGTH_LESS_EQUAL    = 9735
        HYPERPREDICATE_LENGTH_GREATER_EQUAL = 9736
        HYPERPREDICATE_CONTAINS      = 9737


cdef extern from "macaroons.h":

    cdef struct macaroon


cdef extern from "hyperdex/client.h":

    cdef struct hyperdex_client

    cdef struct hyperdex_client_microtransaction

    cdef struct hyperdex_client_attribute:
        const char* attr
        const char* value
        size_t value_sz
        hyperdatatype datatype

    cdef struct hyperdex_client_map_attribute:
        const char* attr
        const char* map_key
        size_t map_key_sz
        hyperdatatype map_key_datatype
        const char* value
        size_t value_sz
        hyperdatatype value_datatype

    cdef struct hyperdex_client_attribute_check:
        const char* attr
        const char* value
        size_t value_sz
        hyperdatatype datatype
        hyperpredicate predicate

    cdef enum hyperdex_client_returncode:
        HYPERDEX_CLIENT_SUCCESS      = 8448
        HYPERDEX_CLIENT_NOTFOUND     = 8449
        HYPERDEX_CLIENT_SEARCHDONE   = 8450
        HYPERDEX_CLIENT_CMPFAIL      = 8451
        HYPERDEX_CLIENT_READONLY     = 8452
        HYPERDEX_CLIENT_UNKNOWNSPACE = 8512
        HYPERDEX_CLIENT_COORDFAIL    = 8513
        HYPERDEX_CLIENT_SERVERERROR  = 8514
        HYPERDEX_CLIENT_POLLFAILED   = 8515
        HYPERDEX_CLIENT_OVERFLOW     = 8516
        HYPERDEX_CLIENT_RECONFIGURE  = 8517
        HYPERDEX_CLIENT_TIMEOUT      = 8519
        HYPERDEX_CLIENT_UNKNOWNATTR  = 8520
        HYPERDEX_CLIENT_DUPEATTR     = 8521
        HYPERDEX_CLIENT_NONEPENDING  = 8523
        HYPERDEX_CLIENT_DONTUSEKEY   = 8524
        HYPERDEX_CLIENT_WRONGTYPE    = 8525
        HYPERDEX_CLIENT_NOMEM        = 8526
        HYPERDEX_CLIENT_INTERRUPTED  = 8530
        HYPERDEX_CLIENT_CLUSTER_JUMP = 8531
        HYPERDEX_CLIENT_INTERNAL     = 8573
        HYPERDEX_CLIENT_EXCEPTION    = 8574
        HYPERDEX_CLIENT_GARBAGE      = 8575

    hyperdex_client* hyperdex_client_create(char* coordinator, uint16_t port)
    hyperdex_client_microtransaction* hyperdex_client_uxact_init(hyperdex_client* _cl, const char* space, hyperdex_client_returncode *status)
    int64_t hyperdex_client_uxact_commit(hyperdex_client* _cl, hyperdex_client_microtransaction *utx, const char* key, size_t key_sz)
    int64_t hyperdex_client_uxact_cond_commit(hyperdex_client* _cl, hyperdex_client_microtransaction *utx, const char* key, size_t key_sz, const hyperdex_client_attribute_check *chks, size_t chks_sz)
    int64_t hyperdex_client_uxact_group_commit(hyperdex_client* _cl, hyperdex_client_microtransaction *utx, const hyperdex_client_attribute_check *chks, size_t chks_sz, uint64_t* count)
    void hyperdex_client_destroy(hyperdex_client* client)
    int64_t hyperdex_client_loop(hyperdex_client* client, int timeout, hyperdex_client_returncode* status)
    void hyperdex_client_destroy_attrs(hyperdex_client_attribute* attrs, size_t attrs_sz)
    char* hyperdex_client_error_message(hyperdex_client* client)
    char* hyperdex_client_error_location(hyperdex_client* client)
    char* hyperdex_client_returncode_to_string(hyperdex_client_returncode)
    void hyperdex_client_clear_auth_context(hyperdex_client* client)
    void hyperdex_client_set_auth_context(hyperdex_client* client, const char** macaroons, size_t macaroons_sz)
    # Begin Automatically Generated Prototypes
    int64_t hyperdex_client_get(hyperdex_client* client, const char* space, const char* key, size_t key_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
    int64_t hyperdex_client_get_partial(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const char** attrnames, size_t attrnames_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
    int64_t hyperdex_client_put(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_put(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_put(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_put_or_create(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_put(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_put_if_not_exist(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_del(hyperdex_client* client, const char* space, const char* key, size_t key_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_del(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_del(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_add(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_add(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_sub(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_sub(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_sub(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_sub(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_mul(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_mul(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_mul(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_mul(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_div(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_div(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_div(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_div(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_mod(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_atomic_mod(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_mod(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_and(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_and(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_and(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_and(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_or(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_atomic_or(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_atomic_or(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_or(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_xor(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_atomic_xor(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_xor(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_min(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_atomic_min(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_min(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_atomic_max(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_atomic_max(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_atomic_max(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_string_prepend(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_string_prepend(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_string_prepend(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_string_prepend(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_string_append(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_string_append(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_string_append(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_string_append(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_string_ltrim(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_string_ltrim(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_string_ltrim(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_string_ltrim(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_string_rtrim(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_string_rtrim(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_string_rtrim(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_string_rtrim(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_list_lpush(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_list_lpush(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_list_lpush(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_list_lpush(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_list_rpush(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_list_rpush(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_list_rpush(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_list_rpush(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_set_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_set_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_set_add(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_set_remove(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_set_remove(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_set_remove(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_set_intersect(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_set_intersect(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_set_intersect(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_set_union(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_set_union(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_set_union(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_document_rename(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_document_rename(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_document_rename(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_document_rename(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_document_unset(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_uxact_document_unset(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_cond_document_unset(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_document_unset(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_add(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_remove(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_remove(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_remove(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_add(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_add(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_sub(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_sub(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_sub(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_mul(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_mul(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_mul(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_div(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_div(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_div(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_mod(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_mod(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_mod(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_and(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_and(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_and(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_or(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_or(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_or(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_xor(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_xor(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_xor(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_string_prepend(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_string_prepend(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_string_prepend(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_string_append(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_string_append(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_string_append(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_map_atomic_min(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_min(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_min(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_uxact_atomic_min(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_map_atomic_max(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_cond_map_atomic_max(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
    int64_t hyperdex_client_group_map_atomic_max(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
    int64_t hyperdex_client_uxact_atomic_max(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
    int64_t hyperdex_client_search(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
    int64_t hyperdex_client_search_describe(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, const char** description)
    int64_t hyperdex_client_sorted_search(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const char* sort_by, uint64_t limit, int maxmin, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
    int64_t hyperdex_client_count(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, uint64_t* count)
    # End Automatically Generated Prototypes


cdef extern from "hyperdex/datastructures.h":

    cdef struct hyperdex_ds_arena

    cdef enum hyperdex_ds_returncode:
        HYPERDEX_DS_SUCCESS
        HYPERDEX_DS_NOMEM
        HYPERDEX_DS_MIXED_TYPES
        HYPERDEX_DS_WRONG_STATE
        HYPERDEX_DS_STRING_TOO_LONG

    hyperdex_ds_arena* hyperdex_ds_arena_create()
    void hyperdex_ds_arena_destroy(hyperdex_ds_arena* arena)

    void* hyperdex_ds_malloc(hyperdex_ds_arena* arena, size_t sz)
    hyperdex_client_attribute* hyperdex_ds_allocate_attribute(hyperdex_ds_arena* arena, size_t sz)
    hyperdex_client_attribute_check* hyperdex_ds_allocate_attribute_check(hyperdex_ds_arena* arena, size_t sz)
    hyperdex_client_map_attribute* hyperdex_ds_allocate_map_attribute(hyperdex_ds_arena* arena, size_t sz)

    int hyperdex_ds_unpack_int(char* buf, size_t buf_sz, int64_t* num)
    int hyperdex_ds_unpack_float(char* buf, size_t buf_sz, double* num)

    int hyperdex_ds_copy_string(hyperdex_ds_arena* arena, char* str, size_t str_sz, hyperdex_ds_returncode* status, char** value, size_t* value_sz)
    int hyperdex_ds_copy_int(hyperdex_ds_arena* arena, int64_t num, hyperdex_ds_returncode* status, char** value, size_t* value_sz)
    int hyperdex_ds_copy_float(hyperdex_ds_arena* arena, double num, hyperdex_ds_returncode* status, char** value, size_t* value_sz)

    cdef struct hyperdex_ds_list
    hyperdex_ds_list* hyperdex_ds_allocate_list(hyperdex_ds_arena* arena)
    int hyperdex_ds_list_append_string(hyperdex_ds_list* list, char* str, size_t str_sz, hyperdex_ds_returncode* status)
    int hyperdex_ds_list_append_int(hyperdex_ds_list* list, int64_t num, hyperdex_ds_returncode* status)
    int hyperdex_ds_list_append_float(hyperdex_ds_list* list, double num, hyperdex_ds_returncode* status)
    int hyperdex_ds_list_finalize(hyperdex_ds_list* list, hyperdex_ds_returncode* status, const char** value, size_t* value_sz, hyperdatatype* datatype)

    cdef struct hyperdex_ds_set
    hyperdex_ds_set* hyperdex_ds_allocate_set(hyperdex_ds_arena* arena)
    int hyperdex_ds_set_insert_string(hyperdex_ds_set* set, char* str, size_t str_sz, hyperdex_ds_returncode* status)
    int hyperdex_ds_set_insert_int(hyperdex_ds_set* set, int64_t num, hyperdex_ds_returncode* status)
    int hyperdex_ds_set_insert_float(hyperdex_ds_set* set, double num, hyperdex_ds_returncode* status)
    int hyperdex_ds_set_finalize(hyperdex_ds_set*, hyperdex_ds_returncode* status, const char** value, size_t* value_sz, hyperdatatype* datatype)

    cdef struct hyperdex_ds_map
    hyperdex_ds_map* hyperdex_ds_allocate_map(hyperdex_ds_arena* arena)
    int hyperdex_ds_map_insert_key_string(hyperdex_ds_map* map, char* str, size_t str_sz, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_insert_val_string(hyperdex_ds_map* map, char* str, size_t str_sz, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_insert_key_int(hyperdex_ds_map* map, int64_t num, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_insert_val_int(hyperdex_ds_map* map, int64_t num, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_insert_key_float(hyperdex_ds_map* map, double num, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_insert_val_float(hyperdex_ds_map* map, double num, hyperdex_ds_returncode* status)
    int hyperdex_ds_map_finalize(hyperdex_ds_map*, hyperdex_ds_returncode* status, const char** value, size_t* value_sz, hyperdatatype* datatype)

    cdef struct hyperdex_ds_iterator:
        pass
    void hyperdex_ds_iterator_init(hyperdex_ds_iterator* it, hyperdatatype datatype, char* value, size_t value_sz)
    int hyperdex_ds_iterate_list_string_next(hyperdex_ds_iterator* it, char** str, size_t* str_sz)
    int hyperdex_ds_iterate_list_int_next(hyperdex_ds_iterator* it, int64_t* num)
    int hyperdex_ds_iterate_list_float_next(hyperdex_ds_iterator* it, double* num)
    int hyperdex_ds_iterate_set_string_next(hyperdex_ds_iterator* it, char** str, size_t* str_sz)
    int hyperdex_ds_iterate_set_int_next(hyperdex_ds_iterator* it, int64_t* num)
    int hyperdex_ds_iterate_set_float_next(hyperdex_ds_iterator* it, double* num)
    int hyperdex_ds_iterate_map_string_string_next(hyperdex_ds_iterator* it, char** key, size_t* key_sz, char** val, size_t* val_sz)
    int hyperdex_ds_iterate_map_string_int_next(hyperdex_ds_iterator* it, char** key, size_t* key_sz, int64_t* val)
    int hyperdex_ds_iterate_map_string_float_next(hyperdex_ds_iterator* it, char** key, size_t* key_sz, double* val)
    int hyperdex_ds_iterate_map_int_string_next(hyperdex_ds_iterator* it, int64_t* key, char** val, size_t* val_sz)
    int hyperdex_ds_iterate_map_int_int_next(hyperdex_ds_iterator* it, int64_t* key, int64_t* val)
    int hyperdex_ds_iterate_map_int_float_next(hyperdex_ds_iterator* it, int64_t* key, double* val)
    int hyperdex_ds_iterate_map_float_string_next(hyperdex_ds_iterator* it, double* key, char** val, size_t* val_sz)
    int hyperdex_ds_iterate_map_float_int_next(hyperdex_ds_iterator* it, double* key, int64_t* val)
    int hyperdex_ds_iterate_map_float_float_next(hyperdex_ds_iterator* it, double* key, double* val)


ctypedef object (*encret_deferred_fptr)(Deferred d)
ctypedef object (*encret_iterator_fptr)(Iterator it)
# Begin Automatically Generated Function Pointers
ctypedef int64_t asynccall__spacename_key__status_attributes_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
ctypedef int64_t asynccall__spacename_key_attributenames__status_attributes_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const char** attrnames, size_t attrnames_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
ctypedef int64_t asynccall__spacename_key_attributes__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
ctypedef int64_t microtransactioncall__microtransaction_attributes___fptr(hyperdex_client* client, hyperdex_client_microtransaction* microtransaction, const hyperdex_client_attribute* attrs, size_t attrs_sz)
ctypedef int64_t asynccall__spacename_key_predicates_attributes__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status)
ctypedef int64_t asynccall__spacename_predicates_attributes__status_count_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_attribute* attrs, size_t attrs_sz, hyperdex_client_returncode* status, uint64_t* count)
ctypedef int64_t asynccall__spacename_key__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, hyperdex_client_returncode* status)
ctypedef int64_t asynccall__spacename_key_predicates__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status)
ctypedef int64_t asynccall__spacename_predicates__status_count_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, uint64_t* count)
ctypedef int64_t asynccall__spacename_key_mapattributes__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
ctypedef int64_t asynccall__spacename_key_predicates_mapattributes__status_fptr(hyperdex_client* client, const char* space, const char* key, size_t key_sz, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status)
ctypedef int64_t asynccall__spacename_predicates_mapattributes__status_count_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, hyperdex_client_returncode* status, uint64_t* count)
ctypedef int64_t iterator__spacename_predicates__status_attributes_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
ctypedef int64_t asynccall__spacename_predicates__status_description_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, hyperdex_client_returncode* status, const char** description)
ctypedef int64_t iterator__spacename_predicates_sortby_limit_maxmin__status_attributes_fptr(hyperdex_client* client, const char* space, const hyperdex_client_attribute_check* checks, size_t checks_sz, const char* sort_by, uint64_t limit, int maxmin, hyperdex_client_returncode* status, const hyperdex_client_attribute** attrs, size_t* attrs_sz)
# End Automatically Generated Function Pointers


class HyperDexClientException(Exception):

    def __init__(self, status, message):
        super(HyperDexClientException, self).__init__(self)
        self._status = status
        self._symbol = hyperdex_client_returncode_to_string(self._status)
        self._message = message

    def status(self):
        return self._status

    def symbol(self):
        return self._symbol

    def message(self):
        return self._message

    def __str__(self):
        return 'HyperDexClientException: {0} [{1}]'.format(self.message(), self.symbol())

    def __repr__(self):
        return str(self)


cdef hyperdex_python_client_convert_string(hyperdex_ds_arena* arena, bytes x,
                                           const char** value,
                                           size_t* value_sz,
                                           hyperdatatype* datatype):
    cdef hyperdex_ds_returncode error
    if hyperdex_ds_copy_string(arena, x, len(x), &error, value, value_sz) < 0:
        raise MemoryError()
    datatype[0] = HYPERDATATYPE_STRING


cdef hyperdex_python_client_convert_int(hyperdex_ds_arena* arena, long x,
                                        const char** value,
                                        size_t* value_sz,
                                        hyperdatatype* datatype):
    cdef hyperdex_ds_returncode error
    if hyperdex_ds_copy_int(arena, x, &error, value, value_sz) < 0:
        raise MemoryError()
    datatype[0] = HYPERDATATYPE_INT64


cdef hyperdex_python_client_convert_float(hyperdex_ds_arena* arena, double x,
                                          const char** value,
                                          size_t* value_sz,
                                          hyperdatatype* datatype):
    cdef hyperdex_ds_returncode error
    if hyperdex_ds_copy_float(arena, x, &error, value, value_sz) < 0:
        raise MemoryError()
    datatype[0] = HYPERDATATYPE_FLOAT


cdef hyperdex_python_client_handle_elem_error(hyperdex_ds_returncode error, str etype):
    if error == HYPERDEX_DS_NOMEM:
        raise MemoryError()
    elif error == HYPERDEX_DS_MIXED_TYPES:
        raise TypeError("Cannot add " + etype + " to a heterogenous container")
    else:
        raise TypeError("Cannot convert " + etype + " to a HyperDex type")


cdef hyperdex_python_client_convert_list(hyperdex_ds_arena* arena, list xs,
                                         const char** value,
                                         size_t* value_sz,
                                         hyperdatatype* datatype):
    cdef hyperdex_ds_list* lst = hyperdex_ds_allocate_list(arena)
    cdef hyperdex_ds_returncode error
    for x in xs:
        if isinstance(x, bytes):
            if hyperdex_ds_list_append_string(lst, x, len(x), &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "string")
        elif isinstance(x, long):
            if hyperdex_ds_list_append_int(lst, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "long")
        elif isinstance(x, int):
            if hyperdex_ds_list_append_int(lst, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "int")
        elif isinstance(x, float):
            if hyperdex_ds_list_append_float(lst, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "float")
        else:
            raise TypeError("Cannot convert object to a HyperDex container element")
    if hyperdex_ds_list_finalize(lst, &error, value, value_sz, datatype) < 0:
        raise MemoryError()


cdef hyperdex_python_client_convert_set(hyperdex_ds_arena* arena, set xs,
                                        const char** value,
                                        size_t* value_sz,
                                        hyperdatatype* datatype):
    cdef hyperdex_ds_set* st = hyperdex_ds_allocate_set(arena)
    cdef hyperdex_ds_returncode error
    for x in xs:
        if isinstance(x, bytes):
            if hyperdex_ds_set_insert_string(st, x, len(x), &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "string")
        elif isinstance(x, long):
            if hyperdex_ds_set_insert_int(st, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "long")
        elif isinstance(x, int):
            if hyperdex_ds_set_insert_int(st, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "int")
        elif isinstance(x, float):
            if hyperdex_ds_set_insert_float(st, x, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "float")
        else:
            raise TypeError("Cannot convert object to a HyperDex container element")
    if hyperdex_ds_set_finalize(st, &error, value, value_sz, datatype) < 0:
        raise MemoryError()


cdef hyperdex_python_client_convert_map(hyperdex_ds_arena* arena, dict xs,
                                        const char** value,
                                        size_t* value_sz,
                                        hyperdatatype* datatype):
    cdef hyperdex_ds_map* mp = hyperdex_ds_allocate_map(arena)
    cdef hyperdex_ds_returncode error
    for k, v in xs.iteritems():
        if isinstance(k, bytes):
            if hyperdex_ds_map_insert_key_string(mp, k, len(k), &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "string")
        elif isinstance(k, long):
            if hyperdex_ds_map_insert_key_int(mp, k, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "long")
        elif isinstance(k, int):
            if hyperdex_ds_map_insert_key_int(mp, k, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "int")
        elif isinstance(k, float):
            if hyperdex_ds_map_insert_key_float(mp, k, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "float")
        else:
            raise TypeError("Cannot convert object to a HyperDex map key")
        if isinstance(v, bytes):
            if hyperdex_ds_map_insert_val_string(mp, v, len(v), &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "string")
        elif isinstance(v, long):
            if hyperdex_ds_map_insert_val_int(mp, v, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "long")
        elif isinstance(v, int):
            if hyperdex_ds_map_insert_val_int(mp, v, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "int")
        elif isinstance(v, float):
            if hyperdex_ds_map_insert_val_float(mp, v, &error) < 0:
                hyperdex_python_client_handle_elem_error(error, "float")
        else:
            raise TypeError("Cannot convert object to a HyperDex map value")
    if hyperdex_ds_map_finalize(mp, &error, value, value_sz, datatype) < 0:
        raise MemoryError()


cdef hyperdex_python_client_convert_type(hyperdex_ds_arena* arena, x,
                                         const char** value,
                                         size_t* value_sz,
                                         hyperdatatype* datatype):
    cdef hyperdatatype _datatype
    if isinstance(x, bytes):
        return hyperdex_python_client_convert_string(arena, x, value, value_sz, datatype)
    elif isinstance(x, long):
        return hyperdex_python_client_convert_int(arena, x, value, value_sz, datatype)
    elif isinstance(x, int):
        return hyperdex_python_client_convert_int(arena, x, value, value_sz, datatype)
    elif isinstance(x, float):
        return hyperdex_python_client_convert_float(arena, x, value, value_sz, datatype)
    elif isinstance(x, list):
        return hyperdex_python_client_convert_list(arena, x, value, value_sz, datatype)
    elif isinstance(x, set):
        return hyperdex_python_client_convert_set(arena, x, value, value_sz, datatype)
    elif isinstance(x, dict):
        return hyperdex_python_client_convert_map(arena, x, value, value_sz, datatype)
    elif isinstance(x, Document):
        datatype[0] = HYPERDATATYPE_DOCUMENT
        return hyperdex_python_client_convert_string(arena, x.inner_str(), value, value_sz, &_datatype)
    elif isinstance(x, datetime.datetime):
        datatype[0] = HYPERDATATYPE_TIMESTAMP_GENERIC
        t = long(calendar.timegm(x.utctimetuple())) * long(1e6) + x.microsecond
        return hyperdex_python_client_convert_int(arena, t, value, value_sz, &_datatype)
    else:
        raise TypeError("Cannot convert object to a HyperDex type")


cdef hyperdex_python_client_build_document(const char* value, size_t value_sz):
    return Document(value)

cdef hyperdex_python_client_build_string(const char* value, size_t value_sz):
    return value[:value_sz]

cdef hyperdex_python_client_build_int(const char* value, size_t value_sz):
    cdef int64_t i = 0
    if hyperdex_ds_unpack_int(value, value_sz, &i) < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed attributes")
    return long(i)


cdef hyperdex_python_client_build_float(const char* value, size_t value_sz):
    cdef double d = 0
    if hyperdex_ds_unpack_float(value, value_sz, &d) < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed attributes")
    return float(d)


cdef hyperdex_python_client_build_list_string(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef const char* tmp
    cdef size_t tmp_sz
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_LIST_STRING, value, value_sz)
    ret = []
    while True:
        result = hyperdex_ds_iterate_list_string_next(&it, &tmp, &tmp_sz)
        if result <= 0:
            break
        ret.append(tmp[:tmp_sz])
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(string)")
    return ret


cdef hyperdex_python_client_build_list_int(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef int64_t tmp
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_LIST_INT64, value, value_sz)
    ret = []
    while True:
        result = hyperdex_ds_iterate_list_int_next(&it, &tmp)
        if result <= 0:
            break
        ret.append(tmp)
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(int)")
    return ret


cdef hyperdex_python_client_build_list_float(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef double tmp
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_LIST_FLOAT, value, value_sz)
    ret = []
    while True:
        result = hyperdex_ds_iterate_list_float_next(&it, &tmp)
        if result <= 0:
            break
        ret.append(tmp)
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(float)")
    return ret


cdef hyperdex_python_client_build_set_string(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef const char* tmp
    cdef size_t tmp_sz
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_SET_STRING, value, value_sz)
    ret = set([])
    while True:
        result = hyperdex_ds_iterate_set_string_next(&it, &tmp, &tmp_sz)
        if result <= 0:
            break
        ret.add(tmp[:tmp_sz])
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(string)")
    return ret


cdef hyperdex_python_client_build_set_int(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef int64_t tmp
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_SET_INT64, value, value_sz)
    ret = set([])
    while True:
        result = hyperdex_ds_iterate_set_int_next(&it, &tmp)
        if result <= 0:
            break
        ret.add(tmp)
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(int)")
    return ret


cdef hyperdex_python_client_build_set_float(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef double tmp
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_SET_FLOAT, value, value_sz)
    ret = set([])
    while True:
        result = hyperdex_ds_iterate_set_float_next(&it, &tmp)
        if result <= 0:
            break
        ret.add(tmp)
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(float)")
    return ret


cdef hyperdex_python_client_build_map_string_string(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef const char* key
    cdef size_t key_sz
    cdef const char* val
    cdef size_t val_sz
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_STRING_STRING, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_string_string_next(&it, &key, &key_sz, &val, &val_sz)
        if result <= 0:
            break
        ret[key[:key_sz]] = val[:val_sz]
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, string)")
    return ret


cdef hyperdex_python_client_build_map_string_int(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef const char* key
    cdef size_t key_sz
    cdef int64_t val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_STRING_INT64, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_string_int_next(&it, &key, &key_sz, &val)
        if result <= 0:
            break
        ret[key[:key_sz]] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, int)")
    return ret


cdef hyperdex_python_client_build_map_string_float(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef const char* key
    cdef size_t key_sz
    cdef double val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_STRING_FLOAT, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_string_float_next(&it, &key, &key_sz, &val)
        if result <= 0:
            break
        ret[key[:key_sz]] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, float)")
    return ret


cdef hyperdex_python_client_build_map_int_string(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef int64_t key
    cdef const char* val
    cdef size_t val_sz
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_INT64_STRING, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_int_string_next(&it, &key, &val, &val_sz)
        if result <= 0:
            break
        ret[key] = val[:val_sz]
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, string)")
    return ret


cdef hyperdex_python_client_build_map_int_int(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef int64_t key
    cdef int64_t val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_INT64_INT64, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_int_int_next(&it, &key, &val)
        if result <= 0:
            break
        ret[key] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, int)")
    return ret


cdef hyperdex_python_client_build_map_int_float(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef int64_t key
    cdef double val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_INT64_FLOAT, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_int_float_next(&it, &key, &val)
        if result <= 0:
            break
        ret[key] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, float)")
    return ret


cdef hyperdex_python_client_build_map_float_string(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef double key
    cdef const char* val
    cdef size_t val_sz
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_FLOAT_STRING, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_float_string_next(&it, &key, &val, &val_sz)
        if result <= 0:
            break
        ret[key] = val[:val_sz]
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, string)")
    return ret


cdef hyperdex_python_client_build_map_float_int(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef double key
    cdef int64_t val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_FLOAT_INT64, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_float_int_next(&it, &key, &val)
        if result <= 0:
            break
        ret[key] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, int)")
    return ret


cdef hyperdex_python_client_build_map_float_float(const char* value, size_t value_sz):
    cdef hyperdex_ds_iterator it
    cdef int result = 0
    cdef double key
    cdef double val
    hyperdex_ds_iterator_init(&it, HYPERDATATYPE_MAP_FLOAT_FLOAT, value, value_sz)
    ret = {}
    while True:
        result = hyperdex_ds_iterate_map_float_float_next(&it, &key, &val)
        if result <= 0:
            break
        ret[key] = val
    if result < 0:
        raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, float)")
    return ret


cdef hyperdex_python_client_build_attributes(const hyperdex_client_attribute* attrs, size_t attrs_sz):
    ret = {}
    for i in range(attrs_sz):
        val = None
        if attrs[i].datatype == HYPERDATATYPE_STRING:
            val = hyperdex_python_client_build_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_INT64:
            val = hyperdex_python_client_build_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_FLOAT:
            val = hyperdex_python_client_build_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_DOCUMENT:
            val = hyperdex_python_client_build_document(attrs[i].value, attrs[i].value_sz)
        elif (attrs[i].datatype in
             [HYPERDATATYPE_TIMESTAMP_SECOND,
             HYPERDATATYPE_TIMESTAMP_MINUTE,
             HYPERDATATYPE_TIMESTAMP_HOUR, HYPERDATATYPE_TIMESTAMP_DAY,
             HYPERDATATYPE_TIMESTAMP_WEEK, HYPERDATATYPE_TIMESTAMP_MONTH]):
            t = long(hyperdex_python_client_build_int(attrs[i].value, attrs[i].value_sz))
            val = datetime.datetime.utcfromtimestamp(t / 1e6)
        elif attrs[i].datatype == HYPERDATATYPE_LIST_STRING:
            val = hyperdex_python_client_build_list_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_LIST_INT64:
            val = hyperdex_python_client_build_list_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_LIST_FLOAT:
            val = hyperdex_python_client_build_list_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_SET_STRING:
            val = hyperdex_python_client_build_set_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_SET_INT64:
            val = hyperdex_python_client_build_set_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_SET_FLOAT:
            val = hyperdex_python_client_build_set_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_STRING_STRING:
            val = hyperdex_python_client_build_map_string_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_STRING_INT64:
            val = hyperdex_python_client_build_map_string_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_STRING_FLOAT:
            val = hyperdex_python_client_build_map_string_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_INT64_STRING:
            val = hyperdex_python_client_build_map_int_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_INT64_INT64:
            val = hyperdex_python_client_build_map_int_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_INT64_FLOAT:
            val = hyperdex_python_client_build_map_int_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_FLOAT_STRING:
            val = hyperdex_python_client_build_map_float_string(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_FLOAT_INT64:
            val = hyperdex_python_client_build_map_float_int(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MAP_FLOAT_FLOAT:
            val = hyperdex_python_client_build_map_float_float(attrs[i].value, attrs[i].value_sz)
        elif attrs[i].datatype == HYPERDATATYPE_MACAROON_SECRET:
            val = hyperdex_python_client_build_string(attrs[i].value, attrs[i].value_sz)
        else:
            raise HyperDexClientException(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed attributes")
        ret[attrs[i].attr] = val
    return ret


cdef hyperdex_python_client_deferred_encode_status(Deferred d):
    if d.status == HYPERDEX_CLIENT_SUCCESS:
        return True
    elif d.status == HYPERDEX_CLIENT_NOTFOUND:
        return None
    elif d.status == HYPERDEX_CLIENT_CMPFAIL:
        return False
    else:
        raise HyperDexClientException(d.status, hyperdex_client_error_message(d.client.client))

cdef hyperdex_python_client_deferred_encode_status_attributes(Deferred d):
    if d.status == HYPERDEX_CLIENT_SUCCESS:
        return hyperdex_python_client_build_attributes(d.attrs, d.attrs_sz)
    elif d.status == HYPERDEX_CLIENT_NOTFOUND:
        return None
    elif d.status == HYPERDEX_CLIENT_CMPFAIL:
        return False
    else:
        raise HyperDexClientException(d.status, hyperdex_client_error_message(d.client.client))

cdef hyperdex_python_client_deferred_encode_status_count(Deferred d):
    if d.status == HYPERDEX_CLIENT_SUCCESS:
        return long(d.count)
    elif d.status == HYPERDEX_CLIENT_NOTFOUND:
        return None
    elif d.status == HYPERDEX_CLIENT_CMPFAIL:
        return False
    else:
        raise HyperDexClientException(d.status, hyperdex_client_error_message(d.client.client))

cdef hyperdex_python_client_deferred_encode_status_description(Deferred d):
    if d.status == HYPERDEX_CLIENT_SUCCESS:
        return str(d.description)
    elif d.status == HYPERDEX_CLIENT_NOTFOUND:
        return None
    elif d.status == HYPERDEX_CLIENT_CMPFAIL:
        return False
    else:
        raise HyperDexClientException(d.status, hyperdex_client_error_message(d.client.client))

cdef hyperdex_python_client_iterator_encode_status_attributes(Iterator it):
    if it.status == HYPERDEX_CLIENT_SUCCESS:
        return hyperdex_python_client_build_attributes(it.attrs, it.attrs_sz)
    elif it.status == HYPERDEX_CLIENT_NOTFOUND:
        return None
    elif it.status == HYPERDEX_CLIENT_CMPFAIL:
        return False
    else:
        raise HyperDexClientException(it.status, hyperdex_client_error_message(it.client.client))

# Json can be represented natively in Python
# However, we add a wrapper class to distinguish dictionaries from documents
cdef class Document:
    cdef _doc

    # Create from either a dict (native representation) or a string
    def __init__(self, doc):
        if isinstance(doc, str):
            if len(doc) is 0:
                self._doc = {}
            else:
                self._doc = json.loads(doc)
        elif isinstance(doc, dict):
            self._doc = doc
        elif isinstance(doc, list):
            self._doc = doc
        else:
            raise ValueError("Input must either be a document (dict or list) or a string")

    def __len__(self):
        return len(self.inner_str())

    def doc(self):
        return self._doc

    def inner_str(self):
        return json.dumps(self._doc)

    def __str__(self):
        return 'Document(' + self.inner_str() + ')'

    def __repr__(self):
        return str(self)

    def __richcmp__(Document self, Document other not None, int op):
        if op == 2:
            return self.doc() == other.doc()
        elif op == 3:
            return self.doc() != other.doc()
        else:
            raise TypeError('No ordering is possible for Documents.')

cdef class Predicate:

    cdef list _checks

    def __init__(self, raw):
        self._checks = list(raw)

    def checks(self, attr):
        return [(attr, p, v) for p, v in self._checks]

cdef class Equals(Predicate):

    def __init__(self, elem):
        Predicate.__init__(self, ((HYPERPREDICATE_EQUALS, elem),))

cdef class LessEqual(Predicate):

    def __init__(self, upper):
        Predicate.__init__(self, ((HYPERPREDICATE_LESS_EQUAL, upper),))

cdef class GreaterEqual(Predicate):

    def __init__(self, lower):
        Predicate.__init__(self, ((HYPERPREDICATE_GREATER_EQUAL, lower),))

cdef class LessThan(Predicate):

    def __init__(self, upper):
        Predicate.__init__(self, ((HYPERPREDICATE_LESS_THAN, upper),))

cdef class GreaterThan(Predicate):

    def __init__(self, lower):
        Predicate.__init__(self, ((HYPERPREDICATE_GREATER_THAN, lower),))

cdef class Range(Predicate):

    def __init__(self, lower, upper):
        Predicate.__init__(self, ((HYPERPREDICATE_GREATER_EQUAL, lower),
                                  (HYPERPREDICATE_LESS_EQUAL, upper)))

cdef class Regex(Predicate):

    def __init__(self, regex):
        Predicate.__init__(self, ((HYPERPREDICATE_REGEX, regex),))

cdef class LengthEquals(Predicate):

    def __init__(self, length):
        Predicate.__init__(self, ((HYPERPREDICATE_LENGTH_EQUALS, length),))

cdef class LengthLessEqual(Predicate):

    def __init__(self, upper):
        Predicate.__init__(self, ((HYPERPREDICATE_LENGTH_LESS_EQUAL, upper),))

cdef class LengthGreaterEqual(Predicate):

    def __init__(self, lower):
        Predicate.__init__(self, ((HYPERPREDICATE_LENGTH_GREATER_EQUAL, lower),))

cdef class Contains(Predicate):

    def __init__(self, elem):
        Predicate.__init__(self, ((HYPERPREDICATE_CONTAINS, elem),))


cdef class Deferred:
    cdef Client client
    cdef hyperdex_ds_arena* arena
    cdef encret_deferred_fptr encode_return
    cdef int64_t reqid
    cdef hyperdex_client_returncode status
    cdef const hyperdex_client_attribute* attrs
    cdef size_t attrs_sz
    cdef const char* description
    cdef uint64_t count
    cdef bint finished

    def __cinit__(self, Client client):
        self.client = client
        self.arena = hyperdex_ds_arena_create()
        if self.arena == NULL:
            raise MemoryError()
        self.reqid = -1
        self.status = HYPERDEX_CLIENT_GARBAGE
        self.attrs = NULL
        self.attrs_sz = 0
        self.description = NULL
        self.count = 0
        self.finished = False

    def __dealloc__(self):
        if self.arena:
            hyperdex_ds_arena_destroy(self.arena)
        if self.attrs:
            hyperdex_client_destroy_attrs(self.attrs, self.attrs_sz)
            self.attrs = NULL
            self.attrs_sz = 0

    def _callback(self):
        self.finished = True
        del self.client.ops[self.reqid]

    def wait(self):
        while not self.finished and self.reqid > 0:
            self.client.loop()
        self.finished = True
        return self.encode_return(self)

# Utility class to easily iterate over a result of search()
# It will dynamically poll the client for more content
cdef class Iterator:
    cdef Client client
    cdef hyperdex_ds_arena* arena
    cdef encret_iterator_fptr encode_return
    cdef int64_t reqid
    cdef hyperdex_client_returncode status
    cdef const hyperdex_client_attribute* attrs
    cdef size_t attrs_sz
    cdef list backlogged
    cdef bint finished

    def __cinit__(self, Client client):
        self.client = client
        self.arena = hyperdex_ds_arena_create()
        if self.arena == NULL:
            raise MemoryError()
        self.reqid = -1
        self.status = HYPERDEX_CLIENT_GARBAGE
        self.attrs = NULL
        self.attrs_sz = 0
        self.backlogged = []
        self.finished = False

    def __dealloc__(self):
        if self.arena:
            hyperdex_ds_arena_destroy(self.arena)
        if self.attrs:
            hyperdex_client_destroy_attrs(self.attrs, self.attrs_sz)
            self.attrs = NULL
            self.attrs_sz = 0

    def __iter__(self):
        return self

    def hasNext(self):
        # Poll for new elements
        while not self.finished and not self.backlogged:
            self.client.loop()

        return self.backlogged

    def __next__(self):
        if self.hasNext():
            return self.backlogged.pop()
        else:
            raise StopIteration()

    def _callback(self):
        if self.status == HYPERDEX_CLIENT_SEARCHDONE:
            self.finished = True
            del self.client.ops[self.reqid]
        else:
            try:
                obj = self.encode_return(self)
                self.backlogged.append(obj)
            except HyperDexClientException as e:
                self.backlogged.append(e)
            finally:
                if self.attrs:
                    hyperdex_client_destroy_attrs(self.attrs, self.attrs_sz)
                    self.attrs = NULL
                    self.attrs_sz = 0



cdef class Microtransaction:
    def __cinit__(self, Client c, bytes spacename):
        cdef const char* in_space
        self.client = c
        self.deferred = Deferred(self.client)
        self.client.convert_spacename(self.deferred.arena, spacename, &in_space);
        self.transaction = hyperdex_client_uxact_init(self.client.client, in_space, &self.deferred.status)
    
    # Begin Automatically Generated UTX Methods
    def put(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_put(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_add(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_add(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_sub(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_sub(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_mul(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_mul(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_div(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_div(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_and(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_and(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_or(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_or(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def string_prepend(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_string_prepend(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def string_append(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_string_append(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def string_ltrim(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_string_ltrim(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def string_rtrim(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_string_rtrim(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def list_lpush(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_list_lpush(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def list_rpush(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_list_rpush(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def document_rename(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_document_rename(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def document_unset(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_document_unset(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_min(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_min(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    def atomic_max(self, dict attributes):
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.client.convert_attributes(self.deferred.arena, attributes, &in_attrs, &in_attrs_sz)
        res = hyperdex_client_uxact_atomic_max(self.client.client, self.transaction, in_attrs, in_attrs_sz)
        if res < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        return True
    # End Automatically Generated UTX Methods
    def async_group_commit(self, dict checks):
        cdef hyperdex_client_attribute_check* chks
        cdef size_t chks_sz
        self.client.convert_predicates(self.deferred.arena, checks, &chks, &chks_sz)

        self.deferred.reqid = hyperdex_client_uxact_group_commit(self.client.client, self.transaction, chks, chks_sz, &self.deferred.count)

        self.client.clear_auth_context()
        if self.deferred.reqid < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        self.deferred.encode_return = hyperdex_python_client_deferred_encode_status

        self.client.ops[self.deferred.reqid] = self.deferred
        self.deferred.encode_return = hyperdex_python_client_deferred_encode_status_count
        return self.deferred

    def group_commit(self, dict checks):
        return self.async_group_commit(checks).wait()
    
    def async_commit(self, bytes key):
        cdef const char* in_key
        cdef size_t in_key_sz
        self.client.convert_key(self.deferred.arena, key, &in_key, &in_key_sz)

        self.deferred.reqid = hyperdex_client_uxact_commit(self.client.client, self.transaction, in_key, in_key_sz)

        self.client.clear_auth_context()
        if self.deferred.reqid < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        self.deferred.encode_return = hyperdex_python_client_deferred_encode_status

        self.client.ops[self.deferred.reqid] = self.deferred
        return self.deferred

    def commit(self, bytes key):
        return self.async_commit(key).wait()
        
    def async_cond_commit(self, bytes key, dict checks):
        cdef const char* in_key
        cdef size_t in_key_sz
        self.client.convert_key(self.deferred.arena, key, &in_key, &in_key_sz)
        
        cdef hyperdex_client_attribute_check* chks
        cdef size_t chks_sz
        self.client.convert_predicates(self.deferred.arena, checks, &chks, &chks_sz)

        self.deferred.reqid = hyperdex_client_uxact_cond_commit(self.client.client, self.transaction, in_key, in_key_sz, chks, chks_sz)

        self.client.clear_auth_context()
        if self.deferred.reqid < 0:
            raise HyperDexClientException(self.deferred.status, hyperdex_client_error_message(self.client.client))
        self.deferred.encode_return = hyperdex_python_client_deferred_encode_status

        self.client.ops[self.deferred.reqid] = self.deferred
        return self.deferred

    def cond_commit(self, bytes key, dict checks):
        return self.async_cond_commit(key, checks).wait()

    cdef Deferred deferred
    cdef Client client
    cdef hyperdex_client_microtransaction* transaction

cdef class Client:
    cdef hyperdex_client* client
    cdef dict ops
    cdef const char** auth
    cdef size_t auth_sz
    cdef list auth_backing

    def __cinit__(self, address, port):
        self.client = hyperdex_client_create(address, port)
        self.ops = {}
        self.auth = NULL
        self.auth_sz = 0

    def __dealloc__(self):
        if self.client:
            hyperdex_client_destroy(self.client)

    cdef convert_spacename(self, hyperdex_ds_arena* arena, bytes spacename, const char** spacename_str):
        spacename_str[0] = spacename

    cdef convert_key(self, hyperdex_ds_arena* arena, key, const char** _key, size_t* _key_sz):
        cdef hyperdatatype dt
        hyperdex_python_client_convert_type(arena, key, _key, _key_sz, &dt)

    cdef convert_attributes(self, hyperdex_ds_arena* arena, attrs,
                            hyperdex_client_attribute** _attrs, size_t* _attrs_sz):
        _attrs[0] = hyperdex_ds_allocate_attribute(arena, len(attrs))
        if _attrs[0] == NULL:
            raise MemoryError()
        for i, (name, value) in enumerate(attrs.iteritems()):
            _attrs[0][i].attr = name
            hyperdex_python_client_convert_type(arena, value,
                                                &_attrs[0][i].value,
                                                &_attrs[0][i].value_sz,
                                                &_attrs[0][i].datatype)
        _attrs_sz[0] = len(attrs)

    cdef convert_predicate(self, attr, pred):
        if isinstance(pred, tuple) and len(pred) == 2 and \
           type(pred[0]) == type(pred[1]):
            return [(attr, HYPERPREDICATE_GREATER_EQUAL, pred[0]),
                    (attr, HYPERPREDICATE_LESS_EQUAL,    pred[1])]
        elif isinstance(pred, Predicate):
            return pred.checks(attr)
        else:
            return [(attr, HYPERPREDICATE_EQUALS, pred)]

    cdef convert_predicates(self, hyperdex_ds_arena* arena, preds,
                            hyperdex_client_attribute_check** _checks, size_t* _checks_sz):
        checks = []
        for attr, pred in preds.iteritems():
            if isinstance(pred, list):
                for p in pred:
                    checks += self.convert_predicate(attr, p)
            else:
                checks += self.convert_predicate(attr, pred)
        _checks_sz[0] = len(checks)
        _checks[0] = hyperdex_ds_allocate_attribute_check(arena, len(checks))
        if _checks[0] == NULL:
            raise MemoryError()
        for i, (attr, pred, val) in enumerate(checks):
            _checks[0][i].attr = attr
            _checks[0][i].predicate = pred
            hyperdex_python_client_convert_type(arena, val,
                                                &_checks[0][i].value,
                                                &_checks[0][i].value_sz,
                                                &_checks[0][i].datatype)


    # Convert from a python dictionary into a list of map attributes
    cdef convert_mapattributes(self, hyperdex_ds_arena* arena, dict mapattrs,
                               hyperdex_client_map_attribute** _mapattrs, size_t* _mapattrs_sz):
        length = sum([len(v) for k, v in mapattrs.iteritems()])
        _mapattrs[0] = hyperdex_ds_allocate_map_attribute(arena, length)
        if _mapattrs[0] == NULL:
            raise MemoryError()
        i = 0

        # This is actually a dictionary of dictionaries
        for name, mapvalue in mapattrs.iteritems():
            for k, v in mapvalue.iteritems():
                assert i < length
                _mapattrs[0][i].attr = name
                hyperdex_python_client_convert_type(arena, k,
                                                    &_mapattrs[0][i].map_key,
                                                    &_mapattrs[0][i].map_key_sz,
                                                    &_mapattrs[0][i].map_key_datatype)
                hyperdex_python_client_convert_type(arena, v,
                                                    &_mapattrs[0][i].value,
                                                    &_mapattrs[0][i].value_sz,
                                                    &_mapattrs[0][i].value_datatype)
                i += 1
        _mapattrs_sz[0] = i

    cdef convert_attributenames(self, hyperdex_ds_arena* arena, attributenames,
                                const char*** _attributenames, size_t* _attributenames_sz):
        _attributenames[0]    = <const char**>hyperdex_ds_malloc(arena, sizeof(char*) * len(attributenames))
        _attributenames_sz[0] = len(attributenames)
        for i, attr in enumerate(attributenames):
            _attributenames[0][i] = attr

    cdef convert_sortby(self, hyperdex_ds_arena* arena, bytes sortby, const char** sortby_str):
        sortby_str[0] = sortby

    cdef convert_limit(self, hyperdex_ds_arena* arena, long limit, uint64_t* count):
        count[0] = limit

    cdef convert_maxmin(self, hyperdex_ds_arena* arena, str compare, int* maximize):
        if compare not in ('maximize', 'max', 'minimize', 'min'):
            raise ValueError("Comparison must be either 'max' or 'min'")
        maximize[0] = 1 if compare in ('max', 'maximize') else 0

    cdef set_auth_context(self, auth):
        self.clear_auth_context()
        if auth is not None:
            self.auth_sz = len(auth)
            self.auth = <const char**>malloc(sizeof(const char*) * self.auth_sz)
            for i in range(self.auth_sz):
                self.auth_backing.append(auth[i])
                self.auth[i] = self.auth_backing[i]
            hyperdex_client_set_auth_context(self.client, self.auth, self.auth_sz)

    cdef clear_auth_context(self):
        hyperdex_client_clear_auth_context(self.client)
        if self.auth != NULL:
            free(self.auth);
        self.auth = NULL
        self.auth_sz = 0
        self.auth_backing = []

    def loop(self):
        cdef hyperdex_client_returncode status
        ret = hyperdex_client_loop(self.client, -1, &status)
        if ret < 0:
            raise HyperDexClientException(status, hyperdex_client_error_message(self.client))
        else:
            assert ret in self.ops
            op = self.ops[ret]
            # We cannot refer to self.ops[ret] after this call as
            # _callback() may remove ret from self.ops.
            op._callback()
            return op
            
    def microtransaction_init(self, bytes spacename):
        return Microtransaction(self, spacename)

    # Begin Automatically Generated Methods
    cdef asynccall__spacename_key__status_attributes(self, asynccall__spacename_key__status_attributes_fptr f, bytes spacename, key, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, &d.status, &d.attrs, &d.attrs_sz);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_attributes
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_attributenames__status_attributes(self, asynccall__spacename_key_attributenames__status_attributes_fptr f, bytes spacename, key, attributenames, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef const char** in_attrnames
        cdef size_t in_attrnames_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_attributenames(d.arena, attributenames, &in_attrnames, &in_attrnames_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_attrnames, in_attrnames_sz, &d.status, &d.attrs, &d.attrs_sz);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_attributes
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_attributes__status(self, asynccall__spacename_key_attributes__status_fptr f, bytes spacename, key, dict attributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_attributes(d.arena, attributes, &in_attrs, &in_attrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_attrs, in_attrs_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_predicates_attributes__status(self, asynccall__spacename_key_predicates_attributes__status_fptr f, bytes spacename, key, dict predicates, dict attributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.convert_attributes(d.arena, attributes, &in_attrs, &in_attrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, in_attrs, in_attrs_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_predicates_attributes__status_count(self, asynccall__spacename_predicates_attributes__status_count_fptr f, bytes spacename, dict predicates, dict attributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        cdef hyperdex_client_attribute* in_attrs
        cdef size_t in_attrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.convert_attributes(d.arena, attributes, &in_attrs, &in_attrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_checks, in_checks_sz, in_attrs, in_attrs_sz, &d.status, &d.count);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_count
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key__status(self, asynccall__spacename_key__status_fptr f, bytes spacename, key, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_predicates__status(self, asynccall__spacename_key_predicates__status_fptr f, bytes spacename, key, dict predicates, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_predicates__status_count(self, asynccall__spacename_predicates__status_count_fptr f, bytes spacename, dict predicates, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_checks, in_checks_sz, &d.status, &d.count);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_count
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_mapattributes__status(self, asynccall__spacename_key_mapattributes__status_fptr f, bytes spacename, key, dict mapattributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef hyperdex_client_map_attribute* in_mapattrs
        cdef size_t in_mapattrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_mapattributes(d.arena, mapattributes, &in_mapattrs, &in_mapattrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_mapattrs, in_mapattrs_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_key_predicates_mapattributes__status(self, asynccall__spacename_key_predicates_mapattributes__status_fptr f, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef const char* in_key
        cdef size_t in_key_sz
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        cdef hyperdex_client_map_attribute* in_mapattrs
        cdef size_t in_mapattrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_key(d.arena, key, &in_key, &in_key_sz);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.convert_mapattributes(d.arena, mapattributes, &in_mapattrs, &in_mapattrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, in_mapattrs, in_mapattrs_sz, &d.status);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status
        self.ops[d.reqid] = d
        return d

    cdef asynccall__spacename_predicates_mapattributes__status_count(self, asynccall__spacename_predicates_mapattributes__status_count_fptr f, bytes spacename, dict predicates, dict mapattributes, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        cdef hyperdex_client_map_attribute* in_mapattrs
        cdef size_t in_mapattrs_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.convert_mapattributes(d.arena, mapattributes, &in_mapattrs, &in_mapattrs_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_checks, in_checks_sz, in_mapattrs, in_mapattrs_sz, &d.status, &d.count);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_count
        self.ops[d.reqid] = d
        return d

    cdef iterator__spacename_predicates__status_attributes(self, iterator__spacename_predicates__status_attributes_fptr f, bytes spacename, dict predicates):
        cdef Iterator it = Iterator(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        self.convert_spacename(it.arena, spacename, &in_space);
        self.convert_predicates(it.arena, predicates, &in_checks, &in_checks_sz);
        it.reqid = f(self.client, in_space, in_checks, in_checks_sz, &it.status, &it.attrs, &it.attrs_sz);
        if it.reqid < 0:
            raise HyperDexClientException(it.status, hyperdex_client_error_message(self.client))
        it.encode_return = hyperdex_python_client_iterator_encode_status_attributes
        self.ops[it.reqid] = it
        return it

    cdef asynccall__spacename_predicates__status_description(self, asynccall__spacename_predicates__status_description_fptr f, bytes spacename, dict predicates, auth=None):
        cdef Deferred d = Deferred(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        self.convert_spacename(d.arena, spacename, &in_space);
        self.convert_predicates(d.arena, predicates, &in_checks, &in_checks_sz);
        self.set_auth_context(auth)
        d.reqid = f(self.client, in_space, in_checks, in_checks_sz, &d.status, &d.description);
        self.clear_auth_context()
        if d.reqid < 0:
            raise HyperDexClientException(d.status, hyperdex_client_error_message(self.client))
        d.encode_return = hyperdex_python_client_deferred_encode_status_description
        self.ops[d.reqid] = d
        return d

    cdef iterator__spacename_predicates_sortby_limit_maxmin__status_attributes(self, iterator__spacename_predicates_sortby_limit_maxmin__status_attributes_fptr f, bytes spacename, dict predicates, bytes sortby, int limit, str maxmin):
        cdef Iterator it = Iterator(self)
        cdef const char* in_space
        cdef hyperdex_client_attribute_check* in_checks
        cdef size_t in_checks_sz
        cdef const char* in_sort_by
        cdef uint64_t in_limit
        cdef int in_maxmin
        self.convert_spacename(it.arena, spacename, &in_space);
        self.convert_predicates(it.arena, predicates, &in_checks, &in_checks_sz);
        self.convert_sortby(it.arena, sortby, &in_sort_by);
        self.convert_limit(it.arena, limit, &in_limit);
        self.convert_maxmin(it.arena, maxmin, &in_maxmin);
        it.reqid = f(self.client, in_space, in_checks, in_checks_sz, in_sort_by, in_limit, in_maxmin, &it.status, &it.attrs, &it.attrs_sz);
        if it.reqid < 0:
            raise HyperDexClientException(it.status, hyperdex_client_error_message(self.client))
        it.encode_return = hyperdex_python_client_iterator_encode_status_attributes
        self.ops[it.reqid] = it
        return it

    def async_get(self, bytes spacename, key, auth=None):
        return self.asynccall__spacename_key__status_attributes(hyperdex_client_get, spacename, key, auth)
    def get(self, bytes spacename, key, auth=None):
        return self.async_get(spacename, key, auth).wait()

    def async_get_partial(self, bytes spacename, key, attributenames, auth=None):
        return self.asynccall__spacename_key_attributenames__status_attributes(hyperdex_client_get_partial, spacename, key, attributenames, auth)
    def get_partial(self, bytes spacename, key, attributenames, auth=None):
        return self.async_get_partial(spacename, key, attributenames, auth).wait()

    def async_put(self, bytes spacename, key, dict attributes, auth=None, secret=None):
        if secret is not None: attributes['__secret'] = secret
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_put, spacename, key, attributes, auth)
    def put(self, bytes spacename, key, dict attributes, auth=None, secret=None):
        if secret is not None: attributes['__secret'] = secret
        return self.async_put(spacename, key, attributes, auth).wait()

    def async_cond_put(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_put, spacename, key, predicates, attributes, auth)
    def cond_put(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_put(spacename, key, predicates, attributes, auth).wait()

    def async_cond_put_or_create(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_put_or_create, spacename, key, predicates, attributes, auth)
    def cond_put_or_create(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_put_or_create(spacename, key, predicates, attributes, auth).wait()

    def async_group_put(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_put, spacename, predicates, attributes, auth)
    def group_put(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_put(spacename, predicates, attributes, auth).wait()

    def async_put_if_not_exist(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_put_if_not_exist, spacename, key, attributes, auth)
    def put_if_not_exist(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_put_if_not_exist(spacename, key, attributes, auth).wait()

    def async_delete(self, bytes spacename, key, auth=None):
        return self.asynccall__spacename_key__status(hyperdex_client_del, spacename, key, auth)
    def delete(self, bytes spacename, key, auth=None):
        return self.async_delete(spacename, key, auth).wait()

    def async_cond_del(self, bytes spacename, key, dict predicates, auth=None):
        return self.asynccall__spacename_key_predicates__status(hyperdex_client_cond_del, spacename, key, predicates, auth)
    def cond_del(self, bytes spacename, key, dict predicates, auth=None):
        return self.async_cond_del(spacename, key, predicates, auth).wait()

    def async_group_del(self, bytes spacename, dict predicates, auth=None):
        return self.asynccall__spacename_predicates__status_count(hyperdex_client_group_del, spacename, predicates, auth)
    def group_del(self, bytes spacename, dict predicates, auth=None):
        return self.async_group_del(spacename, predicates, auth).wait()

    def async_atomic_add(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_add, spacename, key, attributes, auth)
    def atomic_add(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_add(spacename, key, attributes, auth).wait()

    def async_cond_atomic_add(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_add, spacename, key, predicates, attributes, auth)
    def cond_atomic_add(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_add(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_add(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_add, spacename, predicates, attributes, auth)
    def group_atomic_add(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_add(spacename, predicates, attributes, auth).wait()

    def async_atomic_sub(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_sub, spacename, key, attributes, auth)
    def atomic_sub(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_sub(spacename, key, attributes, auth).wait()

    def async_cond_atomic_sub(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_sub, spacename, key, predicates, attributes, auth)
    def cond_atomic_sub(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_sub(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_sub(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_sub, spacename, predicates, attributes, auth)
    def group_atomic_sub(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_sub(spacename, predicates, attributes, auth).wait()

    def async_atomic_mul(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_mul, spacename, key, attributes, auth)
    def atomic_mul(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_mul(spacename, key, attributes, auth).wait()

    def async_cond_atomic_mul(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_mul, spacename, key, predicates, attributes, auth)
    def cond_atomic_mul(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_mul(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_mul(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_mul, spacename, predicates, attributes, auth)
    def group_atomic_mul(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_mul(spacename, predicates, attributes, auth).wait()

    def async_atomic_div(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_div, spacename, key, attributes, auth)
    def atomic_div(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_div(spacename, key, attributes, auth).wait()

    def async_cond_atomic_div(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_div, spacename, key, predicates, attributes, auth)
    def cond_atomic_div(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_div(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_div(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_div, spacename, predicates, attributes, auth)
    def group_atomic_div(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_div(spacename, predicates, attributes, auth).wait()

    def async_atomic_mod(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_mod, spacename, key, attributes, auth)
    def atomic_mod(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_mod(spacename, key, attributes, auth).wait()

    def async_cond_atomic_mod(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_mod, spacename, key, predicates, attributes, auth)
    def cond_atomic_mod(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_mod(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_mod(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_mod, spacename, predicates, attributes, auth)
    def group_atomic_mod(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_mod(spacename, predicates, attributes, auth).wait()

    def async_atomic_and(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_and, spacename, key, attributes, auth)
    def atomic_and(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_and(spacename, key, attributes, auth).wait()

    def async_cond_atomic_and(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_and, spacename, key, predicates, attributes, auth)
    def cond_atomic_and(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_and(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_and(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_and, spacename, predicates, attributes, auth)
    def group_atomic_and(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_and(spacename, predicates, attributes, auth).wait()

    def async_atomic_or(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_or, spacename, key, attributes, auth)
    def atomic_or(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_or(spacename, key, attributes, auth).wait()

    def async_cond_atomic_or(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_or, spacename, key, predicates, attributes, auth)
    def cond_atomic_or(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_or(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_or(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_or, spacename, predicates, attributes, auth)
    def group_atomic_or(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_or(spacename, predicates, attributes, auth).wait()

    def async_atomic_xor(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_xor, spacename, key, attributes, auth)
    def atomic_xor(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_xor(spacename, key, attributes, auth).wait()

    def async_cond_atomic_xor(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_xor, spacename, key, predicates, attributes, auth)
    def cond_atomic_xor(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_xor(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_xor(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_xor, spacename, predicates, attributes, auth)
    def group_atomic_xor(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_xor(spacename, predicates, attributes, auth).wait()

    def async_atomic_min(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_min, spacename, key, attributes, auth)
    def atomic_min(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_min(spacename, key, attributes, auth).wait()

    def async_cond_atomic_min(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_min, spacename, key, predicates, attributes, auth)
    def cond_atomic_min(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_min(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_min(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_min, spacename, predicates, attributes, auth)
    def group_atomic_min(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_min(spacename, predicates, attributes, auth).wait()

    def async_atomic_max(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_atomic_max, spacename, key, attributes, auth)
    def atomic_max(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_atomic_max(spacename, key, attributes, auth).wait()

    def async_cond_atomic_max(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_atomic_max, spacename, key, predicates, attributes, auth)
    def cond_atomic_max(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_atomic_max(spacename, key, predicates, attributes, auth).wait()

    def async_group_atomic_max(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_atomic_max, spacename, predicates, attributes, auth)
    def group_atomic_max(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_atomic_max(spacename, predicates, attributes, auth).wait()

    def async_string_prepend(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_string_prepend, spacename, key, attributes, auth)
    def string_prepend(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_string_prepend(spacename, key, attributes, auth).wait()

    def async_cond_string_prepend(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_string_prepend, spacename, key, predicates, attributes, auth)
    def cond_string_prepend(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_string_prepend(spacename, key, predicates, attributes, auth).wait()

    def async_group_string_prepend(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_string_prepend, spacename, predicates, attributes, auth)
    def group_string_prepend(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_string_prepend(spacename, predicates, attributes, auth).wait()

    def async_string_append(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_string_append, spacename, key, attributes, auth)
    def string_append(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_string_append(spacename, key, attributes, auth).wait()

    def async_cond_string_append(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_string_append, spacename, key, predicates, attributes, auth)
    def cond_string_append(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_string_append(spacename, key, predicates, attributes, auth).wait()

    def async_group_string_append(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_string_append, spacename, predicates, attributes, auth)
    def group_string_append(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_string_append(spacename, predicates, attributes, auth).wait()

    def async_string_ltrim(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_string_ltrim, spacename, key, attributes, auth)
    def string_ltrim(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_string_ltrim(spacename, key, attributes, auth).wait()

    def async_cond_string_ltrim(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_string_ltrim, spacename, key, predicates, attributes, auth)
    def cond_string_ltrim(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_string_ltrim(spacename, key, predicates, attributes, auth).wait()

    def async_group_string_ltrim(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_string_ltrim, spacename, predicates, attributes, auth)
    def group_string_ltrim(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_string_ltrim(spacename, predicates, attributes, auth).wait()

    def async_string_rtrim(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_string_rtrim, spacename, key, attributes, auth)
    def string_rtrim(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_string_rtrim(spacename, key, attributes, auth).wait()

    def async_cond_string_rtrim(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_string_rtrim, spacename, key, predicates, attributes, auth)
    def cond_string_rtrim(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_string_rtrim(spacename, key, predicates, attributes, auth).wait()

    def async_group_string_rtrim(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_string_rtrim, spacename, predicates, attributes, auth)
    def group_string_rtrim(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_string_rtrim(spacename, predicates, attributes, auth).wait()

    def async_list_lpush(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_list_lpush, spacename, key, attributes, auth)
    def list_lpush(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_list_lpush(spacename, key, attributes, auth).wait()

    def async_cond_list_lpush(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_list_lpush, spacename, key, predicates, attributes, auth)
    def cond_list_lpush(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_list_lpush(spacename, key, predicates, attributes, auth).wait()

    def async_group_list_lpush(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_list_lpush, spacename, predicates, attributes, auth)
    def group_list_lpush(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_list_lpush(spacename, predicates, attributes, auth).wait()

    def async_list_rpush(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_list_rpush, spacename, key, attributes, auth)
    def list_rpush(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_list_rpush(spacename, key, attributes, auth).wait()

    def async_cond_list_rpush(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_list_rpush, spacename, key, predicates, attributes, auth)
    def cond_list_rpush(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_list_rpush(spacename, key, predicates, attributes, auth).wait()

    def async_group_list_rpush(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_list_rpush, spacename, predicates, attributes, auth)
    def group_list_rpush(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_list_rpush(spacename, predicates, attributes, auth).wait()

    def async_set_add(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_set_add, spacename, key, attributes, auth)
    def set_add(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_set_add(spacename, key, attributes, auth).wait()

    def async_cond_set_add(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_set_add, spacename, key, predicates, attributes, auth)
    def cond_set_add(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_set_add(spacename, key, predicates, attributes, auth).wait()

    def async_group_set_add(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_set_add, spacename, predicates, attributes, auth)
    def group_set_add(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_set_add(spacename, predicates, attributes, auth).wait()

    def async_set_remove(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_set_remove, spacename, key, attributes, auth)
    def set_remove(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_set_remove(spacename, key, attributes, auth).wait()

    def async_cond_set_remove(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_set_remove, spacename, key, predicates, attributes, auth)
    def cond_set_remove(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_set_remove(spacename, key, predicates, attributes, auth).wait()

    def async_group_set_remove(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_set_remove, spacename, predicates, attributes, auth)
    def group_set_remove(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_set_remove(spacename, predicates, attributes, auth).wait()

    def async_set_intersect(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_set_intersect, spacename, key, attributes, auth)
    def set_intersect(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_set_intersect(spacename, key, attributes, auth).wait()

    def async_cond_set_intersect(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_set_intersect, spacename, key, predicates, attributes, auth)
    def cond_set_intersect(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_set_intersect(spacename, key, predicates, attributes, auth).wait()

    def async_group_set_intersect(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_set_intersect, spacename, predicates, attributes, auth)
    def group_set_intersect(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_set_intersect(spacename, predicates, attributes, auth).wait()

    def async_set_union(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_set_union, spacename, key, attributes, auth)
    def set_union(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_set_union(spacename, key, attributes, auth).wait()

    def async_cond_set_union(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_set_union, spacename, key, predicates, attributes, auth)
    def cond_set_union(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_set_union(spacename, key, predicates, attributes, auth).wait()

    def async_group_set_union(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_set_union, spacename, predicates, attributes, auth)
    def group_set_union(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_set_union(spacename, predicates, attributes, auth).wait()

    def async_document_rename(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_document_rename, spacename, key, attributes, auth)
    def document_rename(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_document_rename(spacename, key, attributes, auth).wait()

    def async_cond_document_rename(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_document_rename, spacename, key, predicates, attributes, auth)
    def cond_document_rename(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_document_rename(spacename, key, predicates, attributes, auth).wait()

    def async_group_document_rename(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_document_rename, spacename, predicates, attributes, auth)
    def group_document_rename(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_document_rename(spacename, predicates, attributes, auth).wait()

    def async_document_unset(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_document_unset, spacename, key, attributes, auth)
    def document_unset(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_document_unset(spacename, key, attributes, auth).wait()

    def async_cond_document_unset(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_document_unset, spacename, key, predicates, attributes, auth)
    def cond_document_unset(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_document_unset(spacename, key, predicates, attributes, auth).wait()

    def async_group_document_unset(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_document_unset, spacename, predicates, attributes, auth)
    def group_document_unset(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_document_unset(spacename, predicates, attributes, auth).wait()

    def async_map_add(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_add, spacename, key, mapattributes, auth)
    def map_add(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_add(spacename, key, mapattributes, auth).wait()

    def async_cond_map_add(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_add, spacename, key, predicates, mapattributes, auth)
    def cond_map_add(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_add(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_add(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_add, spacename, predicates, mapattributes, auth)
    def group_map_add(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_add(spacename, predicates, mapattributes, auth).wait()

    def async_map_remove(self, bytes spacename, key, dict attributes, auth=None):
        return self.asynccall__spacename_key_attributes__status(hyperdex_client_map_remove, spacename, key, attributes, auth)
    def map_remove(self, bytes spacename, key, dict attributes, auth=None):
        return self.async_map_remove(spacename, key, attributes, auth).wait()

    def async_cond_map_remove(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_key_predicates_attributes__status(hyperdex_client_cond_map_remove, spacename, key, predicates, attributes, auth)
    def cond_map_remove(self, bytes spacename, key, dict predicates, dict attributes, auth=None):
        return self.async_cond_map_remove(spacename, key, predicates, attributes, auth).wait()

    def async_group_map_remove(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.asynccall__spacename_predicates_attributes__status_count(hyperdex_client_group_map_remove, spacename, predicates, attributes, auth)
    def group_map_remove(self, bytes spacename, dict predicates, dict attributes, auth=None):
        return self.async_group_map_remove(spacename, predicates, attributes, auth).wait()

    def async_map_atomic_add(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_add, spacename, key, mapattributes, auth)
    def map_atomic_add(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_add(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_add(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_add, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_add(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_add(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_add(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_add, spacename, predicates, mapattributes, auth)
    def group_map_atomic_add(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_add(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_sub(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_sub, spacename, key, mapattributes, auth)
    def map_atomic_sub(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_sub(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_sub(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_sub, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_sub(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_sub(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_sub(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_sub, spacename, predicates, mapattributes, auth)
    def group_map_atomic_sub(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_sub(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_mul(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_mul, spacename, key, mapattributes, auth)
    def map_atomic_mul(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_mul(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_mul(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_mul, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_mul(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_mul(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_mul(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_mul, spacename, predicates, mapattributes, auth)
    def group_map_atomic_mul(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_mul(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_div(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_div, spacename, key, mapattributes, auth)
    def map_atomic_div(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_div(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_div(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_div, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_div(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_div(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_div(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_div, spacename, predicates, mapattributes, auth)
    def group_map_atomic_div(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_div(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_mod(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_mod, spacename, key, mapattributes, auth)
    def map_atomic_mod(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_mod(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_mod(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_mod, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_mod(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_mod(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_mod(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_mod, spacename, predicates, mapattributes, auth)
    def group_map_atomic_mod(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_mod(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_and(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_and, spacename, key, mapattributes, auth)
    def map_atomic_and(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_and(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_and(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_and, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_and(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_and(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_and(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_and, spacename, predicates, mapattributes, auth)
    def group_map_atomic_and(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_and(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_or(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_or, spacename, key, mapattributes, auth)
    def map_atomic_or(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_or(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_or(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_or, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_or(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_or(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_or(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_or, spacename, predicates, mapattributes, auth)
    def group_map_atomic_or(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_or(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_xor(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_xor, spacename, key, mapattributes, auth)
    def map_atomic_xor(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_xor(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_xor(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_xor, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_xor(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_xor(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_xor(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_xor, spacename, predicates, mapattributes, auth)
    def group_map_atomic_xor(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_xor(spacename, predicates, mapattributes, auth).wait()

    def async_map_string_prepend(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_string_prepend, spacename, key, mapattributes, auth)
    def map_string_prepend(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_string_prepend(spacename, key, mapattributes, auth).wait()

    def async_cond_map_string_prepend(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_string_prepend, spacename, key, predicates, mapattributes, auth)
    def cond_map_string_prepend(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_string_prepend(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_string_prepend(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_string_prepend, spacename, predicates, mapattributes, auth)
    def group_map_string_prepend(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_string_prepend(spacename, predicates, mapattributes, auth).wait()

    def async_map_string_append(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_string_append, spacename, key, mapattributes, auth)
    def map_string_append(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_string_append(spacename, key, mapattributes, auth).wait()

    def async_cond_map_string_append(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_string_append, spacename, key, predicates, mapattributes, auth)
    def cond_map_string_append(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_string_append(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_string_append(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_string_append, spacename, predicates, mapattributes, auth)
    def group_map_string_append(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_string_append(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_min(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_min, spacename, key, mapattributes, auth)
    def map_atomic_min(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_min(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_min(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_min, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_min(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_min(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_min(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_min, spacename, predicates, mapattributes, auth)
    def group_map_atomic_min(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_min(spacename, predicates, mapattributes, auth).wait()

    def async_map_atomic_max(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_mapattributes__status(hyperdex_client_map_atomic_max, spacename, key, mapattributes, auth)
    def map_atomic_max(self, bytes spacename, key, dict mapattributes, auth=None):
        return self.async_map_atomic_max(spacename, key, mapattributes, auth).wait()

    def async_cond_map_atomic_max(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_key_predicates_mapattributes__status(hyperdex_client_cond_map_atomic_max, spacename, key, predicates, mapattributes, auth)
    def cond_map_atomic_max(self, bytes spacename, key, dict predicates, dict mapattributes, auth=None):
        return self.async_cond_map_atomic_max(spacename, key, predicates, mapattributes, auth).wait()

    def async_group_map_atomic_max(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.asynccall__spacename_predicates_mapattributes__status_count(hyperdex_client_group_map_atomic_max, spacename, predicates, mapattributes, auth)
    def group_map_atomic_max(self, bytes spacename, dict predicates, dict mapattributes, auth=None):
        return self.async_group_map_atomic_max(spacename, predicates, mapattributes, auth).wait()

    def search(self, bytes spacename, dict predicates):
        return self.iterator__spacename_predicates__status_attributes(hyperdex_client_search, spacename, predicates)

    def async_search_describe(self, bytes spacename, dict predicates, auth=None):
        return self.asynccall__spacename_predicates__status_description(hyperdex_client_search_describe, spacename, predicates, auth)
    def search_describe(self, bytes spacename, dict predicates, auth=None):
        return self.async_search_describe(spacename, predicates, auth).wait()

    def sorted_search(self, bytes spacename, dict predicates, bytes sortby, int limit, str maxmin):
        return self.iterator__spacename_predicates_sortby_limit_maxmin__status_attributes(hyperdex_client_sorted_search, spacename, predicates, sortby, limit, maxmin)

    def async_count(self, bytes spacename, dict predicates, auth=None):
        return self.asynccall__spacename_predicates__status_count(hyperdex_client_count, spacename, predicates, auth)
    def count(self, bytes spacename, dict predicates, auth=None):
        return self.async_count(spacename, predicates, auth).wait()
    # End Automatically Generated Methods
