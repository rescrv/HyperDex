# Copyright (c) 2011, Cornell University
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

cdef extern from "sys/socket.h":

    ctypedef uint16_t in_port_t
    cdef struct sockaddr

cdef extern from "../../hyperdex.h":

    cdef enum hyperdatatype:
        HYPERDATATYPE_GENERIC            = 9216
        HYPERDATATYPE_STRING             = 9217
        HYPERDATATYPE_INT64              = 9218
        HYPERDATATYPE_FLOAT              = 9219
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
        HYPERDATATYPE_GARBAGE            = 9727

    cdef enum hyperpredicate:
        HYPERPREDICATE_FAIL          = 9728
        HYPERPREDICATE_EQUALS        = 9729
        HYPERPREDICATE_LESS_EQUAL    = 9730
        HYPERPREDICATE_GREATER_EQUAL = 9731
        HYPERPREDICATE_REGEX         = 9733
        HYPERPREDICATE_LENGTH_LESS_EQUAL    = 9735
        HYPERPREDICATE_CONTAINS      = 9737

cdef extern from "../hyperclient.h":

    cdef struct hyperclient

    cdef struct hyperclient_attribute:
        char* attr
        char* value
        size_t value_sz
        hyperdatatype datatype

    cdef struct hyperclient_map_attribute:
        char* attr
        char* map_key
        size_t map_key_sz
        hyperdatatype map_key_datatype
        char* value
        size_t value_sz
        hyperdatatype value_datatype

    cdef struct hyperclient_attribute_check:
        char* attr
        char* value
        size_t value_sz
        hyperdatatype datatype
        hyperpredicate predicate

    cdef enum hyperclient_returncode:
        HYPERCLIENT_SUCCESS      = 8448
        HYPERCLIENT_NOTFOUND     = 8449
        HYPERCLIENT_SEARCHDONE   = 8450
        HYPERCLIENT_CMPFAIL      = 8451
        HYPERCLIENT_READONLY     = 8452
        HYPERCLIENT_UNKNOWNSPACE = 8512
        HYPERCLIENT_COORDFAIL    = 8513
        HYPERCLIENT_SERVERERROR  = 8514
        HYPERCLIENT_POLLFAILED   = 8515
        HYPERCLIENT_OVERFLOW     = 8516
        HYPERCLIENT_RECONFIGURE  = 8517
        HYPERCLIENT_TIMEOUT      = 8519
        HYPERCLIENT_UNKNOWNATTR  = 8520
        HYPERCLIENT_DUPEATTR     = 8521
        HYPERCLIENT_NONEPENDING  = 8523
        HYPERCLIENT_DONTUSEKEY   = 8524
        HYPERCLIENT_WRONGTYPE    = 8525
        HYPERCLIENT_NOMEM        = 8526
        HYPERCLIENT_BADCONFIG    = 8527
        HYPERCLIENT_BADSPACE     = 8528
        HYPERCLIENT_DUPLICATE    = 8529
        HYPERCLIENT_INTERRUPTED  = 8530
        HYPERCLIENT_CLUSTER_JUMP = 8531
        HYPERCLIENT_COORD_LOGGED = 8532
        HYPERCLIENT_INTERNAL     = 8573
        HYPERCLIENT_EXCEPTION    = 8574
        HYPERCLIENT_GARBAGE      = 8575

    hyperclient* hyperclient_create(char* coordinator, uint16_t port)
    void hyperclient_destroy(hyperclient* client)
    hyperclient_returncode hyperclient_add_space(hyperclient* client, char* space)
    hyperclient_returncode hyperclient_rm_space(hyperclient* client, char* space)
    int64_t hyperclient_get(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_put_if_not_exist(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_list_lpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_list_lpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_list_rpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_list_rpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_set_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_set_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_intersect(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_set_intersect(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_union(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_set_union(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_cond_map_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_search(hyperclient* client, char* space, hyperclient_attribute_check* chks, size_t chks_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_search_describe(hyperclient* client, char* space, hyperclient_attribute_check* chks, size_t chks_sz, hyperclient_returncode* status, char** text)
    int64_t hyperclient_sorted_search(hyperclient* client, char* space, hyperclient_attribute_check* chks, size_t chks_sz, char* sort_by, uint64_t limit, int maximize, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_group_del(hyperclient* client, char* space, hyperclient_attribute_check* chks, size_t chks_sz, hyperclient_returncode* status)
    int64_t hyperclient_count(hyperclient* client, char* space, hyperclient_attribute_check* chks, size_t chks_sz, hyperclient_returncode* status, uint64_t* result)
    int64_t hyperclient_loop(hyperclient* client, int timeout, hyperclient_returncode* status)
    void hyperclient_destroy_attrs(hyperclient_attribute* attrs, size_t attrs_sz)

ctypedef int64_t (*hyperclient_simple_op)(hyperclient*, char*, char*, size_t, hyperclient_attribute*, size_t, hyperclient_returncode*)
ctypedef int64_t (*hyperclient_map_op)(hyperclient*, char*, char*, size_t, hyperclient_map_attribute*, size_t, hyperclient_returncode*)
ctypedef int64_t (*hyperclient_cond_op)(hyperclient*, char*, char*, size_t, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_attribute*, size_t, hyperclient_returncode*)
ctypedef int64_t (*hyperclient_cond_map_op)(hyperclient*, char*, char*, size_t, hyperclient_attribute_check* condattrs, size_t condattrs_sz, hyperclient_map_attribute*, size_t, hyperclient_returncode*)

import collections
import struct

class HyperClientException(Exception):

    def __init__(self, status, attr=None):
        self._status = status
        self._s = {HYPERCLIENT_SUCCESS: 'Success'
                  ,HYPERCLIENT_NOTFOUND: 'Not Found'
                  ,HYPERCLIENT_SEARCHDONE: 'Search Done'
                  ,HYPERCLIENT_CMPFAIL: 'Conditional Operation Did Not Match Object'
                  ,HYPERCLIENT_READONLY: 'Cluster is in a Read-Only State'
                  ,HYPERCLIENT_UNKNOWNSPACE: 'Unknown Space'
                  ,HYPERCLIENT_COORDFAIL: 'Coordinator Failure'
                  ,HYPERCLIENT_SERVERERROR: 'Server Error'
                  ,HYPERCLIENT_POLLFAILED: 'Polling Failed'
                  ,HYPERCLIENT_OVERFLOW: 'Integer-overflow or divide-by-zero'
                  ,HYPERCLIENT_RECONFIGURE: 'Reconfiguration'
                  ,HYPERCLIENT_TIMEOUT: 'Timeout'
                  ,HYPERCLIENT_UNKNOWNATTR: 'Unknown attribute "%s"' % attr
                  ,HYPERCLIENT_DUPEATTR: 'Duplicate attribute "%s"' % attr
                  ,HYPERCLIENT_NONEPENDING: 'None pending'
                  ,HYPERCLIENT_DONTUSEKEY: "Do not specify the key in a search predicate and do not redundantly specify the key for an insert"
                  ,HYPERCLIENT_WRONGTYPE: 'Attribute "%s" has the wrong type' % attr
                  ,HYPERCLIENT_NOMEM: 'Memory allocation failed'
                  ,HYPERCLIENT_BADCONFIG: 'The coordinator provided a malformed configuration'
                  ,HYPERCLIENT_BADSPACE: 'The space description does not parse'
                  ,HYPERCLIENT_DUPLICATE: 'The space already exists'
                  ,HYPERCLIENT_INTERRUPTED: 'Interrupted by a signal'
                  ,HYPERCLIENT_CLUSTER_JUMP: 'The cluster changed identities'
                  ,HYPERCLIENT_COORD_LOGGED: 'The coordinator has logged an error with details'
                  ,HYPERCLIENT_INTERNAL: 'Internal Error (file a bug)'
                  ,HYPERCLIENT_EXCEPTION: 'Internal Exception (file a bug)'
                  ,HYPERCLIENT_GARBAGE: 'Internal Corruption (file a bug)'
                  }.get(status, 'Unknown Error (file a bug)')
        self._e = {HYPERCLIENT_SUCCESS: 'HYPERCLIENT_SUCCESS'
                  ,HYPERCLIENT_NOTFOUND: 'HYPERCLIENT_NOTFOUND'
                  ,HYPERCLIENT_SEARCHDONE: 'HYPERCLIENT_SEARCHDONE'
                  ,HYPERCLIENT_CMPFAIL: 'HYPERCLIENT_CMPFAIL'
                  ,HYPERCLIENT_READONLY: 'HYPERCLIENT_READONLY'
                  ,HYPERCLIENT_UNKNOWNSPACE: 'HYPERCLIENT_UNKNOWNSPACE'
                  ,HYPERCLIENT_COORDFAIL: 'HYPERCLIENT_COORDFAIL'
                  ,HYPERCLIENT_SERVERERROR: 'HYPERCLIENT_SERVERERROR'
                  ,HYPERCLIENT_POLLFAILED: 'HYPERCLIENT_POLLFAILED'
                  ,HYPERCLIENT_OVERFLOW: 'HYPERCLIENT_OVERFLOW'
                  ,HYPERCLIENT_RECONFIGURE: 'HYPERCLIENT_RECONFIGURE'
                  ,HYPERCLIENT_TIMEOUT: 'HYPERCLIENT_TIMEOUT'
                  ,HYPERCLIENT_UNKNOWNATTR: 'HYPERCLIENT_UNKNOWNATTR'
                  ,HYPERCLIENT_DUPEATTR: 'HYPERCLIENT_DUPEATTR'
                  ,HYPERCLIENT_NONEPENDING: 'HYPERCLIENT_NONEPENDING'
                  ,HYPERCLIENT_DONTUSEKEY: 'HYPERCLIENT_DONTUSEKEY'
                  ,HYPERCLIENT_WRONGTYPE: 'HYPERCLIENT_WRONGTYPE'
                  ,HYPERCLIENT_NOMEM: 'HYPERCLIENT_NOMEM'
                  ,HYPERCLIENT_BADCONFIG: 'HYPERCLIENT_BADCONFIG'
                  ,HYPERCLIENT_BADSPACE: 'HYPERCLIENT_BADSPACE'
                  ,HYPERCLIENT_DUPLICATE: 'HYPERCLIENT_DUPLICATE'
                  ,HYPERCLIENT_INTERRUPTED: 'HYPERCLIENT_INTERRUPTED'
                  ,HYPERCLIENT_CLUSTER_JUMP: 'HYPERCLIENT_CLUSTER_JUMP'
                  ,HYPERCLIENT_COORD_LOGGED: 'HYPERCLIENT_COORD_LOGGED'
                  ,HYPERCLIENT_INTERNAL: 'HYPERCLIENT_INTERNAL'
                  ,HYPERCLIENT_EXCEPTION: 'HYPERCLIENT_EXCEPTION'
                  ,HYPERCLIENT_GARBAGE: 'HYPERCLIENT_GARBAGE'
                  }.get(status, 'BUG')

    def status(self):
        return self._status

    def symbol(self):
        return self._e

    def __str__(self):
        return 'HyperClient(%s, %s)' % (self._e, self._s)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self._status == other._status

    def __ne__(self, other):
        return not (self == other)


def __sort_key(obj):
    if isinstance(obj, bytes):
        return tuple([ord(c) for c in obj])
    return obj


cdef _obj_to_backing(v):
    cdef backing = b''
    cdef datatype = None
    cdef hyperdatatype keytype = HYPERDATATYPE_GARBAGE
    cdef hyperdatatype valtype = HYPERDATATYPE_GARBAGE
    if isinstance(v, bytes):
        backing = v
        datatype = HYPERDATATYPE_STRING
    elif isinstance(v, int):
        backing = struct.pack('<q', v)
        datatype = HYPERDATATYPE_INT64
    elif isinstance(v, long):
        backing = struct.pack('<q', v)
        datatype = HYPERDATATYPE_INT64
    elif isinstance(v, float):
        backing = struct.pack('<d', v)
        datatype = HYPERDATATYPE_FLOAT
    elif isinstance(v, list) or isinstance(v, tuple):
        datatype = HYPERDATATYPE_LIST_GENERIC
        if all([isinstance(x, int) for x in v]):
            for x in v:
                backing += struct.pack('<q', x)
                datatype = HYPERDATATYPE_LIST_INT64
        elif all([isinstance(x, bytes) for x in v]):
            for x in v:
                backing += struct.pack('<L', len(x))
                backing += bytes(x)
                datatype = HYPERDATATYPE_LIST_STRING
        elif all([isinstance(x, float) for x in v]):
            for x in v:
                backing += struct.pack('<d', x)
                datatype = HYPERDATATYPE_LIST_FLOAT
        else:
            raise TypeError("Cannot store heterogeneous lists")
    elif isinstance(v, set):
        keytype = HYPERDATATYPE_SET_GENERIC
        if len(set([x.__class__ for x in v])) > 1:
            raise TypeError("Cannot store heterogeneous sets")
        for x in sorted(v, key=__sort_key):
            if isinstance(x, bytes):
                innerxtype = HYPERDATATYPE_SET_STRING
                innerxbacking = struct.pack('<L', len(bytes(x))) + bytes(x)
            elif isinstance(x, int):
                innerxtype = HYPERDATATYPE_SET_INT64
                innerxbacking = struct.pack('<q', x)
            elif isinstance(x, float):
                innerxtype = HYPERDATATYPE_SET_FLOAT
                innerxbacking = struct.pack('<d', x)
            else:
                raise TypeError("Cannot store heterogeneous sets")
            assert keytype == HYPERDATATYPE_SET_GENERIC or keytype == innerxtype
            keytype = innerxtype
            backing += innerxbacking
        dtypes = {HYPERDATATYPE_SET_GENERIC: HYPERDATATYPE_SET_GENERIC,
                  HYPERDATATYPE_SET_STRING: HYPERDATATYPE_SET_STRING,
                  HYPERDATATYPE_SET_INT64: HYPERDATATYPE_SET_INT64,
                  HYPERDATATYPE_SET_FLOAT: HYPERDATATYPE_SET_FLOAT}
        datatype = dtypes[keytype]
    elif isinstance(v, dict):
        keytype = HYPERDATATYPE_MAP_GENERIC
        valtype = HYPERDATATYPE_MAP_GENERIC
        if len(set([x.__class__ for x in v.keys()])) > 1:
            raise TypeError("Cannot store heterogeneous maps")
        if len(set([x.__class__ for x in v.values()])) > 1:
            raise TypeError("Cannot store heterogeneous maps")
        for x, y in sorted(v.items(), key=__sort_key):
            if isinstance(x, bytes):
                innerxtype = HYPERDATATYPE_STRING
                innerxbacking = struct.pack('<L', len(x)) + bytes(x)
            elif isinstance(x, int):
                innerxtype = HYPERDATATYPE_INT64
                innerxbacking = struct.pack('<q', x)
            elif isinstance(x, float):
                innerxtype = HYPERDATATYPE_FLOAT
                innerxbacking = struct.pack('<d', x)
            else:
                raise TypeError("Cannot store heterogeneous sets")
            if isinstance(y, bytes):
                innerytype = HYPERDATATYPE_STRING
                innerybacking = struct.pack('<L', len(y)) + bytes(y)
            elif isinstance(y, int):
                innerytype = HYPERDATATYPE_INT64
                innerybacking = struct.pack('<q', y)
            elif isinstance(y, float):
                innerxtype = HYPERDATATYPE_FLOAT
                innerxbacking = struct.pack('<d', y)
            else:
                raise TypeError("Cannot store heterogeneous sets")
            assert keytype == HYPERDATATYPE_MAP_GENERIC or keytype == innerxtype
            assert valtype == HYPERDATATYPE_MAP_GENERIC or valtype == innerytype
            keytype = innerxtype
            valtype = innerytype
            backing += innerxbacking + innerybacking
        dtypes = {(HYPERDATATYPE_STRING, HYPERDATATYPE_STRING): HYPERDATATYPE_MAP_STRING_STRING,
                  (HYPERDATATYPE_STRING, HYPERDATATYPE_INT64): HYPERDATATYPE_MAP_STRING_INT64,
                  (HYPERDATATYPE_STRING, HYPERDATATYPE_FLOAT): HYPERDATATYPE_MAP_STRING_FLOAT,
                  (HYPERDATATYPE_INT64, HYPERDATATYPE_STRING): HYPERDATATYPE_MAP_INT64_STRING,
                  (HYPERDATATYPE_INT64, HYPERDATATYPE_INT64): HYPERDATATYPE_MAP_INT64_INT64,
                  (HYPERDATATYPE_INT64, HYPERDATATYPE_FLOAT): HYPERDATATYPE_MAP_INT64_FLOAT,
                  (HYPERDATATYPE_FLOAT, HYPERDATATYPE_STRING): HYPERDATATYPE_MAP_FLOAT_STRING,
                  (HYPERDATATYPE_FLOAT, HYPERDATATYPE_INT64): HYPERDATATYPE_MAP_FLOAT_INT64,
                  (HYPERDATATYPE_FLOAT, HYPERDATATYPE_FLOAT): HYPERDATATYPE_MAP_FLOAT_FLOAT,
                  (HYPERDATATYPE_MAP_GENERIC, HYPERDATATYPE_MAP_GENERIC): HYPERDATATYPE_MAP_GENERIC}
        datatype = dtypes[(keytype, valtype)]
    else:
        raise TypeError("Cannot encode {type} for HyperDex".format(type=str(type(v))[7:-2]))
    return datatype, backing


cdef _dict_to_attrs(list value, hyperclient_attribute** attrs):
    cdef list backings = []
    cdef bytes backing
    attrs[0] = <hyperclient_attribute*> \
               malloc(sizeof(hyperclient_attribute) * len(value))
    if attrs[0] == NULL:
        raise MemoryError()
    for i, a in enumerate(value):
        a, v = a
        datatype, backing = _obj_to_backing(v)
        if backing is None:
            raise TypeError("Do not know how to convert attribute {0}".format(a))
        backings.append(backing)
        attrs[0][i].attr = a
        attrs[0][i].value = backing
        attrs[0][i].value_sz = len(backing)
        attrs[0][i].datatype = datatype
    return backings


cdef _dict_to_map_attrs(list value, hyperclient_map_attribute** attrs, size_t* attrs_sz):
    cdef list backings = []
    cdef bytes kbacking
    cdef bytes vbacking
    cdef tuple a
    cdef bytes name
    cdef long i = 0
    attrs_sz[0] = sum([len(a[1]) for a in value if isinstance(a[1], dict)]) \
                + len([a for a in value if not isinstance(a[1], dict)])
    attrs[0] = <hyperclient_map_attribute*> \
               malloc(sizeof(hyperclient_map_attribute) * attrs_sz[0])
    if attrs[0] == NULL:
        raise MemoryError()
    for a in value:
        name, b = a
        keytype = None
        valtype = None
        j = i
        if isinstance(b, dict):
            for k, v in b.iteritems():
                kdatatype, kbacking = _obj_to_backing(k)
                vdatatype, vbacking = _obj_to_backing(v)
                if kdatatype not in (keytype, None):
                    mixedtype = TypeError("Cannot store heterogeneous maps")
                keytype = kdatatype
                if vdatatype not in (valtype, None):
                    mixedtype = TypeError("Cannot store heterogeneous maps")
                valtype = vdatatype
                backings.append(kbacking)
                backings.append(vbacking)
                attrs[0][i].attr = name
                attrs[0][i].map_key = kbacking
                attrs[0][i].map_key_sz = len(kbacking)
                attrs[0][i].map_key_datatype = kdatatype
                attrs[0][i].value = vbacking
                attrs[0][i].value_sz = len(vbacking)
                attrs[0][i].value_datatype = vdatatype
                i += 1
        else:
            kdatatype, kbacking = _obj_to_backing(b)
            attrs[0][i].attr = name
            attrs[0][i].map_key = kbacking
            attrs[0][i].map_key_sz = len(kbacking)
            attrs[0][i].map_key_datatype = kdatatype
            attrs[0][i].value = NULL
            attrs[0][i].value_sz = 0
            attrs[0][i].value_datatype = HYPERDATATYPE_GENERIC;
            i += 1
    return backings


cdef _attrs_to_dict(hyperclient_attribute* attrs, size_t attrs_sz):
    ret = {}
    for idx in range(attrs_sz):
        if attrs[idx].datatype == HYPERDATATYPE_STRING:
            ret[attrs[idx].attr] = attrs[idx].value[:attrs[idx].value_sz]
        elif attrs[idx].datatype == HYPERDATATYPE_INT64:
            s = attrs[idx].value[:attrs[idx].value_sz]
            i = len(s)
            if i > 8:
                s = s[:8]
            elif i < 8:
                s += (8 - i) * '\x00'
            ret[attrs[idx].attr] = struct.unpack('<q', s)[0]
        elif attrs[idx].datatype == HYPERDATATYPE_FLOAT:
            s = attrs[idx].value[:attrs[idx].value_sz]
            i = len(s)
            if i > 8:
                s = s[:8]
            elif i < 8:
                s += (8 - i) * '\x00'
            ret[attrs[idx].attr] = struct.unpack('<d', s)[0]
        elif attrs[idx].datatype == HYPERDATATYPE_LIST_STRING:
            pos = 0
            rem = attrs[idx].value_sz
            lst = []
            while rem >= 4:
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("list(string) is improperly structured (file a bug)")
                lst.append(attrs[idx].value[pos + 4:pos + 4 + sz])
                pos += 4 + sz
                rem -= 4 + sz
            if rem:
                raise ValueError("list(string) contains excess data (file a bug)")
            ret[attrs[idx].attr] = lst
        elif attrs[idx].datatype == HYPERDATATYPE_LIST_INT64:
            pos = 0
            rem = attrs[idx].value_sz
            lst = []
            while rem >= 8:
                lst.append(struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0])
                pos += 8
                rem -= 8
            if rem:
                raise ValueError("list(int64) contains excess data (file a bug)")
            ret[attrs[idx].attr] = lst
        elif attrs[idx].datatype == HYPERDATATYPE_LIST_FLOAT:
            pos = 0
            rem = attrs[idx].value_sz
            lst = []
            while rem >= 8:
                lst.append(struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0])
                pos += 8
                rem -= 8
            if rem:
                raise ValueError("list(float) contains excess data (file a bug)")
            ret[attrs[idx].attr] = lst
        elif attrs[idx].datatype == HYPERDATATYPE_SET_STRING:
            pos = 0
            rem = attrs[idx].value_sz
            st = set([])
            while rem >= 4:
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("set(string) is improperly structured (file a bug)")
                st.add(attrs[idx].value[pos + 4:pos + 4 + sz])
                pos += 4 + sz
                rem -= 4 + sz
            if rem:
                raise ValueError("set(string) contains excess data (file a bug)")
            ret[attrs[idx].attr] = st
        elif attrs[idx].datatype == HYPERDATATYPE_SET_INT64:
            pos = 0
            rem = attrs[idx].value_sz
            st = set([])
            while rem >= 8:
                st.add(struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0])
                pos += 8
                rem -= 8
            if rem:
                raise ValueError("set(int64) contains excess data (file a bug)")
            ret[attrs[idx].attr] = st
        elif attrs[idx].datatype == HYPERDATATYPE_SET_FLOAT:
            pos = 0
            rem = attrs[idx].value_sz
            st = set([])
            while rem >= 8:
                st.add(struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0])
                pos += 8
                rem -= 8
            if rem:
                raise ValueError("set(float) contains excess data (file a bug)")
            ret[attrs[idx].attr] = st
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_STRING_STRING:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 4:
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(string,string) is improperly structured (file a bug)")
                key = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(string,string) is improperly structured (file a bug)")
                val = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                dct[key] = val
            if rem:
                raise ValueError("map(string,string) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_STRING_INT64:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 4:
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(string,int64) is improperly structured (file a bug)")
                key = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                if rem < 8:
                    raise ValueError("map(string,int64) is improperly structured (file a bug)")
                val = struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                dct[key] = val
            if rem:
                raise ValueError("map(string,int64) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_INT64_STRING:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 8:
                key = struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(int64,string) is improperly structured (file a bug)")
                val = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                dct[key] = val
            if rem:
                raise ValueError("map(int64,string) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_INT64_INT64:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 16:
                key = struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0]
                val = struct.unpack('<q', attrs[idx].value[pos + 8:pos + 16])[0]
                pos += 16
                rem -= 16
                dct[key] = val
            if rem:
                raise ValueError("map(int64,int64) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_FLOAT_STRING:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 8:
                key = struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(float,string) is improperly structured (file a bug)")
                val = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                dct[key] = val
            if rem:
                raise ValueError("map(float,string) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_FLOAT_INT64:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 8:
                key = struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                if rem < 8:
                    raise ValueError("map(float,int64) is improperly structured (file a bug)")
                val = struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                dct[key] = val
            if rem:
                raise ValueError("map(float,int64) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_FLOAT_FLOAT:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 16:
                key = struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0]
                val = struct.unpack('<d', attrs[idx].value[pos + 8:pos + 16])[0]
                pos += 16
                rem -= 16
                dct[key] = val
            if rem:
                raise ValueError("map(float,float) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_STRING_FLOAT:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 4:
                sz = struct.unpack('<i', attrs[idx].value[pos:pos + 4])[0]
                if rem - 4 < sz:
                    raise ValueError("map(string,float) is improperly structured (file a bug)")
                key = attrs[idx].value[pos + 4:pos + 4 + sz]
                pos += 4 + sz
                rem -= 4 + sz
                if rem < 8:
                    raise ValueError("map(string,float) is improperly structured (file a bug)")
                val = struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                dct[key] = val
            if rem:
                raise ValueError("map(string,float) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        elif attrs[idx].datatype == HYPERDATATYPE_MAP_INT64_FLOAT:
            pos = 0
            rem = attrs[idx].value_sz
            dct = {}
            while rem >= 8:
                key = struct.unpack('<q', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                if rem < 8:
                    raise ValueError("map(int64,float) is improperly structured (file a bug)")
                val = struct.unpack('<d', attrs[idx].value[pos:pos + 8])[0]
                pos += 8
                rem -= 8
                dct[key] = val
            if rem:
                raise ValueError("map(int64,float) contains excess data (file a bug)")
            ret[attrs[idx].attr] = dct
        else:
            raise ValueError("Server returned garbage value (file a bug)")
    return ret


cdef _check_reqid(int64_t reqid, hyperclient_returncode status):
    if reqid < 0:
        raise HyperClientException(status)


cdef _check_reqid_key_attrs(int64_t reqid, hyperclient_returncode status,
                            hyperclient_attribute* attrs, size_t attrs_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < attrs_sz and attrs and attrs[idx].attr:
            attr = attrs[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_key_conds(int64_t reqid, hyperclient_returncode status,
                            hyperclient_attribute_check* conds, size_t conds_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < conds_sz and conds and conds[idx].attr:
            attr = conds[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_key_attrs2(int64_t reqid, hyperclient_returncode status,
                             hyperclient_attribute_check* attrs1, size_t attrs_sz1,
                             hyperclient_attribute* attrs2, size_t attrs_sz2):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < attrs_sz1 and attrs1 and attrs1[idx].attr:
            attr = attrs1[idx].attr
        idx -= attrs_sz2
        if idx >= 0 and idx < attrs_sz2 and attrs2 and attrs2[idx].attr:
            attr = attrs2[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_key_cond_map_attrs(int64_t reqid, hyperclient_returncode status,
                                     hyperclient_attribute_check* attrs1, size_t attrs_sz1,
                                     hyperclient_map_attribute* attrs2, size_t attrs_sz2):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < attrs_sz1 and attrs1 and attrs1[idx].attr:
            attr = attrs1[idx].attr
        idx -= attrs_sz2
        if idx >= 0 and idx < attrs_sz2 and attrs2 and attrs2[idx].attr:
            attr = attrs2[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_key_map_attrs(int64_t reqid, hyperclient_returncode status,
                                hyperclient_map_attribute* attrs, size_t attrs_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < attrs_sz and attrs and attrs[idx].attr:
            attr = attrs[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_search(int64_t reqid, hyperclient_returncode status,
                         hyperclient_attribute_check* chks, size_t chks_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -1 - reqid
        attr = None
        if idx >= 0 and idx < chks_sz and chks and chks[idx].attr:
            attr = chks[idx].attr
        raise HyperClientException(status, attr)


cdef class Deferred:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished

    def __cinit__(self, Client client, *args):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_GARBAGE
        self._finished = False

    def _callback(self):
        self._finished = True
        del self._client._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._client.loop()
        self._finished = True


cdef class DeferredGet(Deferred):

    cdef hyperclient_attribute* _attrs
    cdef size_t _attrs_sz
    cdef bytes _space

    def __cinit__(self, Client client, bytes space, key):
        self._attrs = <hyperclient_attribute*> NULL
        self._attrs_sz = 0
        self._space = space
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        self._reqid = hyperclient_get(client._client, space_cstr,
                                      key_cstr, len(key_backing),
                                      &self._status,
                                      &self._attrs, &self._attrs_sz)
        _check_reqid(self._reqid, self._status)
        client._ops[self._reqid] = self

    def __dealloc__(self):
        if self._attrs:
            hyperclient_destroy_attrs(self._attrs, self._attrs_sz)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return _attrs_to_dict(self._attrs, self._attrs_sz)
        elif self._status == HYPERCLIENT_NOTFOUND:
            return None
        else:
            raise HyperClientException(self._status)


cdef class DeferredFromAttrs(Deferred):

    cdef bint _cmped

    def __cinit__(self, Client client):
        self._cmped = False

    cdef setcmp(self):
        self._cmped = True

    cdef call(self, hyperclient_simple_op op, bytes space, key, dict value):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute* attrs = NULL
        try:
            backings = _dict_to_attrs(value.items(), &attrs)
            self._reqid = op(self._client._client, space_cstr,
                             key_cstr, len(key_backing),
                             attrs, len(value), &self._status)
            _check_reqid_key_attrs(self._reqid, self._status, attrs, len(value))
            self._client._ops[self._reqid] = self
        finally:
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        elif self._cmped and self._status == HYPERCLIENT_CMPFAIL:
            return False
        else:
            raise HyperClientException(self._status)


cdef class DeferredCondFromAttrs(Deferred):

    def __cinit__(self, Client client):
        pass

    cdef call(self, hyperclient_cond_op op, bytes space, key, dict condition, dict value):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute_check* condattrs = NULL
        cdef size_t condattrs_sz
        cdef hyperclient_attribute* attrs = NULL
        try:
            backingsc = _predicate_to_c(condition, &condattrs, &condattrs_sz)
            backingsa = _dict_to_attrs(value.items(), &attrs)
            self._reqid = op(self._client._client, space_cstr,
                             key_cstr, len(key_backing),
                             condattrs, condattrs_sz,
                             attrs, len(value),
                             &self._status)
            _check_reqid_key_attrs2(self._reqid, self._status,
                                    condattrs, condattrs_sz,
                                    attrs, len(value))
            self._client._ops[self._reqid] = self
        finally:
            if condattrs:
                free(condattrs)
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        elif self._status == HYPERCLIENT_CMPFAIL:
            return False
        else:
            raise HyperClientException(self._status)


cdef class DeferredDelete(Deferred):

    def __cinit__(self, Client client, bytes space, key):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        self._reqid = hyperclient_del(client._client, space_cstr,
                                      key_cstr, len(key_backing), &self._status)
        _check_reqid(self._reqid, self._status)
        client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        elif self._status == HYPERCLIENT_NOTFOUND:
            return False
        else:
            raise HyperClientException(self._status)


cdef class DeferredCondDelete(Deferred):

    def __cinit__(self, Client client, bytes space, key, dict cond):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute_check* condattrs = NULL
        cdef size_t condattrs_sz
        try:
            backings = _predicate_to_c(cond, &condattrs, &condattrs_sz)
            self._reqid = hyperclient_cond_del(client._client, space_cstr,
                                               key_cstr, len(key_backing),
                                               condattrs, condattrs_sz,
                                               &self._status)
            _check_reqid_key_conds(self._reqid, self._status, condattrs, condattrs_sz);
            client._ops[self._reqid] = self
        finally:
            if condattrs:
                free(condattrs)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        elif self._status == HYPERCLIENT_CMPFAIL:
            return False
        elif self._status == HYPERCLIENT_NOTFOUND:
            return False
        else:
            raise HyperClientException(self._status)


cdef class DeferredMapOp(Deferred):

    def __cinit__(self, Client client):
        pass

    cdef call(self, hyperclient_map_op op, bytes space, key, dict value):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_map_attribute* attrs = NULL
        cdef size_t attrs_sz = 0
        try:
            backings = _dict_to_map_attrs(value.items(), &attrs, &attrs_sz)
            self._reqid = op(self._client._client, space_cstr,
                             key_cstr, len(key_backing),
                             attrs, attrs_sz, &self._status)
            _check_reqid_key_map_attrs(self._reqid, self._status, attrs, attrs_sz)
            self._client._ops[self._reqid] = self
        finally:
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        else:
            raise HyperClientException(self._status)

cdef class DeferredCondMapOp(Deferred):

    def __cinit__(self, Client client):
        pass

    cdef call(self, hyperclient_cond_map_op op, bytes space, key, dict condition, dict value):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute_check* condattrs = NULL
        cdef size_t condattrs_sz = 0
        cdef hyperclient_map_attribute* attrs = NULL
        cdef size_t attrs_sz = 0
        try:
            backingsc = _predicate_to_c(condition, &condattrs, &condattrs_sz)
            backingsa = _dict_to_map_attrs(value.items(), &attrs, &attrs_sz)
            self._reqid = op(self._client._client, space_cstr,
                             key_cstr, len(key_backing),
                             condattrs, condattrs_sz,
                             attrs, attrs_sz, &self._status)
            _check_reqid_key_cond_map_attrs(self._reqid, self._status, condattrs, condattrs_sz, attrs, attrs_sz)
            self._client._ops[self._reqid] = self
        finally:
            if condattrs:
                free(condattrs)
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        elif self._status == HYPERCLIENT_CMPFAIL:
            return False
        else:
            raise HyperClientException(self._status)


cdef _predicate_to_c(dict predicate,
                     hyperclient_attribute_check** chks, size_t* chks_sz):
    raw_checks = []
    for attr, preds in predicate.iteritems():
        if isinstance(preds, list):
            for p in preds:
                if isinstance(p, tuple) and len(p) == 2 and \
                   type(p[0]) == type(p[1]) and type(p[0]):
                    raw_checks.append((attr, HYPERPREDICATE_GREATER_EQUAL, p[0]))
                    raw_checks.append((attr, HYPERPREDICATE_LESS_EQUAL, p[1]))
                elif isinstance(p, Predicate):
                    raw_checks += p._raw(attr)
                else:
                    raw_checks.append((attr, HYPERPREDICATE_EQUALS, p))
        elif isinstance(preds, tuple) and len(preds) == 2 and \
             type(preds[0]) == type(preds[1]):
            raw_checks.append((attr, HYPERPREDICATE_GREATER_EQUAL, preds[0]))
            raw_checks.append((attr, HYPERPREDICATE_LESS_EQUAL, preds[1]))
        elif isinstance(preds, Predicate):
            raw_checks += preds._raw(attr)
        else:
            raw_checks.append((attr, HYPERPREDICATE_EQUALS, preds))
    chks_sz[0] = len(raw_checks)
    chks[0] = <hyperclient_attribute_check*> malloc(sizeof(hyperclient_attribute_check) * chks_sz[0])
    if chks[0] == NULL:
        raise MemoryError()
    backings = []
    for i, (attr, pred, val) in enumerate(raw_checks):
        datatype, backing = _obj_to_backing(val)
        backings.append(backing)
        backings.append(attr)
        chks[0][i].attr = attr
        chks[0][i].value = backing
        chks[0][i].value_sz = len(backing)
        chks[0][i].datatype = datatype
        chks[0][i].predicate = pred
    return backings


cdef class DeferredGroupDel(Deferred):

    def __cinit__(self, Client client, bytes space, dict predicate):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_GARBAGE
        cdef hyperclient_attribute_check* chks = NULL
        cdef size_t chks_sz = 0
        try:
            backings = _predicate_to_c(predicate, &chks, &chks_sz)
            self._reqid = hyperclient_group_del(client._client, space,
                                                chks, chks_sz,
                                                &self._status)
            _check_reqid_search(self._reqid, self._status, chks, chks_sz)
            client._ops[self._reqid] = self
        finally:
            if chks: free(chks)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            return True
        else:
            raise HyperClientException(self._status)


cdef class DeferredSearchDescribe(Deferred):

    cdef char* _text

    def __cinit__(self, Client client, bytes space, dict predicate, bool unsafe):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_GARBAGE
        self._text = NULL
        cdef hyperclient_attribute_check* chks = NULL
        cdef size_t chks_sz = 0
        try:
            backings = _predicate_to_c(predicate, &chks, &chks_sz)
            self._reqid = hyperclient_search_describe(client._client, space,
                                                      chks, chks_sz,
                                                      &self._status, &self._text)
            _check_reqid_search(self._reqid, self._status, chks, chks_sz)
            client._ops[self._reqid] = self
        finally:
            if chks: free(chks)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS:
            if self._text == NULL:
                return None
            return self._text.strip()
        else:
            raise HyperClientException(self._status)


cdef class DeferredCount(Deferred):

    cdef uint64_t _result
    cdef int _unsafe

    def __cinit__(self, Client client, bytes space, dict predicate, bool unsafe):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_GARBAGE
        self._result = 0
        self._unsafe = 1 if unsafe else 0
        cdef hyperclient_attribute_check* chks = NULL
        cdef size_t chks_sz = 0
        try:
            backings = _predicate_to_c(predicate, &chks, &chks_sz)
            self._reqid = hyperclient_count(client._client, space,
                                            chks, chks_sz,
                                            &self._status, &self._result)
            _check_reqid_search(self._reqid, self._status, chks, chks_sz)
            client._ops[self._reqid] = self
        finally:
            if chks: free(chks)

    def wait(self):
        Deferred.wait(self)
        if self._status == HYPERCLIENT_SUCCESS or self._unsafe == 0:
            return self._result
        else:
            raise HyperClientException(self._status)


cdef class SearchBase:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished
    cdef hyperclient_attribute* _attrs
    cdef size_t _attrs_sz
    cdef list _backlogged

    def __cinit__(self, Client client, *args):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_GARBAGE
        self._finished = False
        self._attrs = <hyperclient_attribute*> NULL
        self._attrs_sz = 0
        self._backlogged = []

    def __iter__(self):
        return self

    def __next__(self):
        while not self._finished and not self._backlogged:
            self._client.loop()
        if self._backlogged:
            return self._backlogged.pop()
        raise StopIteration()

    def _callback(self):
        if self._status == HYPERCLIENT_SEARCHDONE:
            self._finished = True
            del self._client._ops[self._reqid]
        elif self._status == HYPERCLIENT_SUCCESS:
            try:
                attrs = _attrs_to_dict(self._attrs, self._attrs_sz)
            finally:
                if self._attrs:
                    hyperclient_destroy_attrs(self._attrs, self._attrs_sz)
            self._backlogged.append(attrs)
        else:
            self._backlogged.append(HyperClientException(self._status))


cdef class Search(SearchBase):

    def __cinit__(self, Client client, bytes space, dict predicate):
        cdef hyperclient_attribute_check* chks = NULL
        cdef size_t chks_sz = 0
        try:
            backings = _predicate_to_c(predicate, &chks, &chks_sz)
            self._reqid = hyperclient_search(client._client, space,
                                             chks, chks_sz,
                                             &self._status,
                                             &self._attrs,
                                             &self._attrs_sz)
            _check_reqid_search(self._reqid, self._status, chks, chks_sz)
            client._ops[self._reqid] = self
        finally:
            if chks: free(chks)


cdef class SortedSearch(SearchBase):

    def __cinit__(self, Client client, bytes space, dict predicate,
                  bytes sort_by, long limit, bytes compare):
        cdef uint64_t lim = limit
        cdef int maxi = 0
        cdef hyperclient_attribute_check* chks = NULL
        cdef size_t chks_sz = 0
        if compare not in ('maximize', 'max', 'minimize', 'min'):
            raise ValueError("'compare' must be either 'max' or 'min'")
        if compare in ('max', 'maximize'):
            maxi = 1
        try:
            backings = _predicate_to_c(predicate, &chks, &chks_sz)
            self._reqid = hyperclient_sorted_search(client._client, space,
                                                    chks, chks_sz,
                                                    sort_by,
                                                    lim,
                                                    maxi,
                                                    &self._status,
                                                    &self._attrs,
                                                    &self._attrs_sz)
            _check_reqid_search(self._reqid, self._status, chks, chks_sz)
            client._ops[self._reqid] = self
        finally:
            if chks: free(chks)


cdef class Predicate:

    cdef list _raw_check

    def __init__(self, raw):
        self._raw_check = raw

    def _raw(self, attr):
        return [(attr, p, v) for p, v in self._raw_check]


cdef class Range(Predicate):

    def __init__(self, lower, upper):
        if type(lower) != type(upper) or type(lower) not in (bytes, int, long, float):
            raise AttributeError("Range search bounds must be of like types")
        Predicate.__init__(self, [(HYPERPREDICATE_GREATER_EQUAL, lower),
                                  (HYPERPREDICATE_LESS_EQUAL, upper)])


cdef class Contains(Predicate):

    def __init__(self, elem):
        if type(elem) not in (bytes, int, long, float):
            raise AttributeError("Contains must be a byte, int, or float")
        Predicate.__init__(self, [(HYPERPREDICATE_CONTAINS, elem)])


cdef class LessEqual(Predicate):

    def __init__(self, upper):
        if type(upper) not in (bytes, int, long, float):
            raise AttributeError("LessEqual must be a byte, int, or float")
        Predicate.__init__(self, [(HYPERPREDICATE_LESS_EQUAL, upper)])


cdef class GreaterEqual(Predicate):

    def __init__(self, lower):
        if type(lower) not in (bytes, int, long, float):
            raise AttributeError("GreaterEqual must be a byte, int, or float")
        Predicate.__init__(self, [(HYPERPREDICATE_GREATER_EQUAL, lower)])


cdef class Regex(Predicate):

    def __init__(self, regex):
        if type(regex) != bytes:
            raise AttributeError("Regex must be a byte")
        Predicate.__init__(self, [(HYPERPREDICATE_REGEX, regex)])


cdef class LengthLessEqual(Predicate):

    def __init__(self, upper):
        if type(upper) not in (int, long):
            raise AttributeError("LengthLessEqual must be int or long")
        Predicate.__init__(self, [(HYPERPREDICATE_LENGTH_LESS_EQUAL, upper)])


cdef class Client:
    cdef hyperclient* _client
    cdef dict _ops

    def __cinit__(self, address, port):
        self._client = hyperclient_create(address, port)
        self._ops = {}

    def __dealloc__(self):
        if self._client:
            hyperclient_destroy(self._client)

    def add_space(self, bytes space):
        cdef hyperclient_returncode rc = hyperclient_add_space(self._client, space)
        if rc != HYPERCLIENT_SUCCESS:
            raise HyperClientException(rc)

    def rm_space(self, bytes space):
        cdef hyperclient_returncode rc = hyperclient_rm_space(self._client, space)
        if rc != HYPERCLIENT_SUCCESS:
            raise HyperClientException(rc)

    def get(self, bytes space, key):
        async = self.async_get(space, key)
        return async.wait()

    def put(self, bytes space, key, dict value):
        async = self.async_put(space, key, value)
        return async.wait()

    def cond_put(self, bytes space, key, dict condition, dict value):
        async = self.async_cond_put(space, key, condition, value)
        return async.wait()

    def put_if_not_exist(self, bytes space, key, dict value):
        async = self.async_put_if_not_exist(space, key, value)
        return async.wait()

    def delete(self, bytes space, key):
        async = self.async_delete(space, key)
        return async.wait()

    def cond_delete(self, bytes space, key, dict cond):
        async = self.async_delete(space, key, cond)
        return async.wait()

    def atomic_add(self, bytes space, key, dict value):
        async = self.async_atomic_add(space, key, value)
        return async.wait()

    def cond_atomic_add(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_add(space, key, value, cond)
        return async.wait()

    def atomic_sub(self, bytes space, key, dict value):
        async = self.async_atomic_sub(space, key, value)
        return async.wait()

    def cond_atomic_sub(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_sub(space, key, value, cond)
        return async.wait()

    def atomic_mul(self, bytes space, key, dict value):
        async = self.async_atomic_mul(space, key, value)
        return async.wait()

    def cond_atomic_mul(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_mul(space, key, value, cond)
        return async.wait()

    def atomic_div(self, bytes space, key, dict value):
        async = self.async_atomic_div(space, key, value)
        return async.wait()

    def cond_atomic_div(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_div(space, key, value, cond)
        return async.wait()

    def atomic_mod(self, bytes space, key, dict value):
        async = self.async_atomic_mod(space, key, value)
        return async.wait()

    def cond_atomic_mod(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_mod(space, key, value, cond)
        return async.wait()

    def atomic_and(self, bytes space, key, dict value):
        async = self.async_atomic_and(space, key, value)
        return async.wait()

    def cond_atomic_and(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_and(space, key, value, cond)
        return async.wait()

    def atomic_or(self, bytes space, key, dict value):
        async = self.async_atomic_or(space, key, value)
        return async.wait()

    def cond_atomic_or(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_or(space, key, value, cond)
        return async.wait()

    def atomic_xor(self, bytes space, key, dict value):
        async = self.async_atomic_xor(space, key, value)
        return async.wait()

    def cond_atomic_xor(self, bytes space, key, dict cond, dict value):
        async = self.async_atomic_xor(space, key, value, cond)
        return async.wait()

    def string_prepend(self, bytes space, key, dict value):
        async = self.async_string_prepend(space, key, value)
        return async.wait()

    def cond_string_prepend(self, bytes space, key, dict cond, dict value):
        async = self.async_string_prepend(space, key, value, cond)
        return async.wait()

    def string_append(self, bytes space, key, dict value):
        async = self.async_string_append(space, key, value)
        return async.wait()

    def cond_string_append(self, bytes space, key, dict cond, dict value):
        async = self.async_string_append(space, key, value, cond)
        return async.wait()

    def list_lpush(self, bytes space, key, dict value):
        async = self.async_list_lpush(space, key, value)
        return async.wait()

    def cond_list_lpush(self, bytes space, key, dict cond, dict value):
        async = self.async_list_lpush(space, key, value, cond)
        return async.wait()

    def list_rpush(self, bytes space, key, dict value):
        async = self.async_list_rpush(space, key, value)
        return async.wait()

    def cond_list_rpush(self, bytes space, key, dict cond, dict value):
        async = self.async_list_rpush(space, key, value, cond)
        return async.wait()

    def set_add(self, bytes space, key, dict value):
        async = self.async_set_add(space, key, value)
        return async.wait()

    def cond_set_add(self, bytes space, key, dict cond, dict value):
        async = self.async_set_add(space, key, value, cond)
        return async.wait()

    def set_remove(self, bytes space, key, dict value):
        async = self.async_set_remove(space, key, value)
        return async.wait()

    def cond_set_remove(self, bytes space, key, dict cond, dict value):
        async = self.async_set_remove(space, key, value, cond)
        return async.wait()

    def set_intersect(self, bytes space, key, dict value):
        async = self.async_set_intersect(space, key, value)
        return async.wait()

    def cond_set_intersect(self, bytes space, key, dict cond, dict value):
        async = self.async_set_intersect(space, key, value, cond)
        return async.wait()

    def set_union(self, bytes space, key, dict value):
        async = self.async_set_union(space, key, value)
        return async.wait()

    def cond_set_union(self, bytes space, key, dict cond, dict value):
        async = self.async_set_union(space, key, value, cond)
        return async.wait()

    def map_add(self, bytes space, key, dict value):
        async = self.async_map_add(space, key, value)
        return async.wait()

    def cond_map_add(self, bytes space, key, dict cond, dict value):
        async = self.async_cond_map_add(space, key, cond, value)
        return async.wait()

    def map_remove(self, bytes space, key, dict value):
        async = self.async_map_remove(space, key, value)
        return async.wait()

    def cond_map_remove(self, bytes space, key, dict cond, dict value):
        async = self.async_cond_map_remove(space, key, cond, value)
        return async.wait()

    def map_atomic_add(self, bytes space, key, dict value):
        async = self.async_map_atomic_add(space, key, value)
        return async.wait()

    def cond_map_atomic_add(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_add(space, key, value, cond)
        return async.wait()

    def map_atomic_sub(self, bytes space, key, dict value):
        async = self.async_map_atomic_sub(space, key, value)
        return async.wait()

    def cond_map_atomic_sub(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_sub(space, key, value, cond)
        return async.wait()

    def map_atomic_mul(self, bytes space, key, dict value):
        async = self.async_map_atomic_mul(space, key, value)
        return async.wait()

    def cond_map_atomic_mul(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_mul(space, key, value, cond)
        return async.wait()

    def map_atomic_div(self, bytes space, key, dict value):
        async = self.async_map_atomic_div(space, key, value)
        return async.wait()

    def cond_map_atomic_div(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_div(space, key, value, cond)
        return async.wait()

    def map_atomic_mod(self, bytes space, key, dict value):
        async = self.async_map_atomic_mod(space, key, value)
        return async.wait()

    def cond_map_atomic_mod(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_mod(space, key, value, cond)
        return async.wait()

    def map_atomic_and(self, bytes space, key, dict value):
        async = self.async_map_atomic_and(space, key, value)
        return async.wait()

    def cond_map_atomic_and(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_and(space, key, value, cond)
        return async.wait()

    def map_atomic_or(self, bytes space, key, dict value):
        async = self.async_map_atomic_or(space, key, value)
        return async.wait()

    def cond_map_atomic_or(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_or(space, key, value, cond)
        return async.wait()

    def map_atomic_xor(self, bytes space, key, dict value):
        async = self.async_map_atomic_xor(space, key, value)
        return async.wait()

    def cond_map_atomic_xor(self, bytes space, key, dict cond, dict value):
        async = self.async_map_atomic_xor(space, key, value, cond)
        return async.wait()

    def map_string_prepend(self, bytes space, key, dict value):
        async = self.async_map_string_prepend(space, key, value)
        return async.wait()

    def cond_map_string_prepend(self, bytes space, key, dict cond, dict value):
        async = self.async_map_string_prepend(space, key, value, cond)
        return async.wait()

    def map_string_append(self, bytes space, key, dict value):
        async = self.async_map_string_append(space, key, value)
        return async.wait()

    def cond_map_string_append(self, bytes space, key, dict cond, dict value):
        async = self.async_map_string_append(space, key, value, cond)
        return async.wait()

    def search_describe(self, bytes space, dict predicate, bool unsafe=False):
        async = self.async_search_describe(space, predicate, unsafe)
        return async.wait()

    def group_del(self, bytes space, dict predicate):
        async = self.async_group_del(space, predicate)
        return async.wait()

    def count(self, bytes space, dict predicate, bool unsafe=False):
        async = self.async_count(space, predicate, unsafe)
        return async.wait()

    def search(self, bytes space, dict predicate):
        return Search(self, space, predicate)

    def sorted_search(self, bytes space, dict predicate, bytes sort_by, long limit, bytes compare):
        return SortedSearch(self, space, predicate, sort_by, limit, compare)

    def async_get(self, bytes space, key):
        return DeferredGet(self, space, key)

    def async_put(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_put, space, key, value)
        return d

    def async_put_if_not_exist(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_put_if_not_exist, space, key, value)
        d.setcmp()
        return d

    def async_cond_put(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_put, space, key, cond, value)
        return d

    def async_delete(self, bytes space, key):
        return DeferredDelete(self, space, key)

    def async_cond_delete(self, bytes space, key, cond):
        return DeferredCondDelete(self, space, key, cond)

    def async_atomic_add(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_add, space, key, value)
        return d

    def async_cond_atomic_add(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_add, space, key, cond, value)
        return d

    def async_atomic_sub(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_sub, space, key, value)
        return d

    def async_cond_atomic_sub(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_sub, space, key, cond, value)
        return d

    def async_atomic_mul(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_mul, space, key, value)
        return d

    def async_cond_atomic_mul(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_mul, space, key, cond, value)
        return d

    def async_atomic_div(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_div, space, key, value)
        return d

    def async_cond_atomic_div(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_div, space, key, cond, value)
        return d

    def async_atomic_mod(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_mod, space, key, value)
        return d

    def async_cond_atomic_mod(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_mod, space, key, cond, value)
        return d

    def async_atomic_and(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_and, space, key, value)
        return d

    def async_cond_atomic_and(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_and, space, key, cond, value)
        return d

    def async_atomic_or(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_or, space, key, value)
        return d

    def async_cond_atomic_or(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_or, space, key, cond, value)
        return d

    def async_atomic_xor(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_atomic_xor, space, key, value)
        return d

    def async_cond_atomic_xor(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_atomic_xor, space, key, cond, value)
        return d

    def async_string_prepend(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_string_prepend, space, key, value)
        return d

    def async_cond_string_prepend(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_string_prepend, space, key, cond, value)
        return d

    def async_string_append(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_string_append, space, key, value)
        return d

    def async_cond_string_append(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_string_append, space, key, cond, value)
        return d

    def async_list_lpush(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_list_lpush, space, key, value)
        return d

    def async_cond_list_lpush(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_list_lpush, space, key, cond, value)
        return d

    def async_list_rpush(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_list_rpush, space, key, value)
        return d

    def async_cond_list_rpush(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_list_rpush, space, key, cond, value)
        return d

    def async_set_add(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_set_add, space, key, value)
        return d

    def async_cond_set_add(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_set_add, space, key, cond, value)
        return d

    def async_set_remove(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_set_remove, space, key, value)
        return d

    def async_cond_set_remove(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_set_remove, space, key, cond, value)
        return d

    def async_set_intersect(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_set_intersect, space, key, value)
        return d

    def async_cond_set_intersect(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_set_intersect, space, key, cond, value)
        return d

    def async_set_union(self, bytes space, key, dict value):
        d = DeferredFromAttrs(self)
        d.call(<hyperclient_simple_op> hyperclient_set_union, space, key, value)
        return d

    def async_cond_set_union(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_set_union, space, key, cond, value)
        return d

    def async_map_add(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_add, space, key, value)
        return d

    def async_cond_map_add(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_add, space, key, cond, value)
        return d

    def async_map_remove(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_remove, space, key, value)
        return d

    def async_cond_map_remove(self, bytes space, key, dict cond, dict value):
        d = DeferredCondFromAttrs(self)
        d.call(<hyperclient_cond_op> hyperclient_cond_map_remove, space, key, cond, value)
        return d

    def async_map_atomic_add(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_add, space, key, value)
        return d

    def async_cond_map_atomic_add(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_add, space, key, cond, value)
        return d

    def async_map_atomic_sub(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_sub, space, key, value)
        return d

    def async_cond_map_atomic_sub(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_sub, space, key, cond, value)
        return d

    def async_map_atomic_mul(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_mul, space, key, value)
        return d

    def async_cond_map_atomic_mul(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_mul, space, key, cond, value)
        return d

    def async_map_atomic_div(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_div, space, key, value)
        return d

    def async_cond_map_atomic_div(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_div, space, key, cond, value)
        return d

    def async_map_atomic_mod(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_mod, space, key, value)
        return d

    def async_cond_map_atomic_mod(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_mod, space, key, cond, value)
        return d

    def async_map_atomic_and(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_and, space, key, value)
        return d

    def async_cond_map_atomic_and(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_and, space, key, cond, value)
        return d

    def async_map_atomic_or(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_or, space, key, value)
        return d

    def async_cond_map_atomic_or(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_or, space, key, cond, value)
        return d

    def async_map_atomic_xor(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_atomic_xor, space, key, value)
        return d

    def async_cond_map_atomic_xor(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_atomic_xor, space, key, cond, value)
        return d

    def async_map_string_prepend(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_string_prepend, space, key, value)
        return d

    def async_cond_map_string_prepend(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_string_prepend, space, key, cond, value)
        return d

    def async_map_string_append(self, bytes space, key, dict value):
        d = DeferredMapOp(self)
        d.call(<hyperclient_map_op> hyperclient_map_string_append, space, key, value)
        return d

    def async_cond_map_string_append(self, bytes space, key, dict cond, dict value):
        d = DeferredCondMapOp(self)
        d.call(<hyperclient_cond_map_op> hyperclient_cond_map_string_append, space, key, cond, value)
        return d

    def async_search_describe(self, bytes space, dict predicate, bool unsafe=False):
        return DeferredSearchDescribe(self, space, predicate, unsafe)

    def async_group_del(self, bytes space, dict predicate):
        return DeferredGroupDel(self, space, predicate)

    def async_count(self, bytes space, dict predicate, bool unsafe=False):
        return DeferredCount(self, space, predicate, unsafe)

    def loop(self):
        cdef hyperclient_returncode rc
        ret = hyperclient_loop(self._client, -1, &rc)
        if ret < 0:
            raise HyperClientException(rc)
        else:
            assert ret in self._ops
            op = self._ops[ret]
            # We cannot refer to self._ops[ret] after this call as
            # _callback() may remove ret from self._ops.
            op._callback()
            return op
