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
        HYPERDATATYPE_STRING      = 8960
        HYPERDATATYPE_INT64       = 8961
        HYPERDATATYPE_LIST_STRING = 8976
        HYPERDATATYPE_LIST_INT64  = 8977
        HYPERDATATYPE_SET_STRING  = 8992
        HYPERDATATYPE_SET_INT64   = 8993
        HYPERDATATYPE_MAP_STRING_STRING = 9008
        HYPERDATATYPE_MAP_STRING_INT64  = 9009
        HYPERDATATYPE_MAP_INT64_STRING  = 9010
        HYPERDATATYPE_MAP_INT64_INT64   = 9011
        HYPERDATATYPE_GARBAGE     = 9087

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
        char* value
        size_t value_sz
        hyperdatatype datatype

    cdef struct hyperclient_range_query:
        char* attr
        unsigned long attr_sz
        uint64_t lower
        uint64_t upper

    cdef enum hyperclient_returncode:
        HYPERCLIENT_SUCCESS      = 8448
        HYPERCLIENT_NOTFOUND     = 8449
        HYPERCLIENT_SEARCHDONE   = 8450
        HYPERCLIENT_CMPFAIL      = 8451
        HYPERCLIENT_UNKNOWNSPACE = 8512
        HYPERCLIENT_COORDFAIL    = 8513
        HYPERCLIENT_SERVERERROR  = 8514
        HYPERCLIENT_CONNECTFAIL  = 8515
        HYPERCLIENT_DISCONNECT   = 8516
        HYPERCLIENT_RECONFIGURE  = 8517
        HYPERCLIENT_LOGICERROR   = 8518
        HYPERCLIENT_TIMEOUT      = 8519
        HYPERCLIENT_UNKNOWNATTR  = 8520
        HYPERCLIENT_DUPEATTR     = 8521
        HYPERCLIENT_SEEERRNO     = 8522
        HYPERCLIENT_NONEPENDING  = 8523
        HYPERCLIENT_DONTUSEKEY   = 8524
        HYPERCLIENT_WRONGTYPE    = 8525
        HYPERCLIENT_EXCEPTION    = 8574
        HYPERCLIENT_ZERO         = 8575
        HYPERCLIENT_A            = 8576
        HYPERCLIENT_B            = 8577

    hyperclient* hyperclient_create(char* coordinator, in_port_t port)
    void hyperclient_destroy(hyperclient* client)
    int64_t hyperclient_get(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_condput(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_list_lpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_list_rpush(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_intersect(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_set_union(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_remove(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_add(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_sub(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_mul(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_div(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_mod(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_and(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_or(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_atomic_xor(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_string_prepend(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_map_string_append(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_map_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_search(hyperclient* client, char* space, hyperclient_attribute* eq, size_t eq_sz, hyperclient_range_query* rn, size_t rn_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_loop(hyperclient* client, int timeout, hyperclient_returncode* status)
    void hyperclient_destroy_attrs(hyperclient_attribute* attrs, size_t attrs_sz)

import collections
import struct

class HyperClientException(Exception):

    def __init__(self, status, attr=None):
        self._status = status
        self._s = {HYPERCLIENT_SUCCESS: 'Success'
                  ,HYPERCLIENT_NOTFOUND: 'Not Found'
                  ,HYPERCLIENT_SEARCHDONE: 'Search Done'
                  ,HYPERCLIENT_CMPFAIL: 'Conditional Operation Did Not Match Object'
                  ,HYPERCLIENT_UNKNOWNSPACE: 'Unknown Space'
                  ,HYPERCLIENT_COORDFAIL: 'Coordinator Failure'
                  ,HYPERCLIENT_SERVERERROR: 'Server Error'
                  ,HYPERCLIENT_CONNECTFAIL: 'Connection Failure'
                  ,HYPERCLIENT_DISCONNECT: 'Connection Reset'
                  ,HYPERCLIENT_RECONFIGURE: 'Reconfiguration'
                  ,HYPERCLIENT_LOGICERROR: 'Logic Error (file a bug)'
                  ,HYPERCLIENT_TIMEOUT: 'Timeout'
                  ,HYPERCLIENT_UNKNOWNATTR: 'Unknown attribute "%s"' % attr
                  ,HYPERCLIENT_DUPEATTR: 'Duplicate attribute "%s"' % attr
                  ,HYPERCLIENT_SEEERRNO: 'See ERRNO'
                  ,HYPERCLIENT_NONEPENDING: 'None pending'
                  ,HYPERCLIENT_DONTUSEKEY: "Do not specify the key in a search predicate and do not redundantly specify the key for an insert"
                  ,HYPERCLIENT_WRONGTYPE: 'Attribute "%s" has the wrong type' % attr
                  ,HYPERCLIENT_EXCEPTION: 'Internal Error (file a bug)'
                  }.get(status, 'Unknown Error (file a bug)')

    def __str__(self):
        return self._s

    def status(self):
       return self._status


def __string_key(obj):
    return len(obj), obj


cdef _obj_to_backing(v):
    backing = None
    datatype = None
    if isinstance(v, bytes):
        backing = v
        datatype = HYPERDATATYPE_STRING
    elif isinstance(v, int):
        backing = struct.pack('<q', v)
        datatype = HYPERDATATYPE_INT64
    elif isinstance(v, list) or isinstance(v, tuple):
        backing = b''
        if all([isinstance(x, int) for x in v]):
            for x in v:
                backing += struct.pack('<q', x)
            datatype = HYPERDATATYPE_LIST_INT64
        elif all([isinstance(x, bytes) for x in v]):
            for x in v:
                backing += struct.pack('<L', len(x))
                backing += bytes(x)
            datatype = HYPERDATATYPE_LIST_STRING
        else:
            raise TypeError("Cannot store heterogeneous lists")
    elif isinstance(v, set):
        backing = b''
        if all([isinstance(x, int) for x in v]):
            for x in sorted(v):
                backing += struct.pack('<q', x)
            datatype = HYPERDATATYPE_SET_INT64
        elif all([isinstance(x, bytes) for x in v]):
            for x in sorted(v, key=__string_key):
                backing += struct.pack('<L', len(x))
                backing += bytes(x)
            datatype = HYPERDATATYPE_SET_STRING
        else:
            raise TypeError("Cannot store heterogeneous sets")
    elif isinstance(v, dict):
        backing = b''
        keytype = None
        valtype = None
        mixedtype = TypeError("Cannot store heterogeneous maps")
        for x, y in v.iteritems():
            if isinstance(x, int):
                if keytype not in ('int64', None):
                    raise mixedtype
                backing += struct.pack('<q', x)
                keytype = 'int64'
            elif isinstance(x, bytes):
                if keytype not in ('string', None):
                    raise mixedtype
                backing += struct.pack('<L', len(x))
                backing += bytes(x)
                keytype = 'string'
            else:
                raise mixedtype
            if isinstance(y, int):
                if valtype not in ('int64', None):
                    raise mixedtype
                backing += struct.pack('<q', y)
                valtype = 'int64'
            elif isinstance(y, bytes):
                if valtype not in ('string', None):
                    raise mixedtype
                backing += struct.pack('<L', len(y))
                backing += bytes(y)
                valtype = 'string'
            else:
                raise mixedtype
        dtypes = {('string', 'string'): HYPERDATATYPE_MAP_STRING_STRING,
                  ('string', 'int64'): HYPERDATATYPE_MAP_STRING_INT64,
                  ('int64', 'string'): HYPERDATATYPE_MAP_INT64_STRING,
                  ('int64', 'int64'): HYPERDATATYPE_MAP_INT64_INT64}
        datatype = dtypes[(keytype, valtype)]
    return (datatype, backing)


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
    cdef dict b
    cdef bytes name
    cdef long i = 0
    attrs_sz[0] = sum([len(a[1]) for a in value])
    attrs[0] = <hyperclient_map_attribute*> \
               malloc(sizeof(hyperclient_map_attribute) * attrs_sz[0])
    if attrs[0] == NULL:
        raise MemoryError()
    for a in value:
        name, b = a
        keytype = None
        valtype = None
        j = i
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
            attrs[0][i].value = vbacking
            attrs[0][i].value_sz = len(vbacking)
            i += 1
        dtypes = {(HYPERDATATYPE_STRING, HYPERDATATYPE_STRING): HYPERDATATYPE_MAP_STRING_STRING,
                  (HYPERDATATYPE_STRING, HYPERDATATYPE_INT64): HYPERDATATYPE_MAP_STRING_INT64,
                  (HYPERDATATYPE_INT64, HYPERDATATYPE_STRING): HYPERDATATYPE_MAP_INT64_STRING,
                  (HYPERDATATYPE_INT64, HYPERDATATYPE_INT64): HYPERDATATYPE_MAP_INT64_INT64}
        for x in range(j, i):
            attrs[0][x].datatype = dtypes[(keytype, valtype)]
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
        else:
            raise ValueError("Server returned garbage value (file a bug)")
    return ret


cdef class Deferred:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished

    def __cinit__(self, Client client, *args):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_ZERO
        self._finished = False

    def _callback(self):
        self._finished = True
        del self._client._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._client.loop()
        self._finished = True
        if self._status not in (HYPERCLIENT_SUCCESS, HYPERCLIENT_NOTFOUND, HYPERCLIENT_CMPFAIL):
            raise HyperClientException(self._status)


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
        if self._reqid < 0:
            raise HyperClientException(self._status)
        client._ops[self._reqid] = self

    def __dealloc__(self):
        if self._attrs:
            hyperclient_destroy_attrs(self._attrs, self._attrs_sz)

    def wait(self):
        Deferred.wait(self)
        if self._status != HYPERCLIENT_SUCCESS:
            return None
        return _attrs_to_dict(self._attrs, self._attrs_sz)


cdef class DeferredFromAttrs(Deferred):

    def __cinit__(self, Client client, bytes space, key, dict value, bytes op = None):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute* attrs = NULL
        try:
            backings = _dict_to_attrs(value.items(), &attrs)
            # XXX I'd like to use something more-constant in time, but I cannot
            # get Cython to work with function pointers correctly
            if not op:
                self._reqid = hyperclient_put(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'add':
                self._reqid = hyperclient_atomic_add(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'sub':
                self._reqid = hyperclient_atomic_sub(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'mul':
                self._reqid = hyperclient_atomic_mul(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'div':
                self._reqid = hyperclient_atomic_div(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'mod':
                self._reqid = hyperclient_atomic_mod(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'and':
                self._reqid = hyperclient_atomic_and(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'or':
                self._reqid = hyperclient_atomic_or(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'xor':
                self._reqid = hyperclient_atomic_xor(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'prepend':
                self._reqid = hyperclient_string_prepend(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'append':
                self._reqid = hyperclient_string_append(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'lpush':
                self._reqid = hyperclient_list_lpush(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'rpush':
                self._reqid = hyperclient_list_rpush(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'set_add':
                self._reqid = hyperclient_set_add(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'set_remove':
                self._reqid = hyperclient_set_remove(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'set_intersect':
                self._reqid = hyperclient_set_intersect(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            elif op == 'set_union':
                self._reqid = hyperclient_set_union(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, len(value), &self._status)
            else:
                raise AttributeError("op == {0} is not valid".format(op))
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if attrs and attrs[idx].attr:
                    attr = attrs[idx].attr
                raise HyperClientException(self._status, attr)
            client._ops[self._reqid] = self
        finally:
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class DeferredCondPut(Deferred):

    def __cinit__(self, Client client, bytes space, key, dict condition, dict value):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_attribute* condattrs = NULL
        cdef hyperclient_attribute* attrs = NULL
        try:
            backingsc = _dict_to_attrs(condition.items(), &condattrs)
            backingsa = _dict_to_attrs(value.items(), &attrs)
            self._reqid = hyperclient_condput(client._client, space_cstr,
                                              key_cstr, len(key_backing),
                                              condattrs, len(condition),
                                              attrs, len(value),
                                              &self._status)
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if idx < len(condition) and condattrs and condattrs[idx].attr:
                    attr = condattrs[idx].attr
                idx -= len(condition)
                if idx >= 0 and attrs and attrs[idx].attr:
                    attr = attrs[idx].attr
                raise HyperClientException(self._status, attr)
            client._ops[self._reqid] = self
        finally:
            if condattrs:
                free(condattrs)
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class DeferredDelete(Deferred):

    def __cinit__(self, Client client, bytes space, key):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        self._reqid = hyperclient_del(client._client, space_cstr,
                                      key_cstr, len(key_backing), &self._status)
        if self._reqid < 0:
            raise HyperClientException(self._status)
        client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class DeferredMapOp(Deferred):

    def __cinit__(self, Client client, bytes space, key, dict value, bytes op = None):
        cdef bytes key_backing
        datatype, key_backing = _obj_to_backing(key)
        cdef char* space_cstr = space
        cdef char* key_cstr = key_backing
        cdef hyperclient_map_attribute* attrs = NULL
        cdef size_t attrs_sz = 0
        try:
            backings = _dict_to_map_attrs(value.items(), &attrs, &attrs_sz)
            # XXX I'd like to use something more-constant in time, but I cannot
            # get Cython to work with function pointers correctly
            if op == 'map_add':
                self._reqid = hyperclient_map_add(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_remove':
                self._reqid = hyperclient_map_remove(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_add':
                self._reqid = hyperclient_map_atomic_add(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_sub':
                self._reqid = hyperclient_map_atomic_sub(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_mul':
                self._reqid = hyperclient_map_atomic_mul(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_div':
                self._reqid = hyperclient_map_atomic_div(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_mod':
                self._reqid = hyperclient_map_atomic_mod(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_and':
                self._reqid = hyperclient_map_atomic_and(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_or':
                self._reqid = hyperclient_map_atomic_or(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_atomic_xor':
                self._reqid = hyperclient_map_atomic_xor(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_string_prepend':
                self._reqid = hyperclient_map_string_prepend(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            elif op == 'map_string_append':
                self._reqid = hyperclient_map_string_append(client._client, space_cstr,
                        key_cstr, len(key_backing), attrs, attrs_sz, &self._status)
            else:
                raise AttributeError("op == {0} is not valid".format(op))
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if attrs and attrs[idx].attr:
                    attr = attrs[idx].attr
                raise HyperClientException(self._status, attr)
            client._ops[self._reqid] = self
        finally:
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class Search:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished
    cdef hyperclient_attribute* _attrs
    cdef size_t _attrs_sz
    cdef bytes _space
    cdef list _backlogged

    def __cinit__(self, Client client, bytes space, dict predicate):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_ZERO
        self._finished = False
        self._attrs = <hyperclient_attribute*> NULL
        self._attrs_sz = 0
        self._space = space
        self._backlogged = []
        cdef uint64_t lower
        cdef uint64_t upper
        equalities = []
        ranges = []
        for attr, params in predicate.iteritems():
            if isinstance(params, tuple):
                (lower, upper) = params
                ranges.append((attr, lower, upper))
            elif isinstance(params, int):
                equalities.append((attr, params))
            elif isinstance(params, bytes):
                equalities.append((attr, params))
            else:
                errstr = "Attribute '{attr}' has incorrect type (expected int, (int, int) or bytes, got {type}"
                raise TypeError(errstr.format(attr=attr, type=str(type(params))[7:-2]))
        cdef hyperclient_attribute* eq = NULL
        cdef hyperclient_range_query* rm = NULL
        try:
            eq = <hyperclient_attribute*> \
                 malloc(sizeof(hyperclient_attribute) * len(equalities))
            rn = <hyperclient_range_query*> \
                 malloc(sizeof(hyperclient_range_query) * len(ranges))
            backings = _dict_to_attrs(equalities, &eq)
            for i, (attr, lower, upper) in enumerate(ranges):
                rn[i].attr = attr
                rn[i].lower = lower
                rn[i].upper = upper
            self._reqid = hyperclient_search(client._client,
                                             self._space,
                                             eq, len(equalities),
                                             rn, len(ranges),
                                             &self._status,
                                             &self._attrs,
                                             &self._attrs_sz)
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if idx < len(equalities) and eq and eq[idx].attr:
                    attr = eq[idx].attr
                idx -= len(equalities)
                if idx < len(ranges) and rn and rn[idx].attr:
                    attr = rn[idx].attr
                raise HyperClientException(self._status, attr)
            client._ops[self._reqid] = self
        finally:
            if eq: free(eq)
            if rn: free(rn)

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
                    free(self._attrs)
            self._backlogged.append(attrs)
        else:
            self._backlogged.append(HyperClientException(self._status))


cdef class Client:
    cdef hyperclient* _client
    cdef dict _ops

    def __cinit__(self, address, port):
        self._client = hyperclient_create(address, port)
        self._ops = {}

    def __dealloc__(self):
        if self._client:
            hyperclient_destroy(self._client)

    def get(self, bytes space, key):
        async = self.async_get(space, key)
        return async.wait()

    def put(self, bytes space, key, dict value):
        async = self.async_put(space, key, value)
        return async.wait()

    def condput(self, bytes space, key, dict condition, dict value):
        async = self.async_condput(space, key, condition, value)
        return async.wait()

    def delete(self, bytes space, key):
        async = self.async_delete(space, key)
        return async.wait()

    def atomic_add(self, bytes space, key, dict value):
        async = self.async_atomic_add(space, key, value)
        return async.wait()

    def atomic_sub(self, bytes space, key, dict value):
        async = self.async_atomic_sub(space, key, value)
        return async.wait()

    def atomic_mul(self, bytes space, key, dict value):
        async = self.async_atomic_mul(space, key, value)
        return async.wait()

    def atomic_div(self, bytes space, key, dict value):
        async = self.async_atomic_div(space, key, value)
        return async.wait()

    def atomic_mod(self, bytes space, key, dict value):
        async = self.async_atomic_mod(space, key, value)
        return async.wait()

    def atomic_and(self, bytes space, key, dict value):
        async = self.async_atomic_and(space, key, value)
        return async.wait()

    def atomic_or(self, bytes space, key, dict value):
        async = self.async_atomic_or(space, key, value)
        return async.wait()

    def atomic_xor(self, bytes space, key, dict value):
        async = self.async_atomic_xor(space, key, value)
        return async.wait()

    def string_prepend(self, bytes space, key, dict value):
        async = self.async_string_prepend(space, key, value)
        return async.wait()

    def string_append(self, bytes space, key, dict value):
        async = self.async_string_append(space, key, value)
        return async.wait()

    def list_lpush(self, bytes space, key, dict value):
        async = self.async_list_lpush(space, key, value)
        return async.wait()

    def list_rpush(self, bytes space, key, dict value):
        async = self.async_list_rpush(space, key, value)
        return async.wait()

    def set_add(self, bytes space, key, dict value):
        async = self.async_set_add(space, key, value)
        return async.wait()

    def set_remove(self, bytes space, key, dict value):
        async = self.async_set_remove(space, key, value)
        return async.wait()

    def set_intersect(self, bytes space, key, dict value):
        async = self.async_set_intersect(space, key, value)
        return async.wait()

    def set_union(self, bytes space, key, dict value):
        async = self.async_set_union(space, key, value)
        return async.wait()

    def map_add(self, bytes space, key, dict value):
        async = self.async_map_add(space, key, value)
        return async.wait()

    def map_remove(self, bytes space, key, dict value):
        async = self.async_map_remove(space, key, value)
        return async.wait()

    def map_atomic_add(self, bytes space, key, dict value):
        async = self.async_map_atomic_add(space, key, value)
        return async.wait()

    def map_atomic_sub(self, bytes space, key, dict value):
        async = self.async_map_atomic_sub(space, key, value)
        return async.wait()

    def map_atomic_mul(self, bytes space, key, dict value):
        async = self.async_map_atomic_mul(space, key, value)
        return async.wait()

    def map_atomic_div(self, bytes space, key, dict value):
        async = self.async_map_atomic_div(space, key, value)
        return async.wait()

    def map_atomic_mod(self, bytes space, key, dict value):
        async = self.async_map_atomic_mod(space, key, value)
        return async.wait()

    def map_atomic_and(self, bytes space, key, dict value):
        async = self.async_map_atomic_and(space, key, value)
        return async.wait()

    def map_atomic_or(self, bytes space, key, dict value):
        async = self.async_map_atomic_or(space, key, value)
        return async.wait()

    def map_atomic_xor(self, bytes space, key, dict value):
        async = self.async_map_atomic_xor(space, key, value)
        return async.wait()

    def map_string_prepend(self, bytes space, key, dict value):
        async = self.async_map_string_prepend(space, key, value)
        return async.wait()

    def map_string_append(self, bytes space, key, dict value):
        async = self.async_map_string_append(space, key, value)
        return async.wait()

    def search(self, bytes space, dict predicate):
        return Search(self, space, predicate)

    def async_get(self, bytes space, key):
        return DeferredGet(self, space, key)

    def async_put(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value)

    def async_condput(self, bytes space, key, dict condition, dict value):
        return DeferredCondPut(self, space, key, condition, value)

    def async_delete(self, bytes space, key):
        return DeferredDelete(self, space, key)

    def async_atomic_add(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'add')

    def async_atomic_sub(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'sub')

    def async_atomic_mul(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'mul')

    def async_atomic_div(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'div')

    def async_atomic_mod(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'mod')

    def async_atomic_and(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'and')

    def async_atomic_or(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'or')

    def async_atomic_xor(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'xor')

    def async_string_prepend(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'prepend')

    def async_string_append(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'append')

    def async_list_lpush(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'lpush')

    def async_list_rpush(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'rpush')

    def async_set_add(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'set_add')

    def async_set_remove(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'set_remove')

    def async_set_intersect(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'set_intersect')

    def async_set_union(self, bytes space, key, dict value):
        return DeferredFromAttrs(self, space, key, value, 'set_union')

    def async_map_add(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_add')

    def async_map_remove(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_remove')

    def async_map_atomic_add(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_add')

    def async_map_atomic_sub(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_sub')

    def async_map_atomic_mul(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_mul')

    def async_map_atomic_div(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_div')

    def async_map_atomic_mod(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_mod')

    def async_map_atomic_and(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_and')

    def async_map_atomic_or(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_or')

    def async_map_atomic_xor(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_atomic_xor')

    def async_map_string_prepend(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_string_prepend')

    def async_map_string_append(self, bytes space, key, dict value):
        return DeferredMapOp(self, space, key, value, 'map_string_append')

    def loop(self):
        cdef hyperclient_returncode rc
        ret = hyperclient_loop(self._client, -1, &rc)
        if ret < 0:
            raise HyperClientException(rc)
        else:
            if ret in self._ops:
                op = self._ops[ret]
                # We cannot refer to self._ops[ret] after this call as
                # _callback() may remove ret from self._ops.
                op._callback()
                return op
            else:
                raise HyperClientException(HYPERCLIENT_LOGICERROR)
