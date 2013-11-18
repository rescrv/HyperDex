# Copyright (c) 2013, Cornell University
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
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

class AsyncCall: pass
class Iterator: pass

class SpaceName(object):
    args = (('const char*', 'space'),)
class Key(object):
    args = (('const char*', 'key'), ('size_t', 'key_sz'))
class Predicates(object):
    args = (('const struct hyperdex_client_attribute_check*', 'checks'),
            ('size_t', 'checks_sz'))
class Attributes(object):
    args = (('const struct hyperdex_client_attribute*', 'attrs'),
            ('size_t', 'attrs_sz'))
class MapAttributes(object):
    args = (('const struct hyperdex_client_map_attribute*', 'mapattrs'),
            ('size_t', 'mapattrs_sz'))
class Status(object):
    args = (('enum hyperdex_client_returncode', 'status'),)
class Description(object):
    args = (('const char*', 'description'),)
class SortBy(object):
    args = (('const char*', 'sort_by'),)
class Limit(object):
    args = (('uint64_t', 'limit'),)
class Count(object):
    args = (('uint64_t', 'count'),)
class MaxMin(object):
    args = (('int', 'maxmin'),)

class Method(object):

    def __init__(self, name, form, args_in, args_out):
        self.name = name
        self.form = form
        self.args_in = args_in
        self.args_out = args_out

Client = [
    Method('get', AsyncCall, (SpaceName, Key), (Status, Attributes)),
    Method('put', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_put', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('put_if_not_exist', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('del', AsyncCall, (SpaceName, Key), (Status,)),
    Method('cond_del', AsyncCall, (SpaceName, Key, Predicates), (Status,)),
    Method('atomic_add', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_add', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_sub', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_sub', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_mul', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_mul', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_div', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_div', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_mod', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_mod', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_and', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_and', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_or', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_or', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('atomic_xor', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_atomic_xor', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('string_prepend', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_string_prepend', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('string_append', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_string_append', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('list_lpush', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_list_lpush', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('list_rpush', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_list_rpush', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('set_add', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_set_add', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('set_remove', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_set_remove', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('set_intersect', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_set_intersect', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('set_union', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_set_union', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('map_add', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_add', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_remove', AsyncCall, (SpaceName, Key, Attributes), (Status,)),
    Method('cond_map_remove', AsyncCall, (SpaceName, Key, Predicates, Attributes), (Status,)),
    Method('map_atomic_add', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_add', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_sub', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_sub', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_mul', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_mul', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_div', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_div', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_mod', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_mod', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_and', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_and', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_or', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_or', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_atomic_xor', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_atomic_xor', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_string_prepend', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_string_prepend', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('map_string_append', AsyncCall, (SpaceName, Key, MapAttributes), (Status,)),
    Method('cond_map_string_append', AsyncCall, (SpaceName, Key, Predicates, MapAttributes), (Status,)),
    Method('search', Iterator, (SpaceName, Predicates), (Status, Attributes)),
    Method('search_describe', AsyncCall, (SpaceName, Predicates), (Status, Description)),
    Method('sorted_search', Iterator, (SpaceName, Predicates, SortBy, Limit, MaxMin), (Status, Attributes)),
    Method('group_del', AsyncCall, (SpaceName, Predicates), (Status,)),
    Method('count', AsyncCall, (SpaceName, Predicates), (Status, Count)),
    None][:-1]
