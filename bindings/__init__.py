# Copyright (c) 2013-2014, Cornell University
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
class SyncCall: pass
class NoFailCall: pass
class Iterator: pass

class StructClient(object):
    args = (('struct hyperdex_client*', 'client'),)

class StructAdmin(object):
    args = (('struct hyperdex_admin*', 'admin'),)

class SpaceName(object):
    args = (('const char*', 'space'),)
class SpaceNameSource(object):
    args = (('const char*', 'source'),)
class SpaceNameTarget(object):
    args = (('const char*', 'target'),)
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
class AttributeNames(object):
    args = (('const char**', 'attrnames'),
            ('size_t', 'attrnames_sz'))
class Status(object):
    args = (('enum hyperdex_client_returncode', 'status'),)
class AdminStatus(object):
    args = (('enum hyperdex_admin_returncode', 'status'),)
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
class ReadOnly(object):
    args = (('int', 'ro'),)
class FaultTolerance(object):
    args = (('uint64_t', 'ft'),)
class SpaceDescription(object):
    args = (('const char*', 'description'),)
class SpaceList(object):
    args = (('const char*', 'spaces'),)
class Token(object):
    args = (('uint64_t', 'token'),)
class Address(object):
    args = (('const char*', 'address'),)
class BackupName(object):
    args = (('const char*', 'backup'),)
class BackupList(object):
    args = (('const char*', 'backups'),)
class PerformanceCounters(object):
    args = (('struct hyperdex_admin_perf_counter', 'pc'),)
class AttributeName(object):
    args = (('const char*', 'attribute'),)
class IndexID(object):
    args = (('uint64_t', 'idxid'),)

class Method(object):

    def __init__(self, name, form, args_in, args_out):
        self.name = name
        self.form = form
        self.args_in = args_in
        self.args_out = args_out

Client = [
    Method('get', AsyncCall, (SpaceName, Key), (Status, Attributes)),
    Method('get_partial', AsyncCall, (SpaceName, Key, AttributeNames), (Status, Attributes)),
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
]

Admin = [
    Method('read_only', AsyncCall, (ReadOnly,), (AdminStatus,)),
    Method('wait_until_stable', AsyncCall, (), (AdminStatus,)),
    Method('fault_tolerance', AsyncCall, (SpaceName, FaultTolerance), (AdminStatus,)),
    Method('validate_space', SyncCall, (SpaceDescription,), (AdminStatus,)),
    Method('add_space', AsyncCall, (SpaceDescription,), (AdminStatus,)),
    Method('rm_space', AsyncCall, (SpaceName,), (AdminStatus,)),
    Method('mv_space', AsyncCall, (SpaceNameSource, SpaceNameTarget), (AdminStatus,)),
    Method('list_spaces', AsyncCall, (), (AdminStatus, SpaceList)),
    Method('add_index', AsyncCall, (SpaceName, AttributeName), (AdminStatus,)),
    Method('rm_index', AsyncCall, (IndexID,), (AdminStatus,)),
    Method('server_register', AsyncCall, (Token, Address), (AdminStatus,)),
    Method('server_online', AsyncCall, (Token,), (AdminStatus,)),
    Method('server_offline', AsyncCall, (Token,), (AdminStatus,)),
    Method('server_forget', AsyncCall, (Token,), (AdminStatus,)),
    Method('server_kill', AsyncCall, (Token,), (AdminStatus,)),
    Method('backup', AsyncCall, (BackupName,), (AdminStatus, BackupList)),
    Method('enable_perf_counters', AsyncCall, (), (AdminStatus, PerformanceCounters)),
    Method('disable_perf_counters', NoFailCall, (), ()),
]

def call_name(x):
    call  = x.form.__name__.lower()
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_in])
    call += '__'
    call += '_'.join([arg.__name__.lower() for arg in x.args_out])
    return call

def copyright(style, date):
    assert style in ('#', '*', '/', '%')
    template = '''{comment} Copyright (c) {date}, Cornell University
{comment} All rights reserved.
{comment}
{comment} Redistribution and use in source and binary forms, with or without
{comment} modification, are permitted provided that the following conditions are met:
{comment}
{comment}     * Redistributions of source code must retain the above copyright notice,
{comment}       this list of conditions and the following disclaimer.
{comment}     * Redistributions in binary form must reproduce the above copyright
{comment}       notice, this list of conditions and the following disclaimer in the
{comment}       documentation and/or other materials provided with the distribution.
{comment}     * Neither the name of HyperDex nor the names of its contributors may be
{comment}       used to endorse or promote products derived from this software without
{comment}       specific prior written permission.
{comment}
{comment} THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
{comment} AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
{comment} IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
{comment} DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
{comment} FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
{comment} DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
{comment} SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
{comment} CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
{comment} OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
{comment} OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''
    if style == '#':
        return template.format(comment='#', date=date)
    if style == '/':
        return template.format(comment='//', date=date)
    if style == '*':
        return '/' + template.format(comment=' *', date=date)[1:] + ' */\n'
    if style == '%':
        return template.format(comment='%', date=date)

def LaTeX(s):
    return s.replace('_', '\\_')

def parameters_c_style(arg):
    label = ', '.join(['\\code{' + LaTeX(a[1]) + '}' for a in arg.args])
    return label

def parameters_script_style(arg):
    label = '\\code{' + LaTeX(str(arg).lower()[17:-2]) + '}'
    return label

def doc_parameter_list(form, args, fragments, label_maker):
    if not args:
        return 'Nothing'
    block = '\\begin{itemize}[noitemsep]\n'
    for arg in args:
        label = label_maker(arg)
        block += '\\item ' + label + '\\\\\n'
        frag = fragments + '_' + form.__name__.lower() + '_' + arg.__name__.lower()
        block += '\\input{\\topdir/api/fragments/' + frag +'}\n'
    block += '\\end{itemize}\n'
    return block

def substitute_generated(name, text, replacement, prefix='//', suffix='\n'):
    if not replacement.endswith('\n'):
        replacement += '\n'
    START = '{0} Begin Automatically Generated {1}{2}'.format(prefix, name, suffix)
    END = '{0} End Automatically Generated {1}{2}'.format(prefix, name, suffix)
    head, tail = text.split(START)
    body, tail = tail.split(END)
    last_line = body.rsplit('\n')[-1]
    return head + START + replacement + last_line + END + tail
