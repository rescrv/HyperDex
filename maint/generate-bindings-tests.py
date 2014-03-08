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

import abc
import collections
import os
import sys

def double_quote(x):
    y = repr(x)
    y = '"' + y[1:-1] + '"'
    return y

def gen_shell(lang, name, cmd, space, precmd=None):
    shell = '#!/bin/sh\n'
    if precmd is not None:
        shell += precmd + '\n'
    shell += '''
python "${{HYPERDEX_SRCDIR}}"/test/runner.py --space="{0}" --daemons=1 -- \\
    {1} {{HOST}} {{PORT}}
'''.format(space, cmd)
    path = 'test/sh/bindings.{0}.{1}.sh'.format(lang, name)
    f = open(path, 'w')
    f.write(shell)
    f.flush()
    f.close()
    os.chmod(path, 0755)
    print 'shellwrappers += ' + path

class LessEqual(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'LessEqual({0!r})'.format(self.x)

class GreaterEqual(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'GreaterEqual({0!r})'.format(self.x)

class LessThan(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'LessThan({0!r})'.format(self.x)

class GreaterThan(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'GreaterThan({0!r})'.format(self.x)

class Range(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y
    def __repr__(self):
        return 'Range({0!r}, {1!r})'.format(self.x, self.y)

class Regex(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'Regex({0!r})'.format(self.x)

class LengthEquals(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'LengthEquals({0!r})'.format(self.x)

class LengthLessEqual(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'LengthLessEqual({0!r})'.format(self.x)

class LengthGreaterEqual(object):
    def __init__(self, x):
        self.x = x
    def __repr__(self):
        return 'LengthGreaterEqual({0!r})'.format(self.x)

class BindingGenerator(object):

    __metaclass__ = abc.ABCMeta

    def test(self, name, space): pass
    def finish(self): pass
    def get(self, space, key, expected): print 'XXX Get', str(self)
    def put(self, space, key, value, expected): print 'XXX Pet', str(self)
    def delete(self, space, key, expected): print 'XXX Del', str(self)
    def search(self, space, predicate, expected): print 'XXX Search', str(self)

class PythonGenerator(BindingGenerator):

    def __init__(self):
        self.f = None

    def test(self, name, space):
        assert self.f is None
        self.name = 'test/python/{0}.py'.format(name)
        gen_shell('python', name, 'python "${HYPERDEX_SRCDIR}"/' + self.name, space)
        self.f = open(self.name, 'w')
        self.f.write('''#!/usr/bin/env python
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
''')

    def finish(self):
        self.f.flush()
        self.f.close()
        self.f = None
        os.chmod(self.name, 0755)

    def get(self, space, key, expected):
        self.f.write('assert c.get({0!r}, {1!r}) == {2!r}\n'.format(space, key, expected))

    def put(self, space, key, value, expected):
        self.f.write('assert c.put({0!r}, {1!r}, {2!r}) == {3!r}\n'.format(space, key, value, expected))

    def delete(self, space, key, expected):
        self.f.write('assert c.delete({0!r}, {1!r}) == {2!r}\n'.format(space, key, expected))

    def search(self, space, predicate, expected):
        self.f.write('assert to_objectset(c.search({0!r}, {1!r})) == to_objectset({2!r})\n'.format(space, predicate, expected))

class RubyGenerator(BindingGenerator):

    def __init__(self):
        self.f = None

    def test(self, name, space):
        assert self.f is None
        self.count = 0
        self.name = 'test/ruby/{0}.rb'.format(name)
        gen_shell('ruby', name, 'ruby "${HYPERDEX_SRCDIR}"/' + self.name, space)
        self.f = open(self.name, 'w')
        self.f.write('''#!/usr/bin/env ruby
require 'hyperdex'

def assert &block
    raise RuntimeError unless yield
end

def collapse_iterator(xs)
    s = Set.new []
    while xs.has_next() do
        x = xs.next()
        x.freeze
        s.add(x)
    end
    s
end

def to_objectset(xs)
    s = Set.new []
    xs.each do |x|
        x.freeze
        s.add(x)
    end
    s
end

c = HyperDex::Client::Client.new(ARGV[0], ARGV[1].to_i)
''')

    def finish(self):
        self.f.flush()
        self.f.close()
        self.f = None
        os.chmod(self.name, 0755)

    def get(self, space, key, expected):
        self.f.write('assert {{ c.get({0}, {1}) == {2} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key), self.to_ruby(expected, symbol=True)))

    def put(self, space, key, value, expected):
        self.f.write('assert {{ c.put({0}, {1}, {2}) == {3} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key), self.to_ruby(value), self.to_ruby(expected)))

    def delete(self, space, key, expected):
        self.f.write('assert {{ c.del({0}, {1}) == {2} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key), self.to_ruby(expected)))

    def search(self, space, predicate, expected):
        c = self.count
        self.count += 1
        self.f.write('X{0} = collapse_iterator(c.search({1}, {2}))\n'
                     .format(c, self.to_ruby(space), self.to_ruby(predicate)))
        self.f.write('Y{0} = to_objectset({1})\n'.format(c, self.to_ruby(expected, symbol=True)))
        self.f.write('assert {{ X{0}.subset?Y{0} and Y{0}.subset?X{0} }}\n'.format(c))

    def to_ruby(self, x, symbol=False):
        if x is True:
            return 'true'
        elif x is False:
            return 'false'
        elif x is None:
            return 'nil'
        elif isinstance(x, LessEqual):
            return '(HyperDex::Client::LessEqual.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, GreaterEqual):
            return '(HyperDex::Client::GreaterEqual.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, LessThan):
            return '(HyperDex::Client::LessThan.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, GreaterThan):
            return '(HyperDex::Client::GreaterThan.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, Range):
            return '(HyperDex::Client::Range.new {0}, {1})'.format(self.to_ruby(x.x), self.to_ruby(x.y))
        elif isinstance(x, Regex):
            return '(HyperDex::Client::Regex.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, LengthEquals):
            return '(HyperDex::Client::LengthEquals.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, LengthLessEqual):
            return '(HyperDex::Client::LengthLessEqual.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, LengthGreaterEqual):
            return '(HyperDex::Client::LengthGreaterEqual.new {0})'.format(self.to_ruby(x.x))
        elif isinstance(x, str):
            return double_quote(x)
        elif isinstance(x, int) or isinstance(x, long):
            return str(x)
        elif isinstance(x, float):
            return str(x)
        elif isinstance(x, list):
            return '[' + ', '.join([self.to_ruby(y, symbol) for y in x]) + ']'
        elif isinstance(x, set):
            return '(Set.new ' + repr(list(x)) + ')'
        elif isinstance(x, dict) and symbol:
            pairs = []
            for k, v in x.iteritems():
                pairs.append(':' + k + ' => ' + self.to_ruby(v))
            return '{' + ', '.join(pairs) + '}'
        elif isinstance(x, dict):
            pairs = []
            for k, v in x.iteritems():
                pairs.append(self.to_ruby(k) + ' => ' + self.to_ruby(v))
            return '{' + ', '.join(pairs) + '}'
        else:
            raise RuntimeError("Cannot convert {0!r} to ruby".format(x))

class JavaGenerator(BindingGenerator):

    def __init__(self):
        self.f = None

    def test(self, name, space):
        assert self.f is None
        self.count = 0
        self.path = 'test/java/{0}.java'.format(name)
        precmd = 'javac -d "${{HYPERDEX_BUILDDIR}}"/test/java "${{HYPERDEX_SRCDIR}}"/test/java/{0}.java'.format(name)
        cmd = 'java -Djava.library.path="${{HYPERDEX_BUILDDIR}}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64 {0}'.format(name)
        gen_shell('java', name, cmd, space, precmd=precmd)
        self.f = open(self.path, 'w')
        self.f.write('''import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.LessEqual;
import org.hyperdex.client.GreaterEqual;
import org.hyperdex.client.LessThan;
import org.hyperdex.client.GreaterThan;
import org.hyperdex.client.Range;
import org.hyperdex.client.Regex;
import org.hyperdex.client.LengthEquals;
import org.hyperdex.client.LengthLessEqual;
import org.hyperdex.client.LengthGreaterEqual;

public class {0}
{{
    public static void main(String[] args) throws HyperDexClientException
    {{
        Client c = new Client(args[0], Integer.parseInt(args[1]));
'''.format(name))

    def finish(self):
        self.f.write('''    }
}
''')
        self.f.flush()
        self.f.close()
        self.f = None
        os.chmod(self.path, 0755)

    def get(self, space, key, expected):
        c = self.count
        self.count += 1
        self.f.write('        Map<String, Object> get{0} = c.get({1}, {2});\n'
                     .format(c, self.to_java(space),
                                self.to_java(key)))
        if expected is None:
            self.f.write('        assert(get{0} == null);\n'.format(c))
        else:
            self.f.write('        assert(get{0} != null);\n'.format(c))
            self.f.write('        Map<String, Object> expected{0} = new HashMap<String, Object>();\n'.format(c))
            for k, v in sorted(expected.iteritems()):
                self.f.write('        expected{0}.put({1}, {2});\n'
                                 .format(c, self.to_java(k), self.to_java(v)))
            self.f.write('        get{0}.entrySet().containsAll(expected{0}.entrySet());\n'.format(c))
            self.f.write('        expected{0}.entrySet().containsAll(get{0}.entrySet());\n'.format(c))

    def put(self, space, key, value, expected):
        assert expected in (True, False)
        c = self.count
        self.count += 1
        self.f.write('        Map<String, Object> attrs{0} = new HashMap<String, Object>();\n'.format(c))
        for k, v in sorted(value.iteritems()):
            self.f.write('        attrs{0}.put({1}, {2});\n'
                             .format(c, self.to_java(k), self.to_java(v)))
        self.f.write('        Object obj{0} = c.put({1}, {2}, attrs{0});\n'
                     .format(c, self.to_java(space),
                                self.to_java(key)))
        self.f.write('        assert(obj{0} != null);\n'.format(c))
        self.f.write('        Boolean bool{0} = (Boolean)obj{0};\n'.format(c))
        self.f.write('        assert(bool{0} == {1});\n'
                     .format(c, self.to_java(expected)))

    def delete(self, space, key, expected):
        assert expected in (True, False)
        c = self.count
        self.count += 1
        self.f.write('        Object obj{0} = c.del({1}, {2});\n'
                     .format(c, self.to_java(space),
                                self.to_java(key)))
        self.f.write('        assert(obj{0} != null);\n'.format(c))
        self.f.write('        Boolean bool{0} = (Boolean)obj{0};\n'.format(c))
        self.f.write('        assert(bool{0} == {1});\n'
                     .format(c, self.to_java(expected)))

    def search(self, space, predicate, expected):
        c = self.count
        self.count += 1
        self.f.write('        Map<String, Object> checks{0} = new HashMap<String, Object>();\n'.format(c))
        for k, v in sorted(predicate.iteritems()):
            self.f.write('        checks{0}.put({1}, {2});\n'
                             .format(c, self.to_java(k), self.to_java(v)))
        self.f.write('        Set<Object> X{0} = new HashSet<Object>();\n'.format(c))
        self.f.write('        Iterator it{0} = c.search({1}, checks{0});\n'
                     .format(c, self.to_java(space)))
        self.f.write('        while (it{0}.hasNext())\n'.format(c))
        self.f.write('        {\n')
        self.f.write('            X{0}.add(it{0}.next());\n'.format(c))
        self.f.write('        }\n')

    def to_java(self, x):
        if x is True:
            return 'true'
        elif x is False:
            return 'false'
        elif x is None:
            return 'null'
        elif isinstance(x, LessEqual):
            return 'new LessEqual({0})'.format(self.to_java(x.x))
        elif isinstance(x, GreaterEqual):
            return 'new GreaterEqual({0})'.format(self.to_java(x.x))
        elif isinstance(x, LessThan):
            return 'new LessThan({0})'.format(self.to_java(x.x))
        elif isinstance(x, GreaterThan):
            return 'new GreaterThan({0})'.format(self.to_java(x.x))
        elif isinstance(x, Range):
            return 'new Range({0}, {1})'.format(self.to_java(x.x), self.to_java(x.y))
        elif isinstance(x, Regex):
            return 'new Regex({0})'.format(self.to_java(x.x))
        elif isinstance(x, LengthEquals):
            return 'new LengthEquals({0})'.format(self.to_java(x.x))
        elif isinstance(x, LengthLessEqual):
            return 'new LengthLessEqual({0})'.format(self.to_java(x.x))
        elif isinstance(x, LengthGreaterEqual):
            return 'new LengthGreaterEqual({0})'.format(self.to_java(x.x))
        elif isinstance(x, str):
            if any([len(repr(c)) > 3 for c in x]):
                c = self.count
                self.count += 1
                def to_java_byte(x):
                    prefix = ''
                    if ord(x) > 127:
                        prefix = '(byte) '
                    return prefix + hex(ord(x))
                bs = ', '.join([to_java_byte(y) for y in x])
                self.f.write('        byte[] bytes{0} = {{{1}}};\n'.format(c, bs))
                return 'new ByteString(bytes{0})'.format(c)
            return double_quote(x)
        elif isinstance(x, long) or isinstance(x, int):
            suffix = ''
            if x > 2**31 - 1 or x < -(2**31):
                suffix = 'L'
            return str(x) + suffix
        elif isinstance(x, float):
            return str(x)
        elif isinstance(x, list):
            c = self.count
            self.count += 1
            self.f.write('        List<Object> list{0} = new ArrayList<Object>();\n'.format(c))
            for y in x:
                self.f.write('        list{0}.add({1});\n'.format(c, self.to_java(y)))
            return 'list{0}'.format(c)
        elif isinstance(x, set):
            c = self.count
            self.count += 1
            self.f.write('        Set<Object> set{0} = new HashSet<Object>();\n'.format(c))
            for y in sorted(x):
                self.f.write('        set{0}.add({1});\n'.format(c, self.to_java(y)))
            return 'set{0}'.format(c)
        elif isinstance(x, dict):
            c = self.count
            self.count += 1
            self.f.write('        Map<Object, Object> map{0} = new HashMap<Object, Object>();\n'.format(c))
            for k, v in sorted(x.iteritems()):
                self.f.write('        map{0}.put({1}, {2});\n'
                             .format(c, self.to_java(k), self.to_java(v)))
            return 'map{0}'.format(c)
        else:
            raise RuntimeError("Cannot convert {0!r} to java".format(x))

class TestGenerator(object):

    def __init__(self):
        self.generators = []
        for x in BindingGenerator.__subclasses__():
            self.generators.append(x())

    def test(self, name, space):
        for x in self.generators:
            x.test(name, space)

    def finish(self):
        for x in self.generators:
            x.finish()

    def get(self, space, key, expected):
        for x in self.generators:
            x.get(space, key, expected)

    def put(self, space, key, value, expected):
        for x in self.generators:
            x.put(space, key, value, expected)

    def delete(self, space, key, expected):
        for x in self.generators:
            x.delete(space, key, expected)

    def search(self, space, predicate, expected):
        for x in self.generators:
            x.search(space, predicate, expected)

t = TestGenerator()

t.test('Basic', 'space kv key k attribute v')
t.get('kv', 'k', None)
t.put('kv', 'k', {'v': 'v1'}, True)
t.get('kv', 'k', {'v': 'v1'})
t.put('kv', 'k', {'v': 'v2'}, True)
t.get('kv', 'k', {'v': 'v2'})
t.put('kv', 'k', {'v': 'v3'}, True)
t.get('kv', 'k', {'v': 'v3'})
t.delete('kv', 'k', True)
t.get('kv', 'k', None)
t.finish()

t.test('MultiAttribute', 'space kv key k attributes v1, v2')
t.get('kv', 'k', None)
t.put('kv', 'k', {'v1': 'ABC'}, True)
t.get('kv', 'k', {'v1': 'ABC', 'v2': ''})
t.put('kv', 'k', {'v2': '123'}, True)
t.get('kv', 'k', {'v1': 'ABC', 'v2': '123'})
t.finish()

t.test('DataTypeString', 'space kv key k attributes v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': ''})
t.put('kv', 'k', {'v': 'xxx'}, True)
t.get('kv', 'k', {'v': 'xxx'})
t.put('kv', 'k', {'v': '\xde\xad\xbe\xef'}, True)
t.get('kv', 'k', {'v': '\xde\xad\xbe\xef'})
t.finish()

t.test('DataTypeInt', 'space kv key k attributes int v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': 0})
t.put('kv', 'k', {'v': 1}, True)
t.get('kv', 'k', {'v': 1})
t.put('kv', 'k', {'v': -1}, True)
t.get('kv', 'k', {'v': -1})
t.put('kv', 'k', {'v': 0}, True)
t.get('kv', 'k', {'v': 0})
t.put('kv', 'k', {'v': 9223372036854775807}, True)
t.get('kv', 'k', {'v': 9223372036854775807})
t.put('kv', 'k', {'v': -9223372036854775808}, True)
t.get('kv', 'k', {'v': -9223372036854775808})
t.finish()

t.test('DataTypeFloat', 'space kv key k attributes float v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': 0.0})
t.put('kv', 'k', {'v': 3.14}, True)
t.get('kv', 'k', {'v': 3.14})
t.finish()

t.test('DataTypeListString', 'space kv key k attributes list(string) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': []})
t.put('kv', 'k', {'v': ['A', 'B', 'C']}, True)
t.get('kv', 'k', {'v': ['A', 'B', 'C']})
t.put('kv', 'k', {'v': []}, True)
t.get('kv', 'k', {'v': []})
t.finish()

t.test('DataTypeListInt', 'space kv key k attributes list(int) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': []})
t.put('kv', 'k', {'v': [1, 2, 3]}, True)
t.get('kv', 'k', {'v': [1, 2, 3]})
t.put('kv', 'k', {'v': []}, True)
t.get('kv', 'k', {'v': []})
t.finish()

t.test('DataTypeListFloat', 'space kv key k attributes list(float) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': []})
t.put('kv', 'k', {'v': [3.14, 0.25, 1.0]}, True)
t.get('kv', 'k', {'v': [3.14, 0.25, 1.0]})
t.put('kv', 'k', {'v': []}, True)
t.get('kv', 'k', {'v': []})
t.finish()

t.test('DataTypeSetString', 'space kv key k attributes set(string) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': set([])})
t.put('kv', 'k', {'v': set(['A', 'B', 'C'])}, True)
t.get('kv', 'k', {'v': set(['A', 'B', 'C'])})
t.put('kv', 'k', {'v': set([])}, True)
t.get('kv', 'k', {'v': set([])})
t.finish()

t.test('DataTypeSetInt', 'space kv key k attributes set(int) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': set([])})
t.put('kv', 'k', {'v': set([1, 2, 3])}, True)
t.get('kv', 'k', {'v': set([1, 2, 3])})
t.put('kv', 'k', {'v': set([])}, True)
t.get('kv', 'k', {'v': set([])})
t.finish()

t.test('DataTypeSetFloat', 'space kv key k attributes set(float) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': set([])})
t.put('kv', 'k', {'v': set([3.14, 0.25, 1.0])}, True)
t.get('kv', 'k', {'v': set([3.14, 0.25, 1.0])})
t.put('kv', 'k', {'v': set([])}, True)
t.get('kv', 'k', {'v': set([])})
t.finish()

t.test('DataTypeMapStringString', 'space kv key k attributes map(string, string) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {'A': 'X', 'B': 'Y', 'C': 'Z'}}, True)
t.get('kv', 'k', {'v': {'A': 'X', 'B': 'Y', 'C': 'Z'}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapStringInt', 'space kv key k attributes map(string, int) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {'A': 1, 'B': 2, 'C': 3}}, True)
t.get('kv', 'k', {'v': {'A': 1, 'B': 2, 'C': 3}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapStringFloat', 'space kv key k attributes map(string, float) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {'A': 3.14, 'B': 0.25, 'C': 1.0}}, True)
t.get('kv', 'k', {'v': {'A': 3.14, 'B': 0.25, 'C': 1.0}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapIntString', 'space kv key k attributes map(int, string) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {1: 'X', 2: 'Y', 3: 'Z'}}, True)
t.get('kv', 'k', {'v': {1: 'X', 2: 'Y', 3: 'Z'}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapIntInt', 'space kv key k attributes map(int, int) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {1: 7, 2: 8, 3: 9}}, True)
t.get('kv', 'k', {'v': {1: 7, 2: 8, 3: 9}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapIntFloat', 'space kv key k attributes map(int, float) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {1: 3.14, 2: 0.25, 3: 1.0}}, True)
t.get('kv', 'k', {'v': {1: 3.14, 2: 0.25, 3: 1.0}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapFloatString', 'space kv key k attributes map(float, string) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {3.14: 'X', 0.25: 'Y', 1.0: 'Z'}}, True)
t.get('kv', 'k', {'v': {3.14: 'X', 0.25: 'Y', 1.0: 'Z'}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapFloatInt', 'space kv key k attributes map(float, int) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {3.14: 1, 0.25: 2, 1.0: 3}}, True)
t.get('kv', 'k', {'v': {3.14: 1, 0.25: 2, 1.0: 3}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('DataTypeMapFloatFloat', 'space kv key k attributes map(float, float) v')
t.put('kv', 'k', {}, True)
t.get('kv', 'k', {'v': {}})
t.put('kv', 'k', {'v': {3.14: 1.0, 0.25: 2.0, 1.0: 3.0}}, True)
t.get('kv', 'k', {'v': {3.14: 1.0, 0.25: 2.0, 1.0: 3.0}})
t.put('kv', 'k', {'v': {}}, True)
t.get('kv', 'k', {'v': {}})
t.finish()

t.test('BasicSearch', 'space kv key k attribute v')
t.search('kv', {'v': 'v1'}, set())
t.put('kv', 'k1', {'v': 'v1'}, True)
t.search('kv', {'v': 'v1'}, [{'k': 'k1', 'v': 'v1'}])
t.put('kv', 'k2', {'v': 'v1'}, True)
t.search('kv', {'v': 'v1'}, [{'k': 'k1', 'v': 'v1'}, {'k': 'k2', 'v': 'v1'}])
t.finish()

t.test('RangeSearchString', 'space kv key k attribute v')
t.put('kv', 'A', {'v': 'A'}, True)
t.put('kv', 'B', {'v': 'B'}, True)
t.put('kv', 'C', {'v': 'C'}, True)
t.put('kv', 'D', {'v': 'D'}, True)
t.put('kv', 'E', {'v': 'E'}, True)
# LessEqual
t.search('kv', {'k': LessEqual('C')},
         [{'k': 'A', 'v': 'A'},
          {'k': 'B', 'v': 'B'},
          {'k': 'C', 'v': 'C'}])
t.search('kv', {'v': LessEqual('C')},
         [{'k': 'A', 'v': 'A'},
          {'k': 'B', 'v': 'B'},
          {'k': 'C', 'v': 'C'}])
# GreaterEqual
t.search('kv', {'k': GreaterEqual('C')},
         [{'k': 'C', 'v': 'C'},
          {'k': 'D', 'v': 'D'},
          {'k': 'E', 'v': 'E'}])
t.search('kv', {'v': GreaterEqual('C')},
         [{'k': 'C', 'v': 'C'},
          {'k': 'D', 'v': 'D'},
          {'k': 'E', 'v': 'E'}])
# LessThan
t.search('kv', {'k': LessThan('C')},
         [{'k': 'A', 'v': 'A'},
          {'k': 'B', 'v': 'B'}])
t.search('kv', {'v': LessThan('C')},
         [{'k': 'A', 'v': 'A'},
          {'k': 'B', 'v': 'B'}])
# GreaterThan
t.search('kv', {'k': GreaterThan('C')},
         [{'k': 'D', 'v': 'D'},
          {'k': 'E', 'v': 'E'}])
t.search('kv', {'v': GreaterThan('C')},
         [{'k': 'D', 'v': 'D'},
          {'k': 'E', 'v': 'E'}])
# Range
t.search('kv', {'k': Range('B', 'D')},
         [{'k': 'B', 'v': 'B'},
          {'k': 'C', 'v': 'C'},
          {'k': 'D', 'v': 'D'}])
t.search('kv', {'v': Range('B', 'D')},
         [{'k': 'B', 'v': 'B'},
          {'k': 'C', 'v': 'C'},
          {'k': 'D', 'v': 'D'}])
# done
t.finish()

t.test('RangeSearchInt', 'space kv key int k attribute int v')
t.put('kv', -2, {'v': -2}, True)
t.put('kv', -1, {'v': -1}, True)
t.put('kv', 0, {'v': 0}, True)
t.put('kv', 1, {'v': 1}, True)
t.put('kv', 2, {'v': 2}, True)
# LessEqual
t.search('kv', {'k': LessEqual(0)},
         [{'k': -2, 'v': -2},
          {'k': -1, 'v': -1},
          {'k': 0, 'v': 0}])
t.search('kv', {'v': LessEqual(0)},
         [{'k': -2, 'v': -2},
          {'k': -1, 'v': -1},
          {'k': 0, 'v': 0}])
# GreaterEqual
t.search('kv', {'k': GreaterEqual(0)},
         [{'k': 0, 'v': 0},
          {'k': 1, 'v': 1},
          {'k': 2, 'v': 2}])
t.search('kv', {'v': GreaterEqual(0)},
         [{'k': 0, 'v': 0},
          {'k': 1, 'v': 1},
          {'k': 2, 'v': 2}])
# LessThan
t.search('kv', {'k': LessThan(0)},
         [{'k': -2, 'v': -2},
          {'k': -1, 'v': -1}])
t.search('kv', {'v': LessThan(0)},
         [{'k': -2, 'v': -2},
          {'k': -1, 'v': -1}])
# GreaterThan
t.search('kv', {'k': GreaterThan(0)},
         [{'k': 1, 'v': 1},
          {'k': 2, 'v': 2}])
t.search('kv', {'v': GreaterThan(0)},
         [{'k': 1, 'v': 1},
          {'k': 2, 'v': 2}])
# Range
t.search('kv', {'k': Range(-1, 1)},
         [{'k': -1, 'v': -1},
          {'k': 0, 'v': 0},
          {'k': 1, 'v': 1}])
t.search('kv', {'v': Range(-1, 1)},
         [{'k': -1, 'v': -1},
          {'k': 0, 'v': 0},
          {'k': 1, 'v': 1}])
# done
t.finish()

t.test('RegexSearch', 'space kv key k')
WORDS = ('foo', 'bar', 'baz')
for x in WORDS:
    for y in WORDS:
        for z in WORDS:
            k = x + '/' + y + '/' + z
            t.put('kv', k, {}, True)
t.search('kv', {'k': Regex('^foo')},
         [{'k': 'foo/foo/foo'}, {'k': 'foo/foo/bar'}, {'k': 'foo/foo/baz'},
          {'k': 'foo/bar/foo'}, {'k': 'foo/bar/bar'}, {'k': 'foo/bar/baz'},
          {'k': 'foo/baz/foo'}, {'k': 'foo/baz/bar'}, {'k': 'foo/baz/baz'}])
t.search('kv', {'k': Regex('foo$')},
         [{'k': 'foo/foo/foo'}, {'k': 'foo/bar/foo'}, {'k': 'foo/baz/foo'},
          {'k': 'bar/foo/foo'}, {'k': 'bar/bar/foo'}, {'k': 'bar/baz/foo'},
          {'k': 'baz/foo/foo'}, {'k': 'baz/bar/foo'}, {'k': 'baz/baz/foo'}])
t.search('kv', {'k': Regex('^b.*/foo/.*$')},
         [{'k': 'bar/foo/foo'}, {'k': 'bar/foo/bar'}, {'k': 'bar/foo/baz'},
          {'k': 'baz/foo/foo'}, {'k': 'baz/foo/bar'}, {'k': 'baz/foo/baz'}])
t.finish()

t.test('LengthString', 'space kv key k')
t.put('kv', 'A', {}, True)
t.put('kv', 'AB', {}, True)
t.put('kv', 'ABC', {}, True)
t.put('kv', 'ABCD', {}, True)
t.put('kv', 'ABCDE', {}, True)
t.search('kv', {'k': LengthEquals(1)}, [{'k': 'A'}])
t.search('kv', {'k': LengthEquals(2)}, [{'k': 'AB'}])
t.search('kv', {'k': LengthEquals(3)}, [{'k': 'ABC'}])
t.search('kv', {'k': LengthEquals(4)}, [{'k': 'ABCD'}])
t.search('kv', {'k': LengthEquals(5)}, [{'k': 'ABCDE'}])
t.search('kv', {'k': LengthLessEqual(3)},
         [{'k': 'A'}, {'k': 'AB'}, {'k': 'ABC'}])
t.search('kv', {'k': LengthGreaterEqual(3)},
         [{'k': 'ABC'}, {'k': 'ABCD'}, {'k': 'ABCDE'}])
t.finish()
