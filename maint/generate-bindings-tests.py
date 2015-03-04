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
import subprocess

def double_quote(x):
    y = repr(x)
    y = '"' + y[1:-1] + '"'
    return y

def gen_shell(lang, name, cmd, space, precmd=None):
    shell = '#!/bin/sh\n'
    if precmd is not None:
        shell += precmd + '\n'
    shell += '''
python2 "${{HYPERDEX_SRCDIR}}"/test/runner.py --space="{0}" --daemons=1 -- \\
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

    @abc.abstractmethod
    def test(self, name, space):
        pass
    @abc.abstractmethod
    def finish(self):
        pass
    @abc.abstractmethod
    def get(self, space, key, expected):
        pass
    @abc.abstractmethod
    def get_partial(self, space, key, attrs, expected):
        pass
    @abc.abstractmethod
    def put(self, space, key, value, expected):
        pass
    @abc.abstractmethod
    def cond_put(self, space, key, pred, value, expected):
        pass
    @abc.abstractmethod
    def delete(self, space, key, expected):
        pass
    @abc.abstractmethod
    def search(self, space, predicate, expected):
        pass

class PythonGenerator(BindingGenerator):

    def __init__(self):
        self.f = None

    def test(self, name, space):
        assert self.f is None
        self.name = 'test/python/{0}.py'.format(name)
        gen_shell('python', name, 'python2 "${HYPERDEX_SRCDIR}"/' + self.name, space)
        self.f = open(self.name, 'w')
        self.f.write('''#!/usr/bin/env python2
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

    def get_partial(self, space, key, attrs, expected):
        self.f.write('assert c.get_partial({0!r}, {1!r}, {2!r}) == {3!r}\n'.format(space, key, attrs, expected))

    def put(self, space, key, value, expected):
        self.f.write('assert c.put({0!r}, {1!r}, {2!r}) == {3!r}\n'.format(space, key, value, expected))

    def cond_put(self, space, key, pred, value, expected):
        self.f.write('assert c.cond_put({0!r}, {1!r}, {2!r}, {3!r}) == {4!r}\n'.format(space, key, pred, value, expected))

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

    def get_partial(self, space, key, attrs, expected):
        self.f.write('assert {{ c.get_partial({0}, {1}, {2}) == {3} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key), self.to_ruby(attrs), self.to_ruby(expected, symbol=True)))

    def put(self, space, key, value, expected):
        self.f.write('assert {{ c.put({0}, {1}, {2}) == {3} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key), self.to_ruby(value), self.to_ruby(expected)))

    def cond_put(self, space, key, predicate, value, expected):
        self.f.write('assert {{ c.cond_put({0}, {1}, {2}, {3}) == {4} }}\n'
                     .format(self.to_ruby(space), self.to_ruby(key),
                             self.to_ruby(predicate),
                             self.to_ruby(value),
                             self.to_ruby(expected)))

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
        cmd = 'java -ea -Djava.library.path="${{HYPERDEX_BUILDDIR}}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64 {0}'.format(name)
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

    def get_partial(self, space, key, attrs, expected):
        c = self.count
        self.count += 1
        self.f.write('        Map<String, Object> get{0} = c.get_partial({1}, {2}, {3});\n'
                     .format(c, self.to_java(space),
                                self.to_java(key),
                                self.to_java(attrs, force='String')))
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

    def cond_put(self, space, key, predicate, value, expected):
        assert expected in (True, False)
        c = self.count
        self.count += 1
        self.f.write('        Map<String, Object> attrs{0} = new HashMap<String, Object>();\n'.format(c))
        for k, v in sorted(value.iteritems()):
            self.f.write('        attrs{0}.put({1}, {2});\n'
                             .format(c, self.to_java(k), self.to_java(v)))
        self.f.write('        Map<String, Object> checks{0} = new HashMap<String, Object>();\n'.format(c))
        for k, v in sorted(predicate.iteritems()):
            self.f.write('        checks{0}.put({1}, {2});\n'
                             .format(c, self.to_java(k), self.to_java(v)))
        self.f.write('        Object obj{0} = c.cond_put({1}, {2}, checks{0}, attrs{0});\n'
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

    def to_java(self, x, force=None):
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
            elem = force if force is not None else 'Object'
            c = self.count
            self.count += 1
            self.f.write('        List<{1}> list{0} = new ArrayList<{1}>();\n'.format(c, elem))
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

class GoGenerator(BindingGenerator):

    def __init__(self):
        self.f = None

    def test(self, name, space):
        assert self.f is None
        self.path = 'test/go/{0}.go'.format(name)
        gen_shell('go', name, 'go run test/go/{0}.go'.format(name), space)
        self.f = open(self.path, 'w')
        self.f.write('package main\n\n')
        self.f.write('import "fmt"\n')
        self.f.write('import "os"\n')
        self.f.write('import "reflect"\n')
        self.f.write('import "strconv"\n')
        self.f.write('import "hyperdex/client"\n')
        # This whole block is a dirty hack because Go won't just compare two
        # maps as equal, and sometimes the casts even fail.  Further, the
        # iteration over the map is random, so comparing by string fails.
        # Cleanup suggestions welcome.
        self.f.write('''
func sloppyEqual(lhs map[interface{}]interface{}, rhs map[interface{}]interface{}) bool {
	if reflect.DeepEqual(lhs, rhs) || fmt.Sprintln(lhs) == fmt.Sprintln(rhs) {
		return true
	}
	for key, lval := range lhs {
		if rval, ok := rhs[key]; ok {
            lstr := fmt.Sprintln(lval)
            rstr := fmt.Sprintln(rval)
            for i := 0; i < 1000; i++ {
                if lstr != rstr {
                    rstr = fmt.Sprintln(rval)
                }
            }
            lmap, lok := lval.(client.Map)
            rmap, rok := rval.(client.Map)
			if !(lval == rval || reflect.DeepEqual(lval, rval) ||
                (lok && rok && sloppyEqualMap(lmap, rmap)) ||
                lstr == rstr) {
				return false
            }
		} else {
			return false
		}
	}
	for key, rval := range rhs {
		if lval, ok := lhs[key]; ok {
            lstr := fmt.Sprintln(lval)
            rstr := fmt.Sprintln(rval)
            for i := 0; i < 1000; i++ {
                if lstr != rstr {
                    rstr = fmt.Sprintln(rval)
                }
            }
            lmap, lok := lval.(client.Map)
            rmap, rok := rval.(client.Map)
			if !(lval == rval || reflect.DeepEqual(lval, rval) ||
                (lok && rok && sloppyEqualMap(lmap, rmap)) ||
                lstr == rstr) {
				return false
            }
		} else {
			return false
		}
	}
	return true
}

func sloppyEqualMap(lhs client.Map, rhs client.Map) bool {
    lmap := map[interface{}]interface{}{}
    rmap := map[interface{}]interface{}{}
    for k, v := range lhs {
        lmap[k] = v
    }
    for k, v := range rhs {
        rmap[k] = v
    }
    return sloppyEqual(lmap, rmap)
}

func sloppyEqualAttributes(lhs client.Attributes, rhs client.Attributes) bool {
    lmap := map[interface{}]interface{}{}
    rmap := map[interface{}]interface{}{}
    for k, v := range lhs {
        lmap[k] = v
    }
    for k, v := range rhs {
        rmap[k] = v
    }
    return sloppyEqual(lmap, rmap)
}
''')
        self.f.write('\nfunc main() {\n')
        self.f.write('\tvar attrs client.Attributes\n')
        self.f.write('\tvar objs chan client.Attributes\n')
        self.f.write('\tvar errs chan client.Error\n')
        self.f.write('\t_ = attrs\n')
        self.f.write('\t_ = objs\n')
        self.f.write('\t_ = errs\n')
        self.f.write('\tvar c *client.Client\n')
        self.f.write('\tvar err client.Error\n')
        # Don't care about error here; wrong port is caught by test anyway
        self.f.write('\tport, _ := strconv.Atoi(os.Args[2])\n')
        self.f.write('\tc, er, _ := client.NewClient(os.Args[1], port)\n')
        self.f.write('\tif er != nil {\n\t\tfmt.Println(err)\n\t\tos.Exit(1)\n\t}\n')


    def finish(self):
        self.f.write('\tos.Exit(0)\n')
        self.f.write('}\n')
        self.f.flush()
        self.f.close()
        self.f = None

    def get(self, space, key, expected):
        self.f.write('\tattrs, err = c.Get({0}, {1})\n'.format(self.to_go(space), self.to_go(key)))
        if expected is None:
            self.f.write('\tif err.Status != client.NOTFOUND {\n')
            self.f.write('\t\tfmt.Printf("bad status: %d (should be NOTFOUND)\\n", err)\n\t}\n')
        else:
            self.f.write('\tif err.Status != client.SUCCESS {\n')
            self.f.write('\t\tfmt.Printf("bad status: %d (should be SUCCESS)\\n", err.Status)\n\t}\n')
            self.f.write('\tif !sloppyEqualAttributes(attrs, {0}) {{\n'.format(self.to_attrs(expected)))
            self.f.write('\t\tfmt.Printf("%s %s\\n", attrs, {0})\n'.format(self.to_go(expected)))
            self.f.write('\t\tpanic("objects not equal")\n\t}\n')

    def get_partial(self, space, key, attrs, expected):
        self.f.write('\tattrs, err = c.GetPartial({0}, {1}, {2})\n'.format(self.to_go(space), self.to_go(key), self.to_attrnames(attrs)))
        if expected is None:
            self.f.write('\tif err.Status != client.NOTFOUND {\n')
            self.f.write('\t\tfmt.Printf("bad status: %d (should be NOTFOUND)\\n", err)\n\t}\n')
        else:
            self.f.write('\tif err.Status != client.SUCCESS {\n')
            self.f.write('\t\tfmt.Printf("bad status: %d (should be SUCCESS)\\n", err.Status)\n\t}\n')
            self.f.write('\tif !sloppyEqualAttributes(attrs, {0}) {{\n'.format(self.to_attrs(expected)))
            self.f.write('\t\tfmt.Printf("%s %s\\n", attrs, {0})\n'.format(self.to_go(expected)))
            self.f.write('\t\tpanic("objects not equal")\n\t}\n')

    def put(self, space, key, value, expected):
        self.f.write('\terr = c.Put({0}, {1}, {2})\n'.format(self.to_go(space),
                                                             self.to_go(key),
                                                             self.to_attrs(value)))
        self.f.write('\tif err.Status != client.SUCCESS {\n')
        self.f.write('\t\tos.Exit(1)\n\t}\n')

    def cond_put(self, space, key, pred, value, expected):
        self.f.write('\terr = c.CondPut({0}, {1}, {2}, {3})\n'.format(self.to_go(space),
                                                                 self.to_go(key),
                                                                 self.to_preds(pred),
                                                                 self.to_attrs(value)))
        if expected:
            self.f.write('\tif err.Status != client.SUCCESS {\n')
        else:
            self.f.write('\tif err.Status != client.CMPFAIL {\n')
        self.f.write('\t\tos.Exit(1)\n\t}\n')

    def delete(self, space, key, expected):
        self.f.write('\terr = c.Del({0}, {1})\n'.format(self.to_go(space),
                                                        self.to_go(key)))
        self.f.write('\tif err.Status != client.SUCCESS {\n')
        self.f.write('\t\tos.Exit(1)\n\t}\n')

    def search(self, space, pred, expected):
        self.f.write('\tobjs, errs = c.Search({0}, {1})\n'.format(self.to_go(space), self.to_preds(pred)))

    def to_preds(self, preds):
        preds_as_strs = []
        for name, val in preds.iteritems():
            pred = 'EQUALS'
            if isinstance(val, Range):
                preds_as_strs.append('{{{0}, {1}, client.{2}}}'.format(self.to_go(name), self.to_go(val.x), 'GREATER_EQUAL'))
                preds_as_strs.append('{{{0}, {1}, client.{2}}}'.format(self.to_go(name), self.to_go(val.y), 'LESS_EQUAL'))
                continue
            if isinstance(val, LessEqual):
                val = val.x
                pred = 'LESS_EQUAL'
            elif isinstance(val, GreaterEqual):
                val = val.x
                pred = 'GREATER_EQUAL'
            elif isinstance(val, LessThan):
                val = val.x
                pred = 'LESS_THAN'
            elif isinstance(val, GreaterThan):
                val = val.x
                pred = 'GREATER_THAN'
            elif isinstance(val, Regex):
                val = val.x
                pred = 'REGEX'
            elif isinstance(val, LengthEquals):
                val = val.x
                pred = 'LENGTH_EQUALS'
            elif isinstance(val, LengthLessEqual):
                val = val.x
                pred = 'LENGTH_LESS_EQUAL'
            elif isinstance(val, LengthGreaterEqual):
                val = val.x
                pred = 'LENGTH_GREATER_EQUAL'
            preds_as_strs.append('{{{0}, {1}, client.{2}}}'.format(self.to_go(name), self.to_go(val), pred))
        return '[]client.Predicate{' + ', '.join(preds_as_strs) + '}'

    def to_attrs(self, attrs):
        s = 'client.Attributes{'
        s += ', '.join(['{0}: {1}'.format(self.to_go(k), self.to_go(v))
                        for k, v in sorted(attrs.items())])
        s += '}'
        return s

    def to_attrnames(self, attrnames):
        return '[]string{' + ', '.join([self.to_go(x) for x in attrnames]) + '}'

    def to_go(self, x):
        if x is True:
            return 'true'
        elif x is False:
            return 'false'
        elif x is None:
            return 'nil'
        elif isinstance(x, str):
            return double_quote(x)
        elif isinstance(x, long) or isinstance(x, int):
            return 'int64(%s)' % str(x)
        elif isinstance(x, float):
            return 'float64(%s)' % str(x)
        elif isinstance(x, list):
            s = 'client.List{'
            s += ', '.join(['{0}'.format(self.to_go(v)) for v in x])
            s += '}'
            return s
        elif isinstance(x, set):
            s = 'client.Set{'
            s += ', '.join(['{0}'.format(self.to_go(v)) for v in sorted(x)])
            s += '}'
            return s
        elif isinstance(x, dict):
            s = 'client.Map{'
            s += ', '.join(['{0}: {1}'.format(self.to_go(k), self.to_go(v))
                            for k, v in sorted(x.items())])
            s += '}'
            return s
        else:
            raise RuntimeError("Cannot convert {0!r} to go".format(x))


class RustGenerator(BindingGenerator):

    def __init__(self):
        self.f = None
        self.lib_dir = 'rust_hyperdex'
        subprocess.call(['git', 'clone', 'https://github.com/derekchiang/rust-hyperdex.git', self.lib_dir])
        subprocess.call(['cargo', 'build'], cwd=self.lib_dir)

    def test(self, name, space):
        assert self.f is None
        self.count = 0
        self.path = 'test/rust/{0}.rs'.format(name)
        precmd = 'rustc -g -L {1} -o "${{HYPERDEX_BUILDDIR}}"/test/rust/{0} "${{HYPERDEX_SRCDIR}}"/test/rust/{0}.rs'.format(name, self.lib_dir + '/target')
        cmd = '"${{HYPERDEX_BUILDDIR}}"/test/rust/{0}'.format(name)
        gen_shell('rust', name, cmd, space, precmd=precmd)
        self.f = open(self.path, 'w')
        self.f.write('''
#[macro_use] extern crate hyperdex;

use std::os;
use std::str::FromStr;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::mem::transmute;
use std::any::Any;

use hyperdex::*;
use hyperdex::HyperValue::*;
use hyperdex::HyperPredicateType::*;

macro_rules! sloppyCompare {
    ($a: ident, $b: ident) => (
        if $a.len() == 0 && $b.len() == 0 {
            true
        } else {
            if $a.get_type_id() != $b.get_type_id() {
                false
            } else {
                unsafe {
                    transmute($a) == $b
                }
            }
        }
    );
}

fn sloppyCompareHyper(a: &HyperObject, b: &HyperObject) -> bool {
    for (ak, av) in a.map.iter() {
        let bv = match b.map.get(ak) {
            None => return false,
            Some(x) => x,
        };

        let av = av.clone();
        let bv = bv.clone();

        // this is dumb but I do not see a better way... fortunately the following lines are generated
        // by a python script
        match (av, bv) {
            (HyperListString(x), HyperListString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListString(x), HyperListInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListString(x), HyperListFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListInt(x), HyperListString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListInt(x), HyperListInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListInt(x), HyperListFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListFloat(x), HyperListString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListFloat(x), HyperListInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListFloat(x), HyperListFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetString(x), HyperSetString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetString(x), HyperSetInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetString(x), HyperSetFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetInt(x), HyperSetString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetInt(x), HyperSetInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetInt(x), HyperSetFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetFloat(x), HyperSetString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetFloat(x), HyperSetInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetFloat(x), HyperSetFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (x, y) => if x != y { return false; },
        }
    }
    return true;
}

fn main() {
    let args = os::args();
    let mut client = Client::new(FromStr::from_str(format!("{}:{}", args[1], args[2]).as_slice()).unwrap()).unwrap();
''')

    def finish(self):
        self.f.write('}\n')
        self.f.flush()
        self.f.close()
        self.f = None

    def get(self, space, key, expected):
        if expected is None:
            self.f.write('''
                match client.get({0}, {1}) {{
                    Ok(obj) => {{
                         panic!("this object should not be found!");
                    }},
                    Err(err) => (),
                }}
            '''.format(self.to_rust(space), self.to_rust(key)))
        else:
            self.f.write('let expected = {0};'.format(self.to_rust(expected)))
            self.f.write('''
                match client.get({0}, {1}) {{
                    Ok(obj) => {{
                        if !sloppyCompareHyper(&obj, &expected) {{
                         panic!("expected: {{:?}}\nactual: {{:?}}", expected, obj);
                        }}
                    }},
                    Err(err) => panic!(err),
                }}
            '''.format(self.to_rust(space), self.to_rust(key)))

    def get_partial(self, space, key, attrs, expected):
        if expected is None:
            self.f.write('''
                match client.get_partial({0}, {1}, {2}) {{
                    Ok(obj) => {{
                         panic!("this object should not be found!");
                    }},
                    Err(err) => (),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_rust(attrs)))
        else:
            self.f.write('let expected = {0};'.format(self.to_rust(expected)))
            self.f.write('''
                match client.get_partial({0}, {1}, {2}) {{
                    Ok(obj) => {{
                        if !sloppyCompareHyper(&obj, &expected) {{
                         panic!("expected: {{:?}}\nactual: {{:?}}", expected, obj);
                        }}
                    }},
                    Err(err) => panic!(err),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_rust(attrs)))

    def put(self, space, key, value, expected):
        if expected:
            self.f.write('''
                match client.put({0}, {1}, {2}) {{
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_rust(value)))
        else:
            self.f.write('''
                match client.put({0}, {1}, {2}) {{
                    Ok(()) => panic!("this PUT operation should have failed"),
                    Err(err) => (),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_rust(value)))

    def cond_put(self, space, key, pred, value, expected):
        if expected:
            self.f.write('''
                match client.cond_put({0}, {1}, {2}, {3}) {{
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_preds(pred), self.to_rust(value)))
        else:
            self.f.write('''
                match client.cond_put({0}, {1}, {2}, {3}) {{
                    Ok(()) => panic!("this CONDPUT operation should have failed"),
                    Err(err) => (),
                }}
            '''.format(self.to_rust(space), self.to_rust(key), self.to_preds(pred), self.to_rust(value)))

    def delete(self, space, key, expected):
        self.f.write('''
            match client.del({0}, {1}) {{
                Ok(()) => (),
                Err(err) => panic!(err),
            }}
        '''.format(self.to_rust(space), self.to_rust(key)))

    def search(self, space, pred, expected):
        self.f.write('''
            let res = client.search({0}, {1});
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert_eq!(elems.len(), {2}.len());
        '''.format(self.to_rust(space), self.to_preds(pred), self.to_rust(expected)))

    def to_preds(self, preds):
        preds_as_strs = []
        for name, val in preds.iteritems():
            pred = 'EQUALS'
            if isinstance(val, Range):
                preds_as_strs.append('HyperPredicate::new({0}, {1}, {2})'.format(self.to_rust(name), 'GREATER_EQUAL', self.to_rust(val.x)))
                preds_as_strs.append('HyperPredicate::new({0}, {1}, {2})'.format(self.to_rust(name), 'LESS_EQUAL', self.to_rust(val.y)))
                continue
            elif isinstance(val, LessEqual):
                val = val.x
                pred = 'LESS_EQUAL'
            elif isinstance(val, GreaterEqual):
                val = val.x
                pred = 'GREATER_EQUAL'
            elif isinstance(val, LessThan):
                val = val.x
                pred = 'LESS_THAN'
            elif isinstance(val, GreaterThan):
                val = val.x
                pred = 'GREATER_THAN'
            elif isinstance(val, Regex):
                val = val.x
                pred = 'REGEX'
            elif isinstance(val, LengthEquals):
                val = val.x
                pred = 'LENGTH_EQUALS'
            elif isinstance(val, LengthLessEqual):
                val = val.x
                pred = 'LENGTH_LESS_EQUAL'
            elif isinstance(val, LengthGreaterEqual):
                val = val.x
                pred = 'LENGTH_GREATER_EQUAL'
            preds_as_strs.append('HyperPredicate::new({0}, {1}, {2})'.format(self.to_rust(name), pred, self.to_rust(val)))
        return 'vec!(' + ', '.join(preds_as_strs) + ')'

    def to_rust(self, x, inner=False):
        if x is True:
            return 'true'
        elif x is False:
            return 'false'
        elif x is None:
            return 'None'
        elif isinstance(x, str):
            return 'r'+double_quote(x)
        elif isinstance(x, long) or isinstance(x, int):
            return '%s as i64' % str(x)
        elif isinstance(x, float):
            if inner:
                return 'F64(%s as f64)' % str(x)
            else:
                return '%s as f64' % str(x)
        elif isinstance(x, list):
            if len(x):
                s = 'vec!('
                s += ', '.join(['{0}'.format(self.to_rust(v)) for v in x])
                s += ')'
                return s
            else:
                return 'Vec::<i64>::new()'
        elif isinstance(x, set):
            if len(x):
                s = '{\n'
                s += 'let mut s = BTreeSet::new();\n'
                for v in x:
                    s += 's.insert({0});\n'.format(self.to_rust(v, inner=True))
                s += 's\n}\n'
                return s
            else:
                return 'BTreeSet::<Vec<u8>>::new()'
        elif isinstance(x, dict):
            if inner:
                if len(x):
                    s = '{\n'
                    s += 'let mut m = HashMap::new();\n'
                    for k, v in x.items():
                        s += 'm.insert({}, {});\n'.format(self.to_rust(k, inner=True), self.to_rust(v))
                    s += 'm\n}\n'
                else:
                    s = 'HashMap::<Vec<u8>, Vec<u8>>::new()'
                return s
            elif len(x):
                s = 'NewHyperObject!('
                s += ', '.join(['{0}, {1}'.format(self.to_rust(k), self.to_rust(v, inner=isinstance(v, dict)))
                                for k, v in sorted(x.items())])
                s += ',)'
                return s
            else:
                return "HyperObject::new()"
        else:
            raise RuntimeError("Cannot convert {0!r} to rust".format(x))


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

    def get_partial(self, space, key, attrs, expected):
        for x in self.generators:
            x.get_partial(space, key, attrs, expected)

    def put(self, space, key, value, expected):
        for x in self.generators:
            x.put(space, key, value, expected)

    def cond_put(self, space, key, pred, value, expected):
        for x in self.generators:
            x.cond_put(space, key, pred, value, expected)

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

t.test('CondPut', 'space kv key k attribute v')
t.get('kv', 'k', None)
t.put('kv', 'k', {'v': 'v1'}, True)
t.get('kv', 'k', {'v': 'v1'})
t.cond_put('kv', 'k', {'v': 'v2'}, {'v': 'v3'}, False)
t.get('kv', 'k', {'v': 'v1'})
t.cond_put('kv', 'k', {'v': 'v1'}, {'v': 'v3'}, True)
t.get('kv', 'k', {'v': 'v3'})
t.finish()

t.test('MultiAttribute', 'space kv key k attributes v1, v2')
t.get('kv', 'k', None)
t.put('kv', 'k', {'v1': 'ABC'}, True)
t.get('kv', 'k', {'v1': 'ABC', 'v2': ''})
t.put('kv', 'k', {'v2': '123'}, True)
t.get('kv', 'k', {'v1': 'ABC', 'v2': '123'})
t.get_partial('kv', 'k', ['v1'], {'v1': 'ABC'})
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
