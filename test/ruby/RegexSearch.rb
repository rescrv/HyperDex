#!/usr/bin/env ruby
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
assert { c.put("kv", "foo/foo/foo", {}) == true }
assert { c.put("kv", "foo/foo/bar", {}) == true }
assert { c.put("kv", "foo/foo/baz", {}) == true }
assert { c.put("kv", "foo/bar/foo", {}) == true }
assert { c.put("kv", "foo/bar/bar", {}) == true }
assert { c.put("kv", "foo/bar/baz", {}) == true }
assert { c.put("kv", "foo/baz/foo", {}) == true }
assert { c.put("kv", "foo/baz/bar", {}) == true }
assert { c.put("kv", "foo/baz/baz", {}) == true }
assert { c.put("kv", "bar/foo/foo", {}) == true }
assert { c.put("kv", "bar/foo/bar", {}) == true }
assert { c.put("kv", "bar/foo/baz", {}) == true }
assert { c.put("kv", "bar/bar/foo", {}) == true }
assert { c.put("kv", "bar/bar/bar", {}) == true }
assert { c.put("kv", "bar/bar/baz", {}) == true }
assert { c.put("kv", "bar/baz/foo", {}) == true }
assert { c.put("kv", "bar/baz/bar", {}) == true }
assert { c.put("kv", "bar/baz/baz", {}) == true }
assert { c.put("kv", "baz/foo/foo", {}) == true }
assert { c.put("kv", "baz/foo/bar", {}) == true }
assert { c.put("kv", "baz/foo/baz", {}) == true }
assert { c.put("kv", "baz/bar/foo", {}) == true }
assert { c.put("kv", "baz/bar/bar", {}) == true }
assert { c.put("kv", "baz/bar/baz", {}) == true }
assert { c.put("kv", "baz/baz/foo", {}) == true }
assert { c.put("kv", "baz/baz/bar", {}) == true }
assert { c.put("kv", "baz/baz/baz", {}) == true }
X0 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::Regex.new "^foo")}))
Y0 = to_objectset([{:k => "foo/foo/foo"}, {:k => "foo/foo/bar"}, {:k => "foo/foo/baz"}, {:k => "foo/bar/foo"}, {:k => "foo/bar/bar"}, {:k => "foo/bar/baz"}, {:k => "foo/baz/foo"}, {:k => "foo/baz/bar"}, {:k => "foo/baz/baz"}])
assert { X0.subset?Y0 and Y0.subset?X0 }
X1 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::Regex.new "foo$")}))
Y1 = to_objectset([{:k => "foo/foo/foo"}, {:k => "foo/bar/foo"}, {:k => "foo/baz/foo"}, {:k => "bar/foo/foo"}, {:k => "bar/bar/foo"}, {:k => "bar/baz/foo"}, {:k => "baz/foo/foo"}, {:k => "baz/bar/foo"}, {:k => "baz/baz/foo"}])
assert { X1.subset?Y1 and Y1.subset?X1 }
X2 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::Regex.new "^b.*/foo/.*$")}))
Y2 = to_objectset([{:k => "bar/foo/foo"}, {:k => "bar/foo/bar"}, {:k => "bar/foo/baz"}, {:k => "baz/foo/foo"}, {:k => "baz/foo/bar"}, {:k => "baz/foo/baz"}])
assert { X2.subset?Y2 and Y2.subset?X2 }
