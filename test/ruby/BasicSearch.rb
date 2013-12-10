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
X0 = collapse_iterator(c.search("kv", {"v" => "v1"}))
Y0 = to_objectset((Set.new []))
assert { X0.subset?Y0 and Y0.subset?X0 }
assert { c.put("kv", "k1", {"v" => "v1"}) == true }
X1 = collapse_iterator(c.search("kv", {"v" => "v1"}))
Y1 = to_objectset([{:k => "k1", :v => "v1"}])
assert { X1.subset?Y1 and Y1.subset?X1 }
assert { c.put("kv", "k2", {"v" => "v1"}) == true }
X2 = collapse_iterator(c.search("kv", {"v" => "v1"}))
Y2 = to_objectset([{:k => "k1", :v => "v1"}, {:k => "k2", :v => "v1"}])
assert { X2.subset?Y2 and Y2.subset?X2 }
