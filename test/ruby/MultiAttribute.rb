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
assert { c.get("kv", "k") == nil }
assert { c.put("kv", "k", {"v1" => "ABC"}) == true }
assert { c.get("kv", "k") == {:v1 => "ABC", :v2 => ""} }
assert { c.put("kv", "k", {"v2" => "123"}) == true }
assert { c.get("kv", "k") == {:v1 => "ABC", :v2 => "123"} }
assert { c.get_partial("kv", "k", ["v1"]) == {:v1 => "ABC"} }
