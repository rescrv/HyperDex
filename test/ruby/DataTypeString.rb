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
assert { c.put("kv", "k", {}) == true }
assert { c.get("kv", "k") == {:v => ""} }
assert { c.put("kv", "k", {"v" => "xxx"}) == true }
assert { c.get("kv", "k") == {:v => "xxx"} }
assert { c.put("kv", "k", {"v" => "\xde\xad\xbe\xef"}) == true }
assert { c.get("kv", "k") == {:v => "\xde\xad\xbe\xef"} }
