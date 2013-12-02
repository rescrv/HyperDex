#!/usr/bin/env ruby
require 'hyperdex'

def assert &block
    raise RuntimeError unless yield
end

c = HyperDex::Client::Client.new(ARGV[0], ARGV[1].to_i)
assert { c.get("kv", "k") == nil }
assert { c.put("kv", "k", {"v" => "v1"}) == true }
assert { c.get("kv", "k") == {:v => "v1"} }
assert { c.put("kv", "k", {"v" => "v2"}) == true }
assert { c.get("kv", "k") == {:v => "v2"} }
assert { c.put("kv", "k", {"v" => "v3"}) == true }
assert { c.get("kv", "k") == {:v => "v3"} }
assert { c.del("kv", "k") == true }
assert { c.get("kv", "k") == nil }
