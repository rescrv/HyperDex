#!/usr/bin/env ruby
require 'hyperdex'

def assert &block
    raise RuntimeError unless yield
end

c = HyperDex::Client::Client.new(ARGV[0], ARGV[1].to_i)
assert { c.put("kv", "k", {}) == true }
assert { c.get("kv", "k") == {:v => {}} }
assert { c.put("kv", "k", {"v" => {0.25 => 2, 1.0 => 3, 3.14 => 1}}) == true }
assert { c.get("kv", "k") == {:v => {0.25 => 2, 1.0 => 3, 3.14 => 1}} }
assert { c.put("kv", "k", {"v" => {}}) == true }
assert { c.get("kv", "k") == {:v => {}} }
