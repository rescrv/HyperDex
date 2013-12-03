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
assert { c.put("kv", -2, {"v" => -2}) == true }
assert { c.put("kv", -1, {"v" => -1}) == true }
assert { c.put("kv", 0, {"v" => 0}) == true }
assert { c.put("kv", 1, {"v" => 1}) == true }
assert { c.put("kv", 2, {"v" => 2}) == true }
X0 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LessEqual.new 0)}))
Y0 = to_objectset([{:k => -2, :v => -2}, {:k => -1, :v => -1}, {:k => 0, :v => 0}])
assert { X0.subset?Y0 and Y0.subset?X0 }
X1 = collapse_iterator(c.search("kv", {"v" => (HyperDex::Client::LessEqual.new 0)}))
Y1 = to_objectset([{:k => -2, :v => -2}, {:k => -1, :v => -1}, {:k => 0, :v => 0}])
assert { X1.subset?Y1 and Y1.subset?X1 }
X2 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::GreaterEqual.new 0)}))
Y2 = to_objectset([{:k => 0, :v => 0}, {:k => 1, :v => 1}, {:k => 2, :v => 2}])
assert { X2.subset?Y2 and Y2.subset?X2 }
X3 = collapse_iterator(c.search("kv", {"v" => (HyperDex::Client::GreaterEqual.new 0)}))
Y3 = to_objectset([{:k => 0, :v => 0}, {:k => 1, :v => 1}, {:k => 2, :v => 2}])
assert { X3.subset?Y3 and Y3.subset?X3 }
X4 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::Range.new -1, 1)}))
Y4 = to_objectset([{:k => -1, :v => -1}, {:k => 0, :v => 0}, {:k => 1, :v => 1}])
assert { X4.subset?Y4 and Y4.subset?X4 }
X5 = collapse_iterator(c.search("kv", {"v" => (HyperDex::Client::Range.new -1, 1)}))
Y5 = to_objectset([{:k => -1, :v => -1}, {:k => 0, :v => 0}, {:k => 1, :v => 1}])
assert { X5.subset?Y5 and Y5.subset?X5 }
