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
assert { c.put("kv", "A", {}) == true }
assert { c.put("kv", "AB", {}) == true }
assert { c.put("kv", "ABC", {}) == true }
assert { c.put("kv", "ABCD", {}) == true }
assert { c.put("kv", "ABCDE", {}) == true }
X0 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthEquals.new 1)}))
Y0 = to_objectset([{:k => "A"}])
assert { X0.subset?Y0 and Y0.subset?X0 }
X1 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthEquals.new 2)}))
Y1 = to_objectset([{:k => "AB"}])
assert { X1.subset?Y1 and Y1.subset?X1 }
X2 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthEquals.new 3)}))
Y2 = to_objectset([{:k => "ABC"}])
assert { X2.subset?Y2 and Y2.subset?X2 }
X3 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthEquals.new 4)}))
Y3 = to_objectset([{:k => "ABCD"}])
assert { X3.subset?Y3 and Y3.subset?X3 }
X4 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthEquals.new 5)}))
Y4 = to_objectset([{:k => "ABCDE"}])
assert { X4.subset?Y4 and Y4.subset?X4 }
X5 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthLessEqual.new 3)}))
Y5 = to_objectset([{:k => "A"}, {:k => "AB"}, {:k => "ABC"}])
assert { X5.subset?Y5 and Y5.subset?X5 }
X6 = collapse_iterator(c.search("kv", {"k" => (HyperDex::Client::LengthGreaterEqual.new 3)}))
Y6 = to_objectset([{:k => "ABC"}, {:k => "ABCD"}, {:k => "ABCDE"}])
assert { X6.subset?Y6 and Y6.subset?X6 }
