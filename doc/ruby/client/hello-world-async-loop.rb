require "hyperdex"
c = HyperDex::Client::Client.new("127.0.0.1", 1982)
p = c.async_put(:kv, "some key", {:v => "Hello World!"})
p1 = c.loop()
puts 'put objects are the same:', (p.equal? p1)
p.wait()
puts 'put "Hello World!"'
g = c.async_get(:kv, "some key")
g1 = c.loop()
puts 'get objects are the same:', (g.equal? g1)
puts 'got:', g.wait()
