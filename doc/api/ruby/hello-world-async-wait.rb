require "hyperdex"
c = HyperDex::Client::Client.new("127.0.0.1", 1982)
p = c.async_put(:kv, "some key", {:v => "Hello World!"})
p.wait()
puts 'put "Hello World!"'
g = c.async_get(:kv, "some key")
puts 'got:', g.wait()
