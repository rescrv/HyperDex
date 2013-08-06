require 'hyperdex'

c = HyperDex::Client::Client.new('127.0.0.1', 1982)
c.put(:kv, 'some key', {:v => 'Hello World'})
print c.get(:kv, 'some key')
