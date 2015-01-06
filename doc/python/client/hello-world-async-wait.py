import hyperdex.client

c = hyperdex.client.Client("127.0.0.1", 1982)
p = c.async_put("kv", "some key", {"v": "Hello World!"})
p.wait()
print 'put "Hello World!"'
g = c.async_get("kv", "some key")
print 'got:', g.wait()
