import hyperdex.client

c = hyperdex.client.Client("127.0.0.1", 1982)
p = c.async_put("kv", "some key", {"v": "Hello World!"})
p1 = c.loop()
print 'put objects are the same:', p is p1
p.wait()
print 'put "Hello World!"'
g = c.async_get("kv", "some key")
g1 = c.loop()
print 'get objects are the same:', g is g1
print 'got:', g.wait()
