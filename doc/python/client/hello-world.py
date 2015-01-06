import hyperdex.client

c = hyperdex.client.Client("127.0.0.1", 1982)
c.put("kv", "some key", {"v": "Hello World!"})
print 'put "Hello World!"'
print 'got:', c.get("kv", "some key")
