import hyperdex.client

c = hyperdex.client.Client('127.0.0.1', 1982)
for i in range(100):
    c.put('phonebook', 'jakek' + i, {'first': 'J', 'last': 'K', 'phone': 7778675903})
