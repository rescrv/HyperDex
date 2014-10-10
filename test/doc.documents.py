# File generated from python blocks in "doc/documents.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space profiles
... key username
... attributes
...    document profile
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> Document = hyperdex.client.Document
>>> c.put('profiles', 'jsmith1', {'profile': Document({"name": "John Smith"})})
True
>>> c.put('profiles', 'jd', {'profile': Document({
...     "name": "John Doe",
...     "www": "http://example.org",
...     "email": "doe@example.org",
...     "friends": ["John Smith"]
... })})
True
>>> print [x for x in c.search('profiles', {'profile.name': 'John Doe'})]
[{'username': 'jd', 'profile': Document({"www": "http://example.org", "friends":
["John Smith"], "name": "John Doe", "email": "doe@example.org"})}]
>>> print [x for x in c.search('profiles', {'profile.name': hyperdex.client.Regex('John')})]
[{'username': 'jsmith1', 'profile': Document({"name": "John Smith"})}, {'username': 'jd', 'profile': Document({"www": "http://example.org", "friends": ["John Smith"], "name": "John Doe", "email": "doe@example.org"})}]
