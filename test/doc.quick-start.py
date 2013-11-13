# File generated from python blocks in "doc/quick-start.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space phonebook
... key username
... attributes first, last, int phone
... subspace first, last, phone
... create 8 partitions
... tolerate 2 failures
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> c.put('phonebook', 'jsmith1', {'first': 'John', 'last': 'Smith',
...                                'phone': 6075551024})
True
>>> c.get('phonebook', 'jsmith1')
{'first': 'John', 'last': 'Smith', 'phone': 6075551024}
>>> [x for x in c.search('phonebook', {'first': 'John'})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook', {'last': 'Smith'})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook', {'phone': 6075551024})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook',
...  {'first': 'John', 'last': 'Smith', 'phone': 6075551024})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> c.put('phonebook', 'jd', {'first': 'John', 'last': 'Doe', 'phone': 6075557878})
True
>>> [x for x in c.search('phonebook',
...  {'first': 'John', 'last': 'Smith', 'phone': 6075551024})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook', {'first': 'John'})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'},
 {'first': 'John',
  'last': 'Doe',
  'phone': 6075557878,
  'username': 'jd'}]
>>> [x for x in c.search('phonebook', {'last': 'Smith'})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook', {'last': 'Doe'})]
[{'first': 'John',
  'last': 'Doe',
  'phone': 6075557878,
  'username': 'jd'}]
>>> c.delete('phonebook', 'jd')
True
>>> [x for x in c.search('phonebook', {'first': 'John'})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075551024,
  'username': 'jsmith1'}]
>>> c.put('phonebook', 'jsmith1', {'phone': 6075552048})
True
>>> c.get('phonebook', 'jsmith1')
{'first': 'John',
  'last': 'Smith',
  'phone': 6075552048}
>>> c.put('phonebook', 'jsmith2',
...          {'first': 'John', 'last': 'Smith', 'phone': 5855552048})
True
>>> c.get('phonebook', 'jsmith2')
{'first': 'John',
  'last': 'Smith',
  'phone': 5855552048}
   >>> [x for x in c.search('phonebook',
   ...  {'last': 'Smith', 'phone': (6070000000, 6080000000)})]
   [{'first': 'John',
     'last': 'Smith',
     'phone': 6075552048,
     'username': 'jsmith1'}]
>>> [x for x in c.search('phonebook',
...  {'first': ('Jack', 'Joseph')})]
[{'first': 'John',
  'last': 'Smith',
  'phone': 6075552048,
  'username': 'jsmith1'},
 {'first': 'John',
  'last': 'Smith',
  'phone': 5855552048,
  'username': 'jsmith2'}]
>>> a.rm_space('phonebook')
True
