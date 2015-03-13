import hyperdex.admin
import hyperdex.client
import time

a = hyperdex.admin.Admin('127.0.0.1', 1982)

a.add_space('''
    space phonebook
    key username
    attributes first, last, int phone
    subspace first, last, phone
    create 8 partitions
    tolerate 2 failures
    ''')

c = hyperdex.client.Client('127.0.0.1', 1982)
c.put('phonebook', 'jsmith1', {'first': 'John', 'last': 'Smith', 'phone': 2228675309})
c.put('phonebook', 'jtk54', {'first': 'Jacob', 'last': 'Kiefer', 'phone': 5556079876})

for i in range(66):
    c.put('phonebook', 'kek' + str(i), {'first': 'Kook', 'last': 'Kookerson', 'phone': i})
