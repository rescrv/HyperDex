import hyperdex.admin

a = hyperdex.admin.Admin('127.0.0.1', 1982)
b = hyperdex.admin.Admin('127.0.0.1', 1984)

a.add_space('''
    space phonebook
    key username
    attributes first, last, int phone
    subspace first, last, phone
    create 8 partitions
    tolerate 2 failures
    ''')

b.add_space('''
    space phonebook
    key username
    attributes first, last, int phone
    subspace first, last, phone
    create 8 partitions
    tolerate 2 failures
    ''')
