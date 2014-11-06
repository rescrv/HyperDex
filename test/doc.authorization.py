# File generated from python blocks in "doc/authorization.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space accounts
... key int account
... attributes
...    string name,
...    int balance
... with authorization
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> SECRET = 'this is the password for the account'
>>> c.put('accounts', 3735928559, {'name': 'John Smith', 'balance': 10, '__secret': SECRET})
True
>>> c.get('accounts', 3735928559)
Traceback (most recent call last):
HyperDexClientException: HyperDexClientException: server ... denied the request because it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> import macaroons
>>> M = macaroons.create('account 3735928559', SECRET, '')
>>> token = M.serialize()
>>> c.get('accounts', 3735928559, auth=[token])
{'name': 'John Smith', 'balance': 10}
>>> c.atomic_add('accounts', 3735928559, {'balance': 5}, auth=[token])
True
>>> c.get('accounts', 3735928559, auth=[token])
{'name': 'John Smith', 'balance': 15}
>>> M = macaroons.create('account 3735928559', SECRET, '')
>>> M = M.add_first_party_caveat('op = read')
>>> token = M.serialize()
>>> c.get('accounts', 3735928559, auth=[token])
{'name': 'John Smith', 'balance': 15}
>>> c.atomic_add('accounts', 3735928559, {'balance': 5}, auth=[token])
Traceback (most recent call last):
HyperDexClientException: HyperDexClientException: server ... denied the request because it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> def add_caveat_rpc(key):
...     return 'identifier associated with key' # usually a random number
...
>>> key = 'a unique key for this caveat; usually a random string'
>>> # then "call" the RPC on the server
>>> ident = add_caveat_rpc(key)
>>> M = M.add_third_party_caveat('http://audit.service/', key, ident)
>>> token = M.serialize()
>>> c.get('accounts', 3735928559, auth=[token])
Traceback (most recent call last):
HyperDexClientException: HyperDexClientException: server ... denied the request because it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> def generate_discharge_rpc(ident):
...     'return the key passed into add_caveat_rpc'
...     D = macaroons.create('', 'a unique key for this caveat; usually a random string', ident)
...     return D
...
>>> D = generate_discharge_rpc(ident)
>>> DP = M.prepare_for_request(D)
>>> c.get('accounts', 3735928559, auth=[token, DP.serialize()])
{'name': 'John Smith', 'balance': 15}
>>> import time
>>> def generate_discharge_rpc(ident):
...     'return the key passed into add_caveat_rpc'
...     D = macaroons.create('', 'a unique key for this caveat; usually a random string', ident)
...     expiration = int(time.time()) + 10
...     D = D.add_first_party_caveat('time < %d' % expiration)
...     return D
...
>>> D = generate_discharge_rpc(ident)
>>> DP = M.prepare_for_request(D)
>>> c.get('accounts', 3735928559, auth=[token, DP.serialize()])
{'name': 'John Smith', 'balance': 15}
>>> time.sleep(15)
>>> c.get('accounts', 3735928559, auth=[token, DP.serialize()])
Traceback (most recent call last):
HyperDexClientException: HyperDexClientException: server ... denied the request because it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
