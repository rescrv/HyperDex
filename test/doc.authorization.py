# File generated from python blocks in "doc/authorization.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space accounts
... key account
... attributes
...    string name,
...    int balance
... with authorization
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> SECRET = 'super secret password'
>>> account = 'account number of john smith'
>>> c.put('accounts', account, {'name': 'John Smith', 'balance': 10}, secret=SECRET)
True
>>> c.get('accounts', account)
Traceback (most recent call last):
HyperDexClientException: ... it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> import macaroons
>>> M = macaroons.create('account number', SECRET, '')
>>> token = M.serialize()
>>> c.get('accounts', account, auth=[token])
{'name': 'John Smith', 'balance': 10}
>>> c.atomic_add('accounts', account, {'balance': 5}, auth=[token])
True
>>> c.get('accounts', account, auth=[token])
{'name': 'John Smith', 'balance': 15}
>>> M = macaroons.create('account number', SECRET, '')
>>> M = M.add_first_party_caveat('op = read')
>>> token = M.serialize()
>>> c.get('accounts', account, auth=[token])
{'name': 'John Smith', 'balance': 15}
>>> c.atomic_add('accounts', account, {'balance': 5}, auth=[token])
Traceback (most recent call last):
HyperDexClientException: ... it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> M = macaroons.create('account number', SECRET, '')
>>> M = M.add_first_party_caveat('op = read')
>>> import time
>>> expiration = int(time.time()) + 30
>>> M = M.add_first_party_caveat('time < %d' % expiration)
>>> token = M.serialize()
>>> c.get('accounts', account, auth=[token])
{'name': 'John Smith', 'balance': 15}
>>> time.sleep(31)
>>> c.get('accounts', account, auth=[token])
Traceback (most recent call last):
HyperDexClientException: ... it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> keys = {}
>>> def add_caveat_rpc(key, user, password):
...     r = 'a random string' # your implementation should gen a rand string
...     keys[r] = (key, user, password)
...     return r
...
>>> key = 'a unique key for this caveat; should be random in the crypto sense'
>>> ident = add_caveat_rpc(key, 'jane.doe@example.org', "jane's password")
>>> M = macaroons.create('account number', SECRET, '')
>>> M = M.add_first_party_caveat('op = read')
>>> M = M.add_third_party_caveat('http://auth.service/', key, ident)
>>> token = M.serialize()
>>> c.get('accounts', account, auth=[token])
Traceback (most recent call last):
HyperDexClientException: ... it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
>>> def generate_discharge_rpc(ident, user, password):
...     if ident not in keys:
...         # unknown caveat
...         print 'fuck1'
...         return None
...     key, exp_user, exp_password = keys[ident]
...     if exp_user != user or exp_password != password:
...         # invalid user/password pair
...         print 'fuck2', repr(exp_user), repr(user), repr(exp_password), repr(password)
...         return None
...     D = macaroons.create('', key, ident)
...     expiration = int(time.time()) + 30
...     D = D.add_first_party_caveat('time < %d' % expiration)
...     return D
...
>>> D = generate_discharge_rpc(ident, 'jane.doe@example.org', "jane's password")
>>> discharge_M = M.prepare_for_request(D)
>>> discharge_token = discharge_M.serialize()
>>> c.get('accounts', account, auth=[token, discharge_token])
{'name': 'John Smith', 'balance': 15}
>>> time.sleep(31)
>>> c.get('accounts', account, auth=[token, discharge_token])
Traceback (most recent call last):
HyperDexClientException: ... it is unauthorized [HYPERDEX_CLIENT_UNAUTHORIZED]
