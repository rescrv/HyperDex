# File generated from python blocks in "doc/data-types.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space profiles
... key username
... attributes
...    string first,
...    string last,
...    int profile_views,
...    list(string) pending_requests,
...    set(string) hobbies,
...    map(string, string) unread_messages,
...    map(string, int) upvotes
... subspace first, last
... subspace profile_views
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> c.put('profiles', 'jsmith1', {'first': 'John', 'last': 'Smith'})
True
>>> c.get('profiles', 'jsmith1')
{'first': 'John', 'last': 'Smith',
 'profile_views': 0,
 'pending_requests': [],
 'hobbies': set([]),
 'unread_messages': {},
 'upvotes': {}}
>>> c.put('profiles', 'jsmith1', {'first': u'Jóhn'.encode('utf8')})
True
>>> c.get('profiles', 'jsmith1')['first']
'J\xc3\x83\xc2\xb3hn'
>>> c.get('profiles', 'jsmith1')['first'].decode('utf8')
u'Jóhn'
>>> c.put('profiles', 'jsmith1', {'first': 'John', 'last': 'Smith'})
True
>>> c.put('profiles', 'jsmith1', {'first': 'John', 'last': 'Smith'})
True
>>> c.get('profiles', 'jsmith1')['first']
'John'
>>> c.atomic_add('profiles', 'jsmith1', {'profile_views': 1})
True
>>> c.get('profiles', 'jsmith1')
{'first': 'John', 'last': 'Smith',
 'profile_views': 1,
 'pending_requests': [],
 'hobbies': set([]),
 'unread_messages': {},
 'upvotes': {}}
>>> c.list_rpush('profiles', 'jsmith1', {'pending_requests': 'bjones1'})
True
>>> c.get('profiles', 'jsmith1')['pending_requests']
['bjones1']
>>> hobbies = set(['hockey', 'basket weaving', 'hacking',
...                'air guitar rocking'])
>>> c.set_union('profiles', 'jsmith1', {'hobbies': hobbies})
True
>>> c.set_add('profiles', 'jsmith1', {'hobbies': 'gaming'})
True
>>> c.get('profiles', 'jsmith1')['hobbies']
set(['hacking', 'air guitar rocking', 'hockey', 'gaming', 'basket weaving'])
>>> c.set_intersect('profiles', 'jsmith1',
...                 {'hobbies': set(['hacking', 'programming'])})
True
>>> c.get('profiles', 'jsmith1')['hobbies']
set(['hacking'])
>>> c.map_add('profiles', 'jsmith1',
...           {'unread_messages' : {'bjones1' : 'Hi John'}})
True
>>> c.map_add('profiles', 'jsmith1',
...           {'unread_messages' : {'timmy' : 'Lunch?'}})
True
>>> c.get('profiles', 'jsmith1')['unread_messages']
{'timmy': 'Lunch?', 'bjones1': 'Hi John'}
>>> c.map_string_append('profiles', 'jsmith1',
...                      {'unread_messages' : {'bjones1' : ' | Want to hang out?'}})
True
>>> c.get('profiles', 'jsmith1')['unread_messages']
{'timmy': 'Lunch?', 'bjones1': 'Hi John | Want to hang out?'}
>>> url1 = "http://url1.com"
>>> url2 = "http://url2.com"
>>> c.map_add('profiles', 'jsmith1',
...           {'upvotes' : {url1 : 1, url2: 1}})
True
>>> c.map_atomic_add('profiles', 'jsmith1', {'upvotes' : {url1: 1}})
True
>>> c.map_atomic_add('profiles', 'jsmith1', {'upvotes' : {url1: 1}})
True
>>> c.map_atomic_add('profiles', 'jsmith1', {'upvotes' : {url1: -1, url2: -1}})
True
>>> c.get('profiles', 'jsmith1')['upvotes']
{'http://url1.com': 2, 'http://url2.com': 0}
