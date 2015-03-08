# File generated from python blocks in "doc/mongo.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import pymongo # the old import
>>> client = pymongo.MongoClient('localhost', 27017)
>>> import hyperdex.mongo as pymongo
>>> client = pymongo.HyperDatabase('localhost', 1982)
>>> # Here's the code you don't change
>>> collection = client.db.profiles
>>> collection.insert({'_id': 'jane', 'name': 'Jane Doe', 'sessioncount': 1})
'jane'
>>> collection.update({'name': 'Jane Doe'}, {'$inc': {'sessioncount': 1}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 0}
>>> collection.insert({'_id': 'andy1', 'name': 'Andy', 'friends': []})
'andy1'
>>> collection.save({'_id': 'andy1', 'name': 'Andrew', 'age': 17, 'friends': []})
'andy1'
>>> list(collection.find({'name': 'Andrew'}))
[{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andrew'}]
>>> collection.find_one({'name': 'Andrew'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andrew'}
>>> collection.update({'name': 'Andrew'}, {'$set': {'is_andrew': True}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andrew'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andrew', 'is_andrew': 1}
>>> collection.update({'_id': 'andy1'}, {'$set': {'name': 'Andy'}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andy', 'is_andrew': 1}
>>> collection.update({'name': 'Andy'}, {'$inc': {'trinkets': 10}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andy',
 u'is_andrew': 1, 'trinkets': 10}
>>> collection.update({'name': 'Andy'}, {'$mul': {'trinkets': 1.01}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andy',
 u'is_andrew': 1, 'trinkets': 10.1}
>>> collection.update({'name': 'Andy'}, {'$div': {'trinkets': 2.0}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [], u'name': u'Andy',
 u'is_andrew': 1, 'trinkets': 5.05}
>>> collection.update({'name': 'Andy'}, {'$push': {'friends': 'Buzz'}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [u'Buzz'], u'name': u'Andy',
 u'is_andrew': 1, 'trinkets': 5.05}
>>> collection.update({'name': 'Andy'}, {'$rename': {'is_andrew': 'is_andy'}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [u'Buzz'], u'name': u'Andy',
 u'is_andy': 1, 'trinkets': 5.05}
>>> collection.update({'name': 'Andy'}, {'$bit': {'perms': {'and': 488}}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [u'Buzz'], u'name': u'Andy',
 u'is_andy': 1, 'trinkets': 5.05, 'perms': 0}

>>> collection.update({'name': 'Andy'}, {'$bit': {'perms': {'or': 055}}})
{'updatedExisting': True, u'nModified': 1, u'ok': 1, u'n': 1}
>>> collection.find_one({'name': 'Andy'})
{u'age': 17, u'_id': u'andy1', u'friends': [u'Buzz'], u'name': u'Andy',
 u'is_andy': 1, 'trinkets': 5.05, 'perms': 45}
>>> list(collection.find({'name': {'$eq': 'Andy'}}))
[{u'trinkets': 5.05, u'name': u'Andy', u'is_andy': 1, u'age': 17,
  u'perms': 45, u'_id': 'andy1', u'friends': [u'Buzz']}]
>>> # Find people who are underage
>>> list(collection.find({'age': {'$lt': 18}}))
[{u'trinkets': 5.05, u'name': u'Andy', u'is_andy': 1, u'age': 17,
  u'perms': 45, u'_id': 'andy1', u'friends': [u'Buzz']}]
>>> # Find people who qualify for AARP
>>> list(collection.find({'age': {'$gt': 65}}))
[]
>>> # Find people in the [25-35] demographic
>>> list(collection.find({'age': {'$gte': 25, '$lte': 35}}))
[]
