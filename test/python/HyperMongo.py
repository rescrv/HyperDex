from testlib import *
from hyperdex.mongo import *
import sys

# This tests the mongo-compatibility layer
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))

assertTrue(db.test.insert({'_id' : 'k1', 'a' : 'b', 'c' : 1}))
db.test.update({'_id' : 'k1'}, {'$inc' : {'c' : 1}})

res = db.test.find_one({'_id' : 'k1'})
assertEquals(res, {'_id' : 'k1', 'a' : 'b', 'c' : 2})

db.test.update({'a' : 'b'}, {'$set' : {'d' : 1}})
res = db.test.find_one({'_id' : 'k1'})
assertEquals(res, {'_id' : 'k1', 'a' : 'b', 'c' : 2, 'd' : 1})

assertTrue(db.test.insert({'_id' : 'k2', 'a' : ['b', 'c'] }))
db.test.update({'_id' : 'k2'}, {'$push' : {'a' : 1}})
res = db.test.find_one({'_id' : 'k2'})
assertEquals(res, {'_id' : 'k2', 'a' : ['b', 'c' , 1] })
db.test.update({'_id' : 'k2'}, {'$rename' : {'a' : 'x'}})
res = db.test.find_one({'_id' : 'k2'})
assertEquals(res, {'_id' : 'k2', 'x' : ['b', 'c' , 1] })
