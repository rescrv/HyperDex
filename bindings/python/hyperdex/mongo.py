from hyperdex.client import *
import hyperdex.admin

class HyperIterator:
    def __init__(self, innerIter_):
        self.innerIter = innerIter_

    def hasNext(self):
        return self.innerIter.hasNext()

    def __iter__(self):
        return self.innerIter

    def next(self):
        if not self.hasNext():
            raise RuntimeError('Cannot get next element. There is none!')

        return self.innerIter.next()['v'].doc()

class HyperSpace:
    Document = hyperdex.client.Document

    def __init__(self, client, admin, name):
        self.client = client
        self.admin = admin
        self.name = name
        self.exists = False

    def init(self):
        if self.exists:
            return

        # Might have been created by a different process
        self.exists = (self.name in self.admin.list_spaces())
        if self.exists:
            return

        self.admin.add_space('space ' + self.name + ' key k attributes document v')
        exists = True

    # Convert from hyperdex to mongo conditions
    def convert_conds(self, conditions):
        if not isinstance(conditions, dict):
            raise ValueError('Conditions must be a dict')

        result = {}

        for k, c in conditions.iteritems():
            if not isinstance(c, dict):
                raise ValueError('Wrong Type')

            hyperpreds = []

            for pred, val in c.iteritems():
                if pred == '$lt':
                    hyperpreds.append(LessThan(val))
                elif pred == '$lte':
                    hyperpreds.append(LessEquals(val))
                elif pred == '$gt':
					hyperpreds.append(GreaterThan(val))
                elif pred == '$gte':
					hyperpreds.append(GreaterEquals(val))
                elif pred == '$eq':
					hyperpreds.append(Equals(val))
                else:
					raise ValueError('Unknown Mongo Predicate: ' + pred)

            result['v.' + k] = hyperpreds

        return result

    def find(self, conditions = {}):
		self.init()
		hyperconds = self.convert_conds(conditions)
		result = self.client.search(self.name, hyperconds)

		return HyperIterator(result)

    def findOne(self, conditions = {}):
		it = self.find(conditions)

		if not it.hasNext():
			return None
		else:
			return it.next()

    def get(self, key):
        self.init()
        result = self.client.get(self.name, key)

        if result == None:
            return None
        else:
            return result['v'].doc()

    def remove(self, key):
        if not self.exists:
            raise RuntimeError("Can't remove. Space doesn't exist yet")

        return self.client.delete(self.name, key)

    def insert(self, value):
        if value is None or value['_id'] is None:
            #TODO auto generate id
            raise ValueError("Document is missing an id field")

        self.init()
        return self.client.put(self.name, value['_id'], {'v' : self.Document(value)})

    def list_keys(self):
        self.init()
        result = []
        for x in self.client.search(self.name, {}):
            result.append(x['k'])

        return result

    def update(self, key, arg):
        for k,v in arg.items():
            if k is "$inc$":
                self.atomic_add(key, v)
            elif pred == '$bit':
                if not isinstance(val, dict):
                    raise ValueError('$bit argument must a dict')
                    
                op, mask = val.iteritems().next()
                    
                if op == 'and':
                    self.atomic_and(key, mask)
                elif op == 'or':
                    self.atomic_or(key, mask)
                elif op == 'mod':
                    self.atomic_mod(key, mask)
                elif op =='xor':
                    self.atomic_xor(key, mask)
                else
                    raise ValueError("Unknown bit-operation")
            
            elif pred == '$mul':
                self.atomic_mul(key, v)        
            elif pred == '$div':
                self.atomic_div(key, v)
            else:
                raise ValueError("Unknown command " + k)
       
    def async_string_prepend(self, key, value):
        self.init()
        return self.client.async_document_string_prepend(self.name, key, {'v' : self.Document(value)})
                
    def string_prepend(self, key, value):
        self.init()
        return self.client.document_string_prepend(self.name, key, {'v' : self.Document(value)})
        
    def async_string_append(self, key, value):
        self.init()
        return self.client.async_document_string_append(self.name, key, {'v' : self.Document(value)})
                
    def string_append(self, key, value):
        self.init()
        return self.client.document_string_append(self.name, key, {'v' : self.Document(value)})
        
    def async_atomic_add(self, key, value):
        self.init()
        return self.client.async_document_atomic_add(self.name, key, {'v' : self.Document(value)})

    def atomic_add(self, key, value):
        self.init()
        return self.client.document_atomic_add(self.name, key, {'v' : self.Document(value)})

    def async_atomic_sub(self, key, value):
        self.init()
        return self.client.async_document_atomic_sub(self.name, key, {'v' : self.Document(value)})

    def atomic_sub(self, key, value):
        self.init()
        return self.client.document_atomic_sub(self.name, key, {'v' : self.Document(value)})

    def atomic_mul(self, key, value):
        self.init()
        return self.client.document_atomic_mul(self.name, key, {'v' : self.Document(value)})

    def atomic_div(self, key, value):
        self.init()
        return self.client.document_atomic_div(self.name, key, {'v' : self.Document(value)})

    def atomic_and(self, key, value):
        self.init()
        return self.client.document_atomic_and(self.name, key, {'v' : self.Document(value)})

    def atomic_xor(self, key, value):
        self.init()
        return self.client.document_atomic_xor(self.name, key, {'v' : self.Document(value)})

    def atomic_or(self, key, value):
        self.init()
        return self.client.document_atomic_or(self.name, key, {'v' : self.Document(value)})

    def atomic_mod(self, key, value):
        self.init()
        return self.client.document_atomic_mod(self.name, key, {'v' : self.Document(value)})


class HyperDatabase:
    function_list = ['clear', 'create_space', 'get_space', 'list_spaces']
    spaces = {}

    def __init__(self, url, port):
        self.client = hyperdex.client.Client(url, port)
        self.admin  = hyperdex.admin.Admin(url, port)

        for name in self.admin.list_spaces():
            self.spaces[name] = HyperSpace(self.client, self.admin, name)

    def create_space(self, name):
        if not isinstance(name, str):
            raise ValueError("Name must be a string")

        if ' ' in name:
            raise ValueError("Name contains whitespace")

        if name in self.function_list:
            raise ValueError(name + " is a reserved name")

        self.spaces[name] = HyperSpace(self.client, self.admin, name)

    def __getattr__(self, name):
        if name in self.function_list:
            return HyperDatabase.__getattribute__(self, name)
        else:
            return self.get_space(name)

    def __getitem__(self, name):
        return self.get_space(name)

    def get_space(self, name):
        if not isinstance(name, str):
            raise ValueError("Name must be a string")

        if name not in self.spaces.keys():
            self.create_space(name)

        return self.spaces[name]

    def list_spaces(self):
        self.spaces.clear()
        for name in self.admin.list_spaces():
            self.spaces[name] = HyperSpace(self.client, self.admin, name)

        return self.spaces.keys()

    def clear(self):
        for k,v in self.spaces.items():
            if v.exists:
                self.admin.rm_space(k)

        self.spaces.clear()



