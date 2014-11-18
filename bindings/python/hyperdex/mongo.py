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

    def clear(self):
        self.client.group_del(self.name, {})

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
            if isinstance(c, dict):
                hyperpreds = []

                for pred, val in c.iteritems():
                    if pred == '$lt':
                        hyperpreds.append(LessThan(val))
                    elif pred == '$lte':
                        hyperpreds.append(LessEqual(val))
                    elif pred == '$gt':
                        hyperpreds.append(GreaterThan(val))
                    elif pred == '$gte':
                        hyperpreds.append(GreaterEqual(val))
                    elif pred == '$eq':
                        hyperpreds.append(Equals(val))
                    else:
                        raise ValueError('Unknown Mongo Predicate: ' + pred)

                result['v.' + k] = hyperpreds
            else:
                result['v.' + k] = Equals(c)

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

    def async_insert(self, value):
        if value is None or value['_id'] is None:
            #TODO auto generate id
            raise ValueError("Document is missing an id field")

        self.init()
        return self.client.async_put(self.name, value['_id'], {'v' : self.Document(value)})

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

    def update(self, select, arg):
        self.init()
        
        for k,v in arg.items():
            if k == "$inc":
                self.group_atomic_add(select, v)
            elif k == '$bit':
                if not isinstance(v, dict):
                    raise ValueError('$bit argument must a dict')

                op, mask = v.iteritems().next()

                if op == 'and':
                    self.group_atomic_and(select, mask)
                elif op == 'or':
                    self.group_atomic_or(select, mask)
                elif op == 'mod':
                    self.group_atomic_mod(select, mask)
                elif op =='xor':
                    self.group_atomic_xor(select, mask)
                else:
                    raise ValueError("Unknown bit-operation")

            elif k == '$set':
                self.group_set(select, v)
            elif k == '$mul':
                self.group_atomic_mul(select, v)
            elif k == '$div':
                self.group_atomic_div(select, v)
            else:
                raise ValueError("Unknown command " + k)

    def convert_docargs(self, args):
        if (not isinstance(args, dict)) or (len(args) is 0):
                raise ValueError("Invalid arguments")

        docargs = {}

        for k,v in args.iteritems():
                docargs['v.' + k] = v

        return docargs

    def async_group_set(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_document_set(self.name, hyperconds, docargs)

    def group_set(self, select, value):
        return self.async_group_set(select, value).wait()

    def async_set(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_document_set(self.name, key, docargs)

    def set(self, key, value):
        return self.async_set(key, value).wait()
        
    def async_group_string_prepend(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_string_prepend(self.name, hyperconds, docargs)

    def group_string_prepend(self, select, value):
        return self.async_group_prepend(select, value).wait()

    def async_string_prepend(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_string_prepend(self.name, key, docargs)

    def string_prepend(self, key, value):
        return self.async_string_prepend(key, value).wait()

    def async_string_append(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_string_append(self.name, key, docargs)

    def async_group_string_append(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_string_append(self.name, hyperconds, docargs)
        
    def group_string_append(self, select, args):
        return self.async_group_string_append(select, args).wait()

    def string_append(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.string_append(self.name, key, docargs)

    def async_group_atomic_add(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_atomic_add(self.name, hyperconds, docargs)
    
    def group_atomic_add(self, select, args):
        return self.async_group_atomic_add(select, args).wait()

    def async_atomic_add(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_atomic_add(self.name, key, docargs)

    def atomic_add(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.atomic_add(self.name, key, docargs)

    def async_group_atomic_sub(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_atomic_sub(self.name, hyperconds, docargs)
    
    def group_atomic_sub(self, select, args):
        return self.async_group_atomic_sub(select, args).wait()

    def async_atomic_sub(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_atomic_sub(self.name, key, docargs)

    def atomic_sub(self, key, value):
        return self.async_atomic_sub(key, value).wait()

    def async_group_atomic_mul(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_atomic_mul(self.name, hyperconds, docargs)
    
    def group_atomic_mul(self, select, args):
        return self.async_group_atomic_mul(select, args).wait()

    def async_atomic_mul(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_atomic_mul(self.name, key, docargs)
        
    def atomic_mul(self, key, value):
        return self.client.async_atomic_mul(key, value)

    def async_atomic_div(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.async_atomic_div(self.name, key, docargs)
        
    def async_group_atomic_mul(self, select, args):
        self.init()
        hyperconds = self.convert_conds(select)
        docargs = self.convert_docargs(args)

        return self.client.async_group_atomic_div(self.name, hyperconds, docargs)
    
    def group_atomic_div(self, select, args):
        return self.async_group_atomic_div(select, args).wait()
        
    def atomic_div(self, key, value):
        return self.client.async_atomic_div(key, value)

    def atomic_and(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.atomic_and(self.name, key, docargs)

    def atomic_xor(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.atomic_xor(self.name, key, docargs)

    def atomic_or(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.atomic_or(self.name, key, docargs)

    def atomic_mod(self, key, value):
        docargs = self.convert_docargs(value)
        self.init()

        return self.client.atomic_mod(self.name, key, docargs)


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



