#!/usr/bin/env python

# Copyright (c) 2011, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This work was inspired by Doug Hellman's article on JSON serialization of 
# complex types located at http://www.doughellmann.com/PyMOTW/json/


import json
import inspect
import sys


# Use these parameters with encoder for human readable output, e.g.:
# hdjson.Encoder(**hdjson.HumanReadable).encode(obj)
HumanReadable = {'sort_keys':True, 'indent':2}

# Key types for dictionary normalization
KEYS_TUPLE, KEYS_INT = range(2)

class Encoder(json.JSONEncoder):
    
    def default(self, obj):
        # sets are not json serializable, need own handler
        if isinstance(obj, set):
            d = { '__class__':'set', 
                  '__value__':list(obj)
                }
            return d
        # all new-style classes are serialized into a dictionary
        d = { '__class__':obj.__class__.__name__, 
              '__module__':obj.__module__,
            }
        d.update(obj.__dict__)
        return d
        
    def normalizeDictKeys(self, d, key_type):
        # dict. with non-string keys must be normalized to strings
        nd = {}
        for key, value in d.iteritems():
            if key_type == KEYS_TUPLE:
                nkey = json.dumps(key)
            elif key_type == KEYS_INT: 
                nkey = str(key)
            else: 
                nkey = str(key)
            nd[nkey] =  value
        return nd

        
class Decoder(json.JSONDecoder):
    
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        if '__class__' not in d:
            return d
        class_name = d.pop('__class__')
        # some built in types need to be customer serialized too
        if class_name == 'set':
            return set(d['__value__'])
        module_name = d.pop('__module__')
        # __import returns top-level package when module name is in form
        # package.module, need to lookup the actual module from sys
        __import__(module_name)
        module = sys.modules[module_name]
        class_ = getattr(module, class_name)
        # encode from unicode to ASCII
        args = dict( (key.encode('ascii'), value) for key, value in d.items())
        # inspect constructor for the object and only use the matching args
        # allow for straight name or _name match
        params = inspect.getargspec(getattr(class_, "__init__"))[0]
        straight = dict((key, args[key]) for key in params if args.has_key(key))
        stripped = dict((key, args['_'+key]) for key in params if args.has_key('_'+key))
        pruned = dict(straight.items() + stripped.items()) 
        inst = class_(**pruned)
        return inst        

    def denormalizeDictKeys(self, d, key_type):
        # decode dict. with non-string keys back to original format
        nd = {}
        for key, value in d.iteritems():
            # not perfect, assumes the complex key was a tuple
            if key_type == KEYS_TUPLE:
                nkey = tuple(json.loads(key))
            elif key_type == KEYS_INT: 
                nkey = int(key)
            else: 
                nkey = str(key)
            nd[nkey] =  value
        return nd

