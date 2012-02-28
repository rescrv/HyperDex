#!/usr/bin/env python

import math
import random
import string

charset = string.ascii_letters + string.digits

NUMOPS = 1000000
PERCGET = 0.95

keys = []

for i in range(NUMOPS):
    if keys and random.random() < PERCGET:
        print(str.join(' ', [ 'GET', random.choice(keys) ]))
    else:
        key = ''.join([random.choice(charset) for i in range(8)])
        value = ''.join([random.choice(charset) for i in range(64)])
        keys.append(key)
        print(str.join(' ', [ 'PUT', key, 'value', value ]))
