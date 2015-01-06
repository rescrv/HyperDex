var hyperdex_client = require('hyperdex-client')
var c = new hyperdex_client.Client('127.0.0.1', 1982);

var NUMBER = 10000000
var CONCURRENT = 1000
var i = 0

function put (success, err) {
    if (i < THRESH) {
        c.put('kv', new String(i), {v: new String(i), put);
    }
    i += 1;
}

for (var x = 0; x < CONCURRENT; ++x) {
    put();
}
