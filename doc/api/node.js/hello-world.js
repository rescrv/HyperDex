var hyperdex_client = require('hyperdex-client')
var c = new hyperdex_client.Client('127.0.0.1', 1982);

function get_callback(obj, err) {
    if (err) { console.log('error ' + err); }
    console.log('get: ' + obj);
}

function put_callback(success, err) {
    if (err) { console.log('error ' + err); }
    console.log('put: ' + success);
    c.get('kv', 'k', get_callback);
}

c.put('kv', 'k', {v: 'Hello World!'}, put_callback);
