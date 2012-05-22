var hyperclient = require("./build/Release/hyperclient");
var util = require('util');

h = hyperclient.Client("127.0.0.1", 1234);

console.log("Testing PUT");

if (h.put("phonebook", "Ashik", { "first": "ashik", "last": "ratnani", "phone": 1234567890 }))
	console.log("Put : PASSED");
else 
	console.log("Put : FAILED");

console.log(h.get("phonebook", "Ashik"));

console.log("----------------------------------------------");

console.log("Testing CONDPUT");

if (h.condput("phonebook", "Ashik", { "phone": 1234567890 }, { "first": "Ashik", "last": "Ratnani" }))
	console.log("Condput : PASSED");
else
	console.log("Condput : FAILED");

console.log(h.get("phonebook", "Ashik"));

console.log("Testing Search");

console.log(h.search("phonebook", {"first": "Ashik", "last": "Ratnani", "phone": [1234567889, 1234567891] }));

console.log("---------------------------------------------");

console.log("Testing Delete");

if (h.delete("phonebook", "Ashik"))
	console.log("Delete : PASSED");
else
	console.log("Delete : FAILED");

console.log(h.get("phonebook", "Ashik"));

console.log("--------------------------------------------");

h.destroy()

