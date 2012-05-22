var hyperclient = require("./build/Release/hyperclient");

h = hyperclient.Client("127.0.0.1",1234);

console.log("Testing Async PUT");

x =  h.async_put("phonebook", "Ashik", { "first": "ashik", "last": "ratnani", "phone": 1234567890 });

if(x.wait())
	console.log("Put : PASSED");
else 
	console.log("PUt : FAILED");

y =  h.async_get("phonebook", "Ashik");
console.log(y.wait());

console.log("----------------------------------------------");

console.log("Testing Async CondPUT");


x =  h.async_condput("phonebook", "Ashik", { "phone": 1234567890 }, { "first" : "Ashik", "last" : "Ratnani" });
if(x.wait())
	console.log("Condput : PASSED");
else
	console.log("Condput : FAILED");

y = h.async_get("phonebook", "Ashik");
console.log(y.wait());

console.log("----------------------------------------------");

console.log("Testing Async Atomic INC");

x = h.async_atomicinc("phonebook", "Ashik", { "phone": 1 });
if(x.wait()) 
	console.log("Atomicinc : PASSED");
else
        console.log("Atomicinc : FAILED");

y = h.async_get("phonebook", "Ashik");
console.log(y.wait());

console.log("----------------------------------------------");

console.log("Testing Async Atomic DEC");

x = h.async_atomicdec("phonebook", "Ashik", { "phone": 1 })
if(x.wait())
         console.log("Atomicdec : PASSED");
else
         console.log("Atomicdec : FAILED");

y = h.async_get("phonebook", "Ashik");
console.log(y.wait());
console.log("---------------------------------------------");

console.log("Testing Async Search");

x = h.async_search("phonebook", { "first": "Ashik", "last": "Ratnani", "phone": [1, 1234567899 ] });
while (y=x.wait())
{
	console.log(y);
}

console.log("---------------------------------------------");

console.log("Testing Async Delete");

x = h.async_delete("phonebook", "Ashik");
if (x.wait())
	console.log("Delete : PASSED");
else
	console.log("Delete : FAILED");

y = h.async_get("phonebook", "Ashik");
console.log(y.wait());

console.log("--------------------------------------------");

h.destroy()
