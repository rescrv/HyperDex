var hyperclient = require('./build/Release/hyperclient');
obj1 = hyperclient.Client("127.0.0.1",1234);
//obj1.async_get("phonebook","ashik");
console.log(obj1.async_put("phonebook","ashik",{"a": "b", "c":"d", "phone":123456 }) );
//obj1.async_atomicinc({"hello":"world"});
