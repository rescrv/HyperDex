require "hyperclient"

h = HyperClient.new("127.0.0.1",1234)

puts "Testing Async PUT"

x =  h.async_put("phonebook", "Ashik", [ ["first", "ashik"], ["last", "ratnani"], ["phone", 1234567890] ])	
if x.wait()
	puts "Put : PASSED";
else 
	puts "PUt : FAILED";
end

y =  h.async_get("phonebook", "Ashik")
puts y.wait()

puts("----------------------------------------------");

puts "Testing Async CondPUT"

x = h.async_condput("phonebook", "Ashik", [ ["phone",1234567890] ], [ ["first", "Ashik"], ["last", "Ratnani"] ])
if x.wait()
	puts "Condput : PASSED";
else
	puts "Condput : FAILED";
end

y = h.async_get("phonebook", "Ashik")
puts y.wait()

puts("----------------------------------------------")

puts("Testing Async Atomic INC")

x = h.async_atomicinc("phonebook", "Ashik", [ ["phone", 1] ])
if x.wait() 
	puts "Atomicinc : PASSED";
else
        puts "Atomicinc : FAILED";
end 
y = h.async_get("phonebook", "Ashik")
puts y.wait()

puts("----------------------------------------------")

puts("Testing Async Atomic DEC")

x = h.async_atomicdec("phonebook", "Ashik", [ ["phone", 1] ])
if x.wait()
         puts "Atomicdec : PASSED";
else
        puts "Atomicdec : FAILED";
end 

y = h.async_get("phonebook", "Ashik")
puts y.wait()

puts("---------------------------------------------")

puts("Testing Async Search");

x = h.async_search("phonebook", [ ["first", "Ashik"], ["last", "Ratnani"], ["phone", [1234567889, 1234567891 ] ] ])
while (y=x.next())!=nil
	puts y
end

puts("---------------------------------------------")

puts("Testing Async Delete")

x = h.async_delete("phonebook", "Ashik")
if x.wait()
	puts "Delete : PASSED"
else
	puts "Delete : FAILED"
end

y = h.async_get("phonebook", "Ashik")
puts y.wait()

puts("--------------------------------------------")

h.destroy()
