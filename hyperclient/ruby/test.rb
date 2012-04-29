require "hyperclient"

h = HyperClient.new("127.0.0.1",1234)

puts "Testing PUT"

if h.put("phonebook", "Ashik", [ ["first", "ashik"], ["last", "ratnani"], ["phone", 1234567890] ])	
	puts "Put : PASSED";
else 
	puts "PUt : FAILED";
end

puts h.get("phonebook", "Ashik")

puts("----------------------------------------------");

puts "Testing CONDPUT"

if h.condput("phonebook", "Ashik", [ ["phone",1234567890] ], [ ["first", "Ashik"], ["last", "Ratnani"] ])
	puts "Condput : PASSED";
else
	puts "Condput : FAILED";
end

puts h.get("phonebook", "Ashik")

puts("----------------------------------------------")

puts("Testing Atomic INC")

if h.atomicinc("phonebook", "Ashik", [ ["phone", 1] ])
	 puts "Atomicinc : PASSED";
else
        puts "Atomicinc : FAILED";
end 

puts h.get("phonebook", "Ashik")

puts("----------------------------------------------")

puts("Testing Atomic DEC")

if h.atomicdec("phonebook", "Ashik", [ ["phone", 1] ])
         puts "Atomicdec : PASSED";
else
        puts "Atomicdec : FAILED";
end 

puts h.get("phonebook", "Ashik")

puts("---------------------------------------------")

puts("Testing Search");

puts h.search("phonebook", [ ["first", "Ashik"], ["last", "Ratnani"], ["phone", [1234567889, 1234567891 ] ] ])

puts("---------------------------------------------")

puts("Testing Delete")

if h.delete("phonebook", "Ashik")
	puts "Delete : PASSED"
else
	puts "Delete : FAILED"
end

puts h.get("phonebook", "Ashik")

puts("--------------------------------------------")

h.destroy()
