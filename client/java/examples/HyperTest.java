import hyperclient.*;
import java.util.*;

public class HyperTest
{
	public static void main(String[] args) throws Exception
	{
        HashMap<String,Object> values = new HashMap<String,Object>();

		HyperClient c = new HyperClient("127.0.0.1",1982);

        String desc = "space phonebook "
                    + "key username "
                    + "attributes first, last, int phone "
                    + "subspace first, last, phone";

        System.out.println("\n\nSpace Description:\n\n" + desc + "\n\n");

        c.add_space(desc);

        try
        {
	        values.put("first","Nick");
	        values.put("last","Tolomiczenko");
	        values.put("phone",4165551024L);
	
			System.out.println("ntolomic put: " + c.put("phonebook","ntolomic",values));
	
	        values.put("first","George");
	        values.put("last","Tolomiczenko");
	        values.put("phone",4165551025L);
	
			System.out.println("gtolomic put: " + c.put("phonebook","gtolomic",values));
	
	        values.put("first","Paul");
	        values.put("last","Tolomiczenko");
	        values.put("phone",4165551026L);
	
			System.out.println("ptolomic put: " + c.put("phonebook","ptolomic",values));
	
	        System.out.println("\nAbout retrieve key ntolomic:\n");
	
	        Map row = c.get("phonebook","ntolomic");
	
	        System.out.println("Got back: " + row);
	
	        System.out.println("\nSearching for last name of 'Tolomiczenko':\n");
	
	        // We'll use the same 'values' map for our search predicate
	
	        // This leaves us with only the last name entry
	        values.remove("first");
	        values.remove("phone");
	
	        SearchBase s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        // Now add a range stipulation on the phone number: [4165551024,4165551026]
	        values.put("phone",
	                    new AbstractMap.SimpleEntry<Long,Long>(4165551024L,4165551026L));
	
	        System.out.println("\nSearching for last name of 'Tolomiczenko'\n\n  AND\n\nphone number in the range [4165551024,4165551026]:\n");
	
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        // Stipulate the range with the Range class.
	        //
	        Range range = new Range(4165551024L,4165551026L);
	        values.put("phone",range);
	
	        System.out.println("\nDo the search again using Range class:\n");
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        // Stipulate the range using a List of Predicate based classes.
	        // In particular, use a Vector implementation of a List and LessEqual,
	        // GreaterEqual both derived from Predicate.
	        //
	        Vector<Predicate> predicates = new Vector<Predicate>(2);
	        predicates.add(new GreaterEqual(4165551024L));
	        predicates.add(new LessEqual(4165551026L));
	        values.put("phone",predicates);
	
	        System.out.println("\nDo the search again using a List of Predicate based classes:\n");
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        System.out.println("\nLet's use the same predicate, values, to get the count:");
	        System.out.println("count = " + c.count("phonebook",values));
	
	        // Now do an async_put, which should work by definition.
	        //
	        values.put("first","Stavroula");
	        values.put("phone",4165551027L);
	
	        Deferred d = c.async_put("phonebook","stolomic",values);;
	
	        System.out.println("\nJust called async_put:\n");
	        System.out.println("\nAbout to call waitFor():\n");
	        
	        System.out.println(d.waitFor());
	
	        values.remove("first"); values.remove("phone");
	
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        // Do a sorted search by first name ascending
	
	        System.out.println("\nDo a sorted search by first name ascending:");
	
	        s = c.sorted_search("phonebook",values,"first",-1,false);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	        
	        // Do the same sorted search, but with a limit of 2
	
	        System.out.println("\nDo the same sorted search, but with a limit of 2:");
	
	        s = c.sorted_search("phonebook",values,"first",2,false);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        System.out.println("\nAbout delete stolomic:\n");
	
	        System.out.println("result: " + c.del("phonebook","stolomic"));
	        
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
	
	        System.out.println("\nGroup delete for last name of 'Tolomiczenko'\n\n  AND\n\nphone number in the range [4165551024,4165551026]:\n");
	        values.put("phone",
	                    new AbstractMap.SimpleEntry<Long,Long>(4165551024L,4165551026L));
	        System.out.println("result: " + c.group_del("phonebook",values));
	
	        values.remove("phone");
	
	        // Do the search again
	        s = c.search("phonebook",values);
	
	        while(s.hasNext())
	        {
	            System.out.println(s.next());
	        }
        }
        finally
        {
            c.rm_space("phonebook");
        }
	}
}
