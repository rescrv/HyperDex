/* This example uses space definition */
/*
space weightbook
dimensions username, first, last, weight (float)
key username auto 1 3
subspace first, last, weight auto 2 3
*/


import hyperclient.*;
import java.util.*;

public class HyperSearchFloatRange
{
	public static void main(String[] args) throws Exception
	{
        HashMap<String,Object> values = new HashMap<String,Object>();

		HyperClient c = new HyperClient("127.0.0.1",1234);

        values.put("first","Nick");
        values.put("last","Tolomiczenko");
        values.put("weight",175.2);

		System.out.println("ntolomic put: " + c.put("weightbook","ntolomic",values));

        values.put("first","George");
        values.put("last","Tolomiczenko");
        values.put("weight",160.7);

		System.out.println("gtolomic put: " + c.put("weightbook","gtolomic",values));

        values.put("first","Paul");
        values.put("last","Tolomiczenko");
        values.put("weight",162.3);

		System.out.println("ptolomic put: " + c.put("weightbook","ptolomic",values));

        System.out.println("\nAbout retrieve key ntolomic:\n");

        Map row = c.get("weightbook","ntolomic");

        System.out.println("Got back: " + row);

        System.out.println("\nSearching for last name of 'Tolomiczenko':\n");

        // We'll use the same 'values' map for our search predicate

        // This leaves us with only the last name entry
        values.remove("first");
        values.remove("weight");

        SearchBase s = c.search("weightbook",values);

        while(s.hasNext())
        {
            System.out.println(s.next());
        }

        // Now add a range stipulation on the weight: [160.1,165)
        values.put("weight",
                    new AbstractMap.SimpleEntry<Double,Double>(160.1D,165D));

        System.out.println("\nSearching for last name of 'Tolomiczenko'\n\n  AND\n\nweight in the range [160.1,165):\n");

        // Do the search again
        s = c.search("weightbook",values);

        while(s.hasNext())
        {
            System.out.println(s.next());
        }

        // Stipulate the range with a List of size 2 instead.
        // In particular, use in Vector. 
        Vector<Double> vrange = new Vector<Double>(2);
        vrange.add(160.1D); vrange.add(165D);
        values.put("weight",vrange);

        System.out.println("\nDo the search again using a List of size 2 for the range stipulation:\n");
        // Do the search again
        s = c.search("weightbook",values);

        while(s.hasNext())
        {
            System.out.println(s.next());
        }

	}
}
