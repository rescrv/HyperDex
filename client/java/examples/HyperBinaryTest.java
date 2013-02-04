import hyperclient.*;
import java.util.*;

public class HyperBinaryTest
{
    public static void printRawBytes(byte[] bytes)
    {
        System.out.print("{" + (bytes[0] & 0x000000ff)); // zero out the negative
                                                         // spilly talky bits
        
        int i = 1;

        do
        {
            System.out.print(", " + (bytes[i++] & 0x000000ff));
        } while ( i < bytes.length );

        System.out.println("}");
    }

	public static void main(String[] args) throws Exception
	{
		HyperClient c = new HyperClient("127.0.0.1",1982);

        String desc = "space phonebook "
                    + "key username "
                    + "attributes first, last, int phone "
                    + "subspace first, last, phone";

        System.out.println("\n\nSpace Description:\n\n" + desc + "\n\n");

        c.add_space(desc);

        try
        {
	        HashMap<Object,Object> attrs = new HashMap<Object,Object>();
	
	        // Make up some arbitrary bytes
	        byte[] inBytes = new byte[256];
	
	        for (int i=0; i<inBytes.length; i++) inBytes[i] = (byte)i;
	
	        // Put the bytes in the field for the first name, 'first'
	        attrs.put("first",inBytes);
	        attrs.put("last","Tolomiczenko");
	        attrs.put("phone",4165551024L);
	
			System.out.println("put success: " + c.put("phonebook", "mykey", attrs));
	
	        Map map = c.get("phonebook", "mykey");
	
	        // String values are returned in the form of a ByteArray.
	        // Instance method 'getBytes()' will yield the byte[] that
	        // it wraps.
	        byte[] outBytes = ((ByteArray)(map.get("first"))).getBytes();
	
	        System.out.println("bytes in:");
	        printRawBytes(inBytes);
	        System.out.println("\nbytes out:");
	        printRawBytes(outBytes);
	
	        // Let's put a proper first name now
	        attrs.put("first","Nick");
	
	        // "Nick" will be encoded using encoding "UTF-8"
	        // no matter what the environment is set at.
	        //
	        System.out.println("put success: " + c.put("phonebook", "mykey", attrs));
	        
	        map = c.get("phonebook", "mykey");
	        System.out.println(map);
	
	        // ByteArray value returned inherits default encoding
	        // of "UTF-8" (from the client)
	        System.out.println(((ByteArray)(map.get("first"))).getDefaultEncoding());
	
	        // That's why we can print it ie., its 'toString()' method
	        // is implicitly called below.
	        System.out.println((ByteArray)(map.get("first")));
	
	        // Let's the change the client's default
	        c.setDefaultStringEncoding("ISO-8859-1");
	
	        // Re-retrieve ...
	        map = c.get("phonebook", "mykey");
	
	        System.out.println(((ByteArray)(map.get("first"))).getDefaultEncoding());
	        System.out.println((ByteArray)(map.get("first")));
	
	        // Or you, the coder, can just defy the client's default
	        // string encoding altogether ...
	
	        // in the form of ByteArray
	        attrs.put("first",new ByteArray("Nick","UTF-16BE"));
	
	        // or in the form of bytes
	        attrs.put("first",ByteArray.encode("Nick","UTF-16BE"));
	
	        System.out.println("put success: " + c.put("phonebook", "mykey", attrs));
	        map = c.get("phonebook", "mykey");
	        System.out.println(((ByteArray)(map.get("first"))).decode("UTF-16BE"));
        }
        finally
        {
            c.rm_space("phonebook");
        }
	}
}
