import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;

public class DataTypeString
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> attrs0 = new HashMap<String, Object>();
        Object obj0 = c.put("kv", "k", attrs0);
        assert(obj0 != null);
        Boolean bool0 = (Boolean)obj0;
        assert(bool0 == true);
        Map<String, Object> get1 = c.get("kv", "k");
        assert(get1 != null);
        Map<String, Object> expected1 = new HashMap<String, Object>();
        expected1.put("v", "");
        get1.equals(expected1);
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v", "xxx");
        Object obj2 = c.put("kv", "k", attrs2);
        assert(obj2 != null);
        Boolean bool2 = (Boolean)obj2;
        assert(bool2 == true);
        Map<String, Object> get3 = c.get("kv", "k");
        assert(get3 != null);
        Map<String, Object> expected3 = new HashMap<String, Object>();
        expected3.put("v", "xxx");
        get3.equals(expected3);
        Map<String, Object> attrs4 = new HashMap<String, Object>();
        byte[] bytes5 = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        attrs4.put("v", new ByteString(bytes5));
        Object obj4 = c.put("kv", "k", attrs4);
        assert(obj4 != null);
        Boolean bool4 = (Boolean)obj4;
        assert(bool4 == true);
        Map<String, Object> get6 = c.get("kv", "k");
        assert(get6 != null);
        Map<String, Object> expected6 = new HashMap<String, Object>();
        byte[] bytes7 = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        expected6.put("v", new ByteString(bytes7));
        get6.equals(expected6);
    }
}
