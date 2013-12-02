import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.HyperDexClientException;

public class DataTypeFloat
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> attrs0 = new HashMap<String, Object>();
        Object obj0 = c.put("kv", "k", attrs0);
        assert obj0 != null;
        Boolean bool0 = (Boolean)obj0;
        assert bool0 == true;
        Map<String, Object> get1 = c.get("kv", "k");
        assert(get1 != null);
        Map<String, Object> expected1 = new HashMap<String, Object>();
        expected1.put("v", 0.0);
        get1.equals(expected1);
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v", 3.14);
        Object obj2 = c.put("kv", "k", attrs2);
        assert obj2 != null;
        Boolean bool2 = (Boolean)obj2;
        assert bool2 == true;
        Map<String, Object> get3 = c.get("kv", "k");
        assert(get3 != null);
        Map<String, Object> expected3 = new HashMap<String, Object>();
        expected3.put("v", 3.14);
        get3.equals(expected3);
    }
}
