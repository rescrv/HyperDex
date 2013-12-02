import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;

public class DataTypeInt
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
        expected1.put("v", 0);
        get1.equals(expected1);
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v", 1);
        Object obj2 = c.put("kv", "k", attrs2);
        assert(obj2 != null);
        Boolean bool2 = (Boolean)obj2;
        assert(bool2 == true);
        Map<String, Object> get3 = c.get("kv", "k");
        assert(get3 != null);
        Map<String, Object> expected3 = new HashMap<String, Object>();
        expected3.put("v", 1);
        get3.equals(expected3);
        Map<String, Object> attrs4 = new HashMap<String, Object>();
        attrs4.put("v", -1);
        Object obj4 = c.put("kv", "k", attrs4);
        assert(obj4 != null);
        Boolean bool4 = (Boolean)obj4;
        assert(bool4 == true);
        Map<String, Object> get5 = c.get("kv", "k");
        assert(get5 != null);
        Map<String, Object> expected5 = new HashMap<String, Object>();
        expected5.put("v", -1);
        get5.equals(expected5);
        Map<String, Object> attrs6 = new HashMap<String, Object>();
        attrs6.put("v", 0);
        Object obj6 = c.put("kv", "k", attrs6);
        assert(obj6 != null);
        Boolean bool6 = (Boolean)obj6;
        assert(bool6 == true);
        Map<String, Object> get7 = c.get("kv", "k");
        assert(get7 != null);
        Map<String, Object> expected7 = new HashMap<String, Object>();
        expected7.put("v", 0);
        get7.equals(expected7);
        Map<String, Object> attrs8 = new HashMap<String, Object>();
        attrs8.put("v", 9223372036854775807L);
        Object obj8 = c.put("kv", "k", attrs8);
        assert(obj8 != null);
        Boolean bool8 = (Boolean)obj8;
        assert(bool8 == true);
        Map<String, Object> get9 = c.get("kv", "k");
        assert(get9 != null);
        Map<String, Object> expected9 = new HashMap<String, Object>();
        expected9.put("v", 9223372036854775807L);
        get9.equals(expected9);
        Map<String, Object> attrs10 = new HashMap<String, Object>();
        attrs10.put("v", -9223372036854775808L);
        Object obj10 = c.put("kv", "k", attrs10);
        assert(obj10 != null);
        Boolean bool10 = (Boolean)obj10;
        assert(bool10 == true);
        Map<String, Object> get11 = c.get("kv", "k");
        assert(get11 != null);
        Map<String, Object> expected11 = new HashMap<String, Object>();
        expected11.put("v", -9223372036854775808L);
        get11.equals(expected11);
    }
}
