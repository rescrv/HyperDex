import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.LessEqual;
import org.hyperdex.client.GreaterEqual;
import org.hyperdex.client.LessThan;
import org.hyperdex.client.GreaterThan;
import org.hyperdex.client.Range;
import org.hyperdex.client.Regex;
import org.hyperdex.client.LengthEquals;
import org.hyperdex.client.LengthLessEqual;
import org.hyperdex.client.LengthGreaterEqual;

public class DataTypeSetInt
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
        Set<Object> set2 = new HashSet<Object>();
        expected1.put("v", set2);
        get1.entrySet().containsAll(expected1.entrySet());
        expected1.entrySet().containsAll(get1.entrySet());
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        Set<Object> set4 = new HashSet<Object>();
        set4.add(1);
        set4.add(2);
        set4.add(3);
        attrs3.put("v", set4);
        Object obj3 = c.put("kv", "k", attrs3);
        assert(obj3 != null);
        Boolean bool3 = (Boolean)obj3;
        assert(bool3 == true);
        Map<String, Object> get5 = c.get("kv", "k");
        assert(get5 != null);
        Map<String, Object> expected5 = new HashMap<String, Object>();
        Set<Object> set6 = new HashSet<Object>();
        set6.add(1);
        set6.add(2);
        set6.add(3);
        expected5.put("v", set6);
        get5.entrySet().containsAll(expected5.entrySet());
        expected5.entrySet().containsAll(get5.entrySet());
        Map<String, Object> attrs7 = new HashMap<String, Object>();
        Set<Object> set8 = new HashSet<Object>();
        attrs7.put("v", set8);
        Object obj7 = c.put("kv", "k", attrs7);
        assert(obj7 != null);
        Boolean bool7 = (Boolean)obj7;
        assert(bool7 == true);
        Map<String, Object> get9 = c.get("kv", "k");
        assert(get9 != null);
        Map<String, Object> expected9 = new HashMap<String, Object>();
        Set<Object> set10 = new HashSet<Object>();
        expected9.put("v", set10);
        get9.entrySet().containsAll(expected9.entrySet());
        expected9.entrySet().containsAll(get9.entrySet());
    }
}
