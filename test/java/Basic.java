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

public class Basic
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> get0 = c.get("kv", "k");
        assert(get0 == null);
        Map<String, Object> attrs1 = new HashMap<String, Object>();
        attrs1.put("v", "v1");
        Object obj1 = c.put("kv", "k", attrs1);
        assert(obj1 != null);
        Boolean bool1 = (Boolean)obj1;
        assert(bool1 == true);
        Map<String, Object> get2 = c.get("kv", "k");
        assert(get2 != null);
        Map<String, Object> expected2 = new HashMap<String, Object>();
        expected2.put("v", "v1");
        get2.entrySet().containsAll(expected2.entrySet());
        expected2.entrySet().containsAll(get2.entrySet());
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        attrs3.put("v", "v2");
        Object obj3 = c.put("kv", "k", attrs3);
        assert(obj3 != null);
        Boolean bool3 = (Boolean)obj3;
        assert(bool3 == true);
        Map<String, Object> get4 = c.get("kv", "k");
        assert(get4 != null);
        Map<String, Object> expected4 = new HashMap<String, Object>();
        expected4.put("v", "v2");
        get4.entrySet().containsAll(expected4.entrySet());
        expected4.entrySet().containsAll(get4.entrySet());
        Map<String, Object> attrs5 = new HashMap<String, Object>();
        attrs5.put("v", "v3");
        Object obj5 = c.put("kv", "k", attrs5);
        assert(obj5 != null);
        Boolean bool5 = (Boolean)obj5;
        assert(bool5 == true);
        Map<String, Object> get6 = c.get("kv", "k");
        assert(get6 != null);
        Map<String, Object> expected6 = new HashMap<String, Object>();
        expected6.put("v", "v3");
        get6.entrySet().containsAll(expected6.entrySet());
        expected6.entrySet().containsAll(get6.entrySet());
        Object obj7 = c.del("kv", "k");
        assert(obj7 != null);
        Boolean bool7 = (Boolean)obj7;
        assert(bool7 == true);
        Map<String, Object> get8 = c.get("kv", "k");
        assert(get8 == null);
    }
}
