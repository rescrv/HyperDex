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

public class MultiAttribute
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> get0 = c.get("kv", "k");
        assert(get0 == null);
        Map<String, Object> attrs1 = new HashMap<String, Object>();
        attrs1.put("v1", "ABC");
        Object obj1 = c.put("kv", "k", attrs1);
        assert(obj1 != null);
        Boolean bool1 = (Boolean)obj1;
        assert(bool1 == true);
        Map<String, Object> get2 = c.get("kv", "k");
        assert(get2 != null);
        Map<String, Object> expected2 = new HashMap<String, Object>();
        expected2.put("v1", "ABC");
        expected2.put("v2", "");
        get2.entrySet().containsAll(expected2.entrySet());
        expected2.entrySet().containsAll(get2.entrySet());
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        attrs3.put("v2", "123");
        Object obj3 = c.put("kv", "k", attrs3);
        assert(obj3 != null);
        Boolean bool3 = (Boolean)obj3;
        assert(bool3 == true);
        Map<String, Object> get4 = c.get("kv", "k");
        assert(get4 != null);
        Map<String, Object> expected4 = new HashMap<String, Object>();
        expected4.put("v1", "ABC");
        expected4.put("v2", "123");
        get4.entrySet().containsAll(expected4.entrySet());
        expected4.entrySet().containsAll(get4.entrySet());
        List<String> list6 = new ArrayList<String>();
        list6.add("v1");
        Map<String, Object> get5 = c.get_partial("kv", "k", list6);
        assert(get5 != null);
        Map<String, Object> expected5 = new HashMap<String, Object>();
        expected5.put("v1", "ABC");
        get5.entrySet().containsAll(expected5.entrySet());
        expected5.entrySet().containsAll(get5.entrySet());
    }
}
