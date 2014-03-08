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

public class BasicSearch
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> checks0 = new HashMap<String, Object>();
        checks0.put("v", "v1");
        Set<Object> X0 = new HashSet<Object>();
        Iterator it0 = c.search("kv", checks0);
        while (it0.hasNext())
        {
            X0.add(it0.next());
        }
        Map<String, Object> attrs1 = new HashMap<String, Object>();
        attrs1.put("v", "v1");
        Object obj1 = c.put("kv", "k1", attrs1);
        assert(obj1 != null);
        Boolean bool1 = (Boolean)obj1;
        assert(bool1 == true);
        Map<String, Object> checks2 = new HashMap<String, Object>();
        checks2.put("v", "v1");
        Set<Object> X2 = new HashSet<Object>();
        Iterator it2 = c.search("kv", checks2);
        while (it2.hasNext())
        {
            X2.add(it2.next());
        }
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        attrs3.put("v", "v1");
        Object obj3 = c.put("kv", "k2", attrs3);
        assert(obj3 != null);
        Boolean bool3 = (Boolean)obj3;
        assert(bool3 == true);
        Map<String, Object> checks4 = new HashMap<String, Object>();
        checks4.put("v", "v1");
        Set<Object> X4 = new HashSet<Object>();
        Iterator it4 = c.search("kv", checks4);
        while (it4.hasNext())
        {
            X4.add(it4.next());
        }
    }
}
