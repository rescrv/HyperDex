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

public class RangeSearchInt
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, Object> attrs0 = new HashMap<String, Object>();
        attrs0.put("v", -2);
        Object obj0 = c.put("kv", -2, attrs0);
        assert(obj0 != null);
        Boolean bool0 = (Boolean)obj0;
        assert(bool0 == true);
        Map<String, Object> attrs1 = new HashMap<String, Object>();
        attrs1.put("v", -1);
        Object obj1 = c.put("kv", -1, attrs1);
        assert(obj1 != null);
        Boolean bool1 = (Boolean)obj1;
        assert(bool1 == true);
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v", 0);
        Object obj2 = c.put("kv", 0, attrs2);
        assert(obj2 != null);
        Boolean bool2 = (Boolean)obj2;
        assert(bool2 == true);
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        attrs3.put("v", 1);
        Object obj3 = c.put("kv", 1, attrs3);
        assert(obj3 != null);
        Boolean bool3 = (Boolean)obj3;
        assert(bool3 == true);
        Map<String, Object> attrs4 = new HashMap<String, Object>();
        attrs4.put("v", 2);
        Object obj4 = c.put("kv", 2, attrs4);
        assert(obj4 != null);
        Boolean bool4 = (Boolean)obj4;
        assert(bool4 == true);
        Map<String, Object> checks5 = new HashMap<String, Object>();
        checks5.put("k", new LessEqual(0));
        Set<Object> X5 = new HashSet<Object>();
        Iterator it5 = c.search("kv", checks5);
        while (it5.hasNext())
        {
            X5.add(it5.next());
        }
        Map<String, Object> checks6 = new HashMap<String, Object>();
        checks6.put("v", new LessEqual(0));
        Set<Object> X6 = new HashSet<Object>();
        Iterator it6 = c.search("kv", checks6);
        while (it6.hasNext())
        {
            X6.add(it6.next());
        }
        Map<String, Object> checks7 = new HashMap<String, Object>();
        checks7.put("k", new GreaterEqual(0));
        Set<Object> X7 = new HashSet<Object>();
        Iterator it7 = c.search("kv", checks7);
        while (it7.hasNext())
        {
            X7.add(it7.next());
        }
        Map<String, Object> checks8 = new HashMap<String, Object>();
        checks8.put("v", new GreaterEqual(0));
        Set<Object> X8 = new HashSet<Object>();
        Iterator it8 = c.search("kv", checks8);
        while (it8.hasNext())
        {
            X8.add(it8.next());
        }
        Map<String, Object> checks9 = new HashMap<String, Object>();
        checks9.put("k", new LessThan(0));
        Set<Object> X9 = new HashSet<Object>();
        Iterator it9 = c.search("kv", checks9);
        while (it9.hasNext())
        {
            X9.add(it9.next());
        }
        Map<String, Object> checks10 = new HashMap<String, Object>();
        checks10.put("v", new LessThan(0));
        Set<Object> X10 = new HashSet<Object>();
        Iterator it10 = c.search("kv", checks10);
        while (it10.hasNext())
        {
            X10.add(it10.next());
        }
        Map<String, Object> checks11 = new HashMap<String, Object>();
        checks11.put("k", new GreaterThan(0));
        Set<Object> X11 = new HashSet<Object>();
        Iterator it11 = c.search("kv", checks11);
        while (it11.hasNext())
        {
            X11.add(it11.next());
        }
        Map<String, Object> checks12 = new HashMap<String, Object>();
        checks12.put("v", new GreaterThan(0));
        Set<Object> X12 = new HashSet<Object>();
        Iterator it12 = c.search("kv", checks12);
        while (it12.hasNext())
        {
            X12.add(it12.next());
        }
        Map<String, Object> checks13 = new HashMap<String, Object>();
        checks13.put("k", new Range(-1, 1));
        Set<Object> X13 = new HashSet<Object>();
        Iterator it13 = c.search("kv", checks13);
        while (it13.hasNext())
        {
            X13.add(it13.next());
        }
        Map<String, Object> checks14 = new HashMap<String, Object>();
        checks14.put("v", new Range(-1, 1));
        Set<Object> X14 = new HashSet<Object>();
        Iterator it14 = c.search("kv", checks14);
        while (it14.hasNext())
        {
            X14.add(it14.next());
        }
    }
}
