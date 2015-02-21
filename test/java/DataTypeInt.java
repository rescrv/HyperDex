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

import org.junit.*;
import static org.junit.Assert.* ;

public class DataTypeInt
{
    private Client c;
    
    @Before
    public void setUp() throws Exception {
        c = new Client(System.getProperty("hyperdex_host"),
                Integer.parseInt(System.getProperty("hyperdex_port")));
    }
    
    @Test
    public void testInsertEmpty() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        Object obj = c.put("kv", "k", attrs);
        assertNotNull(obj);
        Boolean bool = (Boolean)obj;
        assertTrue(bool);
        
        Map<String, Object> get = c.get("kv", "k");
        assertNotNull(get);
        
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", 0);
        
        assertEquals(expected, get);
    }
    
    @Test
    public void testInsertZero() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", 1);
        
        Boolean res = (Boolean)c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        assertNotNull(get);
        
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", 1);
        
        assertEquals(expected, get);
    }
        
    @Test
    public void testInteger() throws HyperDexClientException {
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
        get5.entrySet().containsAll(expected5.entrySet());
        expected5.entrySet().containsAll(get5.entrySet());
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
        get7.entrySet().containsAll(expected7.entrySet());
        expected7.entrySet().containsAll(get7.entrySet());
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
        get9.entrySet().containsAll(expected9.entrySet());
        expected9.entrySet().containsAll(get9.entrySet());
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
        get11.entrySet().containsAll(expected11.entrySet());
        expected11.entrySet().containsAll(get11.entrySet());
    }
}
