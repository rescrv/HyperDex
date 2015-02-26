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
    public void setUpHyperdexClient() throws Exception {
        c = new Client(System.getProperty("hyperdex_host"),
                Integer.parseInt(System.getProperty("hyperdex_port")));
    }
    
    @After
    public void destroyHyperdexClient() throws HyperDexClientException  {
        Map<String, Object> match_all = new HashMap<>();
        c.group_del("kv", match_all);
        c = null;
    }
    
    @Test
    public void testInsertEmpty() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", 0L);
        
        assertEquals(expected, get);
    }
    
    @Test(expected = HyperDexClientException.class)
    public void testWrongTypeThrowsException() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", "Not-an-Int");
        c.put("kv", "k", attrs);
    }
    
    @Test
    public void testInsertOne() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", 1L);
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", 1L);
        
        assertEquals(expected, get);
    }
    
    @Test
    public void testInsertZero() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", 0L);
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", 0L);
        
        assertEquals(expected, get);
    }
    
    @Test
    public void testDivideByZeroNotAllowed() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", 1L);
        c.put("kv", "k", attrs);
    
        Map<String, Object> args = new HashMap<>();
        args.put("v", 0L);
        
        Boolean res = c.atomic_div("kv", "k", args);
        assertFalse(res);
    }
        
    @Test
    public void testNegativeValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", -1L);
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");        
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", -1L);
        
        assertEquals(expected, get);
    }
        
    @Test
    public void testInsertHugePositiveValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", 9223372036854775807L);
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", 9223372036854775807L);
        
        assertEquals(expected, get);
    }
    
    @Test
    public void testInsertHugeNegativeValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", -9223372036854775807L);
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", -9223372036854775807L);
        
        assertEquals(expected, get);
    }
}
