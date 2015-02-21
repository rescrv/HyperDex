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

public class DataTypeString
{
    private Client c;
    
    @Before
    public void setUp() throws Exception {
        c = new Client(System.getProperty("hyperdex_host"),
                Integer.parseInt(System.getProperty("hyperdex_port")));
    }
    
    @Test
    public void insertEmptyValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        
        Object obj = c.put("kv", "k", attrs);
        assertNotNull(obj);
        Boolean bool = (Boolean)obj;
        assertTrue(bool);
        
        Map<String, Object> get = c.get("kv", "k");
        assertNotNull(get);
        
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", "");
        assertEquals(get, expected);
    }
    
    @Test
    public void insertString() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", "xxx");
        
        Boolean res = (Boolean)c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        assertNotNull(get);
        
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", "xxx");
        assertEquals(get, expected);
    }
    
    @Test
    public void insertBytes() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        byte[] bytes = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        attrs.put("v", new ByteString(bytes));
        Boolean res = (Boolean)c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        assertNotNull(get);
        
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new ByteString(bytes));
        assertEquals(get, expected);
    }
}
