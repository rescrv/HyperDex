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
    public void setUpHyperdexClient() throws Exception {
        c = new Client(System.getProperty("hyperdex_host"),
                Integer.parseInt(System.getProperty("hyperdex_port")));
    }
    
    @After
    public void destroyHyperdexClient() throws HyperDexClientException {
        Map<String, Object> match_all = new HashMap<String, Object>();
        c.group_del("kv", match_all);
        c = null;
    }
    
    @Test
    public void insertEmptyValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", "");
        assertEquals(get, expected);
    }
    
    @Test
    public void insertString() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", "xxx");
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", "xxx");
        assertEquals(get, expected);
    }
    
    @Test
    public void insertBytes() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        byte[] bytes = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        attrs.put("v", new ByteString(bytes));
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new ByteString(bytes));
        assertEquals(get, expected);
    }
    
    @Test
    public void correctByteString() {
        String original = "42 is the answer";
        
        ByteString bstring = new ByteString(original.getBytes());
        String result = bstring.toString();
        
        assertEquals(original, result);
    }
}
