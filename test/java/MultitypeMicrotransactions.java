import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.Document;
import org.hyperdex.client.ByteString;
import org.hyperdex.client.Microtransaction;
import org.hyperdex.client.HyperDexClientException;

import org.junit.*;
import static org.junit.Assert.* ;

public class MultitypeMicrotransactions
{
    private Client c;
    
    @Before
    public void setUpHyperdexClient() throws Exception {
        c = new Client(System.getProperty("hyperdex_host"),
                Integer.parseInt(System.getProperty("hyperdex_port")));
    }
    
    @After
    public void destroyHyperdexClient() throws HyperDexClientException {
        Map<String, Object> match_all = new HashMap<>();
        c.group_del("kv", match_all);
        c = null;
    }
    
    @Test
    public void insertEmptyValue() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs = new HashMap<>();
        xact.put(attrs);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("i", new Long(0));
        expected.put("s", new ByteString(""));
        expected.put("f", new Double(0));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void singleField() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("s", "fourty-two");
        xact.put(attrs);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("i", new Long(0));
        expected.put("s", new ByteString("fourty-two"));
        expected.put("f", new Double(0));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void singleOperation() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("i", new Long(42));
        attrs.put("s", "fourty-two");
        attrs.put("f", new Double(4.2));
        xact.put(attrs);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("i", new Long(42));
        expected.put("s", new ByteString("fourty-two"));
        expected.put("f", new Double(4.2));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void independentOperations() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("i", new Long(42));
        xact.put(attrs1);
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("s", "fourty-two");
        xact.put(attrs2);
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("f", new Double(4.2));
        xact.put(attrs3);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("i", new Long(42));
        expected.put("s", new ByteString("fourty-two"));
        expected.put("f", new Double(4.2));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void groupCommitWithOneMatch() throws HyperDexClientException {
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("i", new Long(42));
        attrs1.put("s", "fourty-two");
        attrs1.put("f", new Double(4.2));
        c.put("kv", "k1", attrs1);
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("i", new Long(45));
        attrs2.put("s", "fourty-five");
        attrs2.put("f", new Double(4.2));
        c.put("kv", "k2", attrs2);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("f", new Double(0));
        xact.put(attrs3);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("s", "fourty-two"); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(1), res);
        
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("i", new Long(42));
        expected1.put("s", new ByteString("fourty-two"));
        expected1.put("f", new Double(0));
        
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("i", new Long(45));
        expected2.put("s", new ByteString("fourty-five"));
        expected2.put("f", new Double(4.2));

        assertEquals(expected1, c.get("kv", "k1"));
        assertEquals(expected2, c.get("kv", "k2"));
    }
    
        @Test
    public void groupCommitWithOneNumericMatch() throws HyperDexClientException {
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("i", new Long(42));
        attrs1.put("s", "fourty-two");
        attrs1.put("f", new Double(4.2));
        c.put("kv", "k1", attrs1);
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("i", new Long(45));
        attrs2.put("s", "fourty-five");
        attrs2.put("f", new Double(4.2));
        c.put("kv", "k2", attrs2);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("f", new Double(0));
        xact.put(attrs3);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("i", 42); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(1), res);
        
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("i", new Long(42));
        expected1.put("s", new ByteString("fourty-two"));
        expected1.put("f", new Double(0));
        
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("i", new Long(45));
        expected2.put("s", new ByteString("fourty-five"));
        expected2.put("f", new Double(4.2));

        assertEquals(expected1, c.get("kv", "k1"));
        assertEquals(expected2, c.get("kv", "k2"));
    }
}



