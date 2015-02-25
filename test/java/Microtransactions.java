import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.Document;
import org.hyperdex.client.Microtransaction;
import org.hyperdex.client.HyperDexClientException;

import org.junit.*;
import static org.junit.Assert.* ;

public class Microtransactions
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
    public void singleOperation() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{}"));
        xact.put(attrs);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void setField() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs2);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void setTwoFields() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v.a", "c");
        attrs2.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs2);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{\"a\":\"c\",\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
    
        @Test
    public void setTwoFieldsIndependent() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<String, Object>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        Map<String, Object> attrs3 = new HashMap<String, Object>();
        attrs3.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs3);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{\"a\":\"c\",\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
}


