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
        Map<String, Object> match_all = new HashMap<>();
        c.group_del("kv", match_all);
        c = null;
    }
    
    @Test
    public void singleOperation() throws HyperDexClientException {
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        xact.put(attrs);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void setField() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs2);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void setTwoFields() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        attrs2.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs2);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":\"c\",\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void setTwoFieldsIndependent() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("v.b", new Document("{\"x\":\"y\"}"));
        xact.put(attrs3);
        
        Boolean res = xact.commit("k");
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":\"c\",\"b\":{\"x\":\"y\"}}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void groupCommit() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{\"x\":\"y\"}"));
        c.put("kv", "k1", attrs);
        c.put("kv", "k2", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", "y"); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(2), res);
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"x\":\"y\",\"a\":\"c\"}"));
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> get2 = c.get("kv", "k2");
        
        assertEquals(expected, get1);
        assertEquals(expected, get2);
    }
    
    @Test
    public void condCommitWithMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{\"x\":\"y\"}"));
        c.put("kv", "k1", attrs);
        c.put("kv", "k2", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", "y"); 
        Boolean res = xact.cond_commit("k1", checks);
        assertEquals(true, res);
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("v", new Document("{\"x\":\"y\",\"a\":\"c\"}"));
        
        Map<String, Object> get2 = c.get("kv", "k2");
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("v", new Document("{\"x\":\"y\"}"));
        
        assertEquals(expected1, get1);
        assertEquals(expected2, get2);
    }
    
    @Test
    public void condCommitWithoutMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{\"x\":\"y\"}"));
        c.put("kv", "k1", attrs);
        c.put("kv", "k2", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", "z"); 
        Boolean res = xact.cond_commit("k1", checks);
        assertEquals(false, res);
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("v", new Document("{\"x\":\"y\"}"));
        
        Map<String, Object> get2 = c.get("kv", "k2");
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("v", new Document("{\"x\":\"y\"}"));
        
        assertEquals(expected1, get1);
        assertEquals(expected2, get2);
    }
    
    @Test
    public void groupCommitWithNoMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{\"x\":\"y\"}"));
        c.put("kv", "k1", attrs);
        c.put("kv", "k2", attrs);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "c");
        xact.put(attrs2);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", "xxx"); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(0), res);
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"x\":\"y\"}"));
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> get2 = c.get("kv", "k2");
        
        assertEquals(expected, get1);
        assertEquals(expected, get2);
    }
    
    @Test
    public void groupCommitWithOneMatch() throws HyperDexClientException {
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("v", new Document("{\"x\":\"a\"}"));
        c.put("kv", "k1", attrs1);
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v", new Document("{\"x\":\"b\"}"));
        c.put("kv", "k2", attrs2);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("v.a", "c");
        xact.put(attrs3);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", "a"); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(1), res);
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("v", new Document("{\"x\":\"a\",\"a\":\"c\"}"));
        
        Map<String, Object> get2 = c.get("kv", "k2");
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("v", new Document("{\"x\":\"b\"}"));
        
        assertEquals(expected1, get1);
        assertEquals(expected2, get2);
    }
    
    @Test
    public void groupCommitWithOneNumericMatch() throws HyperDexClientException {
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("v", new Document("{\"x\":1}"));
        c.put("kv", "k1", attrs1);
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v", new Document("{\"x\":2}"));
        c.put("kv", "k2", attrs2);
        
        Microtransaction xact = c.initMicrotransaction("kv");
        Map<String, Object> attrs3 = new HashMap<>();
        attrs3.put("v.a", "c");
        xact.put(attrs3);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.x", new Long(1)); 
        Long res = xact.group_commit(checks);
        assertEquals(new Long(1), res);
        
        Map<String, Object> get1 = c.get("kv", "k1");
        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("v", new Document("{\"x\":1,\"a\":\"c\"}"));
        
        Map<String, Object> get2 = c.get("kv", "k2");
        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("v", new Document("{\"x\":2}"));
        
        assertEquals(expected1, get1);
        assertEquals(expected2, get2);
    }
}


