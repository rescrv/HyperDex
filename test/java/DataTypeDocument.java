import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.Document;
import org.hyperdex.client.HyperDexClientException;

import org.junit.*;
import static org.junit.Assert.* ;

public class DataTypeDocument
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
    public void testToString() {
        String str = "{\"a\":123,\"b\":\"x\"}";
        
        Document doc = new Document(str);
        
        assertEquals(str, doc.toString());
    }
    
    @Test
    public void insertEmptyValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{}"));
        assertEquals(expected, get);
    }
    
    @Test
    public void insertEmptyDocument() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void insertNestedDocument() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{\"a\" : { \"b\" : \"c\"}}"));
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        assert(get.get("v") instanceof Document);
        expected.put("v", new Document("{\"a\":{\"b\":\"c\"}}")); // be careful about formatting...
        assertEquals(expected, get);
    }
    
    @Test
    public void setField() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", "xyz");
        Boolean res = c.put("kv", "k", attrs2);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        assert(get.get("v") instanceof Document);
        expected.put("v", new Document("{\"a\":\"xyz\"}"));
        assertEquals(expected, get);
    }
    
    @Test
    public void setFieldWithDocument() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{}"));
        c.put("kv", "k", attrs);
        
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("v.a", new Document("{\"b\":\"xyz\"}"));
        Boolean res = c.put("kv", "k", attrs2);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":{\"b\":\"xyz\"}}"));
        
        assertEquals(expected, get);
    }
    
    @Test
    public void groupUpdateWithIntegerMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{\"a\":\"xxx\",\"b\":1}"));
        c.put("kv", "k", attrs);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.b", 1);
        Map<String, Object> mod = new HashMap<>();
        mod.put("v.a", "yy");
        
        Long res = c.group_string_append("kv", checks, mod);
        assertEquals(new Long(1), res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":\"xxxyy\",\"b\":1}"));
        assertEquals(get, expected);
    }
    
    @Test
    public void groupUpdateWithMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{\"a\":\"xxx\"}"));
        c.put("kv", "k", attrs);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.a", "xxx");
        Map<String, Object> mod = new HashMap<>();
        mod.put("v.a", "yy");
        
        Long res = c.group_string_append("kv", checks, mod);
        assertEquals(new Long(1), res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":\"xxxyy\"}"));
        assertEquals(get, expected);
    }
    
    @Test
    public void groupUpdateWithoutMatch() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("v", new Document("{\"a\":\"xxx\"}"));
        c.put("kv", "k", attrs);
        
        Map<String, Object> checks = new HashMap<>();
        checks.put("v.a", "xx");
        Map<String, Object> mod = new HashMap<>();
        mod.put("v.a", "yy");
        
        Long res = c.group_string_append("kv", checks, mod);
        assertEquals(new Long(0), res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<>();
        expected.put("v", new Document("{\"a\":\"xxx\"}"));
        assertEquals(get, expected);
    }
}

