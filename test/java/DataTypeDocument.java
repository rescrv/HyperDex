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
    public void destroyHyperdexClient() {
        c = null;
    }
    
    @Test
    public void insertEmptyValue() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{}"));
        assertEquals(expected, get);
    }
    
    @Test
    public void insertEmptyDocument() throws HyperDexClientException {
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", new Document("{}"));
        
        Boolean res = c.put("kv", "k", attrs);
        assertTrue(res);
        
        Map<String, Object> get = c.get("kv", "k");
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("v", new Document("{}"));
        assertEquals(expected, get);
    }
}

