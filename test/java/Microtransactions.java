import java.util.*;

import org.hyperdex.client.Client;
import org.hyperdex.client.Document;
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
    public void destroyHyperdexClient() {
        c = null;
    }
    
    @Test
    public void singleOperation() throws HyperDexClientException {
        Microtransaction utx = c.initMicrotransaction();
    }
}


