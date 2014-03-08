import java.util.*;
import org.hyperdex.client.*;

public class HelloWorldAsyncWait
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client("127.0.0.1", 1982);
        /* put */
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", "Hello World!");
        Deferred dp = c.async_put("kv", "k", attrs);
        System.out.println("put: " + dp.waitForIt());
        /* get */
        Deferred dg = c.async_get("kv", "k");
        System.out.println("got: " + dg.waitForIt());
    }
}
