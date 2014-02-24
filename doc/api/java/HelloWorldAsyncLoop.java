import java.util.*;
import org.hyperdex.client.*;

public class HelloWorldAsyncLoop
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client("127.0.0.1", 1982);
        /* put */
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", "Hello World!");
        Deferred dl;
        Deferred dp = c.async_put("kv", "k", attrs);
        dl = (Deferred)c.loop();
        assert dl == dp;
        System.out.println("put: " + dl.waitForIt());
        /* get */
        Deferred dg = c.async_get("kv", "k");
        dl = (Deferred)c.loop();
        assert dl == dg;
        System.out.println("got: " + dl.waitForIt());
    }
}
