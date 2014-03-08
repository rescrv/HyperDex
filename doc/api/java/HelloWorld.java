import java.util.*;
import org.hyperdex.client.*;

public class HelloWorld
{
    public static void main(String[] args) throws HyperDexClientException
    {
        Client c = new Client("127.0.0.1", 1982);
        /* put */
        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put("v", "Hello World!");
        System.out.println("put: " + c.put("kv", "k", attrs));
        /* get */
        System.out.println("got: " + c.get("kv", "k"));
    }
}
