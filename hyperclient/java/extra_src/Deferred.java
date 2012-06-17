package hyperclient;

import java.util.*;

public class Deferred extends Pending
{
    public Deferred(HyperClient client)
    {
        super(client);
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        while (! finished && reqId > 0)
        {
            client.loop();
        }

        finished = true;
    
        return null;
    }
}
