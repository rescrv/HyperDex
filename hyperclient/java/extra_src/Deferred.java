package hyperclient;

import java.util.*;

public class Deferred
{
    private HyperClient client = null;
    private long reqId = 0; 
    private ReturnCode status = HYPERCLIENT_ZERO;
    private boolean finished = false; 

    public Deferred(HyperClient client)
    {
        this.client = client;
    }

    public void callback()
    {
        finished = true;
        client.opts.remove(reqId);
    }

    public Object wait()
    {
        while (! finished && reqId > 0)
        {
            client.loop();
        }

        finished = true;
    
        return null;
    }
}
