package hyperclient;

import java.util.*;

public class Deferred
{
    protected HyperClient client = null;
    protected long reqId = 0; 
    ReturnCode status = ReturnCode.HYPERCLIENT_ZERO; // Give this package wide access
    protected boolean finished = false; 

    public Deferred(HyperClient client)
    {
        this.client = client;
    }

    public void callback()
    {
        finished = true;
        client.ops.remove(reqId);
    }

    public Object waitFor() throws HyperClientException
    {
        while (! finished && reqId > 0)
        {
            client.loop();
        }

        finished = true;
    
        return null;
    }
}
