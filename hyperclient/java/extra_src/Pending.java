package hyperclient;

import java.util.*;

public class Pending
{
    protected HyperClient client = null;
    protected long reqId = 0; 
    ReturnCode status = ReturnCode.HYPERCLIENT_ZERO; // Give this package wide access
    protected boolean finished = false; 

    public Pending(HyperClient client)
    {
        this.client = client;
    }

    public void callback()
    {
        finished = true;
        client.ops.remove(reqId);
    }
}
