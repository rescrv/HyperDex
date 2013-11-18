package org.hyperdex.client;

import java.util.*;

public class Deferred extends Pending
{
    public Deferred(Client client)
    {
        super(client);
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        while (! finished && reqId > 0)
        {
            client.loop();
        }

        finished = true;
    
        return null;
    }
}
