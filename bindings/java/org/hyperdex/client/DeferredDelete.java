package org.hyperdex.client;

public class DeferredDelete extends Deferred
{
    public DeferredDelete(Client client, Object space, Object key)
                                                    throws HyperDexClientException,
                                                           TypeError
    {
        super(client);

        reqId = client.del(client.getBytes(space,true),
                           client.getBytes(key),
                           rc_ptr);

        checkReqId(reqId, status());

        client.ops.put(reqId,this);
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_NOTFOUND)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }
}
