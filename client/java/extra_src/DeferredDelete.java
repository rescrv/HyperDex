package hyperclient;

public class DeferredDelete extends Deferred
{
    public DeferredDelete(HyperClient client, Object space, Object key)
                                                    throws HyperClientException,
                                                           TypeError
    {
        super(client);

        reqId = client.del(client.getBytes(space,true),
                           client.getBytes(key),
                           rc_ptr);

        checkReqId(reqId, status());

        client.ops.put(reqId,this);
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status() == hyperclient_returncode.HYPERCLIENT_NOTFOUND)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }
}
