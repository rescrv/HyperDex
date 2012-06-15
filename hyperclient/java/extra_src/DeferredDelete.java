package hyperclient;

public class DeferredDelete extends Deferred
{
    public DeferredDelete(HyperClient client, String space, String key)
                                                    throws HyperClientException
    {
        super(client);

	    reqId = client.del(space, key, rc_ptr);

        if (reqId < 0)
        {
            throw new HyperClientException(status());
	    }

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
