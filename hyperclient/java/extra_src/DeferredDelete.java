package hyperclient;

public class DeferredDelete extends Deferred
{
    public DeferredDelete(HyperClient client, String space, String key)
                                                    throws HyperClientException
    {
        super(client);

	    SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();
	
        try
        {
	        reqId = client.del(space, key, rc_int_ptr);
	
	        if (reqId < 0)
	        {
	            status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));
                throw new HyperClientException(status);
	        }

            client.ops.put(reqId,this);
        }
        finally
        {
            hyperclient.delete_int_ptr(rc_int_ptr);
        }
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();
        if (status == ReturnCode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status == ReturnCode.HYPERCLIENT_NOTFOUND)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperClientException(status);
        }
    }
}
