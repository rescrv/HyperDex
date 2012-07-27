package hyperclient;

import java.util.*;
import java.io.*;

public class DeferredCondPut extends Deferred
{
    public DeferredCondPut(HyperClient client, byte[] space, byte[] key,
                                                            Map condition, Map value)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError
    {
        super(client);

        hyperclient_attribute condattrs = null;
        long condattrs_sz = 0;

        hyperclient_attribute attrs = null;
        long attrs_sz = 0;

        try
        {
            condattrs_sz = condition.size();
            condattrs = HyperClient.dict_to_attrs(condition);

            attrs_sz = value.size();
            attrs = HyperClient.dict_to_attrs(value);
	
            reqId = client.condput(space,
                                   key,
                                   condattrs, condattrs_sz,
                                   attrs, attrs_sz,
                                   rc_ptr);

            checkReqIdKeyAttrs2(reqId, status(),
                                condattrs, condattrs_sz,
                                attrs, attrs_sz);

	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( condattrs != null ) HyperClient.free_attrs(condattrs,condattrs_sz);
            if ( attrs != null ) HyperClient.free_attrs(attrs,attrs_sz);
        }
    }

    public DeferredCondPut(HyperClient client, byte[] space, ByteArray key,
                                                            Map condition, Map value)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError
    {
        this(client, space, key.getBytes());
    }

    public DeferredCondPut(HyperClient client, byte[] space, String key,
                                                            Map condition, Map value)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError
    {
        this(client, space, ByteArray.encode(key,client.getDefaultStringEncoding()));
    }

    // Six more constructors

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();

        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status() == hyperclient_returncode.HYPERCLIENT_CMPFAIL)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }
}
