package org.hyperdex.client;

import java.util.*;
import java.io.*;

public class DeferredCondPut extends Deferred
{
    public DeferredCondPut(Client client, Object space, Object key,
                                                            Map condition, Map value)
                                                            throws HyperDexClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        hyperdex_client_attribute_check condattrs = null;
        long condattrs_sz = 0;

        hyperdex_client_attribute attrs = null;
        long attrs_sz = 0;

        if ( condition == null )
            throw new ValueError("Condition critera cannot be null");

        try
        {
            Vector retvals = client.predicate_to_c(condition);
            
            condattrs = (hyperdex_client_attribute_check)(retvals.get(0));
            condattrs_sz = ((Long)(retvals.get(1))).longValue();

            attrs_sz = value.size();
            attrs = client.dict_to_attrs(value);
	
            reqId = client.cond_put(client.getBytes(space,true),
                                   client.getBytes(key),
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
            if ( condattrs != null ) Client.free_attrs_check(condattrs,condattrs_sz);
            if ( attrs != null ) Client.free_attrs(attrs,attrs_sz);
        }
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        super.waitFor();

        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_CMPFAIL)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }
}
