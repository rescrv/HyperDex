package org.hyperdex.client;

import java.util.*;
import java.io.*;

public class DeferredGroupDel extends Deferred
{
    public DeferredGroupDel(Client client, Object space, Map predicate)
                                                            throws HyperDexClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("DeferredGroupDel critera cannot be null");

        hyperdex_client_attribute_check chks = null;
        long chks_sz = 0;

        try
        {
            Vector retvals = client.predicate_to_c(predicate);
            
            chks = (hyperdex_client_attribute_check)(retvals.get(0));
            chks_sz = ((Long)(retvals.get(1))).longValue();

            reqId = client.group_del(client.getBytes(space,true),
                                     chks, chks_sz,
                                     rc_ptr);

	
            checkReqIdSearch(reqId, status(), chks, chks_sz);

            client.ops.put(reqId,this);
        }
        finally
        {
            if ( chks != null ) Client.free_attrs_check(chks, chks_sz);
        }
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
