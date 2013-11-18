package org.hyperdex.client;

import java.util.*;
import java.nio.*;

public class DeferredCount extends Deferred
{
    private SWIGTYPE_p_unsigned_long_long res_ptr = null;

    private int unsafe;

    public DeferredCount(Client client, Object space, Map predicate, boolean unsafe)
                                                            throws HyperDexClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        this.unsafe = unsafe?1:0;

        if ( predicate == null )
            throw new ValueError("DeferredCount critera cannot be null");

        hyperdex_client_attribute_check chks = null;
        long chks_sz = 0;

        try
        {
            Vector retvals = client.predicate_to_c(predicate);
            
            chks = (hyperdex_client_attribute_check)(retvals.get(0));
            chks_sz = ((Long)(retvals.get(1))).longValue();

            res_ptr = hyperdex_client.new_uint64_t_ptr();

            reqId = client.count(client.getBytes(space,true),
                                 chks, chks_sz,
                                 rc_ptr,
                                 res_ptr);

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

        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS || unsafe == 0)
        {
            return hyperdex_client.uint64_t_ptr_value(res_ptr);
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if (res_ptr != null) hyperdex_client.delete_uint64_t_ptr(res_ptr);
    }
}
