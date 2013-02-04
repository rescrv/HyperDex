package hyperclient;

import java.util.*;
import java.io.*;

public class DeferredGroupDel extends Deferred
{
    public DeferredGroupDel(HyperClient client, Object space, Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("DeferredGroupDel critera cannot be null");

        hyperclient_attribute_check chks = null;
        long chks_sz = 0;

        try
        {
            Vector retvals = client.predicate_to_c(predicate);
            
            chks = (hyperclient_attribute_check)(retvals.get(0));
            chks_sz = ((Long)(retvals.get(1))).longValue();

            reqId = client.group_del(client.getBytes(space,true),
                                     chks, chks_sz,
                                     rc_ptr);

	
            checkReqIdSearch(reqId, status(), chks, chks_sz);

            client.ops.put(reqId,this);
        }
        finally
        {
            if ( chks != null ) HyperClient.free_attrs_check(chks, chks_sz);
        }
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
