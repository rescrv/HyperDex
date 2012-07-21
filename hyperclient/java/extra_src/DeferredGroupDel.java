package hyperclient;

import java.util.*;

public class DeferredGroupDel extends Deferred
{
    public DeferredGroupDel(HyperClient client, String space, Map predicate)
                                                    throws HyperClientException,
                                                           TypeError,
                                                           ValueError,
                                                           MemoryError
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("DeferredGroupDel critera cannot be null");

        hyperclient_attribute eq = null;
        int eq_sz = 0;

        hyperclient_range_query rn = null;
        int rn_sz = 0;

        try
        {
            Vector retvals = HyperClient.predicate_to_c(predicate);
            
            eq = (hyperclient_attribute)(retvals.get(0));
            eq_sz = ((Integer)(retvals.get(1))).intValue();
            rn = (hyperclient_range_query)(retvals.get(2));
            rn_sz = ((Integer)(retvals.get(3))).intValue();

            reqId = client.group_del(space,
                                     eq, eq_sz,
                                     rn, rn_sz,
                                     rc_ptr);

	
            checkReqIdSearch(reqId, status(), eq, eq_sz, rn, rn_sz);

            client.ops.put(reqId,this);
        }
        finally
        {
            if ( eq != null ) HyperClient.free_attrs(eq, eq_sz);
            if ( rn != null ) HyperClient.free_range_queries(rn, rn_sz);
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
