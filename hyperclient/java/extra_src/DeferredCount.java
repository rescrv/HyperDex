package hyperclient;

import java.util.*;
import java.nio.*;

public class DeferredCount extends Deferred
{
    private SWIGTYPE_p_unsigned_long_long res_ptr = null;

    private int unsafe;

    public DeferredCount(HyperClient client, byte[] space, Map predicate, boolean unsafe)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        this.unsafe = unsafe?1:0;

        if ( predicate == null )
            throw new ValueError("DeferredCount critera cannot be null");

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

            res_ptr = hyperclient.new_uint64_t_ptr();

            reqId = client.count(space,
                                 eq, eq_sz,
                                 rn, rn_sz,
                                 rc_ptr,
                                 res_ptr);

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

        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS || unsafe == 0)
        {
            return hyperclient.uint64_t_ptr_value(res_ptr);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if (res_ptr != null) hyperclient.delete_uint64_t_ptr(res_ptr);
    }
}
