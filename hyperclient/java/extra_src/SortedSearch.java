package hyperclient;

import java.util.*;
import java.math.*;
import java.io.*;

public class SortedSearch extends SearchBase
{
    public SortedSearch(HyperClient client, String space, Map predicate,
                                                    String sortBy,
                                                    BigInteger limit,
                                                    boolean descending)
                                                    throws HyperClientException,
                                                           TypeError,
                                                           ValueError,
                                                           MemoryError,
                                                           UnsupportedEncodingException
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("Search critera cannot be null");

        try
        {
            Vector retvals = HyperClient.predicate_to_c(predicate);
            
            eq = (hyperclient_attribute)(retvals.get(0));
            eq_sz = ((Integer)(retvals.get(1))).intValue();
            rn = (hyperclient_range_query)(retvals.get(2));
            rn_sz = ((Integer)(retvals.get(3))).intValue();

            reqId = client.sorted_search(space,
                                         eq, eq_sz,
                                         rn, rn_sz,
                                         sortBy,
                                         limit,
                                         descending,
                                         rc_ptr,
                                         attrs_ptr, attrs_sz_ptr);

	
            checkReqIdSearch(reqId, status(), eq, eq_sz, rn, rn_sz);
	
            client.ops.put(reqId,this);
        }
        finally
        {
            if ( eq != null ) HyperClient.free_attrs(eq, eq_sz);
            if ( rn != null ) HyperClient.free_range_queries(rn, rn_sz);
        }
    }
}
