package hyperclient;

import java.util.*;
import java.io.*;

public class Search extends SearchBase
{
    public Search(HyperClient client, Object space, Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("Search critera cannot be null");

        try
        {
            Vector retvals = client.predicate_to_c(predicate);
            
            chks = (hyperclient_attribute_check)(retvals.get(0));
            chks_sz = ((Long)(retvals.get(1))).longValue();

            reqId = client.search(client.getBytes(space,true),
                                  chks, chks_sz,
                                  rc_ptr,
                                  attrs_ptr, attrs_sz_ptr);

	
            checkReqIdSearch(reqId, status(), chks, chks_sz);
	
            client.ops.put(reqId,this);
        }
        finally
        {
            if ( chks != null ) HyperClient.free_attrs_check(chks, chks_sz);
        }
    }
}
