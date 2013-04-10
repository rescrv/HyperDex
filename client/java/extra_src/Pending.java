package hyperclient;

import java.util.*;
import java.math.*;
import java.io.*;

public class Pending
{
    protected HyperClient client = null;
    protected long reqId = 0; 
    protected SWIGTYPE_p_hyperclient_returncode rc_ptr = null;
    protected boolean finished = false; 

    public Pending(HyperClient client)
    {
        this.client = client;
        rc_ptr = hyperclient_lc.new_rc_ptr();
        hyperclient_lc.rc_ptr_assign(rc_ptr,hyperclient_returncode.HYPERCLIENT_GARBAGE);
    }

    public void callback()
    {
        finished = true;
        client.ops.remove(reqId);
    }

    public hyperclient_returncode status()
    {
        return hyperclient_lc.rc_ptr_value(rc_ptr);
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        hyperclient_lc.delete_rc_ptr(rc_ptr);
    }

    protected void checkReqId(long reqId, hyperclient_returncode status)
                                                        throws HyperClientException
    {
        if (reqId < 0)
        {
            throw new HyperClientException(status);
        }
    }

    protected void checkReqIdKeyAttrs(long reqId, hyperclient_returncode status,
                                        hyperclient_attribute attrs, long attrs_sz)
                                                            throws HyperClientException,
                                                                   TypeError
    {
	    if (reqId < 0)
	    {
            int idx = (int)(-2 - reqId);

            if ( attrs != null && idx >= 0 && idx < attrs_sz )
            {
                hyperclient_attribute ha = HyperClient.get_attr(attrs,idx);
                throw new HyperClientException(status,ByteArray.decode(ha.getAttrNameBytes(),client.getDefaultStringEncoding()));
            }
            else
            {
                throw new HyperClientException(status);
            }
	    }
    }
	
    protected void checkReqIdKeyAttrs2(long reqId, hyperclient_returncode status,
                                       hyperclient_attribute_check attrs1, long attrs_sz1,
                                       hyperclient_attribute attrs2, long attrs_sz2)
                                                            throws HyperClientException,
                                                                   TypeError
    {
	    if (reqId < 0)
	    {
            int idx = (int)(-2 - reqId);
            String attrName = null;

            if ( attrs1 != null && idx >= 0 && idx < attrs_sz1 )
                attrName = ByteArray.decode(HyperClient.get_attr_check(attrs1,idx).getAttrNameBytes(),client.getDefaultStringEncoding()); 

            idx -= attrs_sz1;

            if ( attrs2 != null && idx >= 0 && idx < attrs_sz2)
                attrName = ByteArray.decode(HyperClient.get_attr(attrs2,idx).getAttrNameBytes(),client.getDefaultStringEncoding()); 

            if ( attrName != null )
            {
                throw new HyperClientException(status,attrName);
            }
            else
            {
                throw new HyperClientException(status);
            }
	    }
    }

    protected void checkReqIdKeyMapAttrs(long reqid, hyperclient_returncode status,
                                         hyperclient_map_attribute attrs, long attrs_sz)
                                                            throws HyperClientException,
                                                                   TypeError
    {
	    if (reqId < 0)
	    {
            BigInteger idx_bi = BigInteger.valueOf(2).negate().subtract(
                                BigInteger.valueOf(reqId));

            long idx = idx_bi.longValue();
	
            if ( attrs != null && idx_bi.compareTo(BigInteger.ZERO) >= 0
                               && idx_bi.compareTo(attrs.getAttrsSz_bi()) < 0 )
            {
                hyperclient_map_attribute hma = HyperClient.get_map_attr(attrs,idx);
                throw new HyperClientException(status,
                            ByteArray.decode(hma.getMapAttrNameBytes(),
                                        client.getDefaultStringEncoding()));
            }
            else
            {
                throw new HyperClientException(status);
            }
	    }
    }

    protected void checkReqIdSearch(long reqId,  hyperclient_returncode status,
                                    hyperclient_attribute_check chks, long chks_sz)
                                                            throws HyperClientException,
                                                                   TypeError
    {
        if (reqId < 0)
        {
            long idx = -1 - reqId;

            String attrName = null;

            if ( chks != null && idx >= 0 && idx < chks_sz )
            {
                attrName = ByteArray.decode(
                            HyperClient.get_attr_check(chks,idx).getAttrNameBytes(),
                            client.getDefaultStringEncoding()); 
        
                throw new HyperClientException(status,attrName);
            }
            else
            {
                throw new HyperClientException(status);
            }
        }
    }
}
