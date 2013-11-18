package org.hyperdex.client;

import java.util.*;
import java.math.*;
import java.io.*;

public class Pending
{
    protected Client client = null;
    protected long reqId = 0; 
    protected SWIGTYPE_p_hyperdex_client_returncode rc_ptr = null;
    protected boolean finished = false; 

    public Pending(Client client)
    {
        this.client = client;
        rc_ptr = hyperdex_client.new_rc_ptr();
        hyperdex_client.rc_ptr_assign(rc_ptr,hyperdex_client_returncode.HYPERDEX_CLIENT_GARBAGE);
    }

    public void callback()
    {
        finished = true;
        client.ops.remove(reqId);
    }

    public hyperdex_client_returncode status()
    {
        return hyperdex_client.rc_ptr_value(rc_ptr);
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        hyperdex_client.delete_rc_ptr(rc_ptr);
    }

    protected void checkReqId(long reqId, hyperdex_client_returncode status)
                                                        throws HyperDexClientException
    {
        if (reqId < 0)
        {
            throw new HyperDexClientException(status);
        }
    }

    protected void checkReqIdKeyAttrs(long reqId, hyperdex_client_returncode status,
                                        hyperdex_client_attribute attrs, long attrs_sz)
                                                            throws HyperDexClientException,
                                                                   TypeError
    {
	    if (reqId < 0)
	    {
            int idx = (int)(-2 - reqId);

            if ( attrs != null && idx >= 0 && idx < attrs_sz )
            {
                hyperdex_client_attribute ha = Client.get_attr(attrs,idx);
                throw new HyperDexClientException(status,ByteArray.decode(ha.getAttrNameBytes(),client.getDefaultStringEncoding()));
            }
            else
            {
                throw new HyperDexClientException(status);
            }
	    }
    }
	
    protected void checkReqIdKeyAttrs2(long reqId, hyperdex_client_returncode status,
                                       hyperdex_client_attribute_check attrs1, long attrs_sz1,
                                       hyperdex_client_attribute attrs2, long attrs_sz2)
                                                            throws HyperDexClientException,
                                                                   TypeError
    {
	    if (reqId < 0)
	    {
            int idx = (int)(-2 - reqId);
            String attrName = null;

            if ( attrs1 != null && idx >= 0 && idx < attrs_sz1 )
                attrName = ByteArray.decode(Client.get_attr_check(attrs1,idx).getAttrNameBytes(),client.getDefaultStringEncoding()); 

            idx -= attrs_sz1;

            if ( attrs2 != null && idx >= 0 && idx < attrs_sz2)
                attrName = ByteArray.decode(Client.get_attr(attrs2,idx).getAttrNameBytes(),client.getDefaultStringEncoding()); 

            if ( attrName != null )
            {
                throw new HyperDexClientException(status,attrName);
            }
            else
            {
                throw new HyperDexClientException(status);
            }
	    }
    }

    protected void checkReqIdKeyMapAttrs(long reqid, hyperdex_client_returncode status,
                                         hyperdex_client_map_attribute attrs, long attrs_sz)
                                                            throws HyperDexClientException,
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
                hyperdex_client_map_attribute hma = Client.get_map_attr(attrs,idx);
                throw new HyperDexClientException(status,
                            ByteArray.decode(hma.getMapAttrNameBytes(),
                                        client.getDefaultStringEncoding()));
            }
            else
            {
                throw new HyperDexClientException(status);
            }
	    }
    }

    protected void checkReqIdSearch(long reqId,  hyperdex_client_returncode status,
                                    hyperdex_client_attribute_check chks, long chks_sz)
                                                            throws HyperDexClientException,
                                                                   TypeError
    {
        if (reqId < 0)
        {
            long idx = -1 - reqId;

            String attrName = null;

            if ( chks != null && idx >= 0 && idx < chks_sz )
            {
                attrName = ByteArray.decode(
                            Client.get_attr_check(chks,idx).getAttrNameBytes(),
                            client.getDefaultStringEncoding()); 
        
                throw new HyperDexClientException(status,attrName);
            }
            else
            {
                throw new HyperDexClientException(status);
            }
        }
    }
}
