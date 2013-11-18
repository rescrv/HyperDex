package org.hyperdex.client;

import java.util.*;

public class HyperDexClientException extends Exception
{
    private static HashMap<hyperdex_client_returncode,String> errorMap;
    static
    {
        errorMap = new HashMap<hyperdex_client_returncode,String>();

		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS,"Success");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_NOTFOUND,"Not Found");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_SEARCHDONE,"Search Done");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_CMPFAIL,"Conditional Operation Did Not Match Object");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_READONLY,"Cluster is in a Read-Only State");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_UNKNOWNSPACE,"Unknown Space");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_COORDFAIL,"Coordinator Failure");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_SERVERERROR,"Server Error");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_POLLFAILED,"Polling Failed");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_OVERFLOW,"Integer-overflow or divide-by-zero");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_RECONFIGURE,"Reconfiguration");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_TIMEOUT,"Timeout");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_UNKNOWNATTR,"Unknown attribute '%s'");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_DUPEATTR,"Duplicate attribute '%s'");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_NONEPENDING,"None pending");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_DONTUSEKEY,"Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_WRONGTYPE,"Attribute '%s' has the wrong type");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_NOMEM,"Memory allocation failed");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_BADCONFIG,"The coordinator provided a malformed configuration");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_DUPLICATE,"The space already exists");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_INTERRUPTED,"Interrupted by a signal");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_CLUSTER_JUMP,"The cluster changed identities");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_COORD_LOGGED,"HYPERDEX_CLIENT_COORD_LOGGED");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_INTERNAL,"Internal Error (file a bug)");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_EXCEPTION,"Internal Exception (file a bug)");
		errorMap.put(hyperdex_client_returncode.HYPERDEX_CLIENT_GARBAGE,"Internal Corruption (file a bug)");
    }

    private hyperdex_client_returncode rc = hyperdex_client_returncode.HYPERDEX_CLIENT_GARBAGE;

    public HyperDexClientException(hyperdex_client_returncode rc)
    {
        super(errorMap.get(rc));
        this.rc = rc;
    }

    public HyperDexClientException(hyperdex_client_returncode rc, String attr)
    {
        super(String.format(errorMap.get(rc),attr));
        this.rc = rc;
    }

    public String symbol()
    {
        return rc.toString();
    }
}
