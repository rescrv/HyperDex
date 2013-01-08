package hyperclient;

import java.util.*;

public class HyperClientException extends Exception
{
    private static HashMap<hyperclient_returncode,String> errorMap;
    static
    {
        errorMap = new HashMap<hyperclient_returncode,String>();

		errorMap.put(hyperclient_returncode.HYPERCLIENT_SUCCESS,"Success");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_NOTFOUND,"Not Found");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_SEARCHDONE,"Search Done");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_CMPFAIL,"Conditional Operation Did Not Match Object");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_READONLY,"Cluster is in a Read-Only State");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_UNKNOWNSPACE,"Unknown Space");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_COORDFAIL,"Coordinator Failure");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_SERVERERROR,"Server Error");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_POLLFAILED,"Polling Failed");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_OVERFLOW,"Integer-overflow or divide-by-zero");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_RECONFIGURE,"Reconfiguration");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_TIMEOUT,"Timeout");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_UNKNOWNATTR,"Unknown attribute '%s'");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_DUPEATTR,"Duplicate attribute '%s'");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_NONEPENDING,"None pending");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_DONTUSEKEY,"Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_WRONGTYPE,"Attribute '%s' has the wrong type");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_NOMEM,"Memory allocation failed");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_BADCONFIG,"The coordinator provided a malformed configuration");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_BADSPACE,"The space description does not parse");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_DUPLICATE,"The space already exists");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_INTERNAL,"Internal Error (file a bug)");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_EXCEPTION,"Internal Exception (file a bug)");
		errorMap.put(hyperclient_returncode.HYPERCLIENT_GARBAGE,"Internal Corruption (file a bug)");
    }

    private hyperclient_returncode rc = hyperclient_returncode.HYPERCLIENT_GARBAGE;

    public HyperClientException(hyperclient_returncode rc)
    {
        super(errorMap.get(rc));
        this.rc = rc;
    }

    public HyperClientException(hyperclient_returncode rc, String attr)
    {
        super(String.format(errorMap.get(rc),attr));
        this.rc = rc;
    }

    public String symbol()
    {
        return rc.toString();
    }
}
