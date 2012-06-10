package hyperclient;

import java.util.*;

public class HyperClientException extends Exception
{
    private static HashMap<ReturnCode,String> errorMap;
    static
    {
        errorMap = new HashMap<ReturnCode,String>();

		errorMap.put(ReturnCode.HYPERCLIENT_SUCCESS,"Success");
		errorMap.put(ReturnCode.HYPERCLIENT_NOTFOUND,"Not Found");
		errorMap.put(ReturnCode.HYPERCLIENT_SEARCHDONE,"Search Done");
		errorMap.put(ReturnCode.HYPERCLIENT_CMPFAIL,"Conditional Operation Did Not Match Object");
		errorMap.put(ReturnCode.HYPERCLIENT_READONLY,"Cluster is in a Read-Only State");
		errorMap.put(ReturnCode.HYPERCLIENT_UNKNOWNSPACE,"Unknown Space");
		errorMap.put(ReturnCode.HYPERCLIENT_COORDFAIL,"Coordinator Failure");
		errorMap.put(ReturnCode.HYPERCLIENT_SERVERERROR,"Server Error");
		errorMap.put(ReturnCode.HYPERCLIENT_POLLFAILED,"Polling Failed");
		errorMap.put(ReturnCode.HYPERCLIENT_OVERFLOW,"Integer-overflow or divide-by-zero");
		errorMap.put(ReturnCode.HYPERCLIENT_RECONFIGURE,"Reconfiguration");
		errorMap.put(ReturnCode.HYPERCLIENT_TIMEOUT,"Timeout");
		errorMap.put(ReturnCode.HYPERCLIENT_UNKNOWNATTR,"Unknown attribute '%s'");
		errorMap.put(ReturnCode.HYPERCLIENT_DUPEATTR,"Duplicate attribute '%s'");
		errorMap.put(ReturnCode.HYPERCLIENT_NONEPENDING,"None pending");
		errorMap.put(ReturnCode.HYPERCLIENT_DONTUSEKEY,"Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
		errorMap.put(ReturnCode.HYPERCLIENT_WRONGTYPE,"Attribute '%s' has the wrong type");
		errorMap.put(ReturnCode.HYPERCLIENT_NOMEM,"Memory allocation failed");
		errorMap.put(ReturnCode.HYPERCLIENT_EXCEPTION,"Internal Error (file a bug)");
    }

    public HyperClientException(ReturnCode status)
    {
        super(errorMap.get(status));
    }

    public HyperClientException(ReturnCode status, String attr)
    {
        super(String.format(errorMap.get(status),attr));
    }
}
