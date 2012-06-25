package hyperclient;

import java.util.*;

public class Pending
{
    protected HyperClient client = null;
    protected long reqId = 0; 
    protected SWIGTYPE_p_hyperclient_returncode rc_ptr = null;
    protected boolean finished = false; 

    public Pending(HyperClient client)
    {
        this.client = client;
        rc_ptr = hyperclient.new_rc_ptr();
    }

    public void callback()
    {
        finished = true;
        client.ops.remove(reqId);
    }

    public hyperclient_returncode status()
    {
        if ( rc_ptr != null )
        {
            return hyperclient.rc_ptr_value(rc_ptr);
        }
        else
        {
            return hyperclient_returncode.HYPERCLIENT_ZERO;
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        hyperclient.delete_rc_ptr(rc_ptr);
    }
}
