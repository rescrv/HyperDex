package hyperclient;

public class DeferredGet extends Deferred
{
    SWIGTYPE_p_p_hyperclient_attribute pattrs_ptr = 0;
    SWIGTYPE_p_size_t attrs_sz_ptr = 0;

    public DeferredGet(HyperClient client, String space, String key)
    {
        super(client);

        pattrs_ptr = hyperclient.new_phyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

        reqId = client.get(space, key, rc_int_ptr, pattrs_ptr, attrs_sz_ptr);

        status = ReturnCode.swigToEnum(rc_int_ptr.value());

        if (reqId < 0) throw new HyperClientException(status);

        client.opts.put(reqId,this);
    }

    public Object wait()
    {
    }
}
