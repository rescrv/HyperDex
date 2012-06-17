package hyperclient;

import java.util.*;

abstract class SimpleOp
{
    protected HyperClient client;

    public SimpleOp(HyperClient client)
    {
        this.client = client;
    }

    abstract long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr);
}

class SimpleOpPut extends SimpleOp
{
    public SimpleOpPut(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_put(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicAdd extends SimpleOp
{
    public SimpleOpAtomicAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_add(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicSub extends SimpleOp
{
    public SimpleOpAtomicSub(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_sub(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicMul extends SimpleOp
{
    public SimpleOpAtomicMul(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_mul(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicDiv extends SimpleOp
{
    public SimpleOpAtomicDiv(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_div(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicMod extends SimpleOp
{
    public SimpleOpAtomicMod(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_mod(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicAnd extends SimpleOp
{
    public SimpleOpAtomicAnd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_and(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicOr extends SimpleOp
{
    public SimpleOpAtomicOr(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_or(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpAtomicXor extends SimpleOp
{
    public SimpleOpAtomicXor(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_xor(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpStringPrepend extends SimpleOp
{
    public SimpleOpStringPrepend(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_string_prepend(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpStringAppend extends SimpleOp
{
    public SimpleOpStringAppend(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_string_append(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpListLpush extends SimpleOp
{
    public SimpleOpListLpush(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_list_lpush(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpListRpush extends SimpleOp
{
    public SimpleOpListRpush(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_list_rpush(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpSetAdd extends SimpleOp
{
    public SimpleOpSetAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_set_add(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpSetRemove extends SimpleOp
{
    public SimpleOpSetRemove(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_set_remove(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpSetIntersect extends SimpleOp
{
    public SimpleOpSetIntersect(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_set_intersect(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}

class SimpleOpSetUnion extends SimpleOp
{
    public SimpleOpSetUnion(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_set_union(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}
