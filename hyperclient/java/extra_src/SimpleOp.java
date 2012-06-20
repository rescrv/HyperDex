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
        return client.put(space,
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
        return client.atomic_add(space,
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
        return client.atomic_sub(space,
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
        return client.atomic_mul(space,
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
        return client.atomic_div(space,
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
        return client.atomic_mod(space,
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
        return client.atomic_and(space,
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
        return client.atomic_or(space,
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
        return client.atomic_xor(space,
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
        return client.string_prepend(space,
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
        return client.string_append(space,
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
        return client.list_lpush(space,
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
        return client.list_rpush(space,
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
        return client.set_add(space,
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
        return client.set_remove(space,
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
        return client.set_intersect(space,
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
        return client.set_union(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
