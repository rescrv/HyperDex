package org.hyperdex.client;

import java.util.*;

abstract class SimpleOp
{
    protected Client client;

    public SimpleOp(Client client)
    {
        this.client = client;
    }

    abstract long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError;
}

class SimpleOpPut extends SimpleOp
{
    public SimpleOpPut(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.put(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpPutIfNotExist extends SimpleOp
{
    public SimpleOpPutIfNotExist(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.put_if_not_exist(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicAdd extends SimpleOp
{
    public SimpleOpAtomicAdd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_add(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicSub extends SimpleOp
{
    public SimpleOpAtomicSub(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_sub(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicMul extends SimpleOp
{
    public SimpleOpAtomicMul(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_mul(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicDiv extends SimpleOp
{
    public SimpleOpAtomicDiv(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_div(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicMod extends SimpleOp
{
    public SimpleOpAtomicMod(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_mod(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicAnd extends SimpleOp
{
    public SimpleOpAtomicAnd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_and(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicOr extends SimpleOp
{
    public SimpleOpAtomicOr(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_or(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpAtomicXor extends SimpleOp
{
    public SimpleOpAtomicXor(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.atomic_xor(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpStringPrepend extends SimpleOp
{
    public SimpleOpStringPrepend(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.string_prepend(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpStringAppend extends SimpleOp
{
    public SimpleOpStringAppend(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.string_append(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpListLpush extends SimpleOp
{
    public SimpleOpListLpush(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.list_lpush(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpListRpush extends SimpleOp
{
    public SimpleOpListRpush(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.list_rpush(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpSetAdd extends SimpleOp
{
    public SimpleOpSetAdd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.set_add(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpSetRemove extends SimpleOp
{
    public SimpleOpSetRemove(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.set_remove(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpSetIntersect extends SimpleOp
{
    public SimpleOpSetIntersect(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.set_intersect(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpSetUnion extends SimpleOp
{
    public SimpleOpSetUnion(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.set_union(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class SimpleOpMapRemove extends SimpleOp
{
    public SimpleOpMapRemove(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_remove(client.getBytes(space,true),
                                 client.getBytes(key),
                                 attrs, attrs_sz,
                                 rc_ptr);
    }
}
