package org.hyperdex.client;

import java.util.*;

abstract class MapOp
{
    protected Client client;

    public MapOp(Client client)
    {
        this.client = client;
    }

    abstract long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError;
}

class MapOpAdd extends MapOp
{
    public MapOpAdd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_add(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicAdd extends MapOp
{
    public MapOpAtomicAdd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_add(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicSub extends MapOp
{
    public MapOpAtomicSub(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_sub(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicMul extends MapOp
{
    public MapOpAtomicMul(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_mul(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicDiv extends MapOp
{
    public MapOpAtomicDiv(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_div(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicMod extends MapOp
{
    public MapOpAtomicMod(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_mod(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicAnd extends MapOp
{
    public MapOpAtomicAnd(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_and(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicOr extends MapOp
{
    public MapOpAtomicOr(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_or(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicXor extends MapOp
{
    public MapOpAtomicXor(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_atomic_xor(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpStringPrepend extends MapOp
{
    public MapOpStringPrepend(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_string_prepend(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpStringAppend extends MapOp
{
    public MapOpStringAppend(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.map_string_append(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
