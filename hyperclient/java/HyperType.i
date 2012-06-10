// Write our own (safe) getters and setters for attr and value
// in order to avoid warnings
//
%ignore hyperclient_attribute::attr;
%ignore hyperclient_attribute::value;

%typemap(javacode) hyperclient_attribute
%{
  public String getAttrName()
  {
    return HyperClient.read_attr_name(this);
  }

  private byte[] getAttrValueBytes()
  {
    int bytes_sz = HyperClient.size_t_to_int(getValue_sz());

    return getAttrValueBytes(0,bytes_sz);
  }

  private byte[] getAttrValueBytes(long pos, int size)
  {
    byte[] bytes = new byte[size];
    HyperClient.read_attr_value(this,bytes,pos);
    return bytes;
  }

  public java.lang.Object getAttrStringValue()
  {
    return new String(getAttrValueBytes());
  }

  private java.lang.Object getAttrLongValue()
  {
    return
      new Long(
        java.nio.ByteBuffer.wrap(getAttrValueBytes()).order(
                    java.nio.ByteOrder.LITTLE_ENDIAN).getLong());
  }

  private java.lang.Object getAttrCollectionStringValue(
            java.util.AbstractCollection<String> coll) throws ValueError
  {
    String collType = coll instanceof java.util.List?"list":"set"; 

    // Interpret return value of getValue_sz() as unsigned
    //
    java.math.BigInteger value_sz 
        = new java.math.BigInteger(1,java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(getValue_sz()).array());

    java.math.BigInteger rem = new java.math.BigInteger(value_sz.toString());

    java.math.BigInteger four = new java.math.BigInteger("4");

    int c_size = 0; 

    while ( rem.compareTo(four) >= 0 && c_size <= Integer.MAX_VALUE )
    {
        long pos = value_sz.subtract(rem).longValue();
        int e_size
          = java.nio.ByteBuffer.wrap(getAttrValueBytes(pos,4)).order(
                        java.nio.ByteOrder.LITTLE_ENDIAN).getInt();

        java.math.BigInteger e_size_bi
          = new java.math.BigInteger(new Integer(e_size).toString());

        if ( rem.subtract(four).compareTo(e_size_bi) < 0 ) 
        {
            throw new ValueError(collType
                        + "(string) is improperly structured (file a bug)");
        }
   
        coll.add(new String(getAttrValueBytes(pos+4,e_size)));

        rem = rem.subtract(four).subtract(e_size_bi);        
        c_size += 1;
    }

    if ( c_size < Integer.MAX_VALUE && rem.compareTo(java.math.BigInteger.ZERO) > 0 )
    {
        throw new ValueError(collType + "(string) contains excess data (file a bug)");
    }    

    return coll;
  }

  public java.lang.Object getAttrValue() throws ValueError
  {
    switch(getDatatype())
    {
      case HYPERDATATYPE_STRING:
        return getAttrStringValue();

      case HYPERDATATYPE_INT64:
        return getAttrLongValue();

      case HYPERDATATYPE_LIST_STRING:
        java.util.Vector<String> v = new java.util.Vector<String>();
        getAttrCollectionStringValue(v);
        return v;

      case HYPERDATATYPE_SET_STRING:
        java.util.HashSet<String> s = new java.util.HashSet<String>();
        getAttrCollectionStringValue(s);
        return s;

      default:
        return null;
    }
  }
%}

%typemap(javacode) HyperClient
%{
  java.util.HashMap<Long,Deferred> ops = new java.util.HashMap<Long,Deferred>(); 

  void loop() throws HyperClientException
  {
    SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

    long ret = loop(rc_int_ptr);

    ReturnCode rc = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));

    if ( ret < 0 )
    {
      throw new HyperClientException(rc);
    }
    else
    {
      Deferred d = ops.get(ret);
      d.status = rc;
      d.callback();
    }
  }

  // Deal's with Java's int size limit for objects.
  // Return the int closest to size_t representation
  // Ideally, in a future java version, this method will
  // return the same value ie., do nothing or simply be removed.
  //
  static int size_t_to_int(long l)
  {
    int i = 0;

    if ( l < 0 || l > Integer.MAX_VALUE )
    {
        i = Integer.MAX_VALUE;
    }
    else
    {
        i = (int)l;
    }

    return i;
  }

  static java.util.Map attrs_to_dict(hyperclient_attribute attrs, long attrs_sz)
                                                                throws ValueError
  {
    java.util.HashMap<Object,Object> map = new java.util.HashMap<Object,Object>();

    int sz = HyperClient.size_t_to_int(attrs_sz);

    for ( int i=0; i<sz; i++)
    {
        hyperclient_attribute ha = get_attr(attrs,i);
        map.put(ha.getAttrName(),ha.getAttrValue());
    }

    return map;
  }

  static hyperclient_attribute dict_to_attrs(java.util.Map map) throws TypeError,
                                                                        MemoryError
  {
    int attrs_sz = map.size();
    hyperclient_attribute attrs = alloc_attrs(attrs_sz);

    if ( attrs == null ) throw new MemoryError();
    
    int i = 0;

    for (java.util.Iterator it=map.keySet().iterator(); it.hasNext();)
    {
        hyperclient_attribute ha = get_attr(attrs,i);

        String attrStr = (String)(it.next());
        byte[] attrBytes = attrStr.getBytes();

        Object value = map.get(attrStr);

        try
        {
            hyperdatatype type;

            if ( value instanceof String )
            {
                type = hyperdatatype.HYPERDATATYPE_STRING;
                byte[] valueBytes = ((String)value).getBytes();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
    
            }
            else if ( value instanceof Long )
            {
                type = hyperdatatype.HYPERDATATYPE_INT64;
                byte[] valueBytes = java.nio.ByteBuffer.allocate(8).order(
                    java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                        ((Long)value).longValue()).array();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
            }
            else
            {
                throw
                    new TypeError("Unsupported type: " + value.getClass().getName());
            }

            if (write_attr_name(ha, attrBytes, type) == 0) throw new MemoryError();
        }
        catch(MemoryError me)
        {
            destroy_attrs(attrs, attrs_sz);
            throw me;
        }
        catch(TypeError te)
        {
            destroy_attrs(attrs, attrs_sz);
            throw te;
        }

        i++;
    }

    return attrs;
  }

  public Deferred async_get(String space, String key) throws HyperClientException,
                                                             ValueError
  {
    return new DeferredGet(this,space, key);
  }

  public java.util.Map get(String space, String key) throws HyperClientException,
                                                            ValueError
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
  }

  public Deferred async_put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpPut(this), space, key, map);
  }

  public boolean put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_put(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }
%}
