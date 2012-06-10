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
