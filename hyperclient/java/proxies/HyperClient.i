%extend hyperclient
{
    static std::string read_attr_name(hyperclient_attribute *ha)
    {
        std::string str = std::string(ha->attr);
    
        if (str.length() > INT_MAX)
        {
            return str.substr(0,INT_MAX);
        }
    
        return str;
    }
    
    static void read_attr_value(hyperclient_attribute *ha,
                                   char *value, size_t value_sz,
                                   size_t pos)
    {
        size_t available = ha->value_sz - pos;
        memcpy(value, ha->value+pos, value_sz<available?value_sz:available);
    }
    
    static std::string read_map_attr_name(hyperclient_map_attribute *hma)
    {
        std::string str = std::string(hma->attr);
    
        if (str.length() > INT_MAX)
        {
            return str.substr(0,INT_MAX);
        }
    
        return str;
    }
    
    static std::string read_range_query_attr_name(hyperclient_range_query *rq)
    {
        std::string str = std::string(rq->attr);
    
        if (str.length() > INT_MAX)
        {
            return str.substr(0,INT_MAX);
        }
    
        return str;
    }
    
    static hyperclient_attribute *alloc_attrs(size_t attrs_sz)
    {
        return (hyperclient_attribute *)calloc(attrs_sz,sizeof(hyperclient_attribute));
    }
    
    static void free_attrs(hyperclient_attribute *attrs, size_t attrs_sz)
    {
        for (size_t i=0; i<attrs_sz; i++)
        {
            if (attrs[i].attr) free((void*)(attrs[i].attr));
            if (attrs[i].value) free((void*)(attrs[i].value));
        }
    
        free(attrs);
    }
    
    static hyperclient_map_attribute *alloc_map_attrs(size_t attrs_sz)
    {
        return (hyperclient_map_attribute *)calloc(attrs_sz,
                                                   sizeof(hyperclient_map_attribute));
    }
    
    static void free_map_attrs(hyperclient_map_attribute *attrs, size_t attrs_sz)
    {
        for (size_t i=0; i<attrs_sz; i++)
        {
            if (attrs[i].attr) free((void*)(attrs[i].attr));
            if (attrs[i].map_key) free((void*)(attrs[i].map_key));
            if (attrs[i].value) free((void*)(attrs[i].value));
        }
    
        free(attrs);
    }
    
    static hyperclient_range_query *alloc_range_queries(size_t rqs_sz)
    {
        return (hyperclient_range_query *)calloc(rqs_sz,
                                                   sizeof(hyperclient_range_query));
    }
    
    static void free_range_queries(hyperclient_range_query *rqs, size_t rqs_sz)
    {
        for (size_t i=0; i<rqs_sz; i++)
        {
            if (rqs[i].attr) free((void*)(rqs[i].attr));
        }
    
        free(rqs);
    }
    
    // Returns 1 on success. ha->attr will point to allocated memory
    // Returns 0 on failure. ha->attr will be NULL
    static int write_attr_name(hyperclient_attribute *ha,
                                   const char *attr, size_t attr_sz,
                                   hyperdatatype type)
    {
        char *buf;
    
        if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
        memcpy(buf,attr,attr_sz);
        ha->attr = buf;
        ha->datatype = type;
        return 1;
    }
    
    // Returns 1 on success. ha->value will point to allocated memory
    //                       ha->value_sz will hold the size of this memory
    // Returns 0 on failure. ha->value will be NULL
    //                       ha->value_sz will be 0
    //
    // If ha->value is already non-NULL, then we are appending to it. 
    static int write_attr_value(hyperclient_attribute *ha,
                                    const char *value, size_t value_sz)
    {
        char *buf = NULL;
        // Note: Since hyperclient_attribute array was calloced
        //       ha->value = NULL and ha->value_sz = 0 initially
        if ((buf = (char *)realloc((void *)(ha->value), ha->value_sz + value_sz))
                                                                        == NULL) return 0;
        memcpy(buf + ha->value_sz, value, value_sz);
        ha->value = buf;
        ha->value_sz += value_sz;
        return 1;
    }
    
    // Returns 1 on success. hma->attr will point to allocated memory
    // Returns 0 on failure. hma->attr will be NULL
    static int write_map_attr_name(hyperclient_map_attribute *hma,
                                   const char *attr, size_t attr_sz)
    {
        char *buf;
    
        if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
        memcpy(buf,attr,attr_sz);
        hma->attr = buf;
        return 1;
    }
    
    // Returns 1 on success. hma->map_key will point to allocated memory
    //                       hma->map_key_sz will hold the size of this memory
    // Returns 0 on failure. hma->map_key will be NULL
    //                       hma->map_key_sz will be 0
    //
    // If hma->value is already non-NULL, then we are appending to it. 
    static int write_map_attr_map_key(hyperclient_map_attribute *hma,
                                          const char *map_key, size_t map_key_sz,
                                          hyperdatatype type)
    {
        char *buf = NULL;
        // Note: Since hyperclient_map_attribute array was calloced
        //       hma->map_key = NULL and hma->map_key_sz = 0 initially
        if ((buf = (char *)realloc((void *)(hma->map_key), hma->map_key_sz + map_key_sz))
                                                                        == NULL) return 0;
        memcpy(buf + hma->map_key_sz, map_key, map_key_sz);
        hma->map_key = buf;
        hma->map_key_sz += map_key_sz;
        hma->map_key_datatype = type;
        return 1;
    }
    
    // Returns 1 on success. hma->value will point to allocated memory
    //                       hma->value_sz will hold the size of this memory
    // Returns 0 on failure. hma->value will be NULL
    //                       hma->value_sz will be 0
    //
    // If hma->value is already non-NULL, then we are appending to it. 
    static int write_map_attr_value(hyperclient_map_attribute *hma,
                                    const char *value, size_t value_sz,
                                    hyperdatatype type)
    {
        char *buf = NULL;
        // Note: Since hyperclient_map_attribute array was calloced
        //       hma->value = NULL and hma->value_sz = 0 initially
        if ((buf = (char *)realloc((void *)(hma->value), hma->value_sz + value_sz))
                                                                        == NULL) return 0;
        memcpy(buf + hma->value_sz, value, value_sz);
        hma->value = buf;
        hma->value_sz += value_sz;
        hma->value_datatype = type;
        return 1;
    }
    
    static int write_range_query(hyperclient_range_query *rq,
                                   const char *attr, size_t attr_sz,
                                   int64_t lower,
                                   int64_t upper)
    {
        char *buf;
    
        if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
        memcpy(buf,attr,attr_sz);
        rq->attr = buf;
        rq->lower = lower;
        rq->upper = upper;
        return 1;
    }
    
    static hyperclient_attribute *get_attr(hyperclient_attribute *ha, size_t i)
    {
        return ha + i;
    }
    
    static hyperclient_map_attribute *get_map_attr(hyperclient_map_attribute *hma, size_t i)
    {
        return hma + i;
    }
    
    static hyperclient_range_query *get_range_query(hyperclient_range_query *rqs, size_t i)
    {
        return rqs + i;
    }
};

%typemap(javacode) hyperclient
%{
  java.util.HashMap<Long,Pending> ops = new java.util.HashMap<Long,Pending>(); 

  void loop() throws HyperClientException
  {
    SWIGTYPE_p_hyperclient_returncode rc_ptr = hyperclient.new_rc_ptr();

    long ret = loop(-1, rc_ptr);

    hyperclient_returncode rc = hyperclient.rc_ptr_value(rc_ptr);

    hyperclient.delete_rc_ptr(rc_ptr); // Deallocate the pointer

    if ( ret < 0 )
    {
      throw new HyperClientException(rc);
    }
    else
    {
      Pending p = ops.get(ret);
      p.callback();
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

  private static hyperdatatype validateMapType(hyperdatatype type,
                                               String attrStr,
                                               Object key, Object val)
                                                                throws TypeError
  {
    hyperdatatype retType = type;  

    if ( type ==  hyperdatatype.HYPERDATATYPE_MAP_GENERIC )
    {
        // Initialize map type
        //

        if ( key instanceof String && val instanceof String )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING;
        }    
        else if ( key instanceof String && val instanceof Long )
        {
            retType =  hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64;
        }    
        else if ( key instanceof String && val instanceof Double )
        {
            retType =  hyperdatatype.HYPERDATATYPE_MAP_STRING_FLOAT;
        }    
        else if ( key instanceof Long && val instanceof String )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_INT64_STRING;
        }    
        else if ( key instanceof Long && val instanceof Long )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_INT64_INT64;
        }    
        else if ( key instanceof Long && val instanceof Double )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_INT64_FLOAT;
        }    
        else if ( key instanceof Double && val instanceof String )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_FLOAT_STRING;
        }    
        else if ( key instanceof Double && val instanceof Long )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_FLOAT_INT64;
        }    
        else if ( key instanceof Double && val instanceof Double )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_FLOAT_FLOAT;
        }    
        else
        {
            throw new TypeError(
                "Do not know how to convert map type '("
                    + key.getClass().getName() + ","
                    + val.getClass().getName() + ")"
                    + "' for map attribute '" + attrStr + "'");
        }
    }
    else
    {
        // Make sure it is always this map type (not heterogenious)
        //
        if (  (key instanceof String && val instanceof String 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING)
            ||
              (key instanceof String && val instanceof Long 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64)
            ||
              (key instanceof String && val instanceof Double 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_FLOAT)
            ||
              (key instanceof Long && val instanceof String 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_STRING)
            ||
              (key instanceof Long && val instanceof Long 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_INT64)
            ||
              (key instanceof Long && val instanceof Double
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_FLOAT)
            ||
              (key instanceof Double && val instanceof String 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_FLOAT_STRING)
            ||
              (key instanceof Double && val instanceof Long 
                 && type != hyperdatatype.HYPERDATATYPE_MAP_FLOAT_INT64)
            ||
              (key instanceof Double && val instanceof Double
                 && type != hyperdatatype.HYPERDATATYPE_MAP_FLOAT_FLOAT)
        )
        {
            throw new TypeError("Cannot store heterogeneous maps");
        }
    }

    return retType;
  }

  // Using a Vector retvals to return multiple values.
  //
  // retvals at 0 - eq
  // retvals at 1 - eq_sz
  // retvals at 2 - rn
  // retvals at 3 - rn_sz
  static java.util.Vector predicate_to_c(java.util.Map predicate)
                                            throws TypeError, MemoryError, ValueError
  {
      java.util.Vector<Object> retvals = new java.util.Vector<Object>(4); 

      retvals.add(null);
      retvals.add(new Integer(0));
      retvals.add(null);
      retvals.add(new Integer(0));

      java.util.HashMap<String,Object> equalities
            = new java.util.HashMap<String,Object>();

      java.util.HashMap<String,Object> ranges
            = new java.util.HashMap<String,Object>();

      for (java.util.Iterator it=predicate.keySet().iterator(); it.hasNext();)
      {
          String key = (String)(it.next());

          if ( key == null )
              throw new TypeError("Cannot search on a null attribute");

          Object val = predicate.get(key);

          if ( val == null )
              throw new TypeError("Cannot search with a null criteria");


          String errStr = "Attribute '" + key + "' has incorrect type ( expected Long, String, Map.Entry<Long,Long> or List<Long> (of size 2), but got %s";

          if ( val instanceof String || val instanceof Long)
          {
              equalities.put(key,val);
          }
          else
          {
              if ( val instanceof java.util.Map.Entry )
              {
                  try
                  {
                      long lower
                        = ((Long)((java.util.Map.Entry)val).getKey()).longValue();

                      long upper
                        = ((Long)((java.util.Map.Entry)val).getValue()).longValue();
                  }
                  catch(Exception e)
                  {
                      throw
                          new TypeError(String.format(errStr,val.getClass().getName()));
                  }
              }
              else if ( val instanceof java.util.List )
              {
                  try
                  {
                      java.util.List listVal = (java.util.List)val;
      
                      if ( listVal.size() != 2 )
                          throw new TypeError("Attribute '" + key + "': using a List to specify a range requires its size to be 2, but got size " + listVal.size());  
                  }
                  catch (TypeError te)
                  {
                      throw te;
                  }
                  catch (Exception e)
                  {
                      throw
                          new TypeError(
                              String.format(errStr,val.getClass().getName()));
                  }
              }
              else
              {
                  throw
                      new TypeError(String.format(errStr,val.getClass().getName()));
              }

              ranges.put(key,val);
          }
      }

      if ( equalities.size() > 0 )
      {
          
          retvals.set(0, dict_to_attrs(equalities));
          retvals.set(1, equalities.size());
      }

      if ( ranges.size() > 0 )
      {
          int rn_sz = ranges.size();
          hyperclient_range_query rn = HyperClient.alloc_range_queries(rn_sz);

          if ( rn == null ) throw new MemoryError();

          retvals.set(2, rn);
          retvals.set(3, ranges.size());

          int i = 0;
          for (java.util.Iterator it=ranges.keySet().iterator(); it.hasNext();)
          {
              String key = (String)(it.next());
              Object val = ranges.get(key);

              long lower = 0;
              long upper = 0;

              if ( val instanceof java.util.Map.Entry )
              {
                  lower = (Long)(((java.util.Map.Entry)val).getKey());
                  upper = (Long)(((java.util.Map.Entry)val).getValue());
              }
              else // Must be a List of Longs of size = 2
              {
                  lower = (Long)(((java.util.List)val).get(0));
                  upper = (Long)(((java.util.List)val).get(1));
              }
      
              hyperclient_range_query rq = HyperClient.get_range_query(rn,i);

              if ( HyperClient.write_range_query(rq,key.getBytes(),lower,upper) == 0 )
                  throw new MemoryError();

              i++;
          }
      }

      if ( retvals.get(0) == null && retvals.get(2) == null )
            throw new ValueError("Search criteria can't be empty");

      return retvals;
  }

  static hyperclient_attribute dict_to_attrs(java.util.Map attrsMap) throws TypeError,
                                                                            MemoryError
  {
    int attrs_sz = attrsMap.size();
    hyperclient_attribute attrs = alloc_attrs(attrs_sz);
 
    if ( attrs == null ) throw new MemoryError();
    
    try
    {
        int i = 0;
    
        for (java.util.Iterator it=attrsMap.keySet().iterator(); it.hasNext();)
        {
            hyperclient_attribute ha = get_attr(attrs,i);
    
            String attrStr = (String)(it.next());
            if ( attrStr == null ) throw new TypeError("Attribute name cannot be null");
    
            byte[] attrBytes = attrStr.getBytes();
    
            Object value = attrsMap.get(attrStr);
    
        
            if ( value == null ) throw new TypeError(
                                        "Cannot convert null value "
                                    + "for attribute '" + attrStr + "'");
    
            hyperdatatype type = null;
    
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
            else if ( value instanceof Double )
            {
                type = hyperdatatype.HYPERDATATYPE_FLOAT;
                byte[] valueBytes = java.nio.ByteBuffer.allocate(8).order(
                    java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(
                        ((Double)value).doubleValue()).array();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
            }
            else if ( value instanceof java.util.List )
            {
                java.util.List list = (java.util.List)value;
    
                type = hyperdatatype.HYPERDATATYPE_LIST_GENERIC;
    
                for (java.util.Iterator l_it=list.iterator();l_it.hasNext();)
                {
                    Object val = l_it.next();
    
                    if ( val == null ) throw new TypeError(
                                        "Cannot convert null element "
                                    + "for list attribute '" + attrStr + "'");
    
                    if (type == hyperdatatype.HYPERDATATYPE_LIST_GENERIC)
                    {
                        if (val instanceof String)
                            type = hyperdatatype.HYPERDATATYPE_LIST_STRING;
                        else if (val instanceof Long)
                            type = hyperdatatype.HYPERDATATYPE_LIST_INT64;
                        else if (val instanceof Double)
                            type = hyperdatatype.HYPERDATATYPE_LIST_FLOAT;
                        else
                            throw new TypeError(
                                "Do not know how to convert type '"
                                    + val.getClass().getName()
                                    + "' for list attribute '" + attrStr + "'");
                    }
                    else
                    {
                        if ( (val instanceof String
                                && type != hyperdatatype.HYPERDATATYPE_LIST_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_LIST_INT64)
                            || (val instanceof Double
                                && type != hyperdatatype.HYPERDATATYPE_LIST_FLOAT) )
    
                            throw new TypeError("Cannot store heterogeneous lists");
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_STRING )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_INT64 )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_FLOAT )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putDouble(((Double)val).doubleValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else if ( value instanceof java.util.Set )
            {
                // XXX HyderDex seems to quietly fail if set is not sorted
                // So I make sure a TreeSet ie., sorted set is used.
                // I was wondering why the python binding went through 
                // the trouble of sorting a set right before packing it.
    
                java.util.Set<?> set = (java.util.Set<?>)value;
    
                if ( ! (set instanceof java.util.SortedSet) )
                {
                    set = new java.util.TreeSet<Object>(set);
                }
    
                type = hyperdatatype.HYPERDATATYPE_SET_GENERIC;
    
                for (java.util.Iterator s_it=set.iterator();s_it.hasNext();)
                {
                    Object val = s_it.next();
    
                    if ( val == null ) throw new TypeError(
                                        "Cannot convert null element "
                                    + "for set attribute '" + attrStr + "'");
    
                    if (type == hyperdatatype.HYPERDATATYPE_SET_GENERIC)
                    {
                        if (val instanceof String)
                            type = hyperdatatype.HYPERDATATYPE_SET_STRING;
                        else if (val instanceof Long)
                            type = hyperdatatype.HYPERDATATYPE_SET_INT64;
                        else if (val instanceof Double)
                            type = hyperdatatype.HYPERDATATYPE_SET_FLOAT;
                        else
                            throw new TypeError(
                                "Do not know how to convert type '"
                                    + val.getClass().getName()
                                    + "' for set attribute '" + attrStr + "'");
                    }
                    else
                    {
                        if ( (val instanceof String
                                && type != hyperdatatype.HYPERDATATYPE_SET_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_SET_INT64)
                            || (val instanceof Double
                                && type != hyperdatatype.HYPERDATATYPE_SET_FLOAT) )
    
                            throw new TypeError("Cannot store heterogeneous sets");
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_SET_STRING )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_SET_INT64 )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_SET_FLOAT )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putDouble(((Double)val).doubleValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else if ( value instanceof java.util.Map )
            {
                java.util.Map<?,?> map = (java.util.Map<?,?>)value;
    
                // As for set types, the same goes for map type. HyperDex will
                // scoff unless the map is sorted
                //
                if ( ! (map instanceof java.util.SortedMap) )
                {
                    map = new java.util.TreeMap<Object,Object>(map);
                }
    
                type = hyperdatatype.HYPERDATATYPE_MAP_GENERIC;
    
                for (java.util.Iterator m_it=map.keySet().iterator();m_it.hasNext();)
                {
                    Object key = m_it.next();
    
                    if ( key == null ) throw new TypeError(
                                      "In attribute '" + attrStr 
                                    + "': A non-empty map cannot have a null key entry");
    
                    Object val = map.get(key);
    
                    if ( val == null ) throw new TypeError(
                                  "In attribute '" + attrStr 
                                + "': A non-empty map cannot have a null value entry");
    
                    type = validateMapType(type, attrStr, key, val);
    
                    if ( key instanceof String )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)key).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)key).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
                    else if ( key instanceof Long )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)key).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                    else
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putDouble(((Double)key).doubleValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( val instanceof String )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
                    else if ( val instanceof Long )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                    else
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putDouble(((Double)val).doubleValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else
            {
                throw new TypeError(
                     "Do not know how to convert type '"
                        + value.getClass().getName()
                        + "' for attribute '" + attrStr + "'");
            }
    
            if (write_attr_name(ha, attrBytes, type) == 0) throw new MemoryError();
    
            i++;
        }
    }
    catch(Exception e)
    {
        if ( attrs != null ) free_attrs(attrs, attrs_sz);

        if ( e instanceof TypeError ) throw (TypeError)e;
        if ( e instanceof MemoryError ) throw (MemoryError)e;
    }

    return attrs;
  }

  // attrsMap - map of maps keyed by attribute name
  //
  static hyperclient_map_attribute dict_to_map_attrs(java.util.Map attrsMap)
                                                                throws TypeError,
                                                                MemoryError
  {
    java.math.BigInteger attrs_sz_bi = java.math.BigInteger.valueOf(0);

    // For each of the operands keyed by attribute name, 
    // sum all of the operands cardinalities to get the final size of
    // of the hyperclient_map_attribute array
    //
    for (java.util.Iterator it=attrsMap.keySet().iterator(); it.hasNext();)
    {
        Object operand = attrsMap.get(it.next());

        // non-Map operands in the else branch below can only have cardinality
        // of one according to the python bindings, so I will do the same.
        //
        if ( operand instanceof java.util.Map )
            attrs_sz_bi = attrs_sz_bi.add(
                java.math.BigInteger.valueOf(((java.util.Map)operand).size()));
        else
            attrs_sz_bi = attrs_sz_bi.add(java.math.BigInteger.ONE);
    }

    long attrs_sz = attrs_sz_bi.longValue();

    // alloc_map_attrs will treat attrs_sz as unsigned
    hyperclient_map_attribute attrs = alloc_map_attrs(attrs_sz);

    if ( attrs == null ) throw new MemoryError();

    attrs.setAttrsSz_bi(attrs_sz_bi);
    
    java.math.BigInteger i_bi = java.math.BigInteger.valueOf(0);

    for (java.util.Iterator it=attrsMap.keySet().iterator(); it.hasNext();)
    {
        try
        {
            String attrStr = (String)(it.next());
            if ( attrStr == null ) throw new TypeError("Attribute name cannot be null");

            Object operand = attrsMap.get(attrStr);

            if ( operand instanceof java.util.Map )
            {
                java.util.Map<?,?> map = (java.util.Map<?,?>)operand;

                hyperdatatype keyType = hyperdatatype.HYPERDATATYPE_GENERIC;
                hyperdatatype curKeyType = hyperdatatype.HYPERDATATYPE_GENERIC;

                hyperdatatype valType = hyperdatatype.HYPERDATATYPE_GENERIC;
                hyperdatatype curValType = hyperdatatype.HYPERDATATYPE_GENERIC;

                for ( java.util.Iterator iit=map.keySet().iterator(); iit.hasNext(); )
                {
                    Object key = iit.next();

                    if ( key == null ) throw new TypeError(
                                   "In attribute '" + attrStr 
                                 + "': A non-empty map cannot have a null key entry");

                    Object val = map.get(key);

                    if ( val == null ) throw new TypeError(
                                   "In attribute '" + attrStr 
                                 + "': A non-empty map cannot have a null value entry");

                    if ( write_map_attr_name(hma,attrStr.getBytes()) == 0 )
                        throw new MemoryError();

                    if ( key instanceof String )
                    {
                        curKeyType = hyperdatatype.HYPERDATATYPE_STRING;

                        if ( write_map_attr_map_key(hma,((String)key).getBytes(),
                                                                    keyType) == 0 )
                            throw new MemoryError();
                    }
                    else if ( key instanceof Long )
                    {
                        curKeyType = hyperdatatype.HYPERDATATYPE_INT64;

                        byte[] keyBytes = java.nio.ByteBuffer.allocate(8).order(
                            java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                            ((Long)key).longValue()).array();

                        if ( write_map_attr_map_key(hma,keyBytes,keyType) == 0 )
                            throw new MemoryError();
                    }
                    else if ( key instanceof Double )
                    {
                        curKeyType = hyperdatatype.HYPERDATATYPE_FLOAT;

                        byte[] keyBytes = java.nio.ByteBuffer.allocate(8).order(
                            java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(
                            ((Double)key).doubleValue()).array();

                        if ( write_map_attr_map_key(hma,keyBytes,keyType) == 0 )
                            throw new MemoryError();
                    }
                    else
                    {
                        throw new TypeError(
                            "Do not know how to convert map key type '"
                                + key.getClass().getName()
                                + "' for map attribute '" + attrStr + "'");
                    }

                    if ( keyType != curKeyType
                            && keyType != hyperdatatype.HYPERDATATYPE_GENERIC )
                    (
                        throw new TypeError(("In map attribute '" + attrStr
                                     + "': cannot operate with heterogeneous map keys")
                    )

                    keyType = curKeyType;

                    if ( val instanceof String )
                    {
                        curValType = hyperdatatype.HYPERDATATYPE_STRING;

                        if ( write_map_attr_value(hma,((String)val).getBytes(),
                                                                valType) == 0 )
                            throw new MemoryError();
                    }
                    else if ( val instanceof Long )
                    {
                        curValType = hyperdatatype.HYPERDATATYPE_INT64;

                        byte[] valBytes = java.nio.ByteBuffer.allocate(8).order(
                            java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                            ((Long)val).longValue()).array();

                        if ( write_map_attr_value(hma,valBytes,valType) == 0 )
                            throw new MemoryError();
                    }
                    else if ( val instanceof Double )
                    {
                        curValType = hyperdatatype.HYPERDATATYPE_FLOAT;

                        byte[] valBytes = java.nio.ByteBuffer.allocate(8).order(
                            java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(
                            ((Double)val).doubleValue()).array();

                        if ( write_map_attr_value(hma,valBytes,valType) == 0 )
                            throw new MemoryError();
                    }
                    else
                    {
                        throw new TypeError(
                            "Do not know how to convert map value type '"
                                + val.getClass().getName()
                                + "' for map attribute '" + attrStr + "'");
                    }

                    if ( valType != curValType
                            && valType != hyperdatatype.HYPERDATATYPE_GENERIC )
                    (
                        throw new TypeError(("In map attribute '" + attrStr
                                     + "': cannot operate with heterogeneous map values")
                    )

                    valType = curValType;

                    i_bi = i_bi.add(java.math.BigInteger.ONE);
                }

                if ( keyType == hyperdatatype.HYPERDATATYPE_GENERIC )
                    throw new TypeError(
                                      "In attribute '" + attrStr 
                                    + "':  cannot have an empty map operand");
            }
            else
            {
                hyperclient_map_attribute hma = get_map_attr(attrs,i_bi.longValue());

                if ( operand instanceof String )
                {
                    hyperdatatype keyType = hyperdatatype.HYPERDATATYPE_STRING;

                    if ( write_map_attr_name(hma,attrStr.getBytes()) == 0 )
                        throw new MemoryError();

                    if ( write_map_attr_map_key(hma,((String)operand).getBytes(),
                                                                    keyType) == 0 )
                        throw new MemoryError();
                }
                else if ( operand instanceof Long )
                {
                    hyperdatatype keyType = hyperdatatype.HYPERDATATYPE_INT64;

                    if ( write_map_attr_name(hma,attrStr.getBytes()) == 0 )
                        throw new MemoryError();

                    byte[] operandBytes = java.nio.ByteBuffer.allocate(8).order(
                        java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                        ((Long)operand).longValue()).array();

                    if ( write_map_attr_map_key(hma,operandBytes,keyType) == 0 )
                        throw new MemoryError();
                }
                else if ( operand instanceof Double )
                {
                    hyperdatatype keyType = hyperdatatype.HYPERDATATYPE_FLOAT;

                    if ( write_map_attr_name(hma,attrStr.getBytes()) == 0 )
                        throw new MemoryError();

                    byte[] operandBytes = java.nio.ByteBuffer.allocate(8).order(
                        java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(
                        ((Double)operand).doubleValue()).array();

                    if ( write_map_attr_map_key(hma,operandBytes,keyType) == 0 )
                        throw new MemoryError();
                }
                else
                {
                    throw new TypeError( "In attribute '" + attrStr 
                                       + "': a non-map operand must be String, Long or Double");
                }

                hma.setValue_datatyte(hyperdatatype.HYPERDATATYPE_GENERIC);

                i_bi = i_bi.add(java.math.BigInteger.ONE);
            }
        }
        catch(Exception e)
        {
            if ( attrs != null ) free_map_attrs(attrs, attrs_sz);

            if ( e instanceof TypeError ) throw (TypeError)e;
            if ( e instanceof MemoryError ) throw (MemoryError)e;
        }
    }

    return attrs;
  }

  // Synchronous methods
  //
  public java.util.Map get(String space, String key) throws HyperClientException,
                                                            ValueError
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
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

  public boolean condput(String space, String key, java.util.Map condition,
                                                   java.util.Map value)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredCondPut)(async_condput(space, key, condition, value));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean del(String space, String key) throws HyperClientException,
                                                         ValueError
  {
    Deferred d = (DeferredDelete)(async_del(space, key));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean group_del(String space, java.util.Map predicate)
                                                 throws HyperClientException,
                                                        TypeError,
                                                        MemoryError,
                                                        ValueError
  {
    Deferred d = (DeferredGroupDel)(async_group_del(space, predicate));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_sub(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_sub(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_mul(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_mul(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_div(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_div(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_mod(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_mod(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_and(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_and(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_or(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_or(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_xor(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_xor(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean string_prepend(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_string_prepend(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean string_append(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_string_append(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean list_lpush(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_list_lpush(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean list_rpush(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_list_rpush(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_remove(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_remove(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_intersect(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_intersect(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_union(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_union(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_remove(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_remove(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_sub(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_sub(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_mul(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_mul(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_div(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_div(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_mod(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_mod(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_and(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_and(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_or(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_or(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_xor(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_xor(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_string_prepend(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_string_prepend(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_string_append(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_string_append(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public java.math.BigInteger count(String space, java.util.Map predicate)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    return count(space, predicate, false);
  }

  public java.math.BigInteger count(String space, java.util.Map predicate, boolean unsafe)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    Deferred d = (DeferredCount)(async_count(space, predicate, unsafe));
    return (java.math.BigInteger)(d.waitFor());
  }

  public Search search(String space, java.util.Map predicate)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    return new Search(this,space,predicate);
  }

  public SortedSearch sorted_search(String space, java.util.Map predicate,
                                                            String sortBy,
                                                            java.math.BigInteger limit,
                                                            boolean descending)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    return new SortedSearch(this, space, predicate, sortBy, limit, descending);
  }

  public SortedSearch sorted_search(String space, java.util.Map predicate,
                                                            String sortBy,
                                                            long limit,
                                                            boolean descending)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    return new SortedSearch(this, space, predicate, sortBy,
                          new java.math.BigInteger(
                            java.nio.ByteBuffer.allocate(8).order(
                                java.nio.ByteOrder.BIG_ENDIAN).putLong(limit).array()),
                                                                            descending);
  }

  public SortedSearch sorted_search(String space, java.util.Map predicate,
                                                            String sortBy,
                                                            int limit,
                                                            boolean descending)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            ValueError,
                                                            MemoryError
  {
    return new SortedSearch(this, space, predicate, sortBy,
                          new java.math.BigInteger(
                            java.nio.ByteBuffer.allocate(4).order(
                                java.nio.ByteOrder.BIG_ENDIAN).putInt(limit).array()),
                                                                            descending);
  }

  // Asynchronous methods
  //
  public Deferred async_get(String space, String key) throws HyperClientException,
                                                             ValueError
  {
    return new DeferredGet(this,space, key);
  }

  public Deferred async_put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpPut(this), space, key, map);
  }

  public Deferred async_condput(String space, String key, java.util.Map condition,
                                                          java.util.Map value)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredCondPut(this, space, key, condition, value);
  }

  public Deferred async_del(String space, String key) throws HyperClientException
  {
    return new DeferredDelete(this, space, key);
  }

  public Deferred async_group_del(String space, java.util.Map predicate)
                                                 throws HyperClientException,
                                                        TypeError,
                                                        MemoryError,
                                                        ValueError
  {
    return new DeferredGroupDel(this, space, predicate);
  }

  public Deferred async_atomic_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicAdd(this), space, key, map);
  }


  public Deferred async_atomic_sub(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicSub(this), space, key, map);
  }

  public Deferred async_atomic_mul(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicMul(this), space, key, map);
  }

  public Deferred async_atomic_div(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicDiv(this), space, key, map);
  }

  public Deferred async_atomic_mod(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicMod(this), space, key, map);
  }

  public Deferred async_atomic_and(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicAnd(this), space, key, map);
  }

  public Deferred async_atomic_or(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicOr(this), space, key, map);
  }

  public Deferred async_atomic_xor(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicXor(this), space, key, map);
  }

  public Deferred async_string_prepend(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpStringPrepend(this), space, key, map);
  }

  public Deferred async_string_append(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpStringAppend(this), space, key, map);
  }

  public Deferred async_list_lpush(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpListLpush(this), space, key, map);
  }

  public Deferred async_list_rpush(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpListRpush(this), space, key, map);
  }

  public Deferred async_set_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetAdd(this), space, key, map);
  }

  public Deferred async_set_remove(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetRemove(this), space, key, map);
  }

  public Deferred async_set_intersect(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetIntersect(this), space, key, map);
  }

  public Deferred async_set_union(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetUnion(this), space, key, map);
  }

  public Deferred async_map_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAdd(this), space, key, map);
  }

  public Deferred async_map_remove(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpRemove(this), space, key, map);
  }

  public Deferred async_map_atomic_add(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicAdd(this), space, key, map);
  }

  public Deferred async_map_atomic_sub(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicSub(this), space, key, map);
  }

  public Deferred async_map_atomic_mul(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicMul(this), space, key, map);
  }

  public Deferred async_map_atomic_div(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicDiv(this), space, key, map);
  }

  public Deferred async_map_atomic_mod(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicMod(this), space, key, map);
  }

  public Deferred async_map_atomic_and(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicAnd(this), space, key, map);
  }

  public Deferred async_map_atomic_or(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicOr(this), space, key, map);
  }

  public Deferred async_map_atomic_xor(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicXor(this), space, key, map);
  }

  public Deferred async_map_string_prepend(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpStringPrepend(this), space, key, map);
  }

  public Deferred async_map_string_append(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredMapOp(this, new MapOpStringAppend(this), space, key, map);
  }

  public Deferred async_count(String space, java.util.Map predicate)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return async_count(space, predicate, false);
  }

  public Deferred async_count(String space, java.util.Map predicate, boolean unsafe)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredCount(this, space, predicate, unsafe);
  }
%}
