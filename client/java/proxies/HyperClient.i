%extend HyperClient
{
    static int get_attr_name_sz(hyperclient_attribute *ha)
    {
        std::string str = std::string(ha->attr);
        size_t name_sz = str.length();
        return name_sz > INT_MAX ? (int)INT_MAX : (int)name_sz;
    }

    static void read_attr_name(hyperclient_attribute *ha,
                                char *name, size_t name_sz)
    {
        std::string str = std::string(ha->attr);
        memcpy(name, str.data(), name_sz);
    }

    static void read_attr_value(hyperclient_attribute *ha,
                                   char *value, size_t value_sz,
                                   size_t pos)
    {
        size_t available = ha->value_sz - pos;
        memcpy(value, ha->value+pos, value_sz<available?value_sz:available);
    }

    static int get_map_attr_name_sz(hyperclient_map_attribute *hma)
    {
        std::string str = std::string(hma->attr);
        size_t name_sz = str.length();
        return name_sz > INT_MAX ? (int)INT_MAX : (int)name_sz;
    }

    static std::string read_map_attr_name(hyperclient_map_attribute *hma,
                                            char *name, size_t name_sz)
    {
        std::string str = std::string(hma->attr);
        memcpy(name, str.data(), name_sz);
    }

    static int get_attr_check_name_sz(hyperclient_attribute_check *hac)
    {
        std::string str = std::string(hac->attr);
        size_t name_sz = str.length();
        return name_sz > INT_MAX ? (int)INT_MAX : (int)name_sz;
    }

    static void read_attr_check_name(hyperclient_attribute_check *hac,
                                char *name, size_t name_sz)
    {
        std::string str = std::string(hac->attr);
        memcpy(name, str.data(), name_sz);
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

    static hyperclient_attribute_check *alloc_attrs_check(size_t attrs_sz)
    {
        return (hyperclient_attribute_check *)calloc(attrs_sz,
                                                     sizeof(hyperclient_attribute_check));
    }

    static void free_attrs_check(hyperclient_attribute_check *attrs, size_t attrs_sz)
    {
        for (size_t i=0; i<attrs_sz; i++)
        {
            if (attrs[i].attr) free((void*)(attrs[i].attr));
            if (attrs[i].value) free((void*)(attrs[i].value));
        }

        free(attrs);
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

    // Returns 1 on success. hac->attr will point to allocated memory
    // Returns 0 on failure. hac->attr will be NULL
    static int write_attr_check_name(hyperclient_attribute_check *hac,
                                   const char *attr, size_t attr_sz,
                                   hyperpredicate pred)
    {
        char *buf;

        if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
        memcpy(buf,attr,attr_sz);
        hac->attr = buf;
        hac->predicate = pred;
        return 1;
    }

    // Returns 1 on success. hac->value will point to allocated memory
    //                       hac->value_sz will hold the size of this memory
    // Returns 0 on failure. hac->value will be NULL
    //                       hac->value_sz will be 0
    //
    // If hac->value is already non-NULL, then we are appending to it.
    static int write_attr_check_value(hyperclient_attribute_check *hac,
                                        const char *value, size_t value_sz)
    {
        char *buf = NULL;
        // Note: Since hyperclient_attribute_check array was calloced
        //       hac->value = NULL and hac->value_sz = 0 initially
        if ((buf = (char *)realloc((void *)(hac->value), hac->value_sz + value_sz))
                                                                        == NULL) return 0;
        memcpy(buf + hac->value_sz, value, value_sz);
        hac->value = buf;
        hac->value_sz += value_sz;
        hac->datatype = HYPERDATATYPE_STRING;
        return 1;
    }

    // Returns 1 on success. hac->value will point to allocated memory
    //                       hac->value_sz will hold the size of this memory
    // Returns 0 on failure. hac->value will be NULL
    //                       hac->value_sz will be 0
    //
    static int write_attr_check_value(hyperclient_attribute_check *hac, int64_t value)
    {
        char *buf = NULL;
        if ((buf = (char *)malloc(sizeof(int64_t))) == NULL) return 0;
        memcpy(buf, &value, sizeof(int64_t));
        hac->value = buf;
        hac->value_sz = sizeof(int64_t);
        hac->datatype = HYPERDATATYPE_INT64;
        return 1;
    }

    // Returns 1 on success. hac->value will point to allocated memory
    //                       hac->value_sz will hold the size of this memory
    // Returns 0 on failure. hac->value will be NULL
    //                       hac->value_sz will be 0
    //
    static int write_attr_check_value(hyperclient_attribute_check *hac, double value)
    {
        char *buf = NULL;
        if ((buf = (char *)malloc(sizeof(double))) == NULL) return 0;
        memcpy(buf, &value, sizeof(double));
        hac->value = buf;
        hac->value_sz = sizeof(double);
        hac->datatype = HYPERDATATYPE_FLOAT;
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

    static hyperclient_attribute_check *get_attr_check(hyperclient_attribute_check *hac, size_t i)
    {
        return hac + i;
    }
};

%typemap(javacode) HyperClient
%{
  java.util.HashMap<Long,Pending> ops = new java.util.HashMap<Long,Pending>();

  private String defaultStringEncoding = "UTF-8";

  public HyperClient(String coordinator, int port, String defaultStringEncoding)
  {
    this(coordinator, port);
    this.defaultStringEncoding = defaultStringEncoding;
  }

  public String getDefaultStringEncoding()
  {
    return defaultStringEncoding;
  }

  public void setDefaultStringEncoding(String encoding)
  {
    defaultStringEncoding = encoding;
  }

  void loop() throws HyperClientException
  {
    SWIGTYPE_p_hyperclient_returncode rc_ptr = hyperclient_lc.new_rc_ptr();

    long ret = loop(-1, rc_ptr);

    hyperclient_returncode rc = hyperclient_lc.rc_ptr_value(rc_ptr);

    hyperclient_lc.delete_rc_ptr(rc_ptr); // Deallocate the pointer

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

  java.util.Map attrs_to_dict(hyperclient_attribute attrs, long attrs_sz)
                                                                throws ValueError
  {
    ByteArrayKeyedMap<Object> map = new ByteArrayKeyedMap<Object>(defaultStringEncoding);

    int sz = HyperClient.size_t_to_int(attrs_sz);

    for ( int i=0; i<sz; i++)
    {
        hyperclient_attribute ha = get_attr(attrs,i);

        byte[] attrBytes = ha.getAttrNameBytes();

        Object attrValue = ha.getAttrValue(defaultStringEncoding);

        map.put(attrBytes, attrValue);
    }

    return map;
  }

  private static hyperdatatype validateMapType(hyperdatatype type,
                                               String attrStr,
                                               Object key, Object val) throws TypeError
  {
    hyperdatatype retType = type;

    if ( type ==  hyperdatatype.HYPERDATATYPE_MAP_GENERIC )
    {
        // Initialize map type
        //

        if ( isBytes(key) && isBytes(val) )
        {
            retType = hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING;
        }
        else if ( isBytes(key) && val instanceof Long )
        {
            retType =  hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64;
        }
        else if ( isBytes(key) && val instanceof Double )
        {
            retType =  hyperdatatype.HYPERDATATYPE_MAP_STRING_FLOAT;
        }
        else if ( key instanceof Long && isBytes(val) )
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
        else if ( key instanceof Double && isBytes(val) )
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
        if (  (isBytes(key) && isBytes(val)
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING)
            ||
              (isBytes(key) && val instanceof Long
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64)
            ||
              (isBytes(key) && val instanceof Double
                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_FLOAT)
            ||
              (key instanceof Long && isBytes(val)
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_STRING)
            ||
              (key instanceof Long && val instanceof Long
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_INT64)
            ||
              (key instanceof Long && val instanceof Double
                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_FLOAT)
            ||
              (key instanceof Double && isBytes(val)
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

  // write_attr_check private method expects value to satisfy
  // either being instance of type Long or Double or isBytes(value) == true
  //
  private void write_attr_check(hyperclient_attribute_check hac,
                            ByteArray attr, Object value, hyperpredicate pred)
                                                                throws MemoryError,
                                                                       TypeError
  {
      if ( write_attr_check_name(hac,attr.getBytes(),pred) == 0 )
      {
          throw new MemoryError();
      }

      if ( value instanceof Long )
      {
          if ( write_attr_check_value(hac,((Long)value).longValue()) == 0 )
          {
              throw new MemoryError();
          }
      }
      else if ( value instanceof Double )
      {
          if ( write_attr_check_value(hac,((Double)value).doubleValue()) == 0 )
          {
              throw new MemoryError();
          }
      } 
      else // isBytes(value) must be true
      {
          if ( write_attr_check_value(hac,getBytes(value)) == 0 )
          {
              throw new MemoryError();
          }
      }
  }

  // Convenience method used when pred is not an instance of List
  //
  private java.util.Vector<
          java.util.Map.Entry<
            ByteArray,java.util.Map.Entry<hyperpredicate,Object>>>
  getRawChecks(ByteArray attr, Object pred) throws AttributeError,
                                                                  TypeError
  {
    
      String errStr = "Attribute '" + attr + "' has an invalid predicate type ( expected Predicate, Long, Double, String, Map.Entry<Long,Long>, Map.Entry<Double,Double>, Map.Entry<String,String> or a List of any of the former )";
    
      java.util.Vector<
          java.util.Map.Entry<
            ByteArray,java.util.Map.Entry<hyperpredicate,Object>>> rawChecks
            = new java.util.Vector<
                java.util.Map.Entry<
                    ByteArray,java.util.Map.Entry<hyperpredicate,Object>>>();
    
      if ( isBytes(pred) || pred instanceof Long || pred instanceof Double )
      {
          rawChecks.add(new java.util.AbstractMap.SimpleEntry<
                  ByteArray,java.util.Map.Entry<hyperpredicate,Object>>(
                      attr, 
                          new java.util.AbstractMap.SimpleEntry<
                            hyperpredicate,Object>(
                              hyperpredicate.HYPERPREDICATE_EQUALS, pred)));
      }
      else if ( pred instanceof java.util.Map.Entry )
      {
          Object lower = ((java.util.Map.Entry)pred).getKey();
          Object upper = ((java.util.Map.Entry)pred).getValue();

          if ( ! ( lower instanceof Long && upper instanceof Long ) &&
               ! ( lower instanceof Double && upper instanceof Double ) )
          {
              throw
                  new TypeError(errStr);
          }

          rawChecks.addAll(
              (new Range(lower,upper)).getRawChecks(attr));
      }
      else if ( pred instanceof Predicate )
      {
          rawChecks.addAll(
              ((Predicate)pred).getRawChecks(attr));
      }
      else
      {
          throw
              new TypeError(errStr);
      }

      return rawChecks;
  }

  // Using a Vector<Object> retvals to return multiple values.
  //
  // retvals at 0 - hacs (hyperclient_attribute_check type)
  // retvals at 1 - hacs_sz (long)

  java.util.Vector predicate_to_c(java.util.Map predicate) throws TypeError,
                                                                  MemoryError,
                                                                  ValueError
  {
      java.util.Vector<
          java.util.Map.Entry<
            ByteArray,java.util.Map.Entry<hyperpredicate,Object>>> rawChecks
            = new java.util.Vector<
                java.util.Map.Entry<
                    ByteArray,java.util.Map.Entry<hyperpredicate,Object>>>();

      hyperclient_attribute_check hacs = null;
      long hacs_sz = 0;

      try
      {
          for (java.util.Iterator it=predicate.keySet().iterator(); it.hasNext();)
          {
              Object attrObject = it.next();
    
              if ( attrObject == null )
                  throw new TypeError("Cannot search on a null attribute");
    
              byte[] attrBytes = getBytes(attrObject);
    
              Object params = predicate.get(attrObject);
    
              if ( params == null )
                  throw new TypeError("Cannot search with a null criteria");
    
              if ( ! (params instanceof java.util.List) )
              {
                  rawChecks.addAll(getRawChecks(new ByteArray(attrBytes),params));
              }
              else
              {
                  for (java.util.Iterator paramsIt = ((java.util.List)params).iterator();
                                                        paramsIt.hasNext();)
                  {
                      rawChecks.addAll(
                          getRawChecks(new ByteArray(attrBytes),paramsIt.next()));
                  }
              }
          }
    
          if ( rawChecks.size() > 0 )
          {
              hacs_sz = rawChecks.size();
    
              hacs = alloc_attrs_check(hacs_sz);
    
              if ( hacs == null ) throw new MemoryError();
    
              int i = 0; // Collections in java can only hold MAX_INT_SIZE elements

              for ( java.util.Map.Entry<
                            ByteArray,java.util.Map.Entry<
                                hyperpredicate,Object>> rawCheck: rawChecks )
              {
                  ByteArray attr = rawCheck.getKey();
                  hyperpredicate p = rawCheck.getValue().getKey();
                  Object value = rawCheck.getValue().getValue();

                  hyperclient_attribute_check hac = HyperClient.get_attr_check(hacs,i++);
                  write_attr_check(hac,attr, value, p);
              }
          }
          else 
          {
            throw new ValueError("Search criteria can't be empty");
          }
      }
      catch(Exception e)
      {
          if ( hacs != null ) free_attrs_check(hacs, hacs_sz);

          if ( e instanceof TypeError ) throw (TypeError)e;
          if ( e instanceof ValueError ) throw (ValueError)e;
          if ( e instanceof MemoryError ) throw (MemoryError)e;

      }

      java.util.Vector<Object> retvals = new java.util.Vector<Object>(2);

      retvals.add(hacs);
      retvals.add(hacs_sz);

      return retvals;
  }

  static boolean isBytes(Object obj)
  {
    return obj instanceof byte[] || obj instanceof ByteArray || obj instanceof String;
  }

  byte[] getBytes(Object obj, boolean nullTerminate) throws TypeError
  {
    byte[] bytes = null;

    if ( obj instanceof byte[] )
    {
        bytes = (byte[])obj;
    }
    else if ( obj instanceof ByteArray )
    {
        bytes = ((ByteArray)obj).getBytes();
    }
    else if ( obj instanceof String )
    {
        try
        {
            bytes = ((String)obj).getBytes(defaultStringEncoding);
        }
        catch(java.io.UnsupportedEncodingException usee)
        {
            throw new TypeError("Could not encode java string '"
                                    + obj + "' into bytes using encoding '"
                                    + defaultStringEncoding +"'"); 
        }
    }
    else
    {

        throw new TypeError("Expecting bytes in the form of byte[], ByteArray, or String (encoded with HyperClient's current default encoding which is set at '" + defaultStringEncoding + "'. But, instead got type: " + obj.getClass().getName());
    }

    if ( nullTerminate )
    {
        return java.util.Arrays.copyOf(bytes,bytes.length+1);
    }
    else
    {
        return bytes;
    }
  }

  byte[] getBytes(Object obj) throws TypeError
  {
    return getBytes(obj,false);
  }

  hyperclient_attribute dict_to_attrs(java.util.Map attrsMap) throws TypeError,
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

            Object attrObject = it.next();

            if ( attrObject == null )
                throw new TypeError("Attribute name cannot be null");

            byte[] attrBytes = getBytes(attrObject);

            String attrStr = ByteArray.decode(attrBytes,defaultStringEncoding);

            Object value = attrsMap.get(attrObject);

            if ( value == null ) throw new TypeError(
                                        "Cannot convert null value "
                                    + "for attribute '" + attrStr + "'");

            hyperdatatype type = null;

            if ( isBytes(value) )
            {
                type = hyperdatatype.HYPERDATATYPE_STRING;
                byte[] valueBytes = getBytes(value);
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
                        if ( isBytes(val) )
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
                        if ( (isBytes(val)
                                && type != hyperdatatype.HYPERDATATYPE_LIST_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_LIST_INT64)
                            || (val instanceof Double
                                && type != hyperdatatype.HYPERDATATYPE_LIST_FLOAT) )

                            throw new TypeError("Cannot store heterogeneous lists");
                    }

                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_STRING )
                    {
                        byte[] valBytes = getBytes(val);

                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order(
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 valBytes.length).array()) == 0)
                                                 throw new MemoryError();

                        if (write_attr_value(ha, valBytes) == 0)
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

                // Box String elements or byte[] elements with ByteArray to 
                // ensure hyperdex approved byte array ordering

                java.util.Iterator str_s_it=set.iterator();
                Object curElement = null;

                if ( (str_s_it.hasNext()
                        && isBytes((curElement = str_s_it.next())) )
                            && ! (curElement instanceof ByteArray) )
                {
                    java.util.TreeSet<ByteArray> sorted_set
                            = new java.util.TreeSet<ByteArray>();

                    do
                    {
                        sorted_set.add(new ByteArray(getBytes(curElement)));

                        curElement = str_s_it.hasNext()?str_s_it.next():null;

                    } while (curElement != null);

                    set = sorted_set;
                }

                if ( ! (set instanceof java.util.SortedSet) )
                {
                    try // Assuming set elements implement Comparable
                    {
                        set = new java.util.TreeSet<Object>(set);
                    }
                    catch(Exception e)
                    {
                        throw new TypeError(
                            "Could not form a sorted set for attribute '" +
                            attrStr + "'"); 
                    }
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
                        if ( isBytes(val) )
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
                        if ( ( isBytes(val)
                                && type != hyperdatatype.HYPERDATATYPE_SET_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_SET_INT64)
                            || (val instanceof Double
                                && type != hyperdatatype.HYPERDATATYPE_SET_FLOAT) )

                            throw new TypeError("Cannot store heterogeneous sets");
                    }

                    if ( type == hyperdatatype.HYPERDATATYPE_SET_STRING )
                    {
                        byte[] valBytes = getBytes(val);

                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order(
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 valBytes.length).array()) == 0)
                                                 throw new MemoryError();

                        if (write_attr_value(ha, valBytes) == 0)
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
                // As for set types, the same goes for map type. HyperDex will
                // scoff unless the map is sorted

                java.util.Map<?,?> map = (java.util.Map<?,?>)value;

                // Box String keys or byte[] keys with ByteArray to 
                // ensure hyperdex approved byte array ordering

                java.util.Iterator str_m_it=map.keySet().iterator();
                Object curKey = null;

                if ( (str_m_it.hasNext()
                        && isBytes((curKey = str_m_it.next())) )
                            && ! (curKey instanceof ByteArray) )
                {
                    java.util.TreeMap<ByteArray,Object> sorted_map
                            = new java.util.TreeMap<ByteArray,Object>();

                    do
                    {
                        sorted_map.put(new ByteArray(getBytes(curKey)),map.get(curKey));

                        curKey = str_m_it.hasNext()?str_m_it.next():null; 

                    } while (curKey != null);

                    map = sorted_map;
                }

                if ( ! (map instanceof java.util.SortedMap) )
                {
                    try // Assuming map keys implement Comparable
                    {
                        map = new java.util.TreeMap<Object,Object>(map);
                    }
                    catch(Exception e)
                    {
                        throw new TypeError(
                            "Could not form a sorted map for attribute '" +
                            attrStr + "'"); 
                    }
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

                    if ( isBytes(key) )
                    {
                        byte[] keyBytes = getBytes(key);

                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order(
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 keyBytes.length).array()) == 0)
                                                 throw new MemoryError();

                        if (write_attr_value(ha, keyBytes) == 0)
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

                    if ( isBytes(val) )
                    {
                        byte[] valBytes = getBytes(val);

                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order(
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 valBytes.length).array()) == 0)
                                                 throw new MemoryError();

                        if (write_attr_value(ha, valBytes) == 0)
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
  hyperclient_map_attribute dict_to_map_attrs(java.util.Map attrsMap) throws TypeError,
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
            Object attrObject = it.next();

            if ( attrObject == null )
                throw new TypeError("Attribute name cannot be null");

            byte[] attrBytes = getBytes(attrObject);

            String attrStr = ByteArray.decode(attrBytes,defaultStringEncoding);

            Object operand = attrsMap.get(attrObject);

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

                    hyperclient_map_attribute hma = get_map_attr(attrs,i_bi.longValue());

                    if ( write_map_attr_name(hma,attrBytes) == 0 )
                        throw new MemoryError();

                    if ( isBytes(key) )
                    {
                        curKeyType = hyperdatatype.HYPERDATATYPE_STRING;
                        byte[] keyBytes = getBytes(key);

                        if ( write_map_attr_map_key(hma, keyBytes, keyType) == 0 )
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
                    {
                        throw new TypeError("In map attribute '" + attrStr
                                     + "': cannot operate with heterogeneous map keys");
                    }

                    keyType = curKeyType;

                    if ( isBytes(val) )
                    {
                        curValType = hyperdatatype.HYPERDATATYPE_STRING;
                        byte[] valBytes = getBytes(val);

                        if ( write_map_attr_value(hma, valBytes, valType) == 0 )
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
                    {
                        throw new TypeError("In map attribute '" + attrStr
                                     + "': cannot operate with heterogeneous map values");
                    }

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

                if ( isBytes(operand) )
                {
                    hyperdatatype keyType = hyperdatatype.HYPERDATATYPE_STRING;
                    byte[] operandBytes = getBytes(operand);

                    if ( write_map_attr_name(hma,attrBytes) == 0 )
                        throw new MemoryError();

                    if ( write_map_attr_map_key(hma, operandBytes, keyType) == 0 )
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
                                       + "': a non-map operand must be byte array, Long or Double");
                }

                hma.setValue_datatype(hyperdatatype.HYPERDATATYPE_GENERIC);

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

  public void add_space(Object desc) throws HyperClientException, TypeError
  {
    hyperclient_returncode rc = add_space(new String(getBytes(desc,true)));

    if ( rc != hyperclient_returncode.HYPERCLIENT_SUCCESS )
    {
        throw new HyperClientException(rc);
    }
  }

  public void rm_space(Object space) throws HyperClientException, TypeError
  {
    hyperclient_returncode rc = rm_space(new String(getBytes(space,true)));

    if ( rc != hyperclient_returncode.HYPERCLIENT_SUCCESS )
    {
        throw new HyperClientException(rc);
    }
  }

  // Synchronous methods
  //
  public java.util.Map get(Object space, Object key) throws HyperClientException,
                                                            TypeError,
                                                            ValueError
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
  }

  public boolean cond_put(Object space, Object key, java.util.Map condition,
                                                   java.util.Map value)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredCondPut)(async_cond_put(space, key, condition, value));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean del(Object space, Object key) throws HyperClientException,
                                                      TypeError,
                                                      ValueError
  {
    Deferred d = (DeferredDelete)(async_del(space, key));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean group_del(Object space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredGroupDel)(async_group_del(space, predicate));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public java.math.BigInteger count(Object space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return count(space, predicate, false);
  }

  public java.math.BigInteger count(Object space, java.util.Map predicate, boolean unsafe)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    Deferred d = (DeferredCount)(async_count(space, predicate, unsafe));
    return (java.math.BigInteger)(d.waitFor());
  }

  public Search search(Object space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return new Search(this,space,predicate);
  }

  public SortedSearch sorted_search(Object space, java.util.Map predicate,
                                                            Object sortBy,
                                                            java.math.BigInteger limit,
                                                            boolean descending)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return new SortedSearch(this, space, predicate, sortBy, limit, descending);
  }

  public SortedSearch sorted_search(Object space, java.util.Map predicate,
                                                            Object sortBy,
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

  public SortedSearch sorted_search(Object space, java.util.Map predicate,
                                                            Object sortBy,
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


  public boolean put(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_put(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean put_if_not_exist(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_put_if_not_exist(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_sub(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_sub(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_mul(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_mul(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_div(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_div(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_mod(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_mod(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_and(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_and(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_or(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_or(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean atomic_xor(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_atomic_xor(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean string_prepend(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_string_prepend(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean string_append(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_string_append(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean list_lpush(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_list_lpush(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean list_rpush(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_list_rpush(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_remove(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_remove(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_intersect(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_intersect(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean set_union(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_set_union(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_remove(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_map_remove(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_add(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_sub(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_sub(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_mul(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_mul(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_div(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_div(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_mod(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_mod(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_and(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_and(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_or(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_or(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_atomic_xor(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_atomic_xor(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_string_prepend(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_string_prepend(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean map_string_append(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredMapOp)(async_map_string_append(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  // Asynchronous methods
  //
  public Deferred async_get(Object space, Object key) throws HyperClientException,
                                                             TypeError,
                                                             ValueError
  {
    return new DeferredGet(this,space, key);
  }

  public Deferred async_cond_put(Object space, Object key, java.util.Map condition,
                                                          java.util.Map value)
                                                            throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredCondPut(this, space, key, condition, value);
  }

  public Deferred async_del(Object space, Object key) throws HyperClientException,
                                                             TypeError
  {
    return new DeferredDelete(this, space, key);
  }

  public Deferred async_group_del(Object space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredGroupDel(this, space, predicate);
  }

  public Deferred async_count(Object space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return async_count(space, predicate, false);
  }

  public Deferred async_count(Object space, java.util.Map predicate, boolean unsafe)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredCount(this, space, predicate, unsafe);
  }


  public Deferred async_put(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpPut(this), space, key, map);
  }

  public Deferred async_put_if_not_exist(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    DeferredFromAttrs d
        = new DeferredFromAttrs(this, new SimpleOpPutIfNotExist(this), space, key, map);
    d.setComparing();
    return d;

  }

  public Deferred async_atomic_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicAdd(this), space, key, map);
  }

  public Deferred async_atomic_sub(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicSub(this), space, key, map);
  }

  public Deferred async_atomic_mul(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicMul(this), space, key, map);
  }

  public Deferred async_atomic_div(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicDiv(this), space, key, map);
  }

  public Deferred async_atomic_mod(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicMod(this), space, key, map);
  }

  public Deferred async_atomic_and(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicAnd(this), space, key, map);
  }

  public Deferred async_atomic_or(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicOr(this), space, key, map);
  }

  public Deferred async_atomic_xor(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpAtomicXor(this), space, key, map);
  }

  public Deferred async_string_prepend(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpStringPrepend(this), space, key, map);
  }

  public Deferred async_string_append(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpStringAppend(this), space, key, map);
  }

  public Deferred async_list_lpush(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpListLpush(this), space, key, map);
  }

  public Deferred async_list_rpush(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpListRpush(this), space, key, map);
  }

  public Deferred async_set_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetAdd(this), space, key, map);
  }

  public Deferred async_set_remove(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetRemove(this), space, key, map);
  }

  public Deferred async_set_intersect(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetIntersect(this), space, key, map);
  }

  public Deferred async_set_union(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpSetUnion(this), space, key, map);
  }

  public Deferred async_map_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAdd(this), space, key, map);
  }

  public Deferred async_map_remove(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpMapRemove(this), space, key, map);
  }

  public Deferred async_map_atomic_add(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicAdd(this), space, key, map);
  }

  public Deferred async_map_atomic_sub(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicSub(this), space, key, map);
  }

  public Deferred async_map_atomic_mul(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicMul(this), space, key, map);
  }

  public Deferred async_map_atomic_div(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicDiv(this), space, key, map);
  }

  public Deferred async_map_atomic_mod(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicMod(this), space, key, map);
  }

  public Deferred async_map_atomic_and(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicAnd(this), space, key, map);
  }

  public Deferred async_map_atomic_or(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicOr(this), space, key, map);
  }

  public Deferred async_map_atomic_xor(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpAtomicXor(this), space, key, map);
  }

  public Deferred async_map_string_prepend(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpStringPrepend(this), space, key, map);
  }

  public Deferred async_map_string_append(Object space, Object key, java.util.Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredMapOp(this, new MapOpStringAppend(this), space, key, map);
  }
%}
