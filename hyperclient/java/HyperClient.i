
%javamethodmodifiers std::map<std::string, Attribute*>::get "private";
%javamethodmodifiers std::map<std::string, Attribute*>::set "private";
%javamethodmodifiers std::map<std::string, Attribute*>::del "private";
/*%javamethodmodifiers std::map<std::string, Attribute*>::clear "private";*/

%rename("private_get")  std::map<std::string, Attribute*>::get;
%rename("private_set") std::map<std::string, Attribute*>::set;
%rename("private_del") std::map<std::string, Attribute*>::del;
/*%rename("private_clear") std::map<std::string, Attribute*>::clear;*/

%typemap(javacode) std::map<std::string, Attribute*>
%{
  private java.util.HashMap<Long,Attribute> pgcp_refmap = new java.util.HashMap<Long,Attribute>();
  
  private long getCPtrAndAddReference(Attribute attribute)
  {
    long cPtr = Attribute.getCPtr(attribute);
    pgcp_refmap.put(new Long(cPtr),attribute);
    return cPtr;
  }

  public Attribute get(String key)
  {
    long cPtr = hyperclientJNI.$javaclassname_private_get(swigCPtr, this, key);
    Attribute ret = null;

    if (cPtr != 0)
    {
      ret = new Attribute(cPtr, true); 
      pgcp_refmap.put(new Long(cPtr),ret);
      private_del(key);
    }
    return ret;
  }

  public void set(String key, Attribute x)
  {
    hyperclientJNI.$javaclassname_private_set(
      swigCPtr, this, key, getCPtrAndAddReference(x), x);
  }

  public void del(String key)
  {
    long cPtr = hyperclientJNI.$javaclassname_private_get(swigCPtr, this, key);
    if (cPtr != 0)
    {
      pgcp_refmap.remove(new Long(cPtr));
      hyperclientJNI.$javaclassname_private_del(swigCPtr, this, key);
    }
  }
%}
