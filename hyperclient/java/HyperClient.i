%javamethodmodifiers std::map<std::string, Attribute*>::get "private";
%javamethodmodifiers std::map<std::string, Attribute*>::set "private";
%javamethodmodifiers std::map<std::string, Attribute*>::del "private";
%javamethodmodifiers std::map<std::string, Attribute*>::clear "private";
%javamethodmodifiers std::map<std::string, Attribute*>::destr_del "private";
%javamethodmodifiers std::map<std::string, Attribute*>::destr_clear "private";

%rename("private_get")  std::map<std::string, Attribute*>::get;
%rename("private_set") std::map<std::string, Attribute*>::set;
%rename("private_del") std::map<std::string, Attribute*>::del;
%rename("private_clear") std::map<std::string, Attribute*>::clear;

%extend std::map<std::string,Attribute*>
{
    void destr_del(const std::string& key) throw (std::out_of_range)
    {
        std::cout << "destr_del was called!" << std::endl;
        std::map<std::string,Attribute*>::iterator i = self->find(key);
        if (i != self->end())
        {
            delete (*self)[key];
            self->erase(i);
        }
        else
            throw std::out_of_range("key not found");
    }

    void destr_clear() throw()
    {
        std::cout << "destr_clear was called!" << std::endl;
        std::map<std::string,Attribute*>::iterator i = self->begin();
        for (std::map<std::string,Attribute*>::iterator i = self->begin();
                i != self->end(); i++)
        {
            delete (*i).second;
            self->erase(i);
        }
    }
}

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
      hyperclientJNI.$javaclassname_destr_del(swigCPtr, this, key);
    }
  }

  public void clear()
  {
    hyperclientJNI.Attributes_destr_clear(swigCPtr, this);
  }

  public synchronized void destr_delete() {
    if (swigCPtr != 0) {
      if (swigCMemOwn) {
        swigCMemOwn = false;
        hyperclientJNI.Attributes_destr_clear(swigCPtr, this);
        hyperclientJNI.delete_Attributes(swigCPtr);
      }
      swigCPtr = 0;
    }
  }

%}

%typemap(javafinalize) std::map<std::string, Attribute*>
%{
  protected void finalize()
  {
    destr_delete();
  }
%}
