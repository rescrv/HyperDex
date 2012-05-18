%javamethodmodifiers std::map<std::string, HyperType*>::del "private";
%javamethodmodifiers std::map<std::string, HyperType*>::clear "private";
%javamethodmodifiers std::map<std::string, HyperType*>::set "private";
%javamethodmodifiers std::map<std::string, HyperType*>::destr_del "private";
%javamethodmodifiers std::map<std::string, HyperType*>::destr_clear "private";

%rename("private_del") std::map<std::string, HyperType*>::del;
%rename("private_clear") std::map<std::string, HyperType*>::clear;
%rename("private_set") std::map<std::string, HyperType*>::set;

%extend std::map<std::string,HyperType*>
{
    void destr_del(const std::string& key) throw (std::out_of_range)
    {
        std::cout << "destr_del was called!" << std::endl;
        std::map<std::string,HyperType*>::iterator i = self->find(key);
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
        std::map<std::string,HyperType*>::iterator i = self->begin();
        for (std::map<std::string,HyperType*>::iterator i = self->begin();
                i != self->end(); i++)
        {
            delete (*i).second;
            self->erase(i);
        }
    }
}

%typemap(javadestruct, methodname="destr_delete", methodmodifiers="private synchronized") std::map<std::string, HyperType*> %{
  {
    if (swigCPtr != 0) {
      System.out.println("Start deleting $javaclassname " + " (" + swigCPtr + ")");
      hyperclientJNI.$javaclassname_destr_clear(swigCPtr, this);
      if (swigCMemOwn) {
        swigCMemOwn = false;
        hyperclientJNI.delete_$javaclassname(swigCPtr);
      }
      System.out.println("End deleting $javaclassname " + " (" + swigCPtr + ")");
      swigCPtr = 0;
    }
  }
%}

%typemap(javacode) std::map<std::string, HyperType*>
%{
  public void del(String key)
  {
    hyperclientJNI.$javaclassname_destr_del(swigCPtr, this, key);
  }

  public void clear()
  {
    hyperclientJNI.$javaclassname_destr_clear(swigCPtr, this);
  }

  public void set(String key, HyperType x)
  {
    x.loseJvmOwnership();
    hyperclientJNI.HyperMap_private_set(swigCPtr, this, key, HyperType.getCPtr(x), x);
  }

  public void set(String key, String value)
  {
    set(key,new HyperString(value));
  }

  public void set(String key, long value)
  {
    set(key,new HyperInteger(value));
  }

  public synchronized void delete()
  {
    destr_delete();
  }
%}

%typemap(javafinalize) std::map<std::string, HyperType*>
%{
  protected void finalize()
  {
    destr_delete();
  }
%}
