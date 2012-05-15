%javamethodmodifiers std::map<std::string, Attribute*>::del "private";
%javamethodmodifiers std::map<std::string, Attribute*>::clear "private";
%javamethodmodifiers std::map<std::string, Attribute*>::destr_del "private";
%javamethodmodifiers std::map<std::string, Attribute*>::destr_clear "private";

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

%typemap(javadestruct, methodname="destr_delete", methodmodifiers="private synchronized") std::map<std::string, Attribute*> %{
  {
    if (swigCPtr != 0) {
      if (swigCMemOwn) {
        swigCMemOwn = false;
        hyperclientJNI.$javaclassname_destr_clear(swigCPtr, this);
        hyperclientJNI.delete_$javaclassname(swigCPtr);
      }
      swigCPtr = 0;
    }
  }
%}

%typemap(javacode) std::map<std::string, Attribute*>
%{
  public void del(String key)
  {
    hyperclientJNI.$javaclassname_destr_del(swigCPtr, this, key);
  }

  public void clear()
  {
    hyperclientJNI.$javaclassname_destr_clear(swigCPtr, this);
  }

  public synchronized void delete()
  {
    destr_delete();
  }
%}

%typemap(javafinalize) std::map<std::string, Attribute*>
%{
  protected void finalize()
  {
    destr_delete();
  }
%}
