%javamethodmodifiers HyperMap::set "private";
%rename("private_set") HyperMap::set;

%typemap(javadestruct_derived, methodname="destr_delete", methodmodifiers="private synchronized") HyperMap %{
  {
    if (swigCPtr != 0) {
      System.out.println("Start deleting $javaclassname " + " (" + swigCPtr + ")");
      hyperclientJNI.$javaclassname_clear(swigCPtr, this);
      if (swigCMemOwn) {
        swigCMemOwn = false;
        hyperclientJNI.delete_$javaclassname(swigCPtr);
      }
      System.out.println("End deleting $javaclassname " + " (" + swigCPtr + ")");
      swigCPtr = 0;
    }
    super.delete();
  }
%}

%typemap(javadestruct, methodname="destr_delete", methodmodifiers="private synchronized") std::vector<HyperMap*> %{
  {
    if (swigCPtr != 0) {
      System.out.println("Start deleting $javaclassname " + " (" + swigCPtr + ")");
      hyperclientJNI.$javaclassname_clear(swigCPtr, this);
      if (swigCMemOwn) {
        swigCMemOwn = false;
        hyperclientJNI.delete_$javaclassname(swigCPtr);
      }
      System.out.println("End deleting $javaclassname " + " (" + swigCPtr + ")");
      swigCPtr = 0;
    }
  }
%}

%typemap(javacode) HyperMap
%{
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

%typemap(javafinalize) HyperMap
%{
  protected void finalize()
  {
    destr_delete();
  }
%}

%typemap(javacode) std::vector<HyperMap*>
%{
  public synchronized void delete()
  {
    destr_delete();
  }
%}

%typemap(javafinalize) std::vector<HyperMap*>
%{
  protected void finalize()
  {
    destr_delete();
  }
%}
