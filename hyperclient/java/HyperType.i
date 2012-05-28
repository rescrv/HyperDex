%typemap(javacode) HyperType
%{
  public void loseJvmOwnership()
  {
    swigCMemOwn = false;
  }
%}

%exception HyperString::dynamic_cast(HyperType *hypertype)
{
    $action
    if (!result) {
        jclass excep = jenv->FindClass("java/lang/ClassCastException");
        if (excep) {
            jenv->ThrowNew(excep, "dynamic_cast exception");
        }
    }
}
%extend HyperString
{
    static HyperString *dynamic_cast(HyperType *hypertype) {
        return dynamic_cast<HyperString *>(hypertype);
    }
}

%exception HyperInteger::dynamic_cast(HyperType *hypertype)
{
    $action
    if (!result) {
        jclass excep = jenv->FindClass("java/lang/ClassCastException");
        if (excep) {
            jenv->ThrowNew(excep, "dynamic_cast exception");
        }
    }
}
%extend HyperInteger
{
    static HyperInteger *dynamic_cast(HyperType *hypertype) {
        return dynamic_cast<HyperInteger *>(hypertype);
    }
}

%exception HyperVector::dynamic_cast(HyperType *hypertype)
{
    $action
    if (!result) {
        jclass excep = jenv->FindClass("java/lang/ClassCastException");
        if (excep) {
            jenv->ThrowNew(excep, "dynamic_cast exception");
        }
    }
}
%extend HyperVector
{
    static HyperVector *dynamic_cast(HyperType *hypertype) {
        return dynamic_cast<HyperVector *>(hypertype);
    }
}

%exception HyperMap::dynamic_cast(HyperType *hypertype)
{
    $action
    if (!result) {
        jclass excep = jenv->FindClass("java/lang/ClassCastException");
        if (excep) {
            jenv->ThrowNew(excep, "dynamic_cast exception");
        }
    }
}
%extend HyperMap
{
    static HyperMap *dynamic_cast(HyperType *hypertype) {
        return dynamic_cast<HyperMap *>(hypertype);
    }
}

%javamethodmodifiers HyperMap::set "private";
%rename("private_set") HyperMap::set;
%javamethodmodifiers HyperVector::put "private";
%rename("private_put") HyperVector::put;

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

%typemap(javadestruct_derived, methodname="destr_delete", methodmodifiers="private synchronized") HyperVector %{
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

%typemap(javacode) HyperVector
%{
  public void put(HyperType x)
  {
    x.loseJvmOwnership();
    hyperclientJNI.HyperVector_private_put(swigCPtr, this, HyperType.getCPtr(x), x);
  }

  public void put(String value)
  {
    put(new HyperString(value));
  }

  public void set(long value)
  {
    put(new HyperInteger(value));
  }

  public synchronized void delete()
  {
    destr_delete();
  }
%}

%typemap(javafinalize) HyperMap HyperVector
%{
  protected void finalize()
  {
    destr_delete();
  }
%}
