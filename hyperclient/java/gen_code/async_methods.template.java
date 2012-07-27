  // Asynchronous methods
  //
  public Deferred async_get(__BYTES_1__ space, __BYTES_2__ key) throws HyperClientException,
                                                             ValueError
  {
    return new DeferredGet(this,space, key);
  }

  public Deferred async_condput(__BYTES_1__ space, __BYTES_2__ key, java.util.Map condition,
                                                          java.util.Map value)
                                                            throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredCondPut(this, space, key, condition, value);
  }

  public Deferred async_del(__BYTES_1__ space, __BYTES_2__ key) throws HyperClientException
  {
    return new DeferredDelete(this, space, key);
  }

  public Deferred async_group_del(__BYTES_1__ space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredGroupDel(this, space, predicate);
  }

  public Deferred async_count(__BYTES_1__ space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return async_count(space, predicate, false);
  }

  public Deferred async_count(__BYTES_1__ space, java.util.Map predicate, boolean unsafe)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    return new DeferredCount(this, space, predicate, unsafe);
  }
%}
