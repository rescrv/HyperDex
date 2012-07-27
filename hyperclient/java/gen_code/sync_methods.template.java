  // Synchronous methods
  //
  public java.util.Map get(__BYTES_1__ space, __BYTES_2__ key) throws HyperClientException,
                                                            ValueError
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
  }

  public boolean condput(__BYTES_1__ space, __BYTES_2__ key, java.util.Map condition,
                                                   java.util.Map value)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredCondPut)(async_condput(space, key, condition, value));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean del(__BYTES_1__ space, __BYTES_2__ key) throws HyperClientException,
                                                         ValueError
  {
    Deferred d = (DeferredDelete)(async_del(space, key));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public boolean group_del(__BYTES_1__ space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError,
                                                                   ValueError
  {
    Deferred d = (DeferredGroupDel)(async_group_del(space, predicate));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public java.math.BigInteger count(__BYTES_1__ space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return count(space, predicate, false);
  }

  public java.math.BigInteger count(__BYTES_1__ space, java.util.Map predicate, boolean unsafe)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    Deferred d = (DeferredCount)(async_count(space, predicate, unsafe));
    return (java.math.BigInteger)(d.waitFor());
  }

  public Search search(__BYTES_1__ space, java.util.Map predicate)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return new Search(this,space,predicate);
  }

  public SortedSearch sorted_search(__BYTES_1__ space, java.util.Map predicate,
                                                            __BYTES_2__ sortBy,
                                                            java.math.BigInteger limit,
                                                            boolean descending)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   ValueError,
                                                                   MemoryError
  {
    return new SortedSearch(this, space, predicate, sortBy, limit, descending);
  }

  public SortedSearch sorted_search(__BYTES_1__ space, java.util.Map predicate,
                                                            __BYTES_2__ sortBy,
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

  public SortedSearch sorted_search(__BYTES_1__ space, java.util.Map predicate,
                                                            __BYTES_2__ sortBy,
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
