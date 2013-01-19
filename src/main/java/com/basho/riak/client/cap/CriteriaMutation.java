package com.basho.riak.client.cap;

/**
 * Created by IntelliJ IDEA.
 * User: gmedina
 * Date: 18-Oct-2012
 * Time: 16:55:01
 */
public interface CriteriaMutation<T> extends Mutation<T>
{
  public boolean hasMutated();
}
