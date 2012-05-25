package com.basho.riak.client.raw.cluster;

import com.basho.riak.client.raw.RawClient;

/**
 * An interface for creating a task to be executed by a {@link ClusterDelegate}
 *
 * @param <T> The return type for when this task is executed
 *
 * @see com.basho.riak.client.raw.ClusterClient
 * @see ClusterDelegate
 */
public interface ClusterTask<T> {

    /**
     * The method to be called when this task is executed.
     *
     * @param client the {@link RawClient} to execute this task against
     * @return the result
     * @throws Exception
     */
    T call(RawClient client) throws Exception;
}
