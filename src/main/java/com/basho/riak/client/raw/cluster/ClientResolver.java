package com.basho.riak.client.raw.cluster;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.basho.riak.client.raw.RawClient;

/**
 * Basic round-robin strategy for resolving which {@link RawClient} to use from a given {@link List}
 */
public class ClientResolver {

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Return the next client to use from the given {@link List} based on internal state
     *
     * @param cluster the {@link List} to select a client from
     * @return the selected {@link RawClient}
     */
    public RawClient getNextClient(final List<RawClient> cluster) {
        int index = Math.abs(counter.getAndIncrement() % cluster.size());
        return cluster.get(index);
    }

}
