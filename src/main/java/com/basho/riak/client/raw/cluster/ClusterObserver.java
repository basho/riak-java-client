package com.basho.riak.client.raw.cluster;

import java.util.List;

/**
 * Register implementations with a {@link com.basho.riak.client.raw.config.ClusterConfig} to receive
 * update notifications when the cluster state changes.
 *
 * @see com.basho.riak.client.raw.config.ClusterConfig
 */
public interface ClusterObserver {

    /**
     * Notification that the state of the cluster has changed
     *
     * @param healthyNodes
     *     a {@link List} of {@link String} that contains the nodename values for all of the nodes
     *     that are currently in a healthy state
     * @param unhealthyNodes
     *     a {@link List} of {@link String} that contains the nodename values for all of the nodes
     *     that are currently in an unhealthy state
     */
    void clusterHealthChange(List<String> healthyNodes, List<String> unhealthyNodes);

}
