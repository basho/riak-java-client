package com.basho.riak.client.raw.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.ClusterConfig;

/**
 * A delegate class that keeps internal healthy state of the cluster's {@link RawClient}s.
 *
 * Executes {@link ClusterTask}s, resolving the correct {@link RawClient} to delegate to,
 * taking nodes that are determined to be in an unhealthy state out of the cluster until
 * they have been recovered.  A background task executes with a given schedule to attempt
 * to recover unhealthy {@link RawClient}s.
 *
 * @see com.basho.riak.client.raw.ClusterClient
 * @see ClusterConfig
 * @see ClusterObserver
 */
public class ClusterDelegate {
    private static double HEALTH_PCT = .5d;

    private List<RawClient> healthy;
    private final CopyOnWriteArrayList<RawClient> unhealthy;
    private final ClientResolver clientResolver;
    private final List<ClusterObserver> observers;
    private final ScheduledExecutorService healthCheckExecutor;
    private final Object mutex;

    /**
     * @param cluster the {@link List} of {@link RawClient}s that make up the cluster
     * @param clusterConfig the {@link ClusterConfig} to use to initialize this class
     *                      (health check thread configuration, observers to call back, etc)
     */
    public ClusterDelegate(final List<RawClient> cluster, final ClusterConfig<?> clusterConfig) {
        int actualClusterSize = cluster.size();
        int expectedClusterSize = clusterConfig.getClients().size();
        if (actualClusterSize / new Double(expectedClusterSize).doubleValue() < HEALTH_PCT) {
          throw new IllegalStateException("Cluster would come up in unhealthy state. " +
            " Number of health nodes less than half the number of expected nodes");
        }


        this.healthy = Collections.unmodifiableList(cluster);
        this.unhealthy = new CopyOnWriteArrayList<RawClient>();
        this.clientResolver = new ClientResolver();
        this.observers = clusterConfig.getClusterObservers();
        this.healthCheckExecutor = Executors.newScheduledThreadPool(1);
        this.healthCheckExecutor.scheduleAtFixedRate(new HealthChecker(), 0, clusterConfig.getHealthCheckFrequencyMillis(), TimeUnit.MILLISECONDS);
        this.mutex = new Object();

        notifyObservers();
    }

    public List<RawClient> getHealthyClients() {
        return this.healthy;
    }

    public List<RawClient> getUnhealthyClients() {
        return Collections.unmodifiableList(unhealthy);
    }

    /**
     * Executes the given {@link ClusterTask} against a {@link RawClient} that is a member of this cluster.
     * {@link IOException}s and {@link RuntimeException}s are thrown directly.  Any other {@link Exception}s
     * thrown by the {@link ClusterTask} are wrapped by a {@link ClusterTaskException}.
     *
     * {@link IOException}s will cause client nodes to be put in an unhealthy state if they are also unreachable
     * with a ping call.  The {@link IOException} will still be thrown and no retry is attempted.  Retries are
     * expected to be handled by {@link com.basho.riak.client.cap.Retrier}.
     *
     * @param task The {@link ClusterTask} to execute
     * @param <T> The type to be returned
     * @return The value returned by the execution of the {@link ClusterTask}
     * @throws IOException, ClusterTaskException
     *
     * @see ClusterTaskException
     */
    public <T> T execute(final ClusterTask<T> task) throws IOException, ClusterTaskException {
        final RawClient client = clientResolver.getNextClient(healthy);

        try {
            return task.call(client);
        } catch (IOException ioe) {
            registerUnhealthy(client);
            throw ioe;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ex) {
            throw new ClusterTaskException(ex);
        }
    }

    private void registerUnhealthy(final RawClient unhealthyClient) {
        try {
            unhealthyClient.ping();
        } catch (IOException ioe) {
            synchronized (mutex) {
                final List<RawClient> newCluster = new ArrayList<RawClient>(healthy);
                if (newCluster.remove(unhealthyClient)) {
                    unhealthy.add(unhealthyClient);
                    healthy = Collections.unmodifiableList(newCluster);
                }
            }
            notifyObservers();
        }
    }

    public void shutdown() {
        for (RawClient rc : unhealthy) {
            rc.shutdown();
        }
        for (RawClient rc : healthy) {
            rc.shutdown();
        }
        healthCheckExecutor.shutdown();
    }

    private void notifyObservers() {
        for (final ClusterObserver observer : observers) {
            observer.clusterHealthChange(getNodeNames(healthy), getNodeNames(unhealthy));
        }
    }

    private List<String> getNodeNames(final List<RawClient> clients) {
        final List<String> nodeNames = new ArrayList<String>(clients.size());
        for (final RawClient client : clients) {
            nodeNames.add(client.getNodeName());
        }
        return nodeNames;
    }

    private class HealthChecker implements Runnable {
        public void run() {
            try {
                if (unhealthy.size() == 0) {
                    return;
                }
                final List<RawClient> recovering = new ArrayList<RawClient>();
                for (final RawClient client : unhealthy) {
                    if (isClientHealthy(client)){
                      recovering.add(client);
                    }
                }
                synchronized (mutex) {
                    final List<RawClient> newCluster = new ArrayList<RawClient>(ClusterDelegate.this.healthy);
                    for (final RawClient client : recovering) {
                        if (unhealthy.remove(client)) {
                            newCluster.add(client);
                        }
                    }
                    ClusterDelegate.this.healthy = Collections.unmodifiableList(newCluster);
                }
                notifyObservers();
            } catch (RuntimeException re) {
            }
        }
      
        public boolean isClientHealthy(RawClient client) {
          try {
            //-- First check to see if client is up
            client.ping();
            //-- Check to see if client has joined ring by check to see if it has any buckets
            return !client.listBuckets().isEmpty();
          } catch (IOException ioe) {
            return false;
          }
          
        }
    }

}
