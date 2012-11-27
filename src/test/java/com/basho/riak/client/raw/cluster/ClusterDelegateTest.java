package com.basho.riak.client.raw.cluster;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.config.Configuration;

public class ClusterDelegateTest {

    @Test public void nodeLifecycle() throws Exception {
        final Set<String> buckets = mock(Set.class);
        when(buckets.isEmpty()).thenReturn(false);

        final String NODE_0 = "client0@127.0.0.1";
        final String NODE_1 = "client1@127.0.0.1";

        final RawClient client0 = mock(RawClient.class);
        final RawClient client1 = mock(RawClient.class);
        when(client0.getNodeName()).thenReturn(NODE_0);
        when(client1.getNodeName()).thenReturn(NODE_1);
        when(client0.listBuckets()).thenReturn(buckets);
        when(client1.listBuckets()).thenReturn(buckets);
        final List<RawClient> clients = Arrays.asList(client0, client1);

        // set up the client to blow up the first time it's used and behave correctly the second time
        doThrow(new IOException("boom"))
            .doNothing()
            .when(client0)
            .ping();

        final ClusterConfig clusterConfig = new MockClusterConfig(1).setHealthCheckFrequency(5, TimeUnit.MILLISECONDS);
        final ClusterObserver clusterObserver = mock(ClusterObserver.class);

        clusterConfig.addClusterObserver(clusterObserver);
        final ClusterDelegate clusterDelegate = new ClusterDelegate(clients, clusterConfig);
        try {
            synchronized (getMutex(clusterDelegate)) {
                assertEquals(clients, clusterDelegate.getHealthyClients());
                try {
                    clusterDelegate.execute(new ClusterTask<Void>() {
                        public Void call(final RawClient client) throws IOException {
                            throw new IOException("boom");
                        }
                    });
                } catch (IOException ex) {
                    // expected
                }
                // validate only the second client remains in the cluster and observer was called
                assertEquals(Arrays.asList(client1), clusterDelegate.getHealthyClients());
                verify(clusterObserver).clusterHealthChange(Arrays.asList(NODE_1), Arrays.asList(NODE_0));
            }

            // validate it is added back successfully and observer was called
            waitFor(new Runnable() {
                public void run() {
                    assertEquals(asSet(clients), asSet(clusterDelegate.getHealthyClients()));
                    verify(clusterObserver).clusterHealthChange(Arrays.asList(NODE_1, NODE_0), Collections.<String>emptyList());
                }
            }, 1, TimeUnit.SECONDS);
        } finally {
            clusterDelegate.shutdown();
        }
    }

    private void waitFor(final Runnable condition, final long timeout, final TimeUnit timeUnit) throws AssertionError, Exception {
        final long start = System.currentTimeMillis();
        final long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
        while (true) {
            try {
                condition.run();
            } catch (AssertionError ae) {
                if (System.currentTimeMillis() - start < timeoutMillis) {
                    Thread.sleep(1);
                    continue;
                } else {
                    throw ae;
                }
            }
            break;
        }
    }

    private <T> Set<T> asSet(List<T> list) {
      Set<T> set = new HashSet<T>();
      set.addAll(list);
      return set;
    }

    private Object getMutex(ClusterDelegate clusterDelegate) throws Exception {
        Field mutexField = clusterDelegate.getClass().getDeclaredField("mutex");
        mutexField.setAccessible(true);
        Object mutex = mutexField.get(clusterDelegate);
        mutexField.setAccessible(false);
        return mutex;
    }

    private class MockClusterConfig extends ClusterConfig {

        private MockClusterConfig(final int totalMaximumConnections) {
            super(totalMaximumConnections);
        }

        @Override
        protected ClusterConfig addHosts(final String... hosts) {
            return this;
        }

        @Override
        protected ClusterConfig addHosts(final Configuration config, final String... hosts) {
            return this;
        }
    }
}
