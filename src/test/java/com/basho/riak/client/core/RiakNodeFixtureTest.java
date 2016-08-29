/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core;

import com.basho.riak.client.core.RiakNode.State;
import com.basho.riak.client.core.fixture.NetworkTestFixture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import io.netty.channel.Channel;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakNodeFixtureTest extends FixtureTest
{

    @Test
    public void closedConnectionsTriggerHealthCheck() throws InterruptedException, Exception
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                               .withMinConnections(10)
                               .build();
        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.start();
        node.addStateListener(listener);
        assertTrue(listener.get(10));
        node.shutdown().get();


    }

    @Test
    public void failedConnectionsTriggerHealthCheck() throws InterruptedException, Exception
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.NO_LISTENER)
                               .withMinConnections(10)
                               .withConnectionTimeout(10)
                               .build();

        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.addStateListener(listener);
        node.start();
        assertTrue(listener.get(10));
        node.shutdown().get();
    }

    @Test
    public void operationFailuresTriggerHealthCheck() throws InterruptedException, Exception
    {
        RiakNode node =
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_ERROR_STAY_OPEN)
                        .build();

        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.addStateListener(listener);
        node.start();

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");

        for (int i = 0; i < 6; i++)
        {
            FetchOperation operation =
                new FetchOperation.Builder(location)
                        .build();

            boolean accepted = node.execute(operation);
            assertTrue(accepted);
            operation.await();
            assertFalse(operation.isSuccess());
        }

        assertTrue(listener.get(10));
        node.shutdown().get();
    }

    @Test
    public void idleConnectionsAreRemoved() throws InterruptedException, Exception
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();

        node.start();

        // The node has 10 connections that should never be reaped. We want to make
        // it create 2 more by checking out 12, then return them all. The extra 2
        // should get reaped.

        List<Channel> channelList = new LinkedList<>();
        for (int i = 0; i < 12; i++)
        {
            channelList.add(Whitebox.invokeMethod(node, "getConnection"));
        }

        for (Channel c : channelList)
        {
            Whitebox.invokeMethod(node, "returnConnection", c);
        }

        LinkedBlockingDeque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(available.size(), 12);

        Thread.sleep(10000);

        assertEquals(available.size(), 10);

        node.shutdown().get();

    }

    @Test
    public void nodeGoingDown() throws IOException, InterruptedException, ExecutionException
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();

        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.start();
        node.addStateListener(listener);

        try
        {
            fixture.shutdown();
            assertTrue(listener.get(10));
            assertEquals(node.getNodeState(), State.HEALTH_CHECKING);

            node.shutdown().get();
        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
    }

    @Test
    public void nodeRecovery() throws IOException, InterruptedException, ExecutionException
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();

        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.start();
        node.addStateListener(listener);

        try
        {
            fixture.shutdown();

            assertTrue(listener.get(10));
            node.removeStateListener(listener);
            listener = new StateListener(RiakNode.State.RUNNING);
            node.addStateListener(listener);

        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }

        assertTrue(listener.get(10));
        assertEquals(node.getNodeState(), State.RUNNING);
        node.shutdown().get();
    }

    @Test
    public void operationSuccess() throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode node =
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                        .build();

        node.start();

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");

        FetchOperation operation =
            new FetchOperation.Builder(location)
                    .build();

        boolean accepted = node.execute(operation);
        assertTrue(accepted);
        FetchOperation.Response response = operation.get();
        assertEquals(response.getObjectList().get(0).getValue().toString(), "This is a value!");
        assertTrue(!response.isNotFound());
        node.shutdown().get();
    }

    @Test
    public void operationFail() throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode node =
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                        .build();
        node.start();

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");

        FetchOperation operation =
            new FetchOperation.Builder(location)
                    .build();

        boolean accepted = node.execute(operation);
        operation.await();
        assertFalse(operation.isSuccess());
        assertNotNull(operation.cause());
        node.shutdown().get();
    }

    @Test
    public void nodeChangesStateOnPoolState() throws IOException, InterruptedException, ExecutionException
    {
        RiakNode node =
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withMinConnections(10)
                        .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                        .build();

        node.start();

        StateListener listener = new StateListener(RiakNode.State.HEALTH_CHECKING);
        node.addStateListener(listener);

        try
        {
            fixture.shutdown();
            assertTrue(listener.get(10));
            assertEquals(RiakNode.State.HEALTH_CHECKING, node.getNodeState());
        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }

        node.shutdown().get();

    }

    public static class StateListener implements NodeStateListener
    {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final EnumSet<RiakNode.State> expected;


        public StateListener(RiakNode.State... expected)
        {
            this.expected = EnumSet.copyOf(Arrays.asList(expected));
        }

        @Override
        public void nodeStateChanged(RiakNode node, State state)
        {
            expected.remove(state);
            if (expected.size() == 0)
            {
                latch.countDown();
            }
        }

        public boolean get(int timeout) throws InterruptedException
        {
            return latch.await(timeout, TimeUnit.SECONDS);
        }

    }

}
