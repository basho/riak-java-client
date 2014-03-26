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
import com.basho.riak.client.query.Location;
import io.netty.channel.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RiakNode.class)
public class RiakNodeFixtureTest extends FixtureTest
{
    
    @Test
    public void closedConnectionsTriggerHealthCheck() throws UnknownHostException, InterruptedException, Exception
    {
        RiakNode node = PowerMockito.spy(new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                               .withMinConnections(10)
                               .build());
        node.start();
        Thread.sleep(3000);
        node.shutdown();
        
        PowerMockito.verifyPrivate(node, atLeastOnce()).invoke("checkHealth", new Object[0]);
        
    }
    
    @Test
    public void failedConnectionsTriggerHealthCheck() throws UnknownHostException, InterruptedException, Exception
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.NO_LISTENER)
                               .withMinConnections(10)
                               .withConnectionTimeout(10)
                               .build();
        
        node.start();
        Thread.sleep(3000);
        assertEquals(State.HEALTH_CHECKING, node.getNodeState());
        node.shutdown().get();
    }
        
    @Test
    public void operationFailuresTriggerHealthCheck() throws UnknownHostException, InterruptedException, Exception
    {
        RiakNode node = 
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_ERROR_STAY_OPEN)
                        .build();
        
        node.start();
        
        Location location = new Location("test_bucket").setKey("test_key2");
        
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
        
        Thread.sleep(2000);
        assertEquals(State.HEALTH_CHECKING, node.getNodeState());
        node.shutdown().get();
    }
    
    @Test
    public void idleConnectionsAreRemoved() throws UnknownHostException, InterruptedException, Exception
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
        
        List<Channel> channelList = new LinkedList<Channel>();
        for (int i = 0; i < 12; i++)
        {
            channelList.add((Channel)Whitebox.invokeMethod(node, "getConnection", new Object[0]));
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
    public void nodeGoingDown() throws UnknownHostException, IOException, InterruptedException, ExecutionException
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        NodeStateListener mockListener = mock(NodeStateListener.class);
        node.start();
        node.addStateListener(mockListener);
        
        try
        {   
            fixture.shutdown();
            // Heh, a future would sure be useful here
            Thread.sleep(2000);

            verify(mockListener).nodeStateChanged(node, RiakNode.State.HEALTH_CHECKING);
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
    public void nodeRecovery() throws UnknownHostException, IOException, InterruptedException, ExecutionException
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        NodeStateListener mockListener = mock(NodeStateListener.class);
        node.start();
        node.addStateListener(mockListener);
        
        try
        {   
            fixture.shutdown();
        
            Thread.sleep(2000);

            verify(mockListener).nodeStateChanged(node, State.HEALTH_CHECKING);
            assertEquals(node.getNodeState(), State.HEALTH_CHECKING);

        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
        
        Thread.sleep(1000);
        
        verify(mockListener).nodeStateChanged(node, State.RUNNING);
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
        
        Location location = new Location("test_bucket").setKey("test_key2");
        
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
        
        Location location = new Location("test_bucket").setKey("test_key2");
        FetchOperation operation = 
            new FetchOperation.Builder(location)
                    .build();
        
        boolean accepted = node.execute(operation);
        FetchOperation.Response response = operation.get();
        assertFalse(operation.isSuccess());
        assertNotNull(operation.cause());
        assertNotNull(operation.cause().getCause());
        node.shutdown().get();
    }

    @Test
    public void nodeChangesStateOnPoolState() throws UnknownHostException, IOException, InterruptedException, ExecutionException
    {
        RiakNode node = 
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withMinConnections(10)
                        .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                        .build();

        node.start();
        NodeStateListener mockListener = mock(NodeStateListener.class);
        node.addStateListener(mockListener);
        
        try
        {
            fixture.shutdown();
            Thread.sleep(2000);
            verify(mockListener).nodeStateChanged(node, RiakNode.State.HEALTH_CHECKING);
            assertEquals(RiakNode.State.HEALTH_CHECKING, node.getNodeState());
        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
        
        node.shutdown().get();
        
    }
    
}
