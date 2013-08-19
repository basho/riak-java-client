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

import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakNode.State;
import com.basho.riak.client.core.fixture.NetworkTestFixture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

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
    public void idleConnectionsAreRemoved() throws UnknownHostException, InterruptedException, Exception
    {
        RiakNode node = new RiakNode.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        node.start();
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
        
        node.shutdown();
        
    }
    
    @Test
    public void nodeGoingDown() throws UnknownHostException, IOException, InterruptedException
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

            node.shutdown();
        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
    }
    
    @Test
    public void nodeRecovery() throws UnknownHostException, IOException, InterruptedException
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
        FetchOperation<RiakObject> operation = 
            new FetchOperation<RiakObject>(ByteArrayWrapper.unsafeCreate("test_bucket".getBytes()), 
                                            ByteArrayWrapper.unsafeCreate("test_key2".getBytes()))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        boolean accepted = node.execute(operation);
        assertTrue(accepted);
        RiakObject response = operation.get();
        assertEquals(response.getValueAsString(), "This is a value!");
        assertTrue(!response.isNotFound());
    }
    
    @Test(expected=ExecutionException.class)
    public void operationFail() throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode node = 
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                        .build();
        node.start();
        FetchOperation<RiakObject> operation = 
            new FetchOperation<RiakObject>(ByteArrayWrapper.unsafeCreate("test_bucket".getBytes()), 
                                            ByteArrayWrapper.unsafeCreate("test_key2".getBytes()))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        boolean accepted = node.execute(operation);
        RiakObject response = operation.get();
    }
    
    @Test(expected=ExecutionException.class)
    public void operationTimesOut() throws IOException, InterruptedException, ExecutionException
    {
        NetworkTestFixture nonRunningFixture = new NetworkTestFixture(8000);
        RiakNode node = 
            new RiakNode.Builder()
                        .withRemoteAddress("127.0.0.1")
                        .withRemotePort(8000 + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                        .withReadTimeout(5000)
                        .build();
        node.start();
        FetchOperation<RiakObject> operation = 
            new FetchOperation<RiakObject>(ByteArrayWrapper.unsafeCreate("test_bucket".getBytes()), 
                                            ByteArrayWrapper.unsafeCreate("test_key2".getBytes()))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());

        
        boolean accepted = node.execute(operation);
        RiakObject response = operation.get();
    }
    
    @Test
    public void nodeChangesStateOnPoolState() throws UnknownHostException, IOException, InterruptedException
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
        
    }
    
}
