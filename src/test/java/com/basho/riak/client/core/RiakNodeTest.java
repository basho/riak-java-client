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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Bootstrap.class, FutureOperation.class})

public class RiakNodeTest
{
    @Test
    public void builderProducesDefaultNode() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder(Protocol.PB).build();
        
        String remoteAddress = Whitebox.getInternalState(node, "remoteAddress");
        assertEquals(remoteAddress, RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);
        
        EnumMap<Protocol, ConnectionPool> connectionPoolMap =
            Whitebox.getInternalState(node, "connectionPoolMap");
        assertEquals(1, connectionPoolMap.size());
        assertTrue(connectionPoolMap.containsKey(Protocol.PB));
        
    }
    
    @Test 
    public void builderProducesCorrectNode() throws UnknownHostException
    {
        final int IDLE_TIMEOUT = 2000;
        final int CONNECTION_TIMEOUT = 2001;
        final int MIN_CONNECTIONS = 2002;
        final int MAX_CONNECTIONS = 2003;
        final int PORT = 2004;
        final int READ_TIMEOUT = 2005;
        final String REMOTE_ADDRESS = "localhost";
        final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();
        final Bootstrap BOOTSTRAP = PowerMockito.spy(new Bootstrap());
        
        doReturn(BOOTSTRAP).when(BOOTSTRAP).clone();
        
        RiakNode node = new RiakNode.Builder(Protocol.PB)
                        .withIdleTimeout(Protocol.PB, IDLE_TIMEOUT)
                        .withConnectionTimeout(Protocol.PB, CONNECTION_TIMEOUT)
                        .withMinConnections(Protocol.PB, MIN_CONNECTIONS)
                        .withMaxConnections(Protocol.PB, MAX_CONNECTIONS)
                        .withPort(Protocol.PB, PORT)
                        .withReadTimeout(Protocol.PB, READ_TIMEOUT)
                        .withRemoteAddress(REMOTE_ADDRESS)
                        .withExecutor(EXECUTOR)
                        .withBootstrap(BOOTSTRAP)
                        .addProtocol(Protocol.HTTP)
                        .build();
        
        String remoteAddress = Whitebox.getInternalState(node, "remoteAddress");
        assertEquals(remoteAddress, REMOTE_ADDRESS);
        
        EnumMap<Protocol, ConnectionPool> connectionPoolMap =
            Whitebox.getInternalState(node, "connectionPoolMap");
        assertEquals(2, connectionPoolMap.size());
        assertTrue(connectionPoolMap.containsKey(Protocol.PB));
        ConnectionPool pool = connectionPoolMap.get(Protocol.PB);
        assertEquals(pool.getProtocol(), Protocol.PB);
        assertEquals(pool.getPoolState(), ConnectionPool.State.CREATED);
        assertEquals(pool.getMaxConnections(), MAX_CONNECTIONS);
        assertEquals(pool.getConnectionTimeout(), CONNECTION_TIMEOUT);
        assertEquals(pool.getIdleTimeout(), IDLE_TIMEOUT);
        assertEquals(pool.getMinConnections(), MIN_CONNECTIONS);
        assertEquals(pool.getRemoteAddress(), REMOTE_ADDRESS);
        assertEquals(pool.getReadTimeout(), READ_TIMEOUT);
        assertEquals(pool.getExecutor(), EXECUTOR);
        assertEquals(pool.getBootstrap(), BOOTSTRAP);
        assertEquals(pool.availablePermits(), MAX_CONNECTIONS);
        assertEquals(pool.getPort(), PORT);
        
        pool = connectionPoolMap.get(Protocol.HTTP);
        assertEquals(pool.getProtocol(), Protocol.HTTP);
        assertEquals(pool.getPoolState(), ConnectionPool.State.CREATED);
        assertEquals(pool.getMaxConnections(), Integer.MAX_VALUE);
        assertEquals(pool.getConnectionTimeout(), ConnectionPool.Builder.DEFAULT_CONNECTION_TIMEOUT);
        assertEquals(pool.getIdleTimeout(), ConnectionPool.Builder.DEFAULT_IDLE_TIMEOUT);
        assertEquals(pool.getMinConnections(), ConnectionPool.Builder.DEFAULT_MIN_CONNECTIONS);
        assertEquals(pool.getRemoteAddress(), REMOTE_ADDRESS);
        assertEquals(pool.getReadTimeout(), ConnectionPool.Builder.DEFAULT_READ_TIMEOUT);
        assertEquals(pool.getPort(), Protocol.HTTP.defaultPort());
        assertEquals(pool.getExecutor(), EXECUTOR);
        assertEquals(pool.getBootstrap(), BOOTSTRAP);
    }
    
    @Test
    public void nodeRegistersListeners() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder(Protocol.PB).build();
        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        boolean removed = node.removeStateListener(listener);
        assertTrue(removed);
    }
    
    @Test
    public void nodeNotifiesListeners() throws UnknownHostException, Exception
    {
        RiakNode node = new RiakNode.Builder(Protocol.PB).build();
        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        Whitebox.invokeMethod(node, "notifyStateListeners", new Object[0]);
        verify(listener).nodeStateChanged(node, RiakNode.State.CREATED);
    }
    
    @Test
    public void nodeExecutesOperation() throws InterruptedException, UnknownHostException
    {
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        ChannelFuture future = mock(ChannelFuture.class);
        FutureOperation operation = PowerMockito.mock(FutureOperation.class);
        RiakResponse response = mock(RiakResponse.class);
        
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());
        
        doReturn(future).when(channel).closeFuture();
        doReturn(true).when(channel).isOpen();
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(future).when(channel).write(operation);
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(channel).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();
        doReturn(new LinkedList<Protocol>(Arrays.asList(Protocol.values()))).when(operation).getProtocolPreflist();
        
        RiakNode node = new RiakNode.Builder(Protocol.PB).withBootstrap(bootstrap).build();
        node.start();
        boolean accepted = node.execute(operation);
        assertTrue(accepted);
        verify(channel).write(operation);
        verify(operation).setLastNode(node);
        Map<?,?> inProgressMap = Whitebox.getInternalState(node, "inProgressMap");
        assertEquals(1, inProgressMap.size());
        
        node.onSuccess(channel, response);
        assertEquals(0, inProgressMap.size());
        verify(operation).setResponse(response);
        
        accepted = node.execute(operation);
        assertTrue(accepted);
        node.onException(channel, null);
        verify(operation).setException(null);
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nodeThrowsExceptionUnsupportedProtocol() throws InterruptedException, UnknownHostException
    {
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        ChannelFuture future = mock(ChannelFuture.class);
        FutureOperation operation = PowerMockito.mock(FutureOperation.class);
        
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());
        
        doReturn(future).when(channel).closeFuture();
        doReturn(true).when(channel).isOpen();
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(future).when(channel).write(operation);
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(channel).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();
        
        LinkedList<Protocol> httpOnly = new LinkedList<Protocol>();
        httpOnly.add(Protocol.HTTP);
        doReturn(httpOnly).when(operation).getProtocolPreflist();
        
        RiakNode node = new RiakNode.Builder(Protocol.PB).withBootstrap(bootstrap).build();
        node.start();
        node.execute(operation);
    }
}
