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

import com.basho.riak.client.core.netty.RiakResponseHandler;
import com.basho.riak.client.util.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import java.net.UnknownHostException;
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
@PrepareForTest({Bootstrap.class, FutureOperation.class, RiakMessage.class})

public class RiakNodeTest
{
    @Test
    public void builderProducesDefaultNode() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder().build();
        
        String remoteAddress = node.getRemoteAddress();
        assertEquals(remoteAddress, ConnectionPool.Builder.DEFAULT_REMOTE_ADDRESS);
        
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
        
        RiakNode node = new RiakNode.Builder()
                        .withIdleTimeout(IDLE_TIMEOUT)
                        .withConnectionTimeout(CONNECTION_TIMEOUT)
                        .withMinConnections(MIN_CONNECTIONS)
                        .withMaxConnections(MAX_CONNECTIONS)
                        .withRemotePort(PORT)
                        .withReadTimeout(READ_TIMEOUT)
                        .withRemoteAddress(REMOTE_ADDRESS)
                        .withExecutor(EXECUTOR)
                        .withBootstrap(BOOTSTRAP)
                        .build();
        
        String remoteAddress = node.getRemoteAddress();
        assertEquals(remoteAddress, REMOTE_ADDRESS);
        
        ConnectionPool pool =
            Whitebox.getInternalState(node, "connectionPool");
        
        assertEquals(pool.getPoolState(), ConnectionPool.State.CREATED);
        assertEquals(pool.getMaxConnections(), MAX_CONNECTIONS);
        assertEquals(pool.getConnectionTimeout(), CONNECTION_TIMEOUT);
        assertEquals(pool.getIdleTimeout(), IDLE_TIMEOUT);
        assertEquals(pool.getMinConnections(), MIN_CONNECTIONS);
        assertEquals(pool.getRemoteAddress(), REMOTE_ADDRESS);
        assertEquals(pool.availablePermits(), MAX_CONNECTIONS);
        assertEquals(pool.getPort(), PORT);
       
        assertEquals(node.getReadTimeout(), READ_TIMEOUT);
    }
    
    @Test
    public void nodeRegistersListeners() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder().build();
        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        boolean removed = node.removeStateListener(listener);
        assertTrue(removed);
    }
    
    @Test
    public void nodeNotifiesListeners() throws UnknownHostException, Exception
    {
        RiakNode node = new RiakNode.Builder().build();
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
        RiakMessage response = PowerMockito.mock(RiakMessage.class);
        RiakResponseHandler responseHandler = mock(RiakResponseHandler.class);
        
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());
        
        doReturn(future).when(channel).closeFuture();
        doReturn(true).when(channel).isOpen();
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(responseHandler).when(channelPipeline).get(Constants.RESPONSE_HANDLER_CLASS);
        doReturn(future).when(channel).writeAndFlush(operation);
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(channel).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();
        
        RiakNode node = new RiakNode.Builder().withBootstrap(bootstrap).build();
        node.start();
        boolean accepted = node.execute(operation);
        assertTrue(accepted);
        verify(channel).writeAndFlush(operation);
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
    
}
