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
import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.UnknownHostException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Awaitility.fieldIn;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
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

        assertEquals(node.getRemoteAddress(), RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);
        assertEquals(node.getPort(), RiakNode.Builder.DEFAULT_REMOTE_PORT);
        assertEquals(node.getReadTimeout(), RiakNode.Builder.DEFAULT_TCP_READ_TIMEOUT);
        assertEquals(node.getNodeState(), State.CREATED);
        assertEquals(node.getMaxConnections(), Integer.MAX_VALUE);
        assertEquals(node.getConnectionTimeout(), RiakNode.Builder.DEFAULT_CONNECTION_TIMEOUT);
        assertEquals(node.getIdleTimeout(), RiakNode.Builder.DEFAULT_IDLE_TIMEOUT);
        assertEquals(node.getMinConnections(), RiakNode.Builder.DEFAULT_MIN_CONNECTIONS);
        assertEquals(node.availablePermits(), Integer.MAX_VALUE);
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

        assertEquals(node.getRemoteAddress(), REMOTE_ADDRESS);

        assertEquals(node.getNodeState(), RiakNode.State.CREATED);
        assertEquals(node.getMaxConnections(), MAX_CONNECTIONS);
        assertEquals(node.getConnectionTimeout(), CONNECTION_TIMEOUT);
        assertEquals(node.getIdleTimeout(), IDLE_TIMEOUT);
        assertEquals(node.getMinConnections(), MIN_CONNECTIONS);
        assertEquals(node.getRemoteAddress(), REMOTE_ADDRESS);
        assertEquals(node.availablePermits(), MAX_CONNECTIONS);
        assertEquals(node.getPort(), PORT);

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
    public void nodeStartsMinConnections() throws InterruptedException, UnknownHostException
    {
        final int MIN_CONNECTIONS = 5;

        ChannelFuture future = mock(ChannelFuture.class);
        Channel c = mock(Channel.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(c).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
            .withBootstrap(bootstrap)
            .withMinConnections(MIN_CONNECTIONS)
            .build();
        node.start();
        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(MIN_CONNECTIONS, available.size());
        assertEquals(node.getNodeState(), State.RUNNING);
    }

    @Test
    public void NodeRespectsMax() throws InterruptedException, UnknownHostException, Exception
    {
        final int MAX_CONNECTIONS = 2;

        ChannelFuture future = mock(ChannelFuture.class);
        Channel c = mock(Channel.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(c).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
            .withBootstrap(bootstrap)
            .withMaxConnections(MAX_CONNECTIONS)
            .build();
        node.start();

        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        assertNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        assertEquals(0, node.availablePermits());

        node.setMaxConnections(MAX_CONNECTIONS + 1);
        assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        assertEquals(0, node.availablePermits());
    }

    @Test
    public void channelsReturnedCorrectly() throws InterruptedException, UnknownHostException, Exception
    {
        final int MAX_CONNECTIONS = 1;

        ChannelFuture future = mock(ChannelFuture.class);
        Channel c = mock(Channel.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(c).when(future).channel();
        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
            .withBootstrap(bootstrap)
            .withMaxConnections(MAX_CONNECTIONS)
            .build();
        node.start();

        assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        assertNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        Whitebox.invokeMethod(node, "returnConnection", c);
        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(1, available.size());
        assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
    }

    @Test
    public void healthCheckChangesState()
        throws InterruptedException, UnknownHostException, Exception
    {
        ChannelFuture future = mock(ChannelFuture.class);
        Channel c = mock(Channel.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(false).when(future).isSuccess();
        doReturn(c).when(future).channel();

        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
            .withBootstrap(bootstrap)
            .build();

        for (int i = 0; i < 5; i++)
        {
            ChannelFutureListener listener = Whitebox.getInternalState(node, "closeListener", RiakNode.class);
            listener.operationComplete(future);
        }

        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        Whitebox.setInternalState(node, "state", State.RUNNING);
        Whitebox.invokeMethod(node, "checkHealth", new Object[0]);
        verify(listener).nodeStateChanged(node, State.HEALTH_CHECKING);

        doReturn(true).when(future).isSuccess();
        Whitebox.invokeMethod(node, "checkHealth", new Object[0]);
        verify(listener).nodeStateChanged(node, State.RUNNING);
    }

    @Test
    public void idleReaperTest() throws InterruptedException, UnknownHostException, Exception
    {

        ChannelFuture future = mock(ChannelFuture.class);
        Channel c = mock(Channel.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(c).when(future).channel();

        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
            .withBootstrap(bootstrap)
            .withMinConnections(1)
            .withIdleTimeout(1)
            .build();

        node.start();
        Channel[] channelArray = new Channel[6];
        for (int i = 0; i < 6; i++)
        {
            channelArray[i] = Whitebox.invokeMethod(node, "getConnection", new Object[0]);
            assertNotNull(channelArray[i]);
        }

        for (Channel channel : channelArray)
        {
            Whitebox.invokeMethod(node, "returnConnection", channel);
        }

        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(6, available.size());
        Thread.sleep(10);
        Whitebox.invokeMethod(node, "reapIdleConnections", new Object[0]);
        assertEquals(1, available.size());
    }

    @Test
    public void nodeExecutesOperation() throws InterruptedException, UnknownHostException
    {
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        ChannelFuture future = mock(ChannelFuture.class);
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakMessage response = PowerMockito.mock(RiakMessage.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(channel).closeFuture();
        doReturn(true).when(channel).isOpen();
        doReturn(channelPipeline).when(channel).pipeline();
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
        Map<?, ?> inProgressMap = Whitebox.getInternalState(node, "inProgressMap");
        assertEquals(1, inProgressMap.size());

        node.onSuccess(channel, response);
        assertEquals(0, inProgressMap.size());
        verify(operation).isDone();
    }

    @Test
    public void nodeFailsOperation() throws InterruptedException, UnknownHostException
    {
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        ChannelFuture future = mock(ChannelFuture.class);
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        Throwable t = mock(Throwable.class);
        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(channel).closeFuture();
        doReturn(true).when(channel).isOpen();
        doReturn(channelPipeline).when(channel).pipeline();
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
        Map<?, ?> inProgressMap = Whitebox.getInternalState(node, "inProgressMap");
        assertEquals(1, inProgressMap.size());
        node.onException(channel, t);
        await().atMost(500, TimeUnit.MILLISECONDS).until(fieldIn(operation).ofType(Throwable.class).andWithName("exception"), equalTo(t));
    }

    private class FutureOperationImpl extends FutureOperation<String, Message>
    {

        @Override
        protected String convert(List<Message> rawResponse) throws ExecutionException
        {
            return "value";
        }

        @Override
        protected Message decode(RiakMessage rawMessage)
        {
            return null;
        }

        @Override
        protected RiakMessage createChannelMessage()
        {
            return new RiakMessage((byte) 0, new byte[0]);
        }


    }

}
