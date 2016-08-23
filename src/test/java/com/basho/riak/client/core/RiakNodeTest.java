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

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.ListenableFuture;
import com.basho.riak.client.core.RiakNode.State;
import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.BlockingOperationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Awaitility.fieldIn;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Bootstrap.class, FutureOperation.class, RiakMessage.class})

public class RiakNodeTest
{
    @Test
    public void builderProducesDefaultNode()
    {
        RiakNode node = new RiakNode.Builder().build();

        assertEquals(node.getRemoteAddress(), RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);
        assertEquals(node.getPort(), RiakNode.Builder.DEFAULT_REMOTE_PORT);
        assertEquals(node.getNodeState(), State.CREATED);
        assertEquals(node.getMaxConnections(), Integer.MAX_VALUE);
        assertEquals(node.getConnectionTimeout(), RiakNode.Builder.DEFAULT_CONNECTION_TIMEOUT);
        assertEquals(node.getIdleTimeout(), RiakNode.Builder.DEFAULT_IDLE_TIMEOUT);
        assertEquals(node.getMinConnections(), RiakNode.Builder.DEFAULT_MIN_CONNECTIONS);
        assertEquals(node.availablePermits(), Integer.MAX_VALUE);
    }

    @Test
    public void builderProducesCorrectNode()
    {
        final int IDLE_TIMEOUT = 2000;
        final int CONNECTION_TIMEOUT = 2001;
        final int MIN_CONNECTIONS = 2002;
        final int MAX_CONNECTIONS = 2003;
        final int PORT = 2004;
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

    }

    @Test
    public void nodeRegistersListeners()
    {
        RiakNode node = new RiakNode.Builder().build();
        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        boolean removed = node.removeStateListener(listener);
        assertTrue(removed);
    }


    @Test
    public void nodeNotifiesListeners() throws Exception
    {
        RiakNode node = new RiakNode.Builder().build();
        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        Whitebox.invokeMethod(node, "notifyStateListeners");
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
    public void NodeRespectsMax() throws Exception
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
            assertNotNull(Whitebox.invokeMethod(node, "getConnection"));
        }

        assertNull(Whitebox.invokeMethod(node, "getConnection"));
        assertEquals(0, node.availablePermits());

        node.setMaxConnections(MAX_CONNECTIONS + 1);
        assertNotNull(Whitebox.invokeMethod(node, "getConnection"));
        assertEquals(0, node.availablePermits());
    }

    @Test
    public void NodeMaxCanBeExplicitlySetToUnlimited() throws Exception
    {
        final int UNLIMITED = 0;
        new RiakNode.Builder()
            .withMaxConnections(UNLIMITED)
            .build();
    }

    @Test
    public void channelsReturnedCorrectly() throws Exception
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

        assertNotNull(Whitebox.invokeMethod(node, "getConnection"));
        assertNull(Whitebox.invokeMethod(node, "getConnection"));
        Whitebox.invokeMethod(node, "returnConnection", c);
        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(1, available.size());
        assertNotNull(Whitebox.invokeMethod(node, "getConnection"));
    }

    @Test
    public void healthCheckChangesState() throws Exception
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
            ChannelFutureListener listener = Whitebox.getInternalState(node, "inAvailableCloseListener", RiakNode.class);
            listener.operationComplete(future);
        }

        NodeStateListener listener = mock(NodeStateListener.class);
        node.addStateListener(listener);
        Whitebox.setInternalState(node, "state", State.RUNNING);
        Whitebox.invokeMethod(node, "checkHealth");
        verify(listener).nodeStateChanged(node, State.HEALTH_CHECKING);
    }

    @Test
    public void idleReaperTest() throws Exception
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
            channelArray[i] = Whitebox.invokeMethod(node, "getConnection");
            assertNotNull(channelArray[i]);
        }

        for (Channel channel : channelArray)
        {
            Whitebox.invokeMethod(node, "returnConnection", channel);
        }

        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(6, available.size());
        Thread.sleep(10);
        Whitebox.invokeMethod(node, "reapIdleConnections");
        assertEquals(1, available.size());
    }

    @Test
    public void closedConnectionsOnReturnTest() throws Exception
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
                .withMaxConnections(6)
                .build();

        node.start();
        Channel[] channelArray = new Channel[6];
        for (int i = 0; i < 6; i++)
        {
            channelArray[i] = Whitebox.invokeMethod(node, "getConnection");
            assertNotNull(channelArray[i]);
        }

        doReturn(false).when(c).isOpen();

        for (Channel channel : channelArray)
        {
            Whitebox.invokeMethod(node, "returnConnection", channel);
        }

        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(0, available.size());

        assertEquals(6, node.availablePermits());

        doReturn(true).when(c).isOpen();

        Channel c1 = Whitebox.invokeMethod(node, "getConnection");
        assertEquals(0, available.size());
        assertEquals(5, node.availablePermits());

        Whitebox.invokeMethod(node, "returnConnection", c1);

        assertEquals(1, available.size());
        assertEquals(6, node.availablePermits());
    }

    @Test
    public void deadConnectionsOnGetConnection() throws Exception
    {
        ChannelFuture future = mock(ChannelFuture.class);
        ChannelFuture future2 = mock(ChannelFuture.class);

        Channel c = mock(Channel.class);
        Channel c2 = mock(Channel.class);

        Bootstrap bootstrap = PowerMockito.spy(new Bootstrap());

        doReturn(future).when(c).closeFuture();
        doReturn(true).when(c).isOpen();
        doReturn(future).when(future).await();
        doReturn(true).when(future).isSuccess();
        doReturn(c).when(future).channel();

        doReturn(future2).when(c2).closeFuture();
        doReturn(true).when(c2).isOpen();
        doReturn(future2).when(future2).await();
        doReturn(true).when(future2).isSuccess();
        doReturn(c2).when(future2).channel();

        doReturn(future).when(bootstrap).connect();
        doReturn(bootstrap).when(bootstrap).clone();

        RiakNode node = new RiakNode.Builder()
                .withBootstrap(bootstrap)
                .withMinConnections(1)
                .withMaxConnections(1)
                .build();

        node.start();

        Deque<?> available = Whitebox.getInternalState(node, "available");
        assertEquals(1, available.size());
        assertEquals(1, node.availablePermits());

        doReturn(false).when(c).isOpen();
        doReturn(future2).when(bootstrap).connect();

        Channel fetchedChannel = Whitebox.invokeMethod(node, "getConnection");

        doReturn(true).when(c).isOpen();

        assertEquals(0, available.size());
        assertEquals(0, node.availablePermits());
        assertNotSame(fetchedChannel, c);
        assertSame(fetchedChannel, c2);
    }

    @Test
    public void nodeRefreshesInetSocketAddressWhenConnectionsDie() throws Exception
    {
        // Setup mock bootstrap / ChannelFuture / Channel
        // Should behave like good channel.
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

        // Capture arguments passed to InetSocketAddress ctors.
        ArgumentCaptor<InetSocketAddress> addressCaptor = ArgumentCaptor.forClass(InetSocketAddress.class);

        RiakNode node = new RiakNode.Builder()
                            .withBootstrap(bootstrap)
                            .withMinConnections(1)
                            .withMaxConnections(1)
                            .build();

        node.start();

        // Get a connection, return it like all is good.
        Channel fetchedChannel = Whitebox.invokeMethod(node, "getConnection");
        Whitebox.invokeMethod(node, "returnConnection", fetchedChannel);

        // Set the mock channel to return false on calling channel.isOpen, to force a lookup.
        doReturn(false).when(c).isOpen();

        // Get another connection, should fail + do 2nd lookup.
        Whitebox.invokeMethod(node, "getConnection");

        // Verify that the lookup occurred twice. Once on startup, once after the failed 2nd getConnection.
        verify(bootstrap, times(2)).remoteAddress(addressCaptor.capture());

        // Verify that we have two different objects with same info, thus verifying that we had two lookups.
        final List<InetSocketAddress> addressesUsed = addressCaptor.getAllValues();
        assertEquals(2, addressesUsed.size());

        // Make sure we aren't referring to same object instance, but that they are equal.
        final InetSocketAddress firstAddress = addressesUsed.get(0);
        final InetSocketAddress secondAddress = addressesUsed.get(1);
        assertNotSame(firstAddress, secondAddress);
        assertEquals(firstAddress, secondAddress);
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
        assertEquals(1, node.getNumInProgress());

        node.onSuccess(channel, response);
        assertEquals(0, node.getNumInProgress());
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

    @Test(expected = UnknownHostException.class)
    public void failsResolvingHostname() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder().withRemoteAddress("invalid-host-name").build();
        node.start();
    }

    @Test(expected = ExecutionException.class)
    public void BlockingExceptionIsCaughtWhileWaitingForChannelToOpen() throws UnknownHostException, InterruptedException, ExecutionException
    {
        final BlockingExceptionTestSetup setup = new BlockingExceptionTestSetup();
        final CompoundCommand command = setup.getCommand();

        final RiakNode node = new RiakNode.Builder().build();
        RiakCluster cluster = RiakCluster.builder(node).withExecutionAttempts(1)
                                         .withBootstrap(setup.getBootstrap()).build();

        Runnable secondCmdSetup = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Deque<?> available = Whitebox.getInternalState(node, "available");

                    // throw away connection, imitate that they are all used.
                    available.poll();

                    // throw a BlockingOperationException when a new connection opened.
                    doThrow(BlockingOperationException.class).when(setup.getChannelFuture()).await(); //
                }
                catch (InterruptedException ignored) {}
            }
        };

        // Throw away all connections before second command is run, and throw a BlockingOperationException when a new one is opened.
        command.secondCommand.injectSetup(secondCmdSetup);

        startAndRunBlockingExceptionTest(command, setup.getChannel(), setup.getChannelFuture(), node, cluster);
    }

    @Test(expected = ExecutionException.class)
    public void BlockingExceptionIsCaughtWhileBlockedOnMaxConnections()
            throws UnknownHostException, InterruptedException, ExecutionException, NoSuchFieldException,
            IllegalAccessException
    {
        BlockingExceptionTestSetup setup = new BlockingExceptionTestSetup();
        final CompoundCommand command = setup.getCommand();

        final RiakNode node = new RiakNode.Builder().withBlockOnMaxConnections(true).withMaxConnections(1).build();

        Field field = node.getClass().getDeclaredField("permits");
        field.setAccessible(true);
        field.set(node, new BadSync(1));

        RiakCluster cluster = RiakCluster.builder(node)
                                         .withExecutionAttempts(1)
                                         .withBootstrap(setup.getBootstrap()).build();

        Runnable secondCmdSetup = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    RiakNode.Sync permits = Whitebox.getInternalState(node, "permits");

                    // Use up permit, imitate that they are all used.
                    permits.acquire();
                }
                catch (InterruptedException ignored) {}
            }
        };

        // Throw away all connections before second command is run, and throw a BlockingOperationException when a new one is opened.
        command.secondCommand.injectSetup(secondCmdSetup);

        startAndRunBlockingExceptionTest(command, setup.getChannel(), setup.getChannelFuture(), node, cluster);
    }

    private void startAndRunBlockingExceptionTest(CompoundCommand compoundCommand,
                                                  Channel channel,
                                                  ChannelFuture future,
                                                  RiakNode node,
                                                  RiakCluster cluster) throws InterruptedException, ExecutionException
    {
        cluster.start();

        doAnswer(createYesChannelListenerAnswer(node,channel,future))
                .when(future).addListener(any(ChannelFutureListener.class));

        final RiakFuture<String, Void> ccFuture = compoundCommand.executeAsync(cluster);
        ccFuture.await();
        try
        {
            ccFuture.get();
        }
        catch (ExecutionException ex)
        {
            assertEquals(NoNodesAvailableException.class, ex.getCause().getClass());
            throw ex;
        }
    }

    /**
     * Don't wait when we run out of permits, throw an exception
     */
    private class BadSync extends RiakNode.Sync
    {
        public BadSync(int numPermits)
        {
            super(numPermits);
        }

        @Override
        public void acquire() throws InterruptedException
        {
            if(this.availablePermits() == 0)
            {
                throw new BlockingOperationException();
            }
            super.acquire();
        }
    }

    /**
     * Say yes/onSuccess to all listeners that are added, except inProgressCloseListener. Completes operations.
     */
    private Answer<Void> createYesChannelListenerAnswer(final RiakNode node, final Channel channel, final ChannelFuture future)
    {
        return new Answer<Void>()
        {
            final ChannelFutureListener writeListener = Whitebox.getInternalState(node, "writeListener");
            final ChannelFutureListener inProgressCloseListener =
                    Whitebox.getInternalState(node, "inProgressCloseListener");

            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable
            {
                final ChannelFutureListener listener = (ChannelFutureListener) invocationOnMock.getArguments()[0];
                if (listener.equals(inProgressCloseListener))
                {
                    return null;
                }

                listener.operationComplete(future);

                if (listener.equals(writeListener))
                {
                    node.onSuccess(channel, new RiakMessage((byte) 0, new byte[]{}));
                }
                return null;
            }
        };
    }

    private class FutureOperationImpl extends FutureOperation<String, Message, Void>
    {
        @Override
        protected String convert(List<Message> rawResponse)
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

        @Override
        public Void getQueryInfo()
        {
            return null;
        }
    }

    private class NopCommand extends RiakCommand<String, Void>
    {
        public FutureOperationImpl operation = new FutureOperationImpl();
        private Runnable testSetup;

        @Override
        protected RiakFuture<String, Void> executeAsync(RiakCluster cluster)
        {
            if(testSetup != null)
            {
                testSetup.run();
            }

            return cluster.execute(operation);
        }

        public void injectSetup(Runnable testSetup) {this.testSetup = testSetup;}
    }

    /**
     * Imitates a compound command like UpdateValue, where we run 1 command, then run a second in the callback of the first.
     */
    private class CompoundCommand extends RiakCommand<String, Void>
    {
        public NopCommand firstCommand = new NopCommand();
        public NopCommand secondCommand = new NopCommand();

        @Override
        protected RiakFuture<String, Void> executeAsync(final RiakCluster cluster)
        {
            final CompoundFuture cf = new CompoundFuture();

            final RiakFuture<String, Void> firstFuture = firstCommand.executeAsync(cluster);

            RiakFutureListener<String, Void> listener =
                    new RiakFutureListener<String, Void>()
                    {
                        @Override
                        public void handle(RiakFuture<String, Void> f)
                        {
                            final RiakFuture<String, Void> secondFuture =
                                    secondCommand.executeAsync(cluster);
                            secondFuture.addListener(cf);
                        }
                    };

            firstFuture.addListener(listener);

            return cf;
        }

        private class CompoundFuture extends ListenableFuture<String, Void>
            implements RiakFutureListener<String, Void>
        {
            private String finalValue;
            private Throwable exception;
            private final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void handle(RiakFuture<String, Void> f)
            {
                try
                {
                    setResponse(f.get());
                }
                catch (InterruptedException | ExecutionException ex)
                {
                    setException(ex);
                }
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                return false;
            }

            @Override
            public String get() throws InterruptedException, ExecutionException
            {
                latch.await();

                if (exception != null)
                {
                    if(exception.getClass() == ExecutionException.class)
                    {
                        throw (ExecutionException) exception;
                    }

                    throw new ExecutionException(exception);
                }
                else
                {
                    return finalValue;
                }
            }

            @Override
            public String get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                boolean succeed = latch.await(timeout, unit);

                if (!succeed)
                {
                    throw new TimeoutException();
                }
                else if (exception != null)
                {
                    if(exception.getClass() == ExecutionException.class)
                    {
                        throw (ExecutionException) exception;
                    }

                    throw new ExecutionException(exception);
                }
                else
                {
                    return finalValue;
                }
            }

            @Override
            public boolean isCancelled()
            {
                return false;
            }

            @Override
            public boolean isDone()
            {
                return latch.getCount() != 1;
            }

            @Override
            public void await() throws InterruptedException
            {
                latch.await();
            }

            @Override
            public boolean await(long timeout, TimeUnit unit) throws InterruptedException
            {
                return latch.await(timeout, unit);
            }

            @Override
            public String getNow()
            {
                return finalValue;
            }

            @Override
            public boolean isSuccess()
            {
                return isDone() && exception == null;
            }

            @Override
            public Throwable cause()
            {
                return this.exception;
            }

            @Override
            public Void getQueryInfo()
            {
                return null;
            }

            private void setResponse(String response)
            {
                this.finalValue = response;
                latch.countDown();
                notifyListeners();
            }

            private void setException(Throwable t)
            {
                this.exception = t;
                latch.countDown();
                notifyListeners();
            }
        }
    }

    private class BlockingExceptionTestSetup
    {
        private CompoundCommand compoundCommand;
        private Channel channel;
        private ChannelFuture channelFuture;
        private Bootstrap bootstrap;

        public CompoundCommand getCommand()
        {
            return compoundCommand;
        }

        public Channel getChannel()
        {
            return channel;
        }

        public ChannelFuture getChannelFuture()
        {
            return channelFuture;
        }

        public Bootstrap getBootstrap()
        {
            return bootstrap;
        }

        public BlockingExceptionTestSetup() throws InterruptedException
        {
            compoundCommand = new CompoundCommand();

            channel = mock(Channel.class);
            ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
            channelFuture = mock(ChannelFuture.class);

            bootstrap = PowerMockito.spy(new Bootstrap());

            doReturn(channelFuture).when(channel).closeFuture();
            doReturn(true).when(channel).isOpen();
            doReturn(channelPipeline).when(channel).pipeline();
            doReturn(channelFuture).when(channel).writeAndFlush(compoundCommand.firstCommand.operation);
            doReturn(channelFuture).when(channelFuture).await();
            doReturn(true).when(channelFuture).isSuccess();
            doReturn(channel).when(channelFuture).channel();
            doReturn(channelFuture).when(bootstrap).connect();
            doReturn(bootstrap).when(bootstrap).clone();
        }
    }
}
