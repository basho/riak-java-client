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

import com.basho.riak.client.core.ConnectionPool.State;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.UnknownHostException;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
@PrepareForTest(Bootstrap.class)
public class ConnectionPoolTest
{
    @Test
    public void builderProducesDefaultPool() throws UnknownHostException
    {
        ConnectionPool.Builder builder = new ConnectionPool.Builder(Protocol.PB);
        ConnectionPool pool = builder.build();
        
        assertEquals(pool.getProtocol(), Protocol.PB);
        assertEquals(pool.getPoolState(), State.CREATED);
        assertEquals(pool.getMaxConnections(), Integer.MAX_VALUE);
        assertEquals(pool.getConnectionTimeout(), ConnectionPool.Builder.DEFAULT_CONNECTION_TIMEOUT);
        assertEquals(pool.getIdleTimeout(), ConnectionPool.Builder.DEFAULT_IDLE_TIMEOUT);
        assertEquals(pool.getMinConnections(), ConnectionPool.Builder.DEFAULT_MIN_CONNECTIONS);
        assertEquals(pool.getRemoteAddress(), ConnectionPool.Builder.DEFAULT_REMOTE_ADDRESS);
        assertEquals(pool.getReadTimeout(), ConnectionPool.Builder.DEFAULT_READ_TIMEOUT);
        assertNotNull(pool.getExecutor());
        assertNotNull(pool.getBootstrap());
    }
    
    @Test
    public void builderProducesCorrectPool() throws UnknownHostException 
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withIdleTimeout(IDLE_TIMEOUT)
                                    .withConnectionTimeout(CONNECTION_TIMEOUT)
                                    .withMinConnections(MIN_CONNECTIONS)
                                    .withMaxConnections(MAX_CONNECTIONS)
                                    .withPort(PORT)
                                    .withReadTimeout(READ_TIMEOUT)
                                    .withRemoteAddress(REMOTE_ADDRESS)
                                    .withExecutor(EXECUTOR)
                                    .withBootstrap(BOOTSTRAP)
                                    .build();
        
        assertEquals(pool.getProtocol(), Protocol.PB);
        assertEquals(pool.getPoolState(), State.CREATED);
        assertEquals(pool.getMaxConnections(), MAX_CONNECTIONS);
        assertEquals(pool.getConnectionTimeout(), CONNECTION_TIMEOUT);
        assertEquals(pool.getIdleTimeout(), IDLE_TIMEOUT);
        assertEquals(pool.getMinConnections(), MIN_CONNECTIONS);
        assertEquals(pool.getRemoteAddress(), REMOTE_ADDRESS);
        assertEquals(pool.getReadTimeout(), READ_TIMEOUT);
        assertEquals(pool.getExecutor(), EXECUTOR);
        assertEquals(pool.getBootstrap(), BOOTSTRAP);
        assertEquals(pool.availablePermits(), MAX_CONNECTIONS);
    }
    
    @Test
    public void poolRegistersListener() throws UnknownHostException
    {
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB).build();
        PoolStateListener listener = mock(PoolStateListener.class);
        pool.addStateListener(listener);
        boolean removed = pool.removeStateListener(listener);
        assertTrue(removed);
    }
    
    @Test
    public void poolNotifiesListeners() throws UnknownHostException, Exception
    {
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB).build();
        PoolStateListener listener = mock(PoolStateListener.class);
        pool.addStateListener(listener);
        Whitebox.invokeMethod(pool, "notifyStateListeners", new Object[0] );
        verify(listener).poolStateChanged(pool, pool.getPoolState());
    }
    
    @Test
    public void poolStartsMinConnections() throws UnknownHostException, InterruptedException
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withBootstrap(bootstrap)
                                    .withMinConnections(MIN_CONNECTIONS)
                                    .build();
        pool.start();
        Deque<?> available = Whitebox.getInternalState(pool, "available");
        assertEquals(MIN_CONNECTIONS, available.size());
        assertEquals(pool.getPoolState(), State.RUNNING);
    }
    
    @Test
    public void poolRespectsMax() throws InterruptedException, UnknownHostException
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withBootstrap(bootstrap)
                                    .withMaxConnections(MAX_CONNECTIONS)
                                    .build();
        pool.start();
        
        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            assertNotNull(pool.getConnection());
        }
        
        assertNull(pool.getConnection());
        assertEquals(0, pool.availablePermits());
        
        pool.setMaxConnections(MAX_CONNECTIONS + 1);
        assertNotNull(pool.getConnection());
        assertEquals(0, pool.availablePermits());
    }
    
    @Test
    public void channelsReturnedCorrectly() throws InterruptedException, UnknownHostException
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withBootstrap(bootstrap)
                                    .withMaxConnections(MAX_CONNECTIONS)
                                    .build();
        pool.start();
        
        assertNotNull(pool.getConnection());
        assertNull(pool.getConnection());
        pool.returnConnection(c);
        Deque<?> available = Whitebox.getInternalState(pool, "available");
        assertEquals(1, available.size());
        assertNotNull(pool.getConnection());
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withBootstrap(bootstrap)
                                    .build();
        
        for (int i = 0; i < 5; i++)
        {
            pool.operationComplete(future);
        }
        
        PoolStateListener listener = mock(PoolStateListener.class);
        pool.addStateListener(listener);
        Whitebox.setInternalState(pool, "state", State.RUNNING);
        Whitebox.invokeMethod(pool, "checkHealth", new Object[0] );
        verify(listener).poolStateChanged(pool, State.HEALTH_CHECKING);
        
        doReturn(true).when(future).isSuccess();
        Whitebox.invokeMethod(pool, "checkHealth", new Object[0] );
        verify(listener).poolStateChanged(pool, State.RUNNING);
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
        
        ConnectionPool pool = new ConnectionPool.Builder(Protocol.PB)
                                    .withBootstrap(bootstrap)
                                    .withMinConnections(1)
                                    .withIdleTimeout(1)
                                    .build();
        
        pool.start();
        Channel[] channelArray = new Channel[6];
        for (int i = 0; i < 6; i++)
        {
            channelArray[i] = pool.getConnection();
            assertNotNull(channelArray[i]);
        }
        
        for (Channel channel : channelArray)
        {
            pool.returnConnection(channel);
        }
        
        Deque<?> available = Whitebox.getInternalState(pool, "available");
        assertEquals(6, available.size());
        Thread.sleep(10);
        Whitebox.invokeMethod(pool, "reapIdleConnections", new Object[0] );
        assertEquals(1, available.size());
        
    }
}
