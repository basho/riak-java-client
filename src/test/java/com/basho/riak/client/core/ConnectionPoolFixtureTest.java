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
import com.basho.riak.client.core.fixture.NetworkTestFixture;
import io.netty.channel.Channel;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import static org.junit.Assert.assertEquals;
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
@PrepareForTest(ConnectionPool.class)
public class ConnectionPoolFixtureTest extends FixtureTest
{
    
    @Test
    public void closedConnectionsTriggerHealthCheck() throws UnknownHostException, InterruptedException, Exception
    {
        ConnectionPool pool = PowerMockito.spy(new ConnectionPool.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                               .withMinConnections(10)
                               .build());
        pool.start();
        Thread.sleep(3000);
        pool.shutdown();
        
        PowerMockito.verifyPrivate(pool, atLeastOnce()).invoke("checkHealth", new Object[0]);
        
    }
    
    @Test
    public void idleConnectionsAreRemoved() throws UnknownHostException, InterruptedException
    {
        ConnectionPool pool = new ConnectionPool.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        pool.start();
        List<Channel> channelList = new LinkedList<Channel>();
        for (int i = 0; i < 12; i++)
        {
            channelList.add(pool.getConnection());
        }
        
        for (Channel c : channelList)
        {
            pool.returnConnection(c);
        }
        
        LinkedBlockingDeque<?> available = Whitebox.getInternalState(pool, "available");
        assertEquals(available.size(), 12);
        
        Thread.sleep(10000);
        
        assertEquals(available.size(), 10);
        
        pool.shutdown();
        
    }
    
    @Test
    public void nodeGoingDown() throws UnknownHostException, IOException, InterruptedException
    {
        ConnectionPool pool = new ConnectionPool.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        PoolStateListener mockListener = mock(PoolStateListener.class);
        pool.start();
        pool.addStateListener(mockListener);
        
        try
        {   
            fixture.shutdown();
        
            Thread.sleep(2000);

            verify(mockListener).poolStateChanged(pool, ConnectionPool.State.HEALTH_CHECKING);
            assertEquals(pool.getPoolState(), State.HEALTH_CHECKING);

            pool.shutdown();
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
        ConnectionPool pool = new ConnectionPool.Builder()
                               .withRemotePort(startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                               .withMinConnections(10)
                               .withIdleTimeout(1000)
                               .build();
        
        PoolStateListener mockListener = mock(PoolStateListener.class);
        pool.start();
        pool.addStateListener(mockListener);
        
        try
        {   
            fixture.shutdown();
        
            Thread.sleep(2000);

            verify(mockListener).poolStateChanged(pool, ConnectionPool.State.HEALTH_CHECKING);
            assertEquals(pool.getPoolState(), State.HEALTH_CHECKING);

        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
        
        Thread.sleep(1000);
        
        verify(mockListener).poolStateChanged(pool, ConnectionPool.State.RUNNING);
        assertEquals(pool.getPoolState(), State.RUNNING);
    }
    
}
