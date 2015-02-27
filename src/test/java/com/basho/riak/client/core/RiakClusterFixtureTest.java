/*
 * Copyright 2013 Basho Technologies Inc
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

import com.basho.riak.client.core.fixture.NetworkTestFixture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakClusterFixtureTest 
{
    private NetworkTestFixture[] fixtures;
    
    
    @Before
    public void setUp() throws IOException
    {
        fixtures = new NetworkTestFixture[3];
        for (int i = 0, j = 5000; i < 3; i++, j += 1000)
        {
            fixtures[i] = new NetworkTestFixture(j);
            new Thread(fixtures[i]).start();
        }
    }
    
    @After
    public void tearDown() throws IOException
    {
        for (int i = 0; i < fixtures.length; i++)
        {
            fixtures[i].shutdown();
        }
    }
    
    @Test
    public void operationSuccess() throws UnknownHostException, InterruptedException, ExecutionException
    {
        List<RiakNode> list = new LinkedList<RiakNode>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10)
                                        .withRemotePort(i + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN);
            list.add(builder.build());
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        cluster.start();
        
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");
        
        FetchOperation operation = 
            new FetchOperation.Builder(location).build();

        cluster.execute(operation);
        
        try
        {
            FetchOperation.Response response = operation.get();
            assertEquals(response.getObjectList().get(0).getValue().toString(), "This is a value!");
            assertTrue(!response.isNotFound());
        }
        catch(InterruptedException e)
        {
            
        }
        
        cluster.shutdown().get();
        
    }
    
    @Test
    public void operationFail() throws UnknownHostException, ExecutionException, InterruptedException
    {
        List<RiakNode> list = new LinkedList<RiakNode>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10)
                                        .withRemotePort(i + NetworkTestFixture.ACCEPT_THEN_CLOSE);
            list.add(builder.build());
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        
        cluster.start();
        
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");
        
        FetchOperation operation = 
            new FetchOperation.Builder(location)
                    .build();

        cluster.execute(operation);
        
        try
        {
            operation.await();
            assertFalse(operation.isSuccess());
            assertNotNull(operation.cause());
        }
        finally
        {
            cluster.shutdown().get();
        }
    }
    
    @Test
    public void testStateListener() throws UnknownHostException, InterruptedException, ExecutionException
    {
        List<RiakNode> list = new LinkedList<RiakNode>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10)
                                        .withRemotePort(i + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN);
            list.add(builder.build());
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        
        StateListener listener = new StateListener();
        cluster.registerNodeStateListener(listener);
        
        cluster.start();
        
        // Yeah, yeah, fragile ... whatever
        Thread.sleep(3000);
        
        cluster.shutdown().get();
        
        
        // Upon registering the initial node state of each node should be sent.
        assertEquals(3, listener.stateCreated);
        // All three nodes should go through all three states and notify.
        assertEquals(3, listener.stateRunning);
        assertEquals(3, listener.stateShuttingDown);
        assertEquals(3, listener.stateShutdown);
        
        
    }
    
    public static class StateListener implements NodeStateListener
    {

        public int stateCreated;
        public int stateRunning;
        public int stateShuttingDown;
        public int stateShutdown;
        
        @Override
        public void nodeStateChanged(RiakNode node, RiakNode.State state)
        {
            switch(state)
            {
                case CREATED:
                    stateCreated++;
                    break;
                case RUNNING:
                    stateRunning++;
                    break;
                case SHUTTING_DOWN:
                    stateShuttingDown++;
                    break;
                case SHUTDOWN:
                    stateShutdown++;
                    break;
                default:
                    break;
            }
        }
        
    }
    
    
}
