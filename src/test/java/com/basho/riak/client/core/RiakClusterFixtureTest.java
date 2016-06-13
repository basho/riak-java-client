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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;


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
    
    @Test(timeout = 10000)
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
        catch(InterruptedException ignored)
        {
            
        }
        finally
        {
            cluster.shutdown();
        }
    }
    
    @Test(timeout = 10000)
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
            cluster.shutdown();
        }
    }
    
    @Test(timeout = 10000)
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


    @Test(timeout = 10000)
    public void testOperationQueue() throws Exception
    {
        List<RiakNode> list = new LinkedList<RiakNode>();

        RiakNode.Builder goodNodeBuilder = new RiakNode.Builder()
                .withMinConnections(1)
                .withMaxConnections(1)
                .withRemotePort(5000 + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN);
        RiakNode goodNode = goodNodeBuilder.build();

        // Pass in 0 nodes, cause a queue backup.
        RiakCluster cluster = new RiakCluster.Builder(list).withOperationQueueMaxDepth(2).build();

        cluster.start();

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "test_bucket");
        Location location = new Location(ns, "test_key2");
        FetchOperation.Builder opBuilder = new FetchOperation.Builder(location);

        FetchOperation operation1 = opBuilder.build();
        FetchOperation operation2 = opBuilder.build();
        FetchOperation operation3 = opBuilder.build();
        FetchOperation operation4 = opBuilder.build();

        RiakFuture<FetchOperation.Response, Location> future1 = cluster.execute(operation1);
        RiakFuture<FetchOperation.Response, Location> future2 = cluster.execute(operation2);
        RiakFuture<FetchOperation.Response, Location> future3 = cluster.execute(operation3);

        try
        {
            // Verify that the third operation was rejected
            operation3.await();

            assertFalse(operation3.isSuccess());

            Throwable cause = operation3.cause();
            assertNotNull(cause != null && cause.getMessage() != null ? cause.getMessage() : "No message set?", cause);

            // Add a node to start processing the queue backlog
            cluster.addNode(goodNode);

            future1.await();

            // Process the first queue item
            assertEquals(future1.get().getObjectList().get(0).getValue().toString(), "This is a value!");
            assertFalse(future1.get().isNotFound());

            // Add another to fill it back up
            RiakFuture<FetchOperation.Response, Location> future4 = cluster.execute(operation4);

            // Get next item in Queue
            future2.await();

            assertEquals(future2.get().getObjectList().get(0).getValue().toString(), "This is a value!");
            assertFalse(future2.get().isNotFound());

            // Get last item in Queue
            future4.await();

            assertEquals(future4.get().getObjectList().get(0).getValue().toString(), "This is a value!");
            assertFalse(future4.get().isNotFound());
        }
        finally
        {
            cluster.shutdown();
        }
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
