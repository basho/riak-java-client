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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;



/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FutureOperation.class)
public class RiakClusterTest
{
    @Test
    public void builderCreatesCluster() throws UnknownHostException
    {
        RiakNode.Builder nodeBuilder = new RiakNode.Builder();
        RiakCluster cluster = new RiakCluster.Builder(nodeBuilder.build()).build();
        assertTrue(!cluster.getNodes().isEmpty());
    }
    
    @Test
    public void addNodeToCluster() throws UnknownHostException
    {
        NodeManager nodeManager = mock(NodeManager.class);
        RiakNode node = mock(RiakNode.class);
        RiakNode.Builder nodeBuilder = spy(new RiakNode.Builder());
        doReturn(node).when(nodeBuilder).build();
        
        RiakCluster cluster = new RiakCluster.Builder(nodeBuilder.build()).withNodeManager(nodeManager).build();
        cluster.addNode(nodeBuilder.build());
        assertEquals(2, cluster.getNodes().size());
        verify(nodeManager).addNode(node);
    }
    
    @Test
    public void removeNodeFromCluster() throws UnknownHostException
    {
        NodeManager nodeManager = mock(NodeManager.class);
        RiakNode node = mock(RiakNode.class);
        RiakNode.Builder nodeBuilder = spy(new RiakNode.Builder());
        doReturn(node).when(nodeBuilder).build();
        doReturn(true).when(nodeManager).removeNode(node);
        
        RiakCluster cluster = new RiakCluster.Builder(nodeBuilder.build()).withNodeManager(nodeManager).build();
        assertTrue(cluster.removeNode(node));
        verify(nodeManager).removeNode(node);
        assertEquals(0, cluster.getNodes().size());
    }
    
    @Test
    public void allNodesShutdownStopsCluster() throws UnknownHostException
    {
        NodeManager nodeManager = mock(NodeManager.class);
        RiakNode node = mock(RiakNode.class);
        RiakNode.Builder nodeBuilder = spy(new RiakNode.Builder());
        doReturn(node).when(nodeBuilder).build();
        doReturn(true).when(nodeManager).removeNode(node);
        
        RiakCluster cluster = new RiakCluster.Builder(nodeBuilder.build()).withNodeManager(nodeManager).build();
        cluster.nodeStateChanged(node, RiakNode.State.SHUTDOWN);
        RiakCluster.State state = Whitebox.getInternalState(cluster, "state");
        assertEquals(state, RiakCluster.State.SHUTDOWN);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void clusterExecutesOperation() throws UnknownHostException
    {
        NodeManager nodeManager = mock(NodeManager.class);
        FutureOperation operation = PowerMockito.mock(FutureOperation.class);
        RiakNode node = mock(RiakNode.class);
        RiakNode.Builder nodeBuilder = spy(new RiakNode.Builder());
        doReturn(node).when(nodeBuilder).build();
        doReturn(true).when(node).execute(operation);
        
        RiakCluster cluster = new RiakCluster.Builder(nodeBuilder.build()).withNodeManager(nodeManager).build();
        Whitebox.setInternalState(cluster, "state", RiakCluster.State.RUNNING);
        cluster.execute(operation);
        assertEquals(1, cluster.inFlightCount());
        verify(nodeManager).executeOnNode(operation, null);
        cluster.operationComplete(operation, 2);
        assertEquals(0, cluster.inFlightCount());
        
        cluster.execute(operation);
        cluster.operationFailed(operation, 1);
        LinkedBlockingQueue<?> retryQueue = Whitebox.getInternalState(cluster, "retryQueue");
        assertEquals(1, retryQueue.size());
        
        
    }
}
