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

import java.util.LinkedList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.argThat;
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
@PrepareForTest(FutureOperation.class)
public class DefaultNodeManagerTest
{
    private List<RiakNode> mockNodes;
    
    @Before
    public void setUp()
    {
        mockNodes = new LinkedList<RiakNode>();
        for (int i = 0; i < 5; i++)
        {
            RiakNode mock = mock(RiakNode.class);
            mockNodes.add(mock);
        }
    }
    
    
    @Test
    public void init()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        
        List<RiakNode> nodes = Whitebox.getInternalState(nodeManager, "healthy");
        assertEquals(mockNodes.size(), nodes.size());
    }
    
    @Test
    public void executeOnNodeSuccess()
    {
        FutureOperation operation = PowerMockito.mock(FutureOperation.class);
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        doReturn(false).when(mockNodes.get(0)).execute(operation);
        doReturn(true).when(mockNodes.get(1)).execute(operation);
        nodeManager.init(mockNodes);
        nodeManager.executeOnNode(operation, null);
        verify(mockNodes.get(0)).execute(operation);
        verify(mockNodes.get(1)).execute(operation);
        verify(mockNodes.get(2), never()).execute(operation);
        verify(mockNodes.get(3), never()).execute(operation);
        verify(mockNodes.get(4), never()).execute(operation);
    }
    
    @Test
    public void executeOnNodeFailure()
    {
        FutureOperation operation = PowerMockito.mock(FutureOperation.class);
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        nodeManager.executeOnNode(operation, null);
        for (int i = 0; i < mockNodes.size(); i++)
        {
            verify(mockNodes.get(i)).execute(operation);
        }
        verify(operation).setException(argThat(new IsException()));
    }
    
    @Test
    public void removeUnhealthyNode()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        nodeManager.nodeStateChanged(mockNodes.get(0), RiakNode.State.HEALTH_CHECKING);
        List<RiakNode> healthy = Whitebox.getInternalState(nodeManager, "healthy");
        List<RiakNode> unhealthy = Whitebox.getInternalState(nodeManager, "unhealthy");
        assertEquals(mockNodes.size() - 1, healthy.size());
        assertEquals(1, unhealthy.size());
        assertEquals(mockNodes.get(0), unhealthy.get(0));
    }
    
    @Test
    public void restoreHealthyNode()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        nodeManager.nodeStateChanged(mockNodes.get(0), RiakNode.State.HEALTH_CHECKING);
        nodeManager.nodeStateChanged(mockNodes.get(0), RiakNode.State.RUNNING);
        List<RiakNode> healthy = Whitebox.getInternalState(nodeManager, "healthy");
        List<RiakNode> unhealthy = Whitebox.getInternalState(nodeManager, "unhealthy");
        assertEquals(mockNodes.size(), healthy.size());
        assertEquals(0, unhealthy.size());
        assertTrue(healthy.contains(mockNodes.get(0)));
    }
    
    @Test
    public void removeShutdownNode()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        nodeManager.nodeStateChanged(mockNodes.get(0), RiakNode.State.SHUTDOWN);
        List<RiakNode> healthy = Whitebox.getInternalState(nodeManager, "healthy");
        List<RiakNode> unhealthy = Whitebox.getInternalState(nodeManager, "unhealthy");
        assertEquals(mockNodes.size() - 1, healthy.size());
        assertEquals(0, unhealthy.size());
        assertTrue(!healthy.contains(mockNodes.get(0)));
    }
    
    @Test
    public void removeNode()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        nodeManager.removeNode(mockNodes.get(0));
        List<RiakNode> healthy = Whitebox.getInternalState(nodeManager, "healthy");
        List<RiakNode> unhealthy = Whitebox.getInternalState(nodeManager, "unhealthy");
        assertEquals(mockNodes.size() - 1, healthy.size());
        assertEquals(0, unhealthy.size());
        verify(mockNodes.get(0)).removeStateListener(nodeManager);
        verify(mockNodes.get(0)).shutdown();
    }
    
    @Test
    public void addNode()
    {
        DefaultNodeManager nodeManager = new DefaultNodeManager();
        nodeManager.init(mockNodes);
        RiakNode newNode = mock(RiakNode.class);
        nodeManager.addNode(newNode);
        List<RiakNode> healthy = Whitebox.getInternalState(nodeManager, "healthy");
        List<RiakNode> unhealthy = Whitebox.getInternalState(nodeManager, "unhealthy");
        assertEquals(mockNodes.size() + 1, healthy.size());
    }
    
    private class IsException extends ArgumentMatcher<Exception>
    {
        @Override
        public boolean matches(Object argument)
        {
            return argument instanceof Exception;
        }
        
    }
    
}
