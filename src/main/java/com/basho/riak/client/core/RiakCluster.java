/*
 * Copyright 2013 Basho Technologies, Inc
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


import com.basho.riak.client.operations.FutureOperation;
import io.netty.bootstrap.Bootstrap;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakCluster implements OperationRetrier, NodeStateListener
{

    private enum State { CREATED, RUNNING, SHUTDOWN }
    private final Logger logger = LoggerFactory.getLogger(RiakCluster.class);
    private final int executionAttempts;
    private final NodeManager nodeManager;
    private final Map<FutureOperation, RiakNode> inProgressMap =
        Collections.synchronizedMap(new IdentityHashMap<FutureOperation, RiakNode>());
    private final ScheduledExecutorService executor;
    private final Bootstrap bootstrap;
    private final List<RiakNode> nodes;
    
    private volatile State state;
    
    private RiakCluster(Builder builder) throws UnknownHostException
    {
        this.executionAttempts = builder.executionAttempts;
        
        if (null == builder.nodeManager)
        {
            nodeManager = new DefaultNodeManager();
        }
        else
        {
            this.nodeManager = builder.nodeManager;
        }
            
        this.executor = builder.executor;
        this.bootstrap = builder.bootstrap;
        
        nodes = Collections.synchronizedList(new ArrayList<RiakNode>(builder.riakNodeBuilders.size()));
        for (RiakNode.Builder nodeBuilder : builder.riakNodeBuilders)
        {
            if (executor != null)
            {
                nodeBuilder.withExecutor(executor);
            }
            if (bootstrap != null)
            {
                nodeBuilder.withBootstrap(bootstrap);
            }
            nodes.add(nodeBuilder.build());
        }
        
        nodeManager.init(nodes);
        state = State.CREATED;
    }
    
    private void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; required: {} current: {} ",
                         Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: " 
                + Arrays.toString(allowedStates) 
                + " current: " + state );
        }
    }
    
    public synchronized void start()
    {
        stateCheck(State.CREATED);
        for (RiakNode node : nodes)
        {
            node.addStateListener(nodeManager);
            node.start();
        }
        state = State.RUNNING;
    }

    public synchronized void stop()
    {
        stateCheck(State.RUNNING);
        // TODO: everything
        state = State.SHUTDOWN;
        for (RiakNode node : nodes)
        {
            node.addStateListener(this);
            node.stop();
        }
    }
    
    public void execute(FutureOperation operation)
    {
        stateCheck(State.RUNNING);
        if (inProgressMap.containsKey(operation))
        {
            throw new IllegalStateException("Operation already executing");
        }
        operation.setRetrier(this, executionAttempts);
        this.execute(operation, null);
    }
    
    // TODO: Streaming also
    private void execute(FutureOperation operation, RiakNode previousNode) 
    {
        stateCheck(State.RUNNING);
        
        RiakNode node = nodeManager.selectNode(previousNode);
        inProgressMap.put(operation, node);
        
        if (null == node)
        {
            operation.setException(new NoNodesAvailableException());
        }
        else
        {
            node.execute(operation);
        }
    }
    
    public synchronized RiakNode addNode(RiakNode.Builder builder) throws UnknownHostException
    {
        stateCheck(State.CREATED, State.RUNNING);
        if (executor != null)
        {
            builder.withExecutor(executor);
        }
        if (bootstrap != null)
        {
            builder.withBootstrap(bootstrap);
        }
        RiakNode newNode = builder.build();
        nodes.add(newNode);
        nodeManager.addNode(newNode);
        return newNode;
    }
    
    public synchronized RiakNode removeNode(RiakNode node)
    {
        stateCheck(State.CREATED, State.RUNNING);
        nodes.remove(node);
        return nodeManager.removeNode(node);
    }
    
    
    @Override
    public void nodeStateChanged(RiakNode node, RiakNode.State state)
    {
        if (state == RiakNode.State.SHUTDOWN)
        {
            nodes.remove(node);
        }
        
        if (nodes.isEmpty())
        {
            if (executor != null)
            {
                executor.shutdown();
            }
            if (bootstrap != null)
            {
                bootstrap.shutdown();
            }
        }
    }
    
    @Override
    public void operationFailed(FutureOperation operation, int remainingRetries)
    {
        if (remainingRetries > 0)
        {
            RiakNode previousNode = inProgressMap.get(operation);
            execute(operation, previousNode);
        }
    }

    @Override
    public void operationComplete(FutureOperation operation, int remainingRetries)
    {
        inProgressMap.remove(operation);
    }

    public static class Builder
    {
        public final static int DEFAULT_EXECUTION_ATTEMPTS = 3;
        
        private final List<RiakNode.Builder> riakNodeBuilders;
        
        private int executionAttempts = DEFAULT_EXECUTION_ATTEMPTS;
        private NodeManager nodeManager;
        private ScheduledExecutorService executor;
        private Bootstrap bootstrap;
        
        public Builder(List<RiakNode.Builder> riakNodes)
        {
            this.riakNodeBuilders = new ArrayList<>(riakNodes);
        }
        
        public Builder(RiakNode.Builder node)
        {
            this.riakNodeBuilders = new ArrayList<>(1);
            this.riakNodeBuilders.add(node);
        }
        
        public Builder addNode(RiakNode.Builder node)
        {
            this.riakNodeBuilders.add(node);
            return this;
        }
        
        public Builder withExecutionAttempts(int numberOfAttempts)
        {
            this.executionAttempts = numberOfAttempts;
            return this;
        }
        
        public Builder withNodeManager(NodeManager nodeManager)
        {
            this.nodeManager = nodeManager;
            return this;
        }
            
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }
        
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }
        
        public RiakCluster build() throws UnknownHostException
        {
            return new RiakCluster(this);
        }
        
    }
}
