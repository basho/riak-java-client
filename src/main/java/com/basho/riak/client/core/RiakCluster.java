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


import io.netty.bootstrap.Bootstrap;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakCluster implements OperationRetrier, NodeStateListener
{
    enum State { CREATED, RUNNING, SHUTTING_DOWN, SHUTDOWN }
    private final Logger logger = LoggerFactory.getLogger(RiakCluster.class);
    private final int executionAttempts;
    private final NodeManager nodeManager;
    private final AtomicInteger inFlightCount = new AtomicInteger();
    private final ScheduledExecutorService executor;
    private final Bootstrap bootstrap;
    private final List<RiakNode> nodes;
    private final LinkedBlockingQueue<FutureOperation> retryQueue =
        new LinkedBlockingQueue<FutureOperation>();
    
    private ScheduledFuture<?> shutdownFuture;
    private ScheduledFuture<?> retrierFuture;
    
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
            
        this.bootstrap = builder.bootstrap;
        
        nodes = Collections.synchronizedList(new ArrayList<RiakNode>(builder.riakNodeBuilders.size()));
        for (RiakNode.Builder nodeBuilder : builder.riakNodeBuilders)
        {
            if (builder.executor != null)
            {
                nodeBuilder.withExecutor(builder.executor);
            }
            if (bootstrap != null)
            {
                nodeBuilder.withBootstrap(bootstrap);
            }
            nodes.add(nodeBuilder.build());
        }
        
        if (builder.executor != null)
        {
            executor = builder.executor;
        }
        else
        {
            // We still need an executor if none was provided. 
            executor = new ScheduledThreadPoolExecutor(2);
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
        retrierFuture = executor.schedule(new RetryTask(), 0, TimeUnit.SECONDS);
        logger.info("RiakCluster is starting.");
        state = State.RUNNING;
    }

    public synchronized void stop()
    {
        stateCheck(State.RUNNING);
        logger.info("RiakCluster is shutting down.");
        state = State.SHUTTING_DOWN;
        
        // Wait for all in-progress operations to drain
        // then shut down nodes.
        shutdownFuture = executor.scheduleWithFixedDelay(new ShutdownTask(), 
                                                         500, 500, 
                                                         TimeUnit.MILLISECONDS);
    }
    
    // TODO: Harden to keep operations from being execute multiple times?
    public void execute(FutureOperation operation)
    {
        stateCheck(State.RUNNING);
        operation.setRetrier(this, executionAttempts); 
        inFlightCount.incrementAndGet();
        this.execute(operation, null);
    }
    
    // TODO: Streaming also
    private void execute(FutureOperation operation, RiakNode previousNode) 
    {
        nodeManager.executeOnNode(operation, previousNode);
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
    
    public synchronized boolean removeNode(RiakNode node)
    {
        stateCheck(State.CREATED, State.RUNNING);
        nodes.remove(node);
        return nodeManager.removeNode(node);
    }
    
    public List<RiakNode> getNodes()
    {
        stateCheck(State.CREATED, State.RUNNING);
        return Collections.unmodifiableList(nodes);
    }
    
    int inFlightCount()
    {
        return inFlightCount.get();
    }
    
    @Override
    public void nodeStateChanged(RiakNode node, RiakNode.State state)
    {
        // We only listen for state changes after telling all the nodes
        // to shutdown.
        if (state == RiakNode.State.SHUTDOWN)
        {
            nodes.remove(node);
            nodeManager.removeNode(node);
            
            if (nodes.isEmpty())
            {
                this.state = State.SHUTDOWN;
                executor.shutdown();
                if (bootstrap != null)
                {
                    bootstrap.shutdown();
                    logger.debug("RiakCluster shut down bootstrap");
                }
                logger.info("RiakCluster has shut down");
            }
        }
    }
    
    @Override
    public void operationFailed(FutureOperation operation, int remainingRetries)
    {
        logger.debug("operation failed; remaining retries: {}", remainingRetries);
        if (remainingRetries > 0)
        {
            retryQueue.add(operation);
        }
        else
        {
            inFlightCount.decrementAndGet();
        }
    }

    @Override
    public void operationComplete(FutureOperation operation, int remainingRetries)
    {
        inFlightCount.decrementAndGet();
        logger.debug("operation complete; remaining retries: {}", remainingRetries);
    }

    private void retryOperation() throws InterruptedException
    {
        FutureOperation operation = retryQueue.take();
        execute(operation, operation.getLastNode());
    }
    
    private class RetryTask implements Runnable
    {
        @Override
        public void run()
        {
            while (!Thread.interrupted())
            {
                try
                {
                    retryOperation();
                }
                catch (InterruptedException ex)
                {
                    break;
                }
            }
            
            logger.info("Retrier shutting down.");
        }
        
    }
    
    private class ShutdownTask implements Runnable
    {
        @Override
        public void run()
        {
            if (inFlightCount.get() == 0)
            {
                logger.info("All operations have completed");
                retrierFuture.cancel(true);
                for (RiakNode node : nodes)
                {
                    node.addStateListener(RiakCluster.this);
                    node.shutdown();
                }
                shutdownFuture.cancel(false);
            }
        }
        
    }
    
    
    public static class Builder
    {
        public final static int DEFAULT_EXECUTION_ATTEMPTS = 3;
        
        private final List<RiakNode.Builder> riakNodeBuilders;
        
        private int executionAttempts = DEFAULT_EXECUTION_ATTEMPTS;
        private NodeManager nodeManager;
        private ScheduledExecutorService executor;
        private Bootstrap bootstrap;
        
        public Builder(List<RiakNode.Builder> riakNodeBuilders)
        {
            this.riakNodeBuilders = new ArrayList<RiakNode.Builder>(riakNodeBuilders);
        }
        
        public Builder(RiakNode.Builder node)
        {
            this.riakNodeBuilders = new ArrayList<RiakNode.Builder>(1);
            this.riakNodeBuilders.add(node);
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
