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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
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
 * A modeled Riak Cluster.
 * 
 * <p>
 * This class represents a Riak Cluster upon which operations are executed. 
 * Instances are created using the {@link Builder}
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class  RiakCluster implements OperationRetrier, NodeStateListener
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
            
        if (builder.bootstrap != null)
        {
            this.bootstrap = builder.bootstrap.clone();
        }
        else
        {
            this.bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);
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
        
        nodes = Collections.synchronizedList(new ArrayList<RiakNode>(builder.riakNodes.size()));
        for (RiakNode node : builder.riakNodes)
        {
            node.setExecutor(executor);
            node.setBootstrap(bootstrap);
            node.addStateListener(nodeManager);
            nodes.add(node);
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
    
    /**
     * Adds a {@link RiakNode} to this cluster. 
     * The node can not have been started nor have its Bootstrap or Executor
     * set.
     * @param node the RiakNode to add
     * @throws IllegalArgumentException if the node's Bootstrap or Executor are already set.
     */
    public synchronized void addNode(RiakNode node) throws UnknownHostException
    {
        stateCheck(State.CREATED, State.RUNNING);
        node.setExecutor(executor);
        node.setBootstrap(bootstrap);
        nodes.add(node);
        nodeManager.addNode(node);
    }
    
    /**
     * Removes the provided node from the cluster. 
     * @param node
     * @return true if the node was in the cluster, false otherwise.
     */
    public synchronized boolean removeNode(RiakNode node)
    {
        stateCheck(State.CREATED, State.RUNNING);
        nodes.remove(node);
        return nodeManager.removeNode(node);
    }
    
    /**
     * Returns the list of nodes in this cluster.
     * @return An unmodifiable list of RiakNodes
     */
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
                bootstrap.group().shutdownGracefully();
                logger.debug("RiakCluster shut down bootstrap");
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
    
    
    /**
     * Builder used to create {@link RiakCluster} instances.
     */
    public static class Builder
    {
        public final static int DEFAULT_EXECUTION_ATTEMPTS = 3;
        
        private final List<RiakNode> riakNodes;
        
        private int executionAttempts = DEFAULT_EXECUTION_ATTEMPTS;
        private NodeManager nodeManager;
        private ScheduledExecutorService executor;
        private Bootstrap bootstrap;
        
        /**
         * Instantiate a Builder containing the supplied {@link RiakNode}s
         * @param riakNodes - a List of unstarted RiakNode objects
         */
        public Builder(List<RiakNode> riakNodes)
        {
            this.riakNodes = new ArrayList<RiakNode>(riakNodes);
        }
        
        /**
         * Instantiate a Builder containing a single {@link RiakNode}
         * @param node
         */
        public Builder(RiakNode node)
        {
            this.riakNodes = new ArrayList<RiakNode>(1);
            this.riakNodes.add(node);
        }
        
        /**
         * Sets the number of times the {@link RiakCluster} will attempt an 
         * operation before returning it as failed. 
         * @param numberOfAttempts
         * @return this 
         */
        public Builder withExecutionAttempts(int numberOfAttempts)
        {
            this.executionAttempts = numberOfAttempts;
            return this;
        }
        
        /**
         * Sets the {@link NodeManager} for this {@link RiakCluster}
         * 
         * If none is provided the {@link DefaultNodeManager} will be used
         * @param nodeManager
         * @return this
         */
        public Builder withNodeManager(NodeManager nodeManager)
        {
            this.nodeManager = nodeManager;
            return this;
        }
            
        /**
         * Sets the Threadpool for this cluster. 
         * 
         * This threadpool is passed down to the {@link RiakNode}s and their 
         * underlying {@link ConnectionPool}s. At the very least it needs to have
         * two threads available. It is not necessary to supply your own as the 
         * {@link RiakCluster} will instantiate one upon construction if this is
         * not set.
         * @param executor
         * @return this
         */
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }
        
        /**
         * The Netty {@link Bootstrap} this cluster will use.
         * 
         * This Bootstrap is passed down to the {@link RiakNode}s and their 
         * underlying {@link ConnectionPool}s. It is not necessary to supply your
         * own as the {@link RiakCluster} will instantiate one upon construction
         * if this is not set.
         * @param bootstrap
         * @return this
         */
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }
        
        /**
         * Instantiates the {@link RiakCluster}
         * @return a new RiakCluster
         * @throws UnknownHostException if a node fails to start due to a DNS lookup
         */
        public RiakCluster build() throws UnknownHostException
        {
            return new RiakCluster(this);
        }
        
    }
}
