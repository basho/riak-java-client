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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0
 */
public class  RiakCluster implements OperationRetrier, NodeStateListener
{
    enum State { CREATED, RUNNING, QUEUING, SHUTTING_DOWN, SHUTDOWN }
    private final Logger logger = LoggerFactory.getLogger(RiakCluster.class);
    private final int executionAttempts;
    private final int operationQueueMaxDepth;
    private final NodeManager nodeManager;
    private final AtomicInteger inFlightCount = new AtomicInteger();
    private final ScheduledExecutorService executor;
    private final Bootstrap bootstrap;
    private final List<RiakNode> nodeList;
    private final ReentrantReadWriteLock nodeListLock = new ReentrantReadWriteLock();
    private final LinkedBlockingQueue<FutureOperation> retryQueue =
        new LinkedBlockingQueue<FutureOperation>();
    private final boolean queueOperations;
    private final LinkedBlockingDeque<FutureOperation> operationQueue;
    private final List<NodeStateListener> stateListeners =
        Collections.synchronizedList(new LinkedList<NodeStateListener>());


    private volatile ScheduledFuture<?> shutdownFuture;
    private volatile ScheduledFuture<?> retrierFuture;
    private volatile ScheduledFuture<?> queueDrainFuture;

    private volatile State state;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private RiakCluster(Builder builder)
    {
        this.executionAttempts = builder.executionAttempts;
        this.queueOperations =  builder.operationQueueMaxDepth > 0;

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
            // Retry Task, Shutdown Task, (optional) Queue Task
            Integer poolSize = this.queueOperations ? 3 : 2;

            // We still need an executor if none was provided.
            executor = new ScheduledThreadPoolExecutor(poolSize);
        }

        nodeList = new ArrayList<RiakNode>(builder.riakNodes.size());
        for (RiakNode node : builder.riakNodes)
        {
            node.setExecutor(executor);
            node.setBootstrap(bootstrap);
            node.addStateListener(nodeManager);
            nodeList.add(node);
        }

        if (this.queueOperations)
        {
            this.operationQueueMaxDepth = builder.operationQueueMaxDepth;
            this.operationQueue = new LinkedBlockingDeque<FutureOperation>();

            for (RiakNode node : nodeList)
            {
                node.setBlockOnMaxConnections(false);
            }
        }
        else
        {
            this.operationQueueMaxDepth = 0;
            this.operationQueue = null;
        }

        // Pass a *copy* of the list to the NodeManager
        nodeManager.init(new ArrayList<RiakNode>(nodeList));
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

        // Completely unneeded *right now* but operating on a copy
        // of the nodeList defensively prevents a deadlock occuring
        // if a callback were to try and modify the list.
        for (RiakNode node : getNodes())
        {
            try
            {
                node.start();
            }
            catch (UnknownHostException e)
            {
                logger.error("RiakCluster::start - Failed starting node: {}", e.getMessage());
            }
        }

        retrierFuture = executor.schedule(new RetryTask(), 0, TimeUnit.SECONDS);

        if (this.queueOperations)
        {
            queueDrainFuture = executor.schedule(new QueueDrainTask(), 0, TimeUnit.SECONDS);
        }

        logger.info("RiakCluster is starting.");
        state = State.RUNNING;
    }

    public synchronized Future<Boolean> shutdown()
    {
        stateCheck(State.RUNNING, State.QUEUING);
        logger.info("RiakCluster is shutting down.");
        state = State.SHUTTING_DOWN;

        // Wait for all in-progress operations to drain
        // then shut down nodes.
        shutdownFuture = executor.scheduleWithFixedDelay(new ShutdownTask(),
                                                         500, 500,
                                                         TimeUnit.MILLISECONDS);

        return new Future<Boolean>()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                return false;
            }
            @Override
            public Boolean get() throws InterruptedException
            {
                shutdownLatch.await();
                return true;
            }
            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException
            {
                return shutdownLatch.await(timeout, unit);
            }
            @Override
            public boolean isCancelled()
            {
                return false;
            }
            @Override
            public boolean isDone()
            {
                return shutdownLatch.getCount() <= 0;
            }

        };

    }

    public <V,S> RiakFuture<V,S> execute(FutureOperation<V, ?, S> operation)
    {
        stateCheck(State.RUNNING, State.QUEUING);
        operation.setRetrier(this, executionAttempts);
        inFlightCount.incrementAndGet();

        boolean gotConnection = false;

        // Avoid queue if we're not using it, or it's currently empty
        if (notQueuingOrQueueIsEmpty())
        {
            gotConnection = this.execute(operation, null);
        }

        if (!gotConnection) // Operation didn't run
        {
            // Queue it up, run next from queue
            if (this.queueOperations)
            {
                executeWithQueueStrategy(operation, null);
            }
            else // Out of connections, retrier will pick it up later.
            {
                operation.setException(new NoNodesAvailableException());
            }
        }

        return operation;
    }

    private boolean notQueuingOrQueueIsEmpty()
    {
        return !this.queueOperations || this.operationQueue.size() == 0;
    }

    private void executeWithQueueStrategy(FutureOperation operation, RiakNode previousNode)
    {
        if (operationQueue.size() >= operationQueueMaxDepth)
        {
            logger.warn("No Nodes Available, and Operation Queue at Max Depth");
            operation.setRetrier(this, 1);
            operation.setException(new NoNodesAvailableException("No Nodes Available, and Operation Queue at Max Depth"));
            return;
        }

        operationQueue.offer(operation);
        verifyQueueStatus();

        // Get next queued operation
        FutureOperation operationNext = operationQueue.poll();
        if (operationNext == null)
        {
            return;
        }

        executeWithRequeueOnNoConnection(operationNext);
    }

    private boolean executeWithRequeueOnNoConnection(FutureOperation operation)
    {
        // Attempt to run
        boolean gotConnection = this.execute(operation, null);

        // If we can't get a connection, put it back at the beginning of the queue
        if (!gotConnection)
        {
            operationQueue.offerFirst(operation);
        }

        verifyQueueStatus();

        return gotConnection;
    }

    private void verifyQueueStatus()
    {
        Integer queueSize = operationQueue.size();

        if (queueSize > 0 && state == State.RUNNING)
        {
            state = State.QUEUING;
            logger.debug("RiakCluster queuing operations.");
        }
        else if (queueSize == 0 && (state == State.QUEUING || state == State.SHUTTING_DOWN))
        {
            logger.debug("RiakCluster cleared operation queue.");
            if (state == State.QUEUING)
            {
                state = State.RUNNING;
            }
        }
    }

    private boolean execute(FutureOperation operation, RiakNode previousNode)
    {
        return nodeManager.executeOnNode(operation, previousNode);
    }

    /**
     * Adds a {@link RiakNode} to this cluster.
     * The node can not have been started nor have its Bootstrap or Executor
     * asSet.
     * The node will be started as part of this process.
     * @param node the RiakNode to add
     * @throws java.net.UnknownHostException if the RiakNode's hostname cannot be resolved
     * @throws IllegalArgumentException if the node's Bootstrap or Executor are already asSet.
     */
    public void addNode(RiakNode node) throws UnknownHostException
    {
        stateCheck(State.CREATED, State.RUNNING, State.QUEUING);
        node.setExecutor(executor);
        node.setBootstrap(bootstrap);

        try
        {
            nodeListLock.writeLock().lock();
            nodeList.add(node);
            for (NodeStateListener listener : stateListeners)
            {
                node.addStateListener(listener);
            }
        }
        finally
        {
            nodeListLock.writeLock().unlock();
        }

        node.start();

        nodeManager.addNode(node);
    }

    /**
     * Removes the provided node from the cluster.
     * @param node
     * @return true if the node was in the cluster, false otherwise.
     */
    public boolean removeNode(RiakNode node)
    {
        stateCheck(State.CREATED, State.RUNNING, State.QUEUING);
        boolean removed = false;
        try
        {
            nodeListLock.writeLock().lock();
            removed = nodeList.remove(node);
            for (NodeStateListener listener : stateListeners)
            {
                node.removeStateListener(listener);
            }
        }
        finally
        {
            nodeListLock.writeLock().unlock();
        }
        nodeManager.removeNode(node);
        return removed;
    }

    /**
     * Returns a copy of the list of nodes in this cluster.
     * @return A copy of the list of RiakNodes
     */
    public List<RiakNode> getNodes()
    {
        stateCheck(State.CREATED, State.RUNNING, State.SHUTTING_DOWN, State.QUEUING);
        try
        {
            nodeListLock.readLock().lock();
            return new ArrayList<RiakNode>(nodeList);
        }
        finally
        {
            nodeListLock.readLock().unlock();
        }

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
            logger.debug("Node state changed to shutdown; {}:{}", node.getRemoteAddress(), node.getPort());
            try
            {
                nodeListLock.writeLock().lock();
                nodeList.remove(node);
                logger.debug("Active nodes remaining: {}", nodeList.size());

                if (nodeList.isEmpty())
                {
                    this.state = State.SHUTDOWN;
                    executor.shutdown();
                    bootstrap.group().shutdownGracefully();
                    logger.debug("RiakCluster shut down bootstrap");
                    logger.info("RiakCluster has shut down");
                    shutdownLatch.countDown();
                }
            }
            finally
            {
                nodeListLock.writeLock().unlock();
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

        Boolean gotConnection = execute(operation, operation.getLastNode());

        if (!gotConnection)
        {
            operation.setException(new NoNodesAvailableException());
        }
    }

    private void queueDrainOperation() throws InterruptedException
    {
        FutureOperation operation = operationQueue.take();

        boolean connectionSuccess = executeWithRequeueOnNoConnection(operation);

        // If we didn't get a connection here, then we are thrashing on connections
        // Sleep for a bit so we don't spinwait our CPUs to death
        if (!connectionSuccess)
        {
            // TODO: should this timeout be configurable, or based on an
            // average command execution time?
            Thread.sleep(50);
        }
    }

    /**
     * Register a NodeStateListener.
     * <p>
     * Any state change by any of the nodes in the cluster will be sent to
     * the registered NodeStateListener.
     * </p>
     * <p>When registering, the current state of all the nodes is sent to the
     * listener.
     * </p>
     * @param listener The NodeStateListener to register.
     */
    public void registerNodeStateListener(NodeStateListener listener)
    {
        stateCheck(State.CREATED, State.RUNNING, State.SHUTTING_DOWN, State.QUEUING);
        try
        {
            stateListeners.add(listener);
            nodeListLock.readLock().lock();
            for (RiakNode node : nodeList)
            {
                node.addStateListener(listener);
                listener.nodeStateChanged(node, node.getNodeState());
            }
        }
        finally
        {
            nodeListLock.readLock().unlock();
        }
    }

    /**
     * Remove a NodeStateListener.
     * <p>
     * The supplied NodeStateListener will be unregistered and no longer
     * receive state updates.
     * </p>
     * @param listener The NodeStateListener to unregister.
     */
    public void removeNodeStateListener(NodeStateListener listener)
    {
        stateCheck(State.CREATED, State.RUNNING, State.SHUTTING_DOWN, State.QUEUING);
        try
        {
            stateListeners.remove(listener);
            nodeListLock.readLock().lock();
            for (RiakNode node : nodeList)
            {
                node.removeStateListener(listener);
            }
        }
        finally
        {
            nodeListLock.readLock().unlock();
        }
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

    private class QueueDrainTask implements Runnable
    {
        @Override
        public void run()
        {
            while (!Thread.interrupted())
            {
                try
                {
                    queueDrainOperation();
                }
                catch (InterruptedException ex)
                {
                    break;
                }
            }

            logger.info("Queue Worker shutting down.");
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

                if (queueOperations)
                {
                    queueDrainFuture.cancel(true);
                }

                // Copying the list avoids any potential deadlocks on the callbacks.
                for (RiakNode node : getNodes())
                {
                    node.addStateListener(RiakCluster.this);
                    logger.debug("calling shutdown on node {}:{}", node.getRemoteAddress(), node.getPort());
                    node.shutdown();
                }

                shutdownFuture.cancel(false);
            }
        }

    }


        public static Builder builder(List<RiakNode> nodes)
        {
            return new Builder(nodes);
        }

        public static Builder builder(RiakNode node)
        {
            return new Builder(node);
        }

    /**
     * Builder used to create {@link RiakCluster} instances.
     */
    public static class Builder
    {
        public final static int DEFAULT_EXECUTION_ATTEMPTS = 3;
        public final static int DEFAULT_OPERATION_QUEUE_DEPTH = 0;

        private final List<RiakNode> riakNodes;

        private int executionAttempts = DEFAULT_EXECUTION_ATTEMPTS;
        private int operationQueueMaxDepth = DEFAULT_OPERATION_QUEUE_DEPTH;

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
         * Instantiate a Builder containing the {@link RiakNode}s that will be build by using provided builder.
         * The RiakNode.Builder is used for setting common properties among the nodes.
         * @since 2.0.3
         * @see com.basho.riak.client.core.RiakNode.Builder#buildNodes(RiakNode.Builder, List)
         */
        public Builder(RiakNode.Builder nodeBuilder, List<String> remoteAddresses) throws UnknownHostException
        {
            riakNodes = RiakNode.Builder.buildNodes(nodeBuilder, remoteAddresses );
        }

        /**
         * Instantiate a Builder containing the {@link RiakNode}s that will be build by using provided builder.
         * The RiakNode.Builder is used for setting common properties among the nodes.
         * @since 2.0.3
         * @see com.basho.riak.client.core.RiakNode.Builder#buildNodes(RiakNode.Builder, String...)
         */
        public Builder(RiakNode.Builder nodeBuilder, String... remoteAddresses) throws UnknownHostException
        {
            riakNodes = RiakNode.Builder.buildNodes(nodeBuilder, remoteAddresses );
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
         * This threadpool is passed down to the {@link RiakNode}s.
         * At the very least it needs to have
         * two threads available. It is not necessary to supply your own as the
         * {@link RiakCluster} will instantiate one upon construction if this is
         * not asSet.
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
         * This Bootstrap is passed down to the {@link RiakNode}s.
         * It is not necessary to supply your
         * own as the {@link RiakCluster} will instantiate one upon construction
         * if this is not asSet.
         * @param bootstrap
         * @return this
         */
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }

        /**
         * Set the maximum number of operations to queue.
         * A value of 0 disables the command queue.
         * <b>Setting this will override any of this clusters @{link RiakNode}s blockOnMaxConnection settings.</b>
         *
         * @param operationQueueMaxDepth - maximum number of operations to queue if all connections are busy
         * @return this
         * @see #DEFAULT_OPERATION_QUEUE_DEPTH
         */
        public Builder withOperationQueueMaxDepth(int operationQueueMaxDepth)
        {
            this.operationQueueMaxDepth = operationQueueMaxDepth;
            return this;
        }

        /**
         * Instantiates the {@link RiakCluster}
         * @return a new RiakCluster
         */
        public RiakCluster build()
        {
            return new RiakCluster(this);
        }

    }
}
