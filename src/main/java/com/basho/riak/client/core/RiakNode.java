/*
 * Copyright 2013 Basho Technologies, Inc.
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

import com.basho.riak.client.core.netty.RiakChannelInitializer;
import com.basho.riak.client.util.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakNode implements RiakResponseListener
{
    public enum State
    {
        CREATED, RUNNING, HEALTH_CHECKING, SHUTTING_DOWN, SHUTDOWN;
    }

    private final Logger logger = LoggerFactory.getLogger(RiakNode.class);

    private final LinkedBlockingDeque<ChannelWithIdleTime> available =
        new LinkedBlockingDeque<ChannelWithIdleTime>();
    private final ConcurrentLinkedQueue<Channel> inUse =
        new ConcurrentLinkedQueue<Channel>();
    private final ConcurrentLinkedQueue<ChannelWithIdleTime> recentlyClosed =
        new ConcurrentLinkedQueue<ChannelWithIdleTime>();
    private final List<NodeStateListener> stateListeners =
        Collections.synchronizedList(new LinkedList<NodeStateListener>());
    private final Map<Channel, FutureOperation> inProgressMap =
        new ConcurrentHashMap<Channel, FutureOperation>();

    private final Sync permits;
    private final String remoteAddress;
    private final int port;
    private volatile Bootstrap bootstrap;
    private volatile boolean ownsBootstrap;
    private volatile ScheduledExecutorService executor;
    private volatile boolean ownsExecutor;
    private volatile State state;
    private volatile ScheduledFuture<?> idleReaperFuture;
    private volatile ScheduledFuture<?> healthMonitorFuture;
    private volatile int minConnections;
    private volatile long idleTimeoutInNanos;
    private volatile int connectionTimeout;


    private volatile int readTimeoutInMillis;

    private ChannelFutureListener writeListener =
        new ChannelFutureListener()
        {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
                // See how the write worked out ...
                if (!future.isSuccess())
                {
                    FutureOperation inProgress = inProgressMap.remove(future.channel());
                    logger.info("Write failed on RiakNode {}:{}", remoteAddress, port);
                    returnConnection(future.channel());
                    inProgress.setException(future.cause());
                }
            }

        };

    private final ChannelFutureListener closeListener =
        new ChannelFutureListener()
        {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
                // Because we aren't storing raw channels in available we just throw
                // these in the recentlyClosed as an indicator. We'll leave it up
                // to the healthcheck task to purge the available queue of dead
                // connections and make decisions on what to do
                recentlyClosed.add(new ChannelWithIdleTime(future.channel()));
                logger.debug("Channel closed; id:{} {}:{}", future.channel().hashCode(), remoteAddress, port);
            }
        };


    // TODO: Harden to prevent operation from being executed > 1 times?
    // TODO: how many channels on one event loop? 
    private RiakNode(Builder builder) throws UnknownHostException
    {
        this.readTimeoutInMillis = builder.readTimeout;
        this.executor = builder.executor;
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(builder.idleTimeout, TimeUnit.MILLISECONDS);
        this.minConnections = builder.minConnections;
        this.port = builder.port;
        this.remoteAddress = builder.remoteAddress;

        if (builder.bootstrap != null)
        {
            this.bootstrap = builder.bootstrap.clone();
        }

        if (builder.maxConnections < 1)
        {
            permits = new Sync(Integer.MAX_VALUE);
        }
        else
        {
            permits = new Sync(builder.maxConnections);
        }


        this.state = State.CREATED;
    }

    private void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; RiakNode: {}:{} required: {} current: {} ",
                remoteAddress, port, Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: "
                + Arrays.toString(allowedStates)
                + " current: " + state);
        }
    }

    /**
     * exposed for testing only
     *
     * @return number of inprogress tasks
     */
    int getNumInProgress()
    {
        return inProgressMap.size();
    }

    public synchronized RiakNode start()
    {
        stateCheck(State.CREATED);

        if (executor == null)
        {
            executor = Executors.newSingleThreadScheduledExecutor();
            ownsExecutor = true;
        }

        if (bootstrap == null)
        {
            bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);
            ownsBootstrap = true;
        }

        bootstrap.handler(new RiakChannelInitializer(this))
            .remoteAddress(new InetSocketAddress(remoteAddress, port));

        if (connectionTimeout > 0)
        {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
        }

        if (minConnections > 0)
        {
            List<Channel> minChannels = new LinkedList<Channel>();
            for (int i = 0; i < minConnections; i++)
            {
                Channel channel;
                try
                {
                    channel = doGetConnection();
                    minChannels.add(channel);
                }
                catch (ConnectionFailedException ex)
                {
                    // no-op, we don't care right now
                }
            }

            for (Channel c : minChannels)
            {
                available.offerFirst(new ChannelWithIdleTime(c));
            }
        }

        idleReaperFuture = executor.scheduleWithFixedDelay(new IdleReaper(), 1, 5, TimeUnit.SECONDS);
        healthMonitorFuture = executor.scheduleWithFixedDelay(new HealthMonitorTask(), 1000, 500, TimeUnit.MILLISECONDS);

        state = State.RUNNING;
        logger.info("RiakNode started; {}:{}", remoteAddress, port);
        notifyStateListeners();
        return this;
    }

    public synchronized void shutdown()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        state = State.SHUTTING_DOWN;
        logger.info("RiakNode shutting down; {}:{}", remoteAddress, port);
        notifyStateListeners();
        idleReaperFuture.cancel(true);
        healthMonitorFuture.cancel(true);
        ChannelWithIdleTime cwi = available.poll();
        while (cwi != null)
        {
            Channel c = cwi.getChannel();
            closeConnection(c);
            cwi = available.poll();
        }

        executor.schedule(new ShutdownTask(), 0, TimeUnit.SECONDS);
    }

    /**
     * Sets the Netty {@link Bootstrap} for this Node's connections.
     * {@link Bootstrap#clone()} is called to clone the bootstrap.
     *
     * @param bootstrap - the Netty Bootstrap to use
     * @return a reference to this RiakNode
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException    if the node has already been started.
     * @see Builder#withBootstrap(io.netty.bootstrap.Bootstrap)
     */
    public RiakNode setBootstrap(Bootstrap bootstrap)
    {
        stateCheck(State.CREATED);
        if (this.bootstrap != null)
        {
            throw new IllegalArgumentException("Bootstrap already set");
        }

        this.bootstrap = bootstrap.clone();
        return this;
    }

    /**
     * Sets the {@link ScheduledExecutorService} for this Node and its pool(s).
     *
     * @param executor - the ScheduledExecutorService to use.
     * @return a reference to this RiakNode
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException    if the node has already been started.
     * @see Builder#withExecutor(java.util.concurrent.ScheduledExecutorService)
     */
    public RiakNode setExecutor(ScheduledExecutorService executor)
    {
        stateCheck(State.CREATED);
        if (this.executor != null)
        {
            throw new IllegalArgumentException("Executor already set");
        }
        this.executor = executor;
        return this;
    }

    /**
     * Sets the read timeout for connections.
     *
     * @param readTimeoutInMillis the readTimeout to set
     * @return a reference to this RiakNode
     * @see Builder#withReadTimeout(int)
     */
    public RiakNode setReadTimeout(int readTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.readTimeoutInMillis = readTimeoutInMillis;
        return this;
    }

    /**
     * Returns the read timeout in milliseconds for connections.
     *
     * @return the readTimeout
     * @see Builder#withReadTimeout(int)
     */
    public int getReadTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return readTimeoutInMillis;
    }

    /**
     * Sets the maximum number of connections allowed.
     *
     * @param maxConnections the maxConnections to set.
     * @return a reference to this RiakNode.
     * @see Builder#withMaxConnections(int)
     */
    public RiakNode setMaxConnections(int maxConnections)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        if (maxConnections >= getMinConnections())
        {
            permits.setMaxPermits(maxConnections);
        }
        else
        {
            throw new IllegalArgumentException("Max connections less than min connections");
        }
        // TODO: reap delta? 
        return this;
    }

    /**
     * Returns the maximum number of connections allowed.
     *
     * @return the maxConnections
     * @see Builder#withMaxConnections(int)
     */
    public int getMaxConnections()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return permits.getMaxPermits();
    }

    /**
     * Sets the minimum number of active connections to be maintained.
     *
     * @param minConnections the minConnections to set
     * @return a reference to this RiakNode
     * @see Builder#withMinConnections(int)
     */
    public RiakNode setMinConnections(int minConnections)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        if (minConnections <= getMaxConnections())
        {
            this.minConnections = minConnections;
        }
        else
        {
            throw new IllegalArgumentException("Min connections greater than max connections");
        }
        // TODO: Start / reap delta?
        return this;
    }

    /**
     * Returns the current minimum number of active connections to be maintained.
     *
     * @return the minConnections
     * @see Builder#withMinConnections(int)
     */
    public int getMinConnections()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return minConnections;
    }

    /**
     * Sets the connection idle timeout for connections.
     *
     * @param idleTimeoutInMillis the idleTimeout to set
     * @return a reference to this RiakNode
     * @see Builder#withIdleTimeout(int)
     */
    public RiakNode setIdleTimeout(int idleTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(idleTimeoutInMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    /**
     * Returns the connection idle timeout for connections in milliseconds.
     *
     * @return the idleTimeout in milliseconds
     * @see Builder#withIdleTimeout(int)
     */
    public int getIdleTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return (int) TimeUnit.MILLISECONDS.convert(idleTimeoutInNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Sets the connection timeout for new connections.
     *
     * @param connectionTimeoutInMillis the connectionTimeout to set
     * @return a reference to this RiakNode
     * @see Builder#withConnectionTimeout(int)
     */
    public RiakNode setConnectionTimeout(int connectionTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.connectionTimeout = connectionTimeoutInMillis;
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
        return this;
    }

    /**
     * Returns the connection timeout in milliseconds.
     *
     * @return the connectionTimeout
     * @see Builder#withConnectionTimeout(int)
     */
    public int getConnectionTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return connectionTimeout;
    }

    /**
     * Returns the number of permits currently available.
     * The number of available permits indicates how many additional
     * connections can be made without blocking.
     *
     * @return the number of available permits.
     * @see Builder#withMaxConnections(int)
     */
    public int availablePermits()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return permits.availablePermits();
    }

    public void addStateListener(NodeStateListener listener)
    {
        stateListeners.add(listener);
    }

    public boolean removeStateListener(NodeStateListener listener)
    {
        return stateListeners.remove(listener);
    }

    private void notifyStateListeners()
    {
        synchronized (stateListeners)
        {
            for (Iterator<NodeStateListener> it = stateListeners.iterator(); it.hasNext(); )
            {
                NodeStateListener listener = it.next();
                listener.nodeStateChanged(this, state);
            }
        }
    }

    // TODO: Streaming also - idea: BlockingDeque extension with listener

    /**
     * Submits the operation to be executed on this node.
     *
     * @param operation The operation to perform
     * @return {@code true} if this operation was accepted, {@code false} if there
     *         were no available connections.
     * @throws IllegalStateException    if this node is not in the {@code RUNNING} or {@code HEALTH_CHECKING} state
     * @throws IllegalArgumentException if the protocol required for the operation is not supported by this node
     */
    public boolean execute(FutureOperation operation)
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);

        operation.setLastNode(this);
        Channel channel = getConnection();
        if (channel != null)
        {
            // Add a timeout handler to the pipeline if the readTIeout is set
            if (readTimeoutInMillis > 0)
            {
                channel.pipeline()
                    .addAfter(Constants.OPERATION_ENCODER, Constants.TIMEOUT_HANDLER,
                        new ReadTimeoutHandler(readTimeoutInMillis, TimeUnit.MILLISECONDS));
            }

            inProgressMap.put(channel, operation);
            ChannelFuture writeFuture = channel.writeAndFlush(operation);
            writeFuture.addListener(writeListener);
            logger.debug("Operation executed on RiakNode {}:{}", remoteAddress, port);
            return true;
        }
        else
        {
            return false;
        }
    }

    // ConnectionPool Stuff

    /**
     * Get a Netty channel from the pool.
     *
     * @return a connected channel or {@code null} if all connections are in use or
     *         a new connection can not be made.
     */
    private Channel getConnection()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        boolean acquired = permits.tryAcquire();
        Channel channel = null;
        if (acquired)
        {
            try
            {
                channel = doGetConnection();
                inUse.add(channel);
            }
            catch (ConnectionFailedException ex)
            {
                permits.release();
            }
        }
        return channel;
    }

    private Channel doGetConnection() throws ConnectionFailedException
    {
        ChannelWithIdleTime cwi;
        while ((cwi = available.poll()) != null)
        {
            Channel channel = cwi.getChannel();
            // If the channel from available is closed, try again. This will result in
            // the caller always getting a connection or an exception. If closed
            // the channel is simply discarded so this also acts as a purge
            // for dead channels during a health check.
            if (channel.isOpen())
            {
                return channel;
            }
        }


        ChannelFuture f = bootstrap.connect();
        // Any channels that don't connect will trigger a close operation as well
        f.channel().closeFuture().addListener(closeListener);

        try
        {
            f.await();
        }
        catch (InterruptedException ex)
        {
            logger.info("Thread interrupted waiting for new connection to be made; {}",
                remoteAddress);
            Thread.currentThread().interrupt();
            throw new ConnectionFailedException(ex);
        }

        if (!f.isSuccess())
        {
            logger.error("Connection attempt failed: {}:{}; {}",
                remoteAddress, port, f.cause());
            throw new ConnectionFailedException(f.cause());
        }

        return f.channel();

    }

    /**
     * Return a Netty channel to the pool.
     *
     * @param c The Netty channel to return to the pool
     * @throws IllegalArgumentException If the channel did not originate from this pool
     */
    private void returnConnection(Channel c)
    {
        stateCheck(State.RUNNING, State.SHUTTING_DOWN, State.SHUTDOWN, State.HEALTH_CHECKING);
        if (!inUse.remove(c))
        {
            throw new IllegalArgumentException("Channel not managed by this pool");
        }

        switch (state)
        {
            case SHUTTING_DOWN:
            case SHUTDOWN:
                closeConnection(c);
                break;
            case RUNNING:
            case HEALTH_CHECKING:
            default:
                if (c.isOpen())
                {
                    logger.debug("Channel id:{} returned to pool", c.hashCode());
                    available.offerFirst(new ChannelWithIdleTime(c));
                }
                else
                {
                    logger.debug("Closed channel id:{} returned to pool; discarding", c.hashCode());
                }
                permits.release();
                break;
        }
    }

    private void closeConnection(Channel c)
    {
        // If we are explicitly closing the connection we don't want to hear
        // about it.
        c.closeFuture().removeListener(closeListener);
        c.close();
    }


    // End ConnectionPool stuff

    @Override
    public void onSuccess(Channel channel, final RiakMessage response)
    {
        logger.debug("Operation onSuccess() channel: id:{} {}:{}", channel.hashCode(),
            remoteAddress, port);
        if (readTimeoutInMillis > 0)
        {
            channel.pipeline().remove(Constants.TIMEOUT_HANDLER);
        }

        final FutureOperation inProgress = inProgressMap.get(channel);
        inProgress.setResponse(response);

        if (inProgress.isDone())
        {
            inProgressMap.remove(channel);
            returnConnection(channel);
        }

    }

    @Override
    public void onException(Channel channel, final Throwable t)
    {
        logger.debug("Operation onException() channel: id:{} {}:{} {}",
            channel.hashCode(), remoteAddress, port, t);

        final FutureOperation inProgress = inProgressMap.remove(channel);
        // There are fail cases where multiple exceptions are thrown from 
        // the pipeline. In that case we'll get an exception from the 
        // handler but will not have an entry in inProgress because it's
        // already been handled.
        if (inProgress != null)
        {
            if (readTimeoutInMillis > 0)
            {
                channel.pipeline().remove(Constants.TIMEOUT_HANDLER);
            }
            returnConnection(channel);
            inProgress.setException(t);

        }
    }

    /**
     * Returns the {@code remoteAddress} for this RiakNode
     *
     * @return The IP address or FQDN as a {@code String}
     */
    public String getRemoteAddress()
    {
        return remoteAddress;
    }

    /**
     * returns the remote port for this RiakNode
     *
     * @return the port number
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Returns the current state of this node.
     *
     * @return The state
     */
    public State getNodeState()
    {
        return this.state;
    }

    private class ChannelWithIdleTime
    {
        private Channel channel;
        private long idleStart;

        public ChannelWithIdleTime(Channel channel)
        {
            this.channel = channel;
            idleStart = System.nanoTime();
        }

        public Channel getChannel()
        {
            return channel;
        }

        public long getIdleStart()
        {
            return idleStart;
        }
    }

    private class Sync extends Semaphore
    {
        private static final long serialVersionUID = -5118488872281021072L;
        private volatile int maxPermits;

        public Sync(int numPermits)
        {
            super(numPermits);
            this.maxPermits = numPermits;
        }

        public Sync(int numPermits, boolean fair)
        {
            super(numPermits, fair);
            this.maxPermits = numPermits;
        }

        public int getMaxPermits()
        {
            return maxPermits;
        }

        // Synchronized because we're (potentially) changing this.maxPermits
        synchronized void setMaxPermits(int maxPermits)
        {
            int diff = maxPermits - this.maxPermits;

            if (diff == 0)
            {
                return;
            }
            else if (diff > 0)
            {
                release(diff);
            }
            else if (diff < 0)
            {
                reducePermits(diff);
            }

            this.maxPermits = maxPermits;
        }

    }

    private class IdleReaper implements Runnable
    {
        @Override
        public void run()
        {
            reapIdleConnections();
        }
    }

    private void reapIdleConnections()
    {
        // with all the concurrency there's really no reason to keep 
        // checking the sizes. This is really just a "best guess"
        int currentNum = inUse.size() + available.size();
        if (currentNum > minConnections)
        {
            // Note this will not throw a ConncurrentModificationException
            // and if hasNext() returns true you are guaranteed that
            // the next() will return a value (even if it has already
            // been removed from the Deque between those calls). 
            Iterator<ChannelWithIdleTime> i = available.descendingIterator();
            while (i.hasNext() && currentNum > minConnections)
            {
                ChannelWithIdleTime cwi = i.next();
                if (cwi.getIdleStart() + idleTimeoutInNanos < System.nanoTime())
                {
                    boolean removed = available.remove(cwi);
                    if (removed)
                    {
                        Channel c = cwi.getChannel();
                        logger.debug("Idle channel closed; {}:{}", remoteAddress, port);
                        closeConnection(c);
                        currentNum--;
                    }
                }
                else
                {
                    // Since we are descending and this is a LIFO, 
                    // if the current connection hasn't been idle beyond 
                    // the threshold, there's no reason to descend further
                    break;
                }
            }
        }
    }

    // TODO: Magic numbers; probably should be configurable and less magic
    private class HealthMonitorTask implements Runnable
    {
        @Override
        public void run()
        {
            if ((state == State.RUNNING || state == State.HEALTH_CHECKING) &&
                (recentlyClosed.size() > 4 || state == State.HEALTH_CHECKING))
            {
                checkHealth();
            }
        }
    }

    // TODO: Magic numbers; probably should be configurable and less magic
    // TODO: Maybe ping the node or something more conclusive than if it just accepts a connection
    private void checkHealth()
    {
        try
        {
            // purge recentlyClosed past a certain age
            // sliding window should be larger than the
            // frequency of this task
            long current = System.nanoTime();
            long window = 3000000000L; // 3 seconds 
            for (ChannelWithIdleTime cwi = recentlyClosed.peek();
                 cwi != null && current - cwi.getIdleStart() > window;
                 cwi = recentlyClosed.peek())
            {
                recentlyClosed.poll();
            }

            // See: doGetConnection() - this will purge closed
            // connections from the available queue and either 
            // return/create a new one (meaning the node is up) or throw
            // an exception if a connection can't be made.
            Channel c = doGetConnection();
            closeConnection(c);

            if (state == State.HEALTH_CHECKING)
            {
                logger.info("RiakNode recovered; {}:{}", remoteAddress, port);
                state = State.RUNNING;
                notifyStateListeners();
            }

        }
        catch (ConnectionFailedException ex)
        {
            if (state == State.RUNNING)
            {
                logger.error("RiakNode offline; health checking; {}:{} {}",
                    remoteAddress, port, ex);
                state = State.HEALTH_CHECKING;
                notifyStateListeners();
            }
            else
            {
                logger.error("RiakNode failed health check; {}:{} {}",
                    remoteAddress, port, ex);
            }
        }
        catch (IllegalStateException e)
        {
            // no-op; there's a race condition where the bootstrap is shutting down
            // right when a healthcheck occurs and netty will throw this
        }


    }

    private class ShutdownTask implements Runnable
    {
        @Override
        public void run()
        {
            if (inUse.isEmpty())
            {
                state = State.SHUTDOWN;
                notifyStateListeners();
                if (ownsExecutor)
                {
                    executor.shutdown();
                }
                if (ownsBootstrap)
                {
                    bootstrap.group().shutdownGracefully();
                }
                logger.debug("RiakNode shut down {}:{}", remoteAddress, port);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder used to construct a RiakNode.
     */
    public static class Builder
    {
        /**
         * The default remote address to be used if not specified: {@value #DEFAULT_REMOTE_ADDRESS}
         *
         * @see #withRemoteAddress(java.lang.String)
         */
        public final static String DEFAULT_REMOTE_ADDRESS = "127.0.0.1";
        /**
         * The default port number to be used if not specified: {@value #DEFAULT_REMOTE_PORT}
         *
         * @see AbstractCollection#withRemotePort(int)
         */
        public final static int DEFAULT_REMOTE_PORT = 8087;
        /**
         * The default minimum number of connections to maintain if not specified: {@value #DEFAULT_MIN_CONNECTIONS}
         *
         * @see #withMinConnections(int)
         */
        public final static int DEFAULT_MIN_CONNECTIONS = 1;
        /**
         * The default maximum number of connections allowed if not specified: {@value #DEFAULT_MAX_CONNECTIONS}
         *
         * @see #withMaxConnections(int)
         */
        public final static int DEFAULT_MAX_CONNECTIONS = 0;
        /**
         * The default idle timeout in milliseconds for connections if not specified: {@value #DEFAULT_IDLE_TIMEOUT}
         *
         * @see #withIdleTimeout(int)
         */
        public final static int DEFAULT_IDLE_TIMEOUT = 1000;
        /**
         * The default connection timeout in milliseconds if not specified: {@value #DEFAULT_CONNECTION_TIMEOUT}
         *
         * @see #withConnectionTimeout(int)
         */
        public final static int DEFAULT_CONNECTION_TIMEOUT = 0;
        /**
         * The default TCP read timeout in milliseconds if not specified: {@value #DEFAULT_TCP_READ_TIMEOUT}
         * A value of {@code 0} means to wait indefinitely
         *
         * @see #withReadTimeout(int)
         */
        public final static int DEFAULT_TCP_READ_TIMEOUT = 0;

        private int port = DEFAULT_REMOTE_PORT;
        private String remoteAddress = DEFAULT_REMOTE_ADDRESS;
        private int minConnections = DEFAULT_MIN_CONNECTIONS;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private int readTimeout = DEFAULT_TCP_READ_TIMEOUT;
        private Bootstrap bootstrap;
        private ScheduledExecutorService executor;


        /**
         * Default constructor. Returns a new builder for a RiakNode with
         * default values set.
         */
        public Builder()
        {

        }

        /**
         * Sets the remote address for this RiakNode.
         *
         * @param remoteAddress Can either be a FQDN or IP address
         * @return this
         * @see #DEFAULT_REMOTE_ADDRESS
         */
        public Builder withRemoteAddress(String remoteAddress)
        {
            this.remoteAddress = remoteAddress;
            return this;
        }

        /**
         * Specifies the remote port for this RiakNode.
         *
         * @param port - the port
         * @return this
         * @see #DEFAULT_REMOTE_PORT
         */
        public Builder withRemotePort(int port)
        {
            this.port = port;
            return this;
        }

        /**
         * Set the minimum number of active connections to maintain.
         * These connections are exempt from the idle timeout.
         *
         * @param minConnections - number of connections to maintain.
         * @return this
         * @see #DEFAULT_MIN_CONNECTIONS
         */
        public Builder withMinConnections(int minConnections)
        {
            if (maxConnections == DEFAULT_MAX_CONNECTIONS || minConnections <= maxConnections)
            {
                this.minConnections = minConnections;
            }
            else
            {
                throw new IllegalArgumentException("Min connections greater than max connections");
            }
            return this;
        }

        /**
         * Set the maximum number of connections allowed.
         * A value of 0 sets this to unlimited.
         *
         * @param maxConnections - maximum number of connections to allow
         * @return this
         * @see #DEFAULT_MAX_CONNECTIONS
         */
        public Builder withMaxConnections(int maxConnections)
        {
            if (maxConnections >= minConnections)
            {
                this.maxConnections = maxConnections;
            }
            else
            {
                throw new IllegalArgumentException("Max connections less than min connections");
            }
            return this;
        }

        /**
         * Set the idle timeout used to reap inactive connections.
         * Any connection that has been idle for this amount of time
         * becomes eligible to be closed and discarded unless {@code minConnections}
         * has been set via {@link #withMinConnections(int) }
         *
         * @param idleTimeoutInMillis - idle timeout in milliseconds
         * @return this
         * @see #DEFAULT_IDLE_TIMEOUT
         */
        public Builder withIdleTimeout(int idleTimeoutInMillis)
        {
            this.idleTimeout = idleTimeoutInMillis;
            return this;
        }

        /**
         * Set the connection timeout used when making new connections
         *
         * @param connectionTimeoutInMillis
         * @return this
         * @see #DEFAULT_CONNECTION_TIMEOUT
         */
        public Builder withConnectionTimeout(int connectionTimeoutInMillis)
        {
            this.connectionTimeout = connectionTimeoutInMillis;
            return this;
        }

        //TODO: Now that we have operation timeouts, do we really want to expose the TCP read timeout?

        /**
         * Specifies the TCP read timeout when waiting for a reply from Riak.
         *
         * @param readTimeoutInMillis - a timeout in milliseconds
         * @return this
         * @see #DEFAULT_READ_TIMEOUT
         */
        public Builder withReadTimeout(int readTimeoutInMillis)
        {
            this.readTimeout = readTimeoutInMillis;
            return this;
        }

        /**
         * Provides an executor for this node to use for internal maintenance tasks.
         * If not provided one will be created via
         * {@link Executors#newSingleThreadScheduledExecutor()}
         *
         * @param executor the ScheduledExecutorService to use.
         * @return this
         */
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }

        /**
         * Provides a Netty Bootstrap for this node to use.
         * If not provided one
         * will be created with its own {@code NioEventLoopGroup}.
         *
         * @param bootstrap
         * @return this
         */
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }


        /**
         * Builds a RiakNode.
         * If a Netty {@code Bootstrap} and/or a {@code ScheduledExecutorService} has not been provided they
         * will be created.
         *
         * @return a new Riaknode
         * @throws UnknownHostException if the DNS lookup fails for the supplied hostname
         */
        public RiakNode build() throws UnknownHostException
        {
            return new RiakNode(this);
        }


        /**
         * Build a set of RiakNodes.
         * The provided builder will be used to construct a set of RiakNodes
         * using the supplied addresses.
         *
         * @param builder         a configured builder
         * @param remoteAddresses a list of IP addresses or FQDN
         * @return a list of constructed RiakNodes
         * @throws UnknownHostException if a supplied FQDN can not be resolved.
         */
        public static List<RiakNode> buildNodes(Builder builder, List<String> remoteAddresses)
            throws UnknownHostException
        {
            List<RiakNode> nodes = new ArrayList<RiakNode>(remoteAddresses.size());
            for (String remoteAddress : remoteAddresses)
            {
                builder.withRemoteAddress(remoteAddress);
                nodes.add(builder.build());
            }
            return nodes;
        }
    }
}
