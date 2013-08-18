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

import com.basho.riak.client.core.netty.RiakChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection pool that manages Netty channels.
 * <p/>
 * <p>
 * A instance of ConnectionPool is created via its {@link Builder}.
 * </p>
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ConnectionPool implements ChannelFutureListener
{
    public enum State
    {
        CREATED, RUNNING, HEALTH_CHECKING, SHUTTING_DOWN, SHUTDOWN;
    }

    private final LinkedBlockingDeque<ChannelWithIdleTime> available;
    private final ConcurrentLinkedQueue<Channel> inUse;
    private final ConcurrentLinkedQueue<ChannelWithIdleTime> recentlyClosed;
    private final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    private final Sync permits;
    private final String remoteAddress;
    private final int port;
    private final List<PoolStateListener> stateListeners =
        Collections.synchronizedList(new LinkedList<PoolStateListener>());

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

    /**
     * Constructs a ConnectionPool from the supplied Builder.
     * @param builder a configured ConnectionPool.Builder
     * @throws UnknownHostException if the remote address can not be resolved
     */
    private ConnectionPool(Builder builder) throws UnknownHostException
    {
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(builder.idleTimeout, TimeUnit.MILLISECONDS);
        this.minConnections = builder.minConnections;
        this.port = builder.port;
        this.remoteAddress = builder.remoteAddress;
        this.executor = builder.executor;

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

        this.available = new LinkedBlockingDeque<ChannelWithIdleTime>();
        this.inUse = new ConcurrentLinkedQueue<Channel>();
        this.recentlyClosed = new ConcurrentLinkedQueue<ChannelWithIdleTime>();
        this.state = State.CREATED;
    }

    private void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; remote: {} required: {} current: {} ",
                remoteAddress, Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: "
                + Arrays.toString(allowedStates)
                + " current: " + state);
        }
    }

    public ConnectionPool addStateListener(PoolStateListener listener)
    {
        stateListeners.add(listener);
        return this;
    }

    public boolean removeStateListener(PoolStateListener listener)
    {
        return stateListeners.remove(listener);
    }

    private void notifyStateListeners()
    {
        synchronized (stateListeners)
        {
            for (Iterator<PoolStateListener> it = stateListeners.iterator(); it.hasNext(); )
            {
                PoolStateListener listener = it.next();
                listener.poolStateChanged(this, state);
            }
        }
    }

    // We're listening to Netty close futures
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

    /**
     * Starts this connection pool.
     * <p/>
     * If {@code minConnections} has been set, that number of active connections
     * will be started and placed into the available queue. Note that if they
     * can not be established due to the Riak node being down, a network issue,
     * etc this will not prevent the pool from changing to a started state.
     *
     * @return this
     */
    public synchronized ConnectionPool start()
    {
        stateCheck(State.CREATED);
        if (bootstrap == null)
        {
            this.bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);
            ownsBootstrap = true;
        }

        bootstrap.handler(new RiakChannelInitializer())
                 .remoteAddress(new InetSocketAddress(remoteAddress, port));

        if (connectionTimeout > 0)
        {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
        }

        if (executor == null)
        {
            executor = Executors.newSingleThreadScheduledExecutor();
            ownsExecutor = true;
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
        logger.info("ConnectionPool started; {}:{}", remoteAddress, port);
        notifyStateListeners();
        return this;
    }

    /**
     * Shuts down this connection pool and all resources owned by it.
     * <p/>
     * If the {@code executor} and/or {@code bootstrap} were provided to the
     * {@link Builder} when constructing the pool they will *not* be shut down
     * in this call.
     */
    public synchronized void shutdown()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        state = State.SHUTTING_DOWN;
        logger.info("Connection pool shutting down {}:{}", remoteAddress, port);
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
     * Get a Netty channel from this pool.
     *
     * @return a connected channel or {@code null} if all connections are in use or
     *         a new connection can not be made.
     */
    public Channel getConnection()
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
        f.channel().closeFuture().addListener(this);

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
     * Return a Netty channel to this pool.
     *
     * @param c The Netty channel to return to this pool
     * @throws IllegalArgumentException If the channel did not originate from this pool
     */
    public void returnConnection(Channel c)
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
        c.closeFuture().removeListener(this);
        c.close();
    }

    /**
     * Sets the {@link ScheduledExecutorService} for this pool.
     *
     * @param executor a ScheduledExecutorService for this pool.
     * @return a reference to this ConnectionPool
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException    if the pool has already been started.
     * @see Builder#withExecutor(java.util.concurrent.ScheduledExecutorService)
     */
    public ConnectionPool setExecutor(ScheduledExecutorService executor)
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
     * Sets the Netty {@link Bootstrap} for this pool.
     * {@link Bootstrap#clone()} is called to clone the bootstrap.
     *
     * @param bootstrap a netty Bootstrap
     * @return a reference to this ConnectionPool
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException    if the pool has already been started.
     * @see Builder#withBootstrap(io.netty.bootstrap.Bootstrap)
     */
    public ConnectionPool setBootstrap(Bootstrap bootstrap)
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
     * Returns the number of permits currently available in this pool.
     * The number of available permits indicates how many additional
     * connections can be acquired from this pool without blocking
     *
     * @return the number of available permits.
     * @see Builder#withMaxConnections(int)
     */
    public int availablePermits()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return permits.availablePermits();
    }

    /**
     * Returns the current minimum number of active connections maintained in this pool.
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
     * Sets the minimum number of active connections to be maintained by this pool.
     *
     * @param minConnections the minConnections to set
     * @return a reference to this ConnectionPool
     * @see Builder#withMinConnections(int)
     */
    public ConnectionPool setMinConnections(int minConnections)
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
     * Returns the maximum number of connections allowed in this pool
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
     * Sets the maximum number of connections allowed in this pool.
     *
     * @param maxConnections the maxConnections to set.
     * @return a reference to this ConnectionPool.
     */
    public ConnectionPool setMaxConnections(int maxConnections)
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
     * Returns the connection idle timeout in milliseconds for this pool.
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
     * Sets the connection idle timeout for this pool
     *
     * @param idleTimeoutInMillis the idleTimeout to set
     * @return a reference to this ConnectionPool
     * @see Builder#withIdleTimeout(int)
     */
    public ConnectionPool setIdleTimeout(int idleTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(idleTimeoutInMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    /**
     * Returns the connection timeout in milliseconds for this pool
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
     * Sets the connection timeout for this pool
     *
     * @param connectionTimeoutInMillis the connectionTimeout to set
     * @return a reference to this ConnectionPool
     * @see Builder#withConnectionTimeout(int)
     */
    public ConnectionPool setConnectionTimeout(int connectionTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.connectionTimeout = connectionTimeoutInMillis;
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
        return this;
    }

    /**
     * Returns the current state of this pool.
     *
     * @return the current state
     */
    public State getPoolState()
    {
        return state;
    }

    /**
     * returns the port for this pool
     *
     * @return the port number
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Returns the remote address this pool is connecting to.
     *
     * @return The remote address, either an IP or a FQDN
     */
    public String getRemoteAddress()
    {
        return this.remoteAddress;
    }

    ScheduledExecutorService getExecutor()
    {
        return this.executor;
    }

    Bootstrap getBootstrap()
    {
        return this.bootstrap;
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
                logger.info("ConnectionPool recovered; {}:{} {}", remoteAddress, port);
                state = State.RUNNING;
                notifyStateListeners();
            }

        }
        catch (ConnectionFailedException ex)
        {
            if (state == State.RUNNING)
            {
                logger.error("ConnectionPool health checking; {}:{} {}",
                    remoteAddress, port, ex);
                state = State.HEALTH_CHECKING;
                notifyStateListeners();
            }
            else
            {
                logger.error("ConnectionPool failed health check; {}:{} {}",
                    remoteAddress, port, ex);
            }
        }
        catch (IllegalStateException e)
        {
            // no-op; there's a race condition where the bootstrap is shutting down
            // right when a healthcheck occurs and netty will throw this
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
                logger.debug("ConnectionPool shut down {}:{}", remoteAddress, port);
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

    /**
     *
     * @author Brian Roach <roach at basho dot com>
     * @since 2.0
     */
    static class Sync extends Semaphore
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

        private int port = DEFAULT_REMOTE_PORT;
        private String remoteAddress = DEFAULT_REMOTE_ADDRESS;
        private int minConnections = DEFAULT_MIN_CONNECTIONS;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private Bootstrap bootstrap;
        private ScheduledExecutorService executor;
        private ChannelInitializer<SocketChannel> channelInitializer;
        
        /**
         * Constructs a Builder with all default values
         */
        protected Builder()
        {
        }

        /**
         * Set the port this pool will connect to.
         *
         * @param port the remote port number
         * @return this
         * @see #DEFAULT_REMOTE_PORT
         */
        public Builder withRemotePort(int port)
        {
            this.port = port;
            return this;
        }

        /**
         * Set the remote address this pool will connect to.
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
         * Set the minimum number of active connections to maintain in the pool.
         * These connections are exempt from the idle timeout.
         *
         * @param minConnections
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
         * Set the maximum number of connections allowed by the pool. 
         * A value of 0 sets this to unlimited.
         *
         * @param maxConnections
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
         * @param idleTimeoutInMillis
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

        /**
         * The Netty bootstrap to be used with this pool. If not provided one
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
         * The {@link ScheduledExecutorService} to be used by this pool. This is
         * used for internal maintenance tasks. If not provided one will be created via
         * {@link Executors#newSingleThreadScheduledExecutor()}
         *
         * @param executor
         * @return this
         */
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }

        /**
         * The Netty {@code ChannelInitializer} to use with this pool.
         * @param initializer the initializer. 
         * @return this
         */
        public Builder withChannelInitializer(ChannelInitializer<SocketChannel> initializer)
        {
            this.channelInitializer = initializer;
            return this;
        }
        
        /**
         * Constructs and returns a ConnectionPool.
         * @return a new instance of ConnectionPool.
         * @throws UnknownHostException if the supplied remote address can not be resolved.
         */
        public ConnectionPool build() throws UnknownHostException
        {
            return new ConnectionPool(this);
        }

    }

}
