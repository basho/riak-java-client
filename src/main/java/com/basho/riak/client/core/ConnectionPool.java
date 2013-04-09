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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection pool that manages Netty channels. 
 *
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

    private final Bootstrap bootstrap;
    private final LinkedBlockingDeque<ChannelWithIdleTime> available;
    private final ConcurrentLinkedQueue<Channel> inUse;
    private final ConcurrentLinkedQueue<ChannelWithIdleTime> recentlyClosed;
    private final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    private final AdjustableSemaphore permits;
    private final String remoteAddress;
    private final Protocol protocol;
    private final int port;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private final boolean ownsBootstrap;
    private final List<PoolStateListener> stateListeners = 
        Collections.synchronizedList(new LinkedList<PoolStateListener>());
    
    private volatile State state;
    private ScheduledFuture<?> idleReaperFuture;
    private ScheduledFuture<?> healthMonitorFuture;
    private int minConnections;
    private long idleTimeoutInNanos;
    private int connectionTimeout;
    private int readTimeout;
    private int permitTimeout;
    

    private ConnectionPool(Builder builder) throws UnknownHostException
    {
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(builder.idleTimeout, TimeUnit.MILLISECONDS);
        this.minConnections = builder.minConnections;
        this.readTimeout = builder.readTimeout;
        this.permitTimeout = builder.permitTimeout;
        this.protocol = builder.protocol;
        this.port = builder.port;
        this.remoteAddress = builder.remoteAddress;
        
        if (builder.bootstrap == null)
        {
            this.bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);
            ownsBootstrap = true;
        }
        else
        {
            this.bootstrap = builder.bootstrap.duplicate();
            this.ownsBootstrap = false;
        }
        
        this.bootstrap.handler(protocol.channelInitializer())
                      .remoteAddress(new InetSocketAddress(remoteAddress, port));
        
        
        if (builder.executor == null)
        {
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.ownsExecutor = true;
        }
        else
        {
            this.executor = builder.executor;
            this.ownsExecutor = false;
        }
        
        if (builder.maxConnections < 1)
        {
            permits = new UnlimitedSemaphore();
        }
        else
        {
            permits = new AdjustableSemaphore(builder.maxConnections);
        }
        
        if (connectionTimeout > 0)
        {
            this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
        }
        
        this.available = new LinkedBlockingDeque<>(builder.maxConnections);
        this.inUse = new ConcurrentLinkedQueue<>();
        this.recentlyClosed = new ConcurrentLinkedQueue<>();
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
                + " current: " + state );
        }
    }
    
    public void addStateListener(PoolStateListener listener)
    {
        stateListeners.add(listener);
    }
    
    public void removeStateListener(PoolStateListener listener)
    {
        stateListeners.remove(listener);
    }
    
    private void notifyStateListeners()
    {
        synchronized(stateListeners)
        {
            for (Iterator<PoolStateListener> it = stateListeners.iterator(); it.hasNext();)
            {
                PoolStateListener listener = it.next();
                listener.poolStateChanged(this, state);
            }
        }
    }
    
    @Override
    public void operationComplete(ChannelFuture future) throws Exception
    {
        // Because we aren't storing raw channels in available we just throw
        // these in the recentlyClosed as an indicator. We'll leave it up
        // to the healthcheck task to purge the available queue of dead
        // connections and make decisions on what to do 
        recentlyClosed.add(new ChannelWithIdleTime(future.channel()));
        logger.debug("Channel closed; {} {}", remoteAddress, protocol.toString());
    }
    
    /**
     * Starts this connection pool. 
     * 
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
        if (minConnections > 0)
        {
            List<Channel> minChannels = new LinkedList<>();
            for (int i = 0; i < minConnections; i++)
            {
                try
                {
                    minChannels.add(doGetConnection());
                }
                catch (ConnectionFailedException | PermitTimeoutException | InterruptedException ex)
                {
                    // No-op. Error is logged in doGetConnection() and
                    // at this point we don't really care
                }
                
            }
            
            for (Channel c : minChannels)
            {
                returnConnection(c);
            }
        }
        
        idleReaperFuture = executor.scheduleWithFixedDelay(new IdleReaper(), 5, 15, TimeUnit.SECONDS);
        healthMonitorFuture = executor.scheduleWithFixedDelay(new HealthMonitorTask(), 1000, 500, TimeUnit.MILLISECONDS);
        state = State.RUNNING;
        logger.info("ConnectionPool started; {} {}", remoteAddress, protocol);
        notifyStateListeners();
        return this;
    }
    
    /**
     * Shuts down this connection pool and all resources owned by it. 
     * 
     * If the {@code executor} and/or {@code bootstrap} were provided to the
     * {@link Builder} when constructing the pool they will *not* be shut down
     * in this call. 
     */
    public synchronized void shutdown()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        state = State.SHUTTING_DOWN;
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
     * @return a connected channel.
     * @throws ConnectionFailedException if a connection can not be made
     */
    public Channel getConnection() throws ConnectionFailedException, InterruptedException, PermitTimeoutException
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        return doGetConnection();
    }
        
    private Channel doGetConnection() throws ConnectionFailedException, InterruptedException, PermitTimeoutException
    {
        boolean acquired = false;
        Channel channel = null;
        try
        {
            acquired = permits.tryAcquire(permitTimeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted getting permit for connection");
            throw ex;
        }
            
        if (acquired)
        {
            ChannelWithIdleTime cwi = available.poll();
            if (cwi == null)
            {
                ChannelFuture f = bootstrap.connect();
                try
                {
                    f.await();
                }
                catch (InterruptedException ex)
                {
                    permits.release();
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrupted waiting for connection", ex);
                    throw ex;
                }
                
                if (!f.isSuccess())
                {
                    permits.release();
                    logger.error("Connection attempt failed: {}:{}; {}", 
                                 remoteAddress, port, f.cause().toString());
                    throw new ConnectionFailedException(f.cause());
                }

                channel = f.channel();
                channel.closeFuture().addListener(this);
            }
            else
            {
                channel = cwi.getChannel();
                // If the channel from available is closed, try again. This will result in
                // the caller always getting a connection or an exception. If closed 
                // the channel is simply discarded so this also acts as a purge
                // for dead channels during a health check. 
                if (channel.closeFuture().isDone())
                {
                    return doGetConnection();
                }
                
            }
        }
        else
        {
            logger.info("Could not aquire pool permit; {}:{}", remoteAddress, port);
            throw new PermitTimeoutException();
        }

        inUse.add(channel);
        return channel;
    }
    
    /**
     * Return a Netty channel to this pool. 
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
        
        switch(state)
        {
            case SHUTTING_DOWN:
            case SHUTDOWN:
                closeConnection(c);
                break;
            case RUNNING:
            case HEALTH_CHECKING:
            default:
                permits.release();
                if (c.isOpen())
                {
                    available.offerFirst(new ChannelWithIdleTime(c));
                }
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
     * Returns the number of permits currently available in this pool. 
     * The number of available permits indicates how many additional
     * connections can be acquired from this pool without blocking /
     * waiting on a permit.
     * 
     * @return the number of available permits.
     * @see Builder#withMaxConnections(int) 
     * @see Builder#withPermitTimeout(int) 
     */
    public int availablePermits()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        return permits.availablePermits();
    }
    
    /**
     * Returns the current minimum number of active connections maintained in this pool.
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
     * @param minConnections the minConnections to set
     * @see Builder#withMinConnections(int) 
     */
    public void setMinConnections(int minConnections)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.minConnections = minConnections;
        // TODO: Start / reap delta?
    }

    /**
     * Returns the maximum number of connections allowed in this pool
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
     * @param maxConnections the maxConnections to set
     */
    public void setMaxConnections(int maxConnections)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        permits.setMaxPermits(maxConnections);
        // TODO: reap delta? 
    }

    /**
     * Returns the connection idle timeout in milliseconds for this pool.
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
     * @param idleTimeout the idleTimeout to set
     * @see Builder#withIdleTimeout(int) 
     */
    public void setIdleTimeout(int idleTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(idleTimeoutInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the connection timeout in milliseconds for this pool
     * @return the connectionTimeout
     * @see Builder#withIdleTimeout(int) 
     */
    public int getConnectionTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return connectionTimeout;
    }

    /**
     * Sets the connection timeout for this pool
     * @param connectionTimeout the connectionTimeout to set
     * @see Builder#withConnectionTimeout(int) 
     */
    public void setConnectionTimeout(int connectionTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.connectionTimeout = connectionTimeoutInMillis;
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
    }

    /**
     * Returns the read timeout in milliseconds for connections in this pool
     * @return the readTimeout
     * @see Builder#withReadTimeout(int) 
     */
    public int getReadTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return readTimeout;
    }

    /**
     * Sets the read timeout for connections in this pool
     * @param readTimeout the readTimeout to set
     * @see Builder#withReadTimeout(int) 
     */
    public void setReadTimeout(int readTimeout)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.readTimeout = readTimeout;
    }

    /**
     * Returns the permit timeout in milliseconds for this pool.
     * @return the permitTimeout
     * @see Builder#withPermitTimeout(int) 
     */
    public int getPermitTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return permitTimeout;
    }

    /**
     * Sets the permit timeout for this pool
     * @param permitTimeout the permitTimeout to set
     * @see Builder#withPermitTimeout(int) 
     */
    public void setPermitTimeout(int permitTimeout)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.permitTimeout = permitTimeout;
    }
    
    /**
     * Returns the current state of this pool.
     * @return the current state
     */
    public State getPoolState()
    {
        return state;
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
    
    private class IdleReaper implements Runnable
    {
        @Override
        public void run()
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
                while(i.hasNext() && currentNum > minConnections)
                {
                    ChannelWithIdleTime cwi = i.next();
                    if (cwi.getIdleStart() + idleTimeoutInNanos < System.nanoTime())
                    {
                        boolean removed = available.remove(cwi);
                        if (removed)
                        {
                            Channel c = cwi.getChannel();
                            closeConnection(c);
                            currentNum--;
                            logger.debug("Idle channel closed; {} {}", remoteAddress, protocol );
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
    }
    
    private class ShutdownTask implements Runnable
    {
        @Override
        public void run()
        {
            if (inUse.isEmpty())
            {
                logger.debug("ConnectionPool shutting down {} {}", remoteAddress, protocol);
                state = State.SHUTDOWN;
                notifyStateListeners();
                if (ownsBootstrap)
                {
                    bootstrap.shutdown();
                }
                if (ownsExecutor)
                {
                    executor.shutdown();
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
            if (state == State.RUNNING || state == State.HEALTH_CHECKING)
            {
                if (recentlyClosed.size() > 5 || state == State.HEALTH_CHECKING)
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
                        
                        // See: getConnection() - this will purge closed
                        // connections from the available queue and either 
                        // return/create a new one (meaning the node is up) or throw
                        // an exception if a connection can't be made.
                        Channel c = getConnection();
                        returnConnection(c);
                        if (state == State.HEALTH_CHECKING)
                        {
                            logger.debug("ConnectionPool recovered; {} {}", remoteAddress, protocol);
                            state = State.RUNNING;
                            notifyStateListeners();
                        }
                        
                    }
                    catch (ConnectionFailedException | InterruptedException ex)
                    {
                        if (state == State.RUNNING)
                        {
                            logger.debug("ConnectionPool health checking; {} {}", remoteAddress, protocol);
                            state = State.HEALTH_CHECKING;
                            notifyStateListeners();
                        }
                    }
                    catch (PermitTimeoutException ex)
                    {
                        // No-op; non-deterministic (?)
                    }
                    
                }
            }
        }
    }
    
    public static class Builder 
    {
        /**
         * The default remote address to be used if not specified: {@value #DEFAULT_REMOTE_ADDRESS}
         * @see #withRemoteAddress(java.lang.String) 
         */
        public final static String DEFAULT_REMOTE_ADDRESS = "127.0.0.1";
        /**
         * The default permit timeout in milliseconds to be used if not specified: {@value #DEFAULT_PERMIT_TIMEOUT}
         * @see #withPermitTimeout(int) 
         */
        public final static int DEFAULT_PERMIT_TIMEOUT = 1000;
        /**
         * The default minimum number of connections to maintain if not specified: {@value #DEFAULT_MIN_CONNECTIONS}
         * @see #withMinConnections(int) 
         */
        public final static int DEFAULT_MIN_CONNECTIONS = 1;
        /**
         * The default maximum number of connections allowed if not specified: {@value #DEFAULT_MAX_CONNECTIONS}
         * @see #withMaxConnections(int) 
         */
        public final static int DEFAULT_MAX_CONNECTIONS = 0;
        /**
         * The default idle timeout in milliseconds for connections if not specified: {@value #DEFAULT_IDLE_TIMEOUT}
         * @see #withIdleTimeout(int) 
         */
        public final static int DEFAULT_IDLE_TIMEOUT = 1000;
        /**
         * The default connection timeout in milliseconds if not specified: {@value #DEFAULT_CONNECTION_TIMEOUT}
         * @see #withConnectionTimeout(int) 
         */
        public final static int DEFAULT_CONNECTION_TIMEOUT = 0;
        /**
         * The default read timeout in milliseconds if not specified: {@value #DEFAULT_READ_TIMEOUT}
         * @see #withReadTimeout(int) 
         */
        public final static int DEFAULT_READ_TIMEOUT = 0;
        
        private final Protocol protocol;
        private int port;
        private String remoteAddress = DEFAULT_REMOTE_ADDRESS;
        private int minConnections = DEFAULT_MIN_CONNECTIONS;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private int readTimeout = DEFAULT_READ_TIMEOUT;
        private int permitTimeout = DEFAULT_PERMIT_TIMEOUT;
        private Bootstrap bootstrap;
        private boolean ownsBootstrap;
        private ScheduledExecutorService executor;
        private boolean ownsExecutor;

        /**
         * Constructs a Builder 
         * @param port the port on the remote host 
         */
        public Builder(Protocol protocol)
        {
            this.protocol = protocol;
            this.port = protocol.defaultPort();
        }
        
        /**
         * Set the port to be used 
         * @param port
         * @return this
         */
        public Builder withPort(int port)
        {
            this.port = port;
            return this;
        }
        
        /**
         * Set the remote address this pool will connect to
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
         * @param minConnections
         * @return this
         * @see #DEFAULT_MIN_CONNECTIONS
         */
        public Builder withMinConnections(int minConnections)
        {
            this.minConnections = minConnections;
            return this;
        }
        
        /**
         * Set the maximum number of connections allowed by the pool. A value
         * of 0 sets this to unlimited. 
         * @param maxConnections
         * @return this
         * @see #DEFAULT_MAX_CONNECTIONS
         */
        public Builder withMaxConnections(int maxConnections)
        {
            this.maxConnections = maxConnections;
            return this;
        }
        
        /**
         * Set the idle timeout used to reap inactive connections.
         * Any connection that has been idle for this amount of time
         * becomes eligible to be closed and discarded unless {@code minConnections}
         * has been set via {@link #withMinConnections(int) }
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
         * Set the read timeout used when reading from a socket
         * @param readTimeoutInMillis
         * @return this
         * @see #DEFAULT_READ_TIMEOUT
         */
        public Builder withReadTimeout(int readTimeoutInMillis)
        {
            this.readTimeout = readTimeoutInMillis;
            return this;
        }
        
        /**
         * Set the timeout to be used when acquiring a connection from the
         * pool. This only comes into play when all connections are in use
         * and you are waiting for one to be returned by another thread. 
         * @param permitTimeoutInMillis
         * @return this
         * @see #DEFAULT_PERMIT_TIMEOUT
         */
        public Builder withPermitTimeout(int permitTimeoutInMillis)
        {
            this.permitTimeout = permitTimeoutInMillis;
            return this;
        }
        
        /**
         * The Netty bootstrap to be used with this pool. If not provided one 
         * will be created with its own {@code NioEventLoopGroup}.
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
         * @param executor
         * @return this
         */
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }
        
        public ConnectionPool build() throws UnknownHostException
        {
            
            
            return new ConnectionPool(this);
        }
        
    }
    
}
