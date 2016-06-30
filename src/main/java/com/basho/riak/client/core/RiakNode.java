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

import com.basho.riak.client.core.netty.*;
import com.basho.riak.client.core.util.Constants;
import com.basho.riak.client.core.util.HostAndPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.Security;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Brian Roach <roach at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @author Alex Moore <amoore at basho dot com>
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
    private final ConcurrentLinkedQueue<ChannelWithIdleTime> recentlyClosed =
        new ConcurrentLinkedQueue<ChannelWithIdleTime>();
    private final List<NodeStateListener> stateListeners =
        Collections.synchronizedList(new LinkedList<NodeStateListener>());
    private final Map<Channel, FutureOperation> inProgressMap =
        new ConcurrentHashMap<Channel, FutureOperation>();

    private final Sync permits;
    private final String remoteAddress;
    private final int port;
    private final String username;
    private final String password;
    private final KeyStore trustStore;
    private final KeyStore keyStore;
    private final String keyPassword;
    private final AtomicLong consecutiveFailedOperations = new AtomicLong(0);
    private final AtomicLong consecutiveFailedConnectionAttempts = new AtomicLong(0);

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
    private volatile boolean blockOnMaxConnections;

    private HealthCheckFactory healthCheckFactory;

    private final ChannelFutureListener writeListener =
        new ChannelFutureListener()
        {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
                // If there's a write failure, we yank the operation, close
                // the channel, and set the exception. Returning the closed
                // channel to the pool discards it and records a disconnect
                // for the health check.
                if (!future.isSuccess())
                {
                    logger.error("Write failed on RiakNode {}:{} id: {}; cause: {}",
                                remoteAddress, port, future.channel().hashCode(),
                                future.cause());
                    FutureOperation inProgress = inProgressMap.remove(future.channel());
                    if (inProgress != null)
                    {
                        future.channel().close();
                        returnConnection(future.channel()); // to release permit
                        recentlyClosed.add(new ChannelWithIdleTime(future.channel()));
                        inProgress.setException(future.cause());
                    }
                }
                else
                {
                    // On a successful write we add the in-progress close listener
                    // and let it handle a disco during an op.
                    future.channel().closeFuture().addListener(inProgressCloseListener);
                }
            }

        };

    private final ChannelFutureListener inAvailableCloseListener =
        new ChannelFutureListener()
        {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
                // Rather than having to do an O(n) search here, we just leave
                // the channel in available. Because it's closed it'll be discarded
                // the next time it's pulled from the pool.
                // We record the disco for the health check.
                recentlyClosed.add(new ChannelWithIdleTime(future.channel()));
                logger.error("inAvailable channel closed; id:{} {}:{}",
                             future.channel().hashCode(), remoteAddress, port);
            }
        };

    private final ChannelFutureListener inProgressCloseListener =
        new ChannelFutureListener()
        {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
                FutureOperation inProgress = inProgressMap.remove(future.channel());
                logger.error("Channel closed while operation in progress; id:{} {}:{}",
                             future.channel().hashCode(), remoteAddress, port);
                if (inProgress != null)
                {
                    returnConnection(future.channel()); // to release permit
                    recentlyClosed.add(new ChannelWithIdleTime(future.channel()));

                    // Netty seems to not bother telling you *why* the connection
                    // was closed.
                    if (future.cause() != null)
                    {
                        inProgress.setException(future.cause());
                    }
                    else
                    {
                        inProgress.setException(new Exception("Connection closed unexpectantly"));
                    }
                }

            }
        };


    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private RiakNode(Builder builder)
    {
        this.executor = builder.executor;
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeoutInNanos = TimeUnit.NANOSECONDS.convert(builder.idleTimeout, TimeUnit.MILLISECONDS);
        this.minConnections = builder.minConnections;
        this.port = builder.port;
        this.remoteAddress = builder.remoteAddress;
        this.blockOnMaxConnections = builder.blockOnMaxConnections;
        this.username = builder.username;
        this.password = builder.password;
        this.trustStore = builder.trustStore;
        this.keyStore = builder.keyStore;
        this.keyPassword = builder.keyPassword;
        this.healthCheckFactory = builder.healthCheckFactory;

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

        checkNetworkAddressCacheSettings();

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

    public synchronized RiakNode start() throws UnknownHostException
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

        bootstrap.handler(new RiakChannelInitializer(this));

        refreshBootstrapRemoteAddress();

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
                    channel = doGetConnection(false);
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
                c.closeFuture().addListener(inAvailableCloseListener);
            }
        }

        idleReaperFuture = executor.scheduleWithFixedDelay(new IdleReaper(), 1, 5, TimeUnit.SECONDS);
        healthMonitorFuture = executor.scheduleWithFixedDelay(new HealthMonitorTask(), 1000, 1000, TimeUnit.MILLISECONDS);

        state = State.RUNNING;
        logger.info("RiakNode started; {}:{}", remoteAddress, port);
        notifyStateListeners();
        return this;
    }

    private void refreshBootstrapRemoteAddress() throws UnknownHostException
    {
        // Refresh the address, hope their DNS TTL settings allow this.
        InetSocketAddress socketAddress = new InetSocketAddress(remoteAddress, port);

        if (socketAddress.isUnresolved())
        {
            throw new UnknownHostException("RiakNode:start - Failed resolving host " + remoteAddress);
        }

        bootstrap.remoteAddress(socketAddress);
    }

    public synchronized Future<Boolean> shutdown()
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
     * Set whether to block when all connections are in use.
     * @param block true to block.
     * @see Builder#withBlockOnMaxConnections(boolean)
     */
    public void setBlockOnMaxConnections(boolean block)
    {
        this.blockOnMaxConnections = block;
    }

    /**
     * Returns if this node is set to block when all connections are in use.
     * @return true if set to block, false otherwise.
     * @see Builder#withBlockOnMaxConnections(boolean)
     */
    public boolean getBlockOnMaxConnections()
    {
        return blockOnMaxConnections;
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
            for (NodeStateListener listener : stateListeners)
            {
                listener.nodeStateChanged(this, state);
            }
        }
    }

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
            inProgressMap.put(channel, operation);
            ChannelFuture writeFuture = channel.writeAndFlush(operation);
            writeFuture.addListener(writeListener);
            logger.debug("Operation being executed on RiakNode {}:{}", remoteAddress, port);
            return true;
        }
        else
        {
            logger.debug("Operation not being executed Riaknode {}:{}; no connections available",
                            remoteAddress, port);
            return false;
        }
    }

    // ConnectionPool Stuff

    /**
     * Get a Netty channel from the pool.
     * <p>
     * The first thing this method does is attempt to acquire a permit from the
     * Semaphore that controls the pool's behavior. Depending on whether
     * {@code blockOnMaxConnections} is set, this will either block until one
     * becomes available or return null.
     * </p>
     * <p>
     * Once a permit has been acquired, a channel from the pool or a newly
     * created one will be returned. If an attempt to create a new connection
     * fails, null will then be returned.
     * </p>
     * @return a connected channel or {@code null}
     * @see Builder#withBlockOnMaxConnections(boolean)
     */
    private Channel getConnection()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        boolean acquired = false;
        if (blockOnMaxConnections)
        {
            try
            {
                logger.debug("Attempting to acquire channel permit");

                if (!permits.tryAcquire())
                {
                    logger.info("All connections in use for {}; had to wait for one.",
                                remoteAddress);
                    permits.acquire();
                }
                acquired = true;
            }
            catch (InterruptedException ex)
            {
                // no-op, don't care
            }
        }
        else
        {
            logger.debug("Attempting to acquire channel permit");
            acquired = permits.tryAcquire();
        }

        Channel channel = null;
        if (acquired)
        {
            try
            {
                channel = doGetConnection(true);
                channel.closeFuture().removeListener(inAvailableCloseListener);
            }
            catch (ConnectionFailedException ex)
            {
                permits.release();
            }
            catch (UnknownHostException ex)
            {
                permits.release();
                logger.error("Unknown host encountered while trying to open connection; {}", ex);
            }
        }
        return channel;
    }

    private Channel doGetConnection(boolean forceAddressRefresh) throws ConnectionFailedException, UnknownHostException
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

        if(forceAddressRefresh)
        {
            refreshBootstrapRemoteAddress();
        }

        ChannelFuture f = bootstrap.connect();

        try
        {
            f.await();
        }
        catch (InterruptedException ex)
        {
            logger.error("Thread interrupted waiting for new connection to be made; {}",
                remoteAddress);
            Thread.currentThread().interrupt();
            throw new ConnectionFailedException(ex);
        }

        if (!f.isSuccess())
        {
            logger.error("Connection attempt failed: {}:{}; {}",
                remoteAddress, port, f.cause());
            consecutiveFailedConnectionAttempts.incrementAndGet();
            throw new ConnectionFailedException(f.cause());
        }

        consecutiveFailedConnectionAttempts.set(0);
        Channel c = f.channel();

        if (trustStore != null)
        {
            setupTLSAndAuthenticate(c);
        }

        return c;

    }

    private void setupTLSAndAuthenticate(Channel c) throws ConnectionFailedException
    {
        SSLContext context;
        try
        {
            context = SSLContext.getInstance("TLS");
            TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
                if(keyStore!=null)
            {
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, keyPassword==null?"".toCharArray():keyPassword.toCharArray());
                context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            }
            else
            {
                context.init(null, tmf.getTrustManagers(), null);
            }

        }
        catch (Exception ex)
        {
            c.close();
            logger.error("Failure configuring SSL; {}:{} {}", remoteAddress, port, ex);
            throw new ConnectionFailedException(ex);
        }

        SSLEngine engine = context.createSSLEngine();

        Set<String> protocols = new HashSet<String>(Arrays.asList(engine.getSupportedProtocols()));

        if (protocols.contains("TLSv1.2"))
        {
            engine.setEnabledProtocols(new String[] {"TLSv1.2"});
            logger.debug("Using TLSv1.2");
        }
        else if (protocols.contains("TLSv1.1"))
        {
            engine.setEnabledProtocols(new String[] {"TLSv1.1"});
            logger.debug("Using TLSv1.1");
        }

        engine.setUseClientMode(true);
        RiakSecurityDecoder decoder = new RiakSecurityDecoder(engine, username, password);
        c.pipeline().addFirst(decoder);

        try
        {
            DefaultPromise<Void> promise = decoder.getPromise();
                logger.debug("Waiting on SSL Promise");
            promise.await();

            if (promise.isSuccess())
            {
                logger.debug("Auth succeeded; {}:{}", remoteAddress, port);
            }
            else
            {
                c.close();
                logger.error("Failure during Auth; {}:{} {}",remoteAddress, port, promise.cause());
                throw new ConnectionFailedException(promise.cause());
            }


        }
        catch (InterruptedException e)
        {
            c.close();
            logger.error("Thread interrupted during Auth; {}:{}",
                remoteAddress, port);
            Thread.currentThread().interrupt();
            throw new ConnectionFailedException(e);
        }
    }

    /**
     * Return a Netty channel.
     *
     * @param c The Netty channel to return to the pool
     */
    private void returnConnection(Channel c)
    {
        switch (state)
        {
            case SHUTTING_DOWN:
            case SHUTDOWN:
                closeConnection(c);
                break;
            case RUNNING:
            case HEALTH_CHECKING:
            default:
                if (inProgressMap.containsKey(c))
                {
                    logger.error("Channel returned to pool while still in use. id: {}",
                        c.hashCode());
                }
                else
                {
                    if (c.isOpen())
                    {
                        logger.debug("Channel id:{} returned to pool", c.hashCode());
                        c.closeFuture().removeListener(inProgressCloseListener);
                        c.closeFuture().addListener(inAvailableCloseListener);
                        available.offerFirst(new ChannelWithIdleTime(c));
                    }
                    else
                    {
                        logger.debug("Closed channel id:{} returned to pool; discarding", c.hashCode());
                    }
                    logger.debug("Released pool permit");
                    permits.release();
                }
            }
    }

    private void closeConnection(Channel c)
    {
        // If we are explicitly closing the connection we don't want to hear
        // about it.
        c.closeFuture().removeListener(inProgressCloseListener);
        c.closeFuture().removeListener(inAvailableCloseListener);
        c.close();
    }


    // End ConnectionPool stuff

    @Override
    public void onSuccess(Channel channel, final RiakMessage response)
    {
        logger.debug("Operation onSuccess() channel: id:{} {}:{}", channel.hashCode(),
            remoteAddress, port);
        consecutiveFailedOperations.set(0);
        final FutureOperation inProgress = inProgressMap.get(channel);

        // Especially with a streaming op, the close listener may trigger causing
        // a race. This check guards that.
        if (inProgress != null)
        {
            inProgress.setResponse(response);

            if (inProgress.isDone())
            {
                try
                {
                    inProgressMap.remove(channel);
                    returnConnection(channel); // return permit
                }
                finally
                {
                    inProgress.setComplete();
                }
            }
        }
    }

    @Override
    public void onRiakErrorResponse(Channel channel, RiakResponseException ex)
    {
        logger.debug("Riak replied with error; {}:{}", ex.getCode(), ex.getMessage());
        final FutureOperation inProgress = inProgressMap.remove(channel);
        consecutiveFailedOperations.incrementAndGet();
        if (inProgress != null)
        {
            returnConnection(channel); // release permit
            inProgress.setException(ex);
        }
    }

    @Override
    public void onException(Channel channel, final Throwable t)
    {
        logger.error("Operation onException() channel: id:{} {}:{} {}",
            channel.hashCode(), remoteAddress, port, t);

        final FutureOperation inProgress = inProgressMap.remove(channel);
        // There are fail cases where multiple exceptions are thrown from
        // the pipeline. In that case we'll get an exception from the
        // handler but will not have an entry in inProgress because it's
        // already been handled.
        if (inProgress != null)
        {
            returnConnection(channel); // release permit
            inProgress.setException(t);
        }
    }

    private void checkNetworkAddressCacheSettings()
    {
        final String property = Security.getProperty("networkaddress.cache.ttl");

        final boolean usingSecurityMgr = System.getSecurityManager() != null;
        final boolean propertyUndefined = property == null;
        boolean logWarning = false;

        if(propertyUndefined && usingSecurityMgr)
        {
            logWarning = true;
        }
        else if(!propertyUndefined)
        {
            final int cacheTTL = Integer.parseInt(property);
            logWarning = (cacheTTL == -1);
        }

        if (logWarning)
        {
            logger.warn(
                    "The Java Security \"networkaddress.cache.ttl\" property may be set to cache DNS lookups forever. " +
                    "Using domain names for Riak nodes or an intermediate load balancer could result in stale IP " +
                    "addresses being used for new connections, causing connection errors. " +
                    "If you use domain names for Riak nodes, please set this property to a value greater than zero.");
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
        int currentNum = inProgressMap.size() + available.size();
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

    // TODO: Revisit if we ever support multiple protocols or change protocols.
    // As-is the parameters work well for protocol buffers.
    /**
     * Task to see if a criteria should trigger a health check.
     * <p>
     * We keep a list of connections that triggered the closeListener. We also
     * track the number of consecutive failed connection attempts, and the
     * number of consecutive error responses from Riak.
     * </p>
     */
    private class HealthMonitorTask implements Runnable
    {
        @Override
        public void run()
        {
            // Purge recentlyClosed past a certain age
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

            // If we more than 5 recently closed in 3 seconds, more than 1 consecutive failed
            // connection attempts, more than 5 consecutive error responses from Riak,
            // or we failed a healthcheck
            if ((state == State.RUNNING &&
                    (recentlyClosed.size() > 5 ||
                     consecutiveFailedConnectionAttempts.get() > 1 ||
                     consecutiveFailedOperations.get() > 5)
                 ) ||
                state == State.HEALTH_CHECKING)
            {
                checkHealth();
            }
        }
    }

    private void checkHealth()
    {
        try
        {
            HealthCheckDecoder healthCheck = healthCheckFactory.makeDecoder();
            RiakFuture<RiakMessage, Void> future = healthCheck.getFuture();
            // See: doGetConnection() - this will purge closed
            // connections from the available queue and either
            // return/create a new one (meaning the node is up) or throw
            // an exception if a connection can't be made.
            Channel c = doGetConnection(true);
            logger.debug("Healthcheck channel: {} isOpen: {} handlers:{}", c.hashCode(), c.isOpen(), c.pipeline().names());



            // If the channel closes between when we got it and now, the pipeline is emptied. If the handlers
            // aren't there we fail the healthcheck

            try
            {
                if (c.pipeline().names().contains(Constants.SSL_HANDLER))
                {
                    c.pipeline().addAfter(Constants.SSL_HANDLER, Constants.HEALTHCHECK_CODEC, healthCheck);
                }
                else
                {
                    c.pipeline().addBefore(Constants.MESSAGE_CODEC, Constants.HEALTHCHECK_CODEC, healthCheck);
                }

                logger.debug("healthCheck added to pipeline.");

                //future.await(5, TimeUnit.SECONDS);
                future.await();
                if (future.isSuccess())
                {
                    healthCheckSucceeded();
                }
                else
                {
                    healthCheckFailed(future.cause());
                }

            }
            catch (InterruptedException ex)
            {
                logger.error("Thread interrupted performing healthcheck.");
            }
            catch (NoSuchElementException e)
            {
                healthCheckFailed(new IOException("Channel closed during health check"));
            }
            finally
            {
                closeConnection(c);
            }
        }
        catch (ConnectionFailedException ex)
        {
            healthCheckFailed(ex);
        }
        catch (UnknownHostException ex)
        {
            healthCheckFailed(ex);
        }
        catch (IllegalStateException ex)
        {
            // no-op; there's a race condition where the bootstrap is shutting down
            // right when a healthcheck occurs and netty will throw this
            logger.debug("Illegal state exception during healthcheck.");
            logger.debug("Stack: {}", ex);
        }
        catch (RuntimeException ex)
        {
            logger.error("Runtime exception during healthcheck: {}", ex);
        }
    }

    private void healthCheckFailed(Throwable cause)
    {
        if (state == State.RUNNING)
        {
            logger.error("RiakNode failed healthcheck operation; health checking; {}:{} {}",
                remoteAddress, port, cause);
            state = State.HEALTH_CHECKING;
            notifyStateListeners();
        }
        else
        {
            logger.error("RiakNode failed healthcheck operation; {}:{} {}",
                remoteAddress, port, cause);
        }
    }

    private void healthCheckSucceeded()
    {
        if (state == State.HEALTH_CHECKING)
        {
            logger.info("RiakNode recovered; {}:{}", remoteAddress, port);
            state = State.RUNNING;
            notifyStateListeners();
        }
    }

    private class ShutdownTask implements Runnable
    {
        @Override
        public void run()
        {
            if (inProgressMap.isEmpty())
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
                shutdownLatch.countDown();
            }
        }
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
         * @see #withRemotePort(int)
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
         * The default HealthCheckFactory.
         * <p>
         * By default this is the {@link PingHealthCheck}
         * </p>
         * @see HealthCheckFactory
         * @see HealthCheckDecoder
         */
        public final static HealthCheckFactory DEFAULT_HEALTHCHECK_FACTORY = new PingHealthCheck();

        private int port = DEFAULT_REMOTE_PORT;
        private String remoteAddress = DEFAULT_REMOTE_ADDRESS;
        private int minConnections = DEFAULT_MIN_CONNECTIONS;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private HealthCheckFactory healthCheckFactory = DEFAULT_HEALTHCHECK_FACTORY;
        private Bootstrap bootstrap;
        private ScheduledExecutorService executor;
        private boolean blockOnMaxConnections;
        private String username;
        private String password;
        private KeyStore trustStore;
        private KeyStore keyStore;
        private String keyPassword;


        /**
         * Default constructor. Returns a new builder for a RiakNode with
         * default values set.
         */
        public Builder()
        {

        }

        /**
         * Sets the remote host and port for this RiakNode.
         *
         * @param hostAndPOrt
         * @return this
         */
        public Builder withRemoteHost(HostAndPort hostAndPOrt)
        {
            this.withRemoteAddress(hostAndPOrt.getHost());
            this.withRemotePort(hostAndPOrt.getPort());
            return this;
        }

        /**
         * Sets the remote address for this RiakNode.
         *
         * @param remoteAddress Can either be a FQDN or IP address. Since 2.0.3 it may contain port delimited by ':'.
         * @return this
         * @see #DEFAULT_REMOTE_ADDRESS
         */
        public Builder withRemoteAddress(String remoteAddress)
        {
            withRemoteAddress(HostAndPort.fromString(remoteAddress, this.port));
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
         * Specifies the remote host and remote port for this RiakNode.
         *
         * @param hp - host and port
         * @return this
         * @see #DEFAULT_REMOTE_PORT
         *
         * @since 2.0.3
         * @see HostAndPort
         */
        public Builder withRemoteAddress(HostAndPort hp)
        {
            this.port = hp.getPortOrDefault(DEFAULT_REMOTE_PORT);
            this.remoteAddress = hp.getHost();
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
            if (maxConnections == DEFAULT_MAX_CONNECTIONS ||  maxConnections >= minConnections)
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
         * Set whether to block if all connections are in use.
         * <p>
         * If a maximum number of connections is specified and all those
         * connections are in use, the default
         * behavior when an operation is submitted to a node is to
         * fail-fast and return. Setting this to true will cause the
         * call to block (fair-scheduled, FIFO) until a connection becomes
         * available.
         * </p>
         * @param block whether to block when an operation is submitted and
         * all connections are in use.
         * @return this
         */
        public Builder withBlockOnMaxConnections(boolean block)
        {
            this.blockOnMaxConnections = block;
            return this;
        }

        /**
         * Set the credentials for Riak security and authentication.
         * <p>
         * Riak supports authentication and authorization features.
         * These credentials will be used for all connections.
         * </p>
         * <p>
         * Note this requires Riak to have been configured with security enabled.
         * </p>
         *
         * @param username the riak user name.
         * @param password the password for this user.
         * @param trustStore A Java KeyStore loaded with the CA certificate required for TLS/SSL
         * @return a reference to this object.
         */
        public Builder withAuth(String username, String password, KeyStore trustStore)
        {
            this.username = username;
            this.password = password;
            this.trustStore = trustStore;
            return this;
        }

        /**
         * Set the credentials for Riak security and authentication.
         * <p>
         * Riak supports authentication and authorization features.
         * These credentials will be used for all connections.
         * </p>
         * <p>
         * Note this requires Riak to have been configured with security enabled.
         * </p>
         *
         * @param username the riak user name.
         * @param password the password for this user.
         * @param trustStore A Java KeyStore loaded with the CA certificate required for TLS/SSL
         * @param keyStore A Java KeyStore loaded with User's Private certificate required for TLS/SSL
         * @param keyPassword the password for User's Private certificate.
         * @return a reference to this object.
         */
        public Builder withAuth(String username, String password, KeyStore trustStore, KeyStore keyStore, String keyPassword)
        {
            this.username = username;
            this.password = password;
            this.trustStore = trustStore;
            this.keyStore = keyStore;
            this.keyPassword = keyPassword;
            return this;
        }

        /**
         * Set the HealthCheckFactory used to determine if this RiakNode is healthy.
         * <p>
         * If not set the {@link PingHealthCheck} is used.
         * </p>
         * @param factory a HealthCheckFactory instance that produces HealthCheckDecoders
         * @return a reference to this object.
         * @see HealthCheckDecoder
         */
        public Builder withHealthCheck(HealthCheckFactory factory)
        {
            this.healthCheckFactory = factory;
            return this;
        }

        /**
         * Builds a RiakNode.
         * If a Netty {@code Bootstrap} and/or a {@code ScheduledExecutorService} has not been provided they
         * will be created.
         *
         * @return a new Riaknode
         */
        public RiakNode build()
        {
            return new RiakNode(this);
        }

        /**
         * Build a set of RiakNodes.
         * The provided builder will be used to construct a set of RiakNodes
         * using the supplied addresses.
         *
         * @param builder         a configured builder, used for common properties among the nodes
         * @param remoteAddresses a list of IP addresses or FQDN.
         *                        Since 2.0.3 each of list item is treated as a comma separated list
         *                        of FQDN or IP addresses with the optional remote port delimited by ':'.
         * @return a list of constructed RiakNodes
         */
        public static List<RiakNode> buildNodes(Builder builder, List<String> remoteAddresses)
        {
            final Set<HostAndPort> hps = new HashSet<HostAndPort>();
            for (String remoteAddress: remoteAddresses)
            {
                hps.addAll( HostAndPort.hostsFromString(remoteAddress, builder.port) );
            }

            final List<RiakNode> nodes = new ArrayList<RiakNode>(hps.size());
            for (HostAndPort hp : hps)
            {
                builder.withRemoteAddress(hp);
                nodes.add(builder.build());
            }
            return nodes;
        }

        /**
         * Build a set of RiakNodes.
         * The provided builder will be used to construct a set of RiakNodes using the supplied addresses.
         *
         * @see #buildNodes(Builder, List)
         * @since 2.0.3
         */
        public static List<RiakNode> buildNodes(Builder builder, String... remoteAddresses)
                throws UnknownHostException
        {
            return buildNodes(builder, Arrays.asList(remoteAddresses));
        }
    }
}
