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

import com.basho.riak.client.util.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakNode implements ChannelFutureListener, RiakResponseListener, PoolStateListener
{
    public enum State
    {
        CREATED, RUNNING, HEALTH_CHECKING, SHUTTING_DOWN, SHUTDOWN;
    }
    
    private final Logger logger = LoggerFactory.getLogger(RiakNode.class);
    private final ConnectionPool connectionPool;
    private final List<NodeStateListener> stateListeners = 
        Collections.synchronizedList(new LinkedList<NodeStateListener>());
    
    private volatile ScheduledExecutorService executor; 
    private volatile boolean ownsExecutor;
    private volatile Bootstrap bootstrap;
    private volatile boolean ownsBootstrap;
    private volatile State state;
    
    private Map<Channel, FutureOperation> inProgressMap = 
        new ConcurrentHashMap<Channel, FutureOperation>();
    private volatile int readTimeoutInMillis;
    
    // TODO: Harden to prevent operation from being executed > 1 times?
    // TODO: how many channels on one event loop? 
    private RiakNode(Builder builder) throws UnknownHostException
    {
        this.readTimeoutInMillis = builder.readTimeout;
        this.executor = builder.executor;
        
        if (builder.bootstrap != null)
        {
            this.bootstrap = builder.bootstrap.clone();
        }
        
        this.connectionPool = builder.poolBuilder.build();
        this.state = State.CREATED;
    }
        
    private void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; remote: {} required: {} current: {} ",
                         connectionPool.getRemoteAddress(), Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: " 
                + Arrays.toString(allowedStates) 
                + " current: " + state );
        }
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
        
        connectionPool.addStateListener(this)
                      .setBootstrap(bootstrap)
                      .setExecutor(executor)
                      .start();
        
        state = State.RUNNING;
        notifyStateListeners();
        return this;
    }
    
    public synchronized void shutdown()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        logger.info("Riak node shutting down; {}", connectionPool.getRemoteAddress());
        connectionPool.shutdown();
        // Notifications from the pool change our state. 
    }
    
    /**
     * Sets the Netty {@link Bootstrap} for this Node's connection pool(s).
     * {@link Bootstrap#clone()} is called to clone the bootstrap.
     * 
     * @param bootstrap
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException if the node has already been started.
     * @see Builder#withBootstrap(io.netty.bootstrap.Bootstrap) 
     */
    public void setBootstrap(Bootstrap bootstrap)
    {
        stateCheck(State.CREATED);
        if (this.bootstrap != null)
        {
            throw new IllegalArgumentException("Bootstrap already set");
        }
        
        this.bootstrap = bootstrap.clone();
    }
    
    /**
     * Sets the {@link ScheduledExecutorService} for this Node and its pool(s).
     * 
     * @param executor
     * @throws IllegalArgumentException if it was already set via the builder.
     * @throws IllegalStateException if the node has already been started.
     * @see Builder#withExecutor(java.util.concurrent.ScheduledExecutorService) 
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        stateCheck(State.CREATED);
        if (this.executor != null)
        {
            throw new IllegalArgumentException("Executor already set");
        }
        this.executor = executor;
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
        synchronized(stateListeners)
        {
            for (Iterator<NodeStateListener> it = stateListeners.iterator(); it.hasNext();)
            {
                NodeStateListener listener = it.next();
                listener.nodeStateChanged(this, state);
            }
        }
    }
    
    // TODO: Streaming also - idea: BlockingDeque extension with listener
    
    /**
     * Submits the operation to be executed on this node.  
     * @param operation The operation to perform
     * @return {@code true} if this operation was accepted, {@code false} if there
     * were no available connections. 
     * @throws IllegalStateException if this node is not in the {@code RUNNING} or {@code HEALTH_CHECKING} state
     * @throws IllegalArgumentException if the protocol required for the operation is not supported by this node
     */
    public boolean execute(FutureOperation operation) 
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        
        operation.setLastNode(this);
        Channel channel = connectionPool.getConnection();
        if (channel != null)
        {
            // Add a timeout handler to the pipeline if the readTIeout is set
            if (readTimeoutInMillis > 0)
            {
                channel.pipeline()
                    .addAfter(Constants.OPERATION_ENCODER, Constants.TIMEOUT_HANDLER, 
                             new ReadTimeoutHandler(readTimeoutInMillis, TimeUnit.MILLISECONDS));
            }
            
            //TODO: figure out a cleaner way to do this? 
            channel.pipeline().get(Constants.RESPONSE_HANDLER_CLASS).setListener(this);
            inProgressMap.put(channel, operation);
            ChannelFuture writeFuture = channel.writeAndFlush(operation); 
            writeFuture.addListener(this);
            logger.debug("Operation executed on node {} {}", connectionPool.getRemoteAddress(), channel.remoteAddress());
            return true;
        }
        else
        {
            return false;
        }
    }
    
    @Override
    public synchronized void poolStateChanged(ConnectionPool pool, ConnectionPool.State newState)
    {
        switch (newState)
        {
            case HEALTH_CHECKING:
                this.state = State.HEALTH_CHECKING;
                notifyStateListeners();
                logger.info("RiakNode offline, health checking; {}", connectionPool.getRemoteAddress());
                break;
            case RUNNING:
                this.state = State.RUNNING;
                notifyStateListeners();
                logger.info("RiakNode running; {}", connectionPool.getRemoteAddress());
                break;
            case SHUTTING_DOWN:
                if (this.state == State.RUNNING ||  this.state == State.HEALTH_CHECKING)
                {
                    this.state = State.SHUTTING_DOWN;
                    notifyStateListeners();
                    logger.info("RiakNode shutting down due to pool shutdown; {}", connectionPool.getRemoteAddress());
                }
                break;
            case SHUTDOWN:
                this.state = State.SHUTDOWN;
                notifyStateListeners();
                if (ownsExecutor)
                {
                    executor.shutdown();
                }
                if (ownsBootstrap)
                {
                    bootstrap.group().shutdownGracefully();
                }
                logger.info("RiakNode shut down due to pool shutdown; {}", connectionPool.getRemoteAddress());
                break;
            default:
                break;
        }
    }

    @Override
    public void onSuccess(Channel channel, RiakMessage response)
    {
        logger.debug("Operation onSuccess() channel: id:{} {}", channel.hashCode(), channel.remoteAddress());
        if (readTimeoutInMillis > 0)
        {
            channel.pipeline().remove(Constants.TIMEOUT_HANDLER);
        }
        FutureOperation inProgress = inProgressMap.remove(channel);
        connectionPool.returnConnection(channel);
        inProgress.setResponse(response);
    }

    @Override
    public void onException(Channel channel, Throwable t)
    {
        logger.debug("Operation onException() channel: id:{} {} {}", 
                     channel.hashCode(), channel.remoteAddress(), t);
        if (readTimeoutInMillis > 0)
        {
            channel.pipeline().remove(Constants.TIMEOUT_HANDLER);
        }
        FutureOperation inProgress = inProgressMap.remove(channel);
        // There are fail cases where multiple exceptions are thrown from 
        // the pipeline. In that case we'll get an exception from the 
        // handler but will not have an entry in inProgress because it's
        // already been handled.
        if (inProgress != null)
        {
            connectionPool.returnConnection(channel);
            inProgress.setException(t);
        }
    }
    
    // The only Netty future we are listening to is the WriteFuture
    @Override
    public void operationComplete(ChannelFuture future) throws Exception
    {
        // See how the write worked out ...
        if (!future.isSuccess())
        {
            FutureOperation inProgress = inProgressMap.remove(future.channel());
            logger.info("Write failed on node {}", connectionPool.getRemoteAddress());
            connectionPool.returnConnection(future.channel());
            inProgress.setException(future.cause());
        }
    }
    
    
    
    /**
     * Returns the {@code remoteAddress} for this RiakNode
     * @return The IP address or FQDN as a {@code String}
     */
    public String getRemoteAddress()
    {
        return connectionPool.getRemoteAddress();
    }
    
    /**
     * Returns the current state of this node.
     * @return The state
     */
    public State getNodeState()
    {
        return this.state;
    }
    
    /**
     * Returns the read timeout in milliseconds for connections in this pool
     * @return the readTimeout
     * @see Builder#withReadTimeout(int) 
     */
    public int getReadTimeout()
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        return readTimeoutInMillis;
    }

    /**
     * Sets the read timeout for connections in this pool
     * @param readTimeoutInMillis the readTimeout to set
     * @see Builder#withReadTimeout(int) 
     */
    public void setReadTimeout(int readTimeoutInMillis)
    {
        stateCheck(State.CREATED, State.RUNNING, State.HEALTH_CHECKING);
        this.readTimeoutInMillis = readTimeoutInMillis;
    }
    
    /**
     * Builder used to construct a RiakNode.
     * Most of these options are passed directly to a {@link ConnectionPool.Builder}. 
     * The default values are documented in that class. 
     * 
     */
    public static class Builder
    {
        /**
         * The default TCP read timeout in milliseconds if not specified: {@value #DEFAULT_TCP_READ_TIMEOUT}
         * A value of {@code 0} means to wait indefinitely 
         * @see #withReadTimeout(int) 
         */
        public final static int DEFAULT_TCP_READ_TIMEOUT = 0;
        
        private final ConnectionPool.Builder poolBuilder = new ConnectionPool.Builder();
        
        private ScheduledExecutorService executor;
        private Bootstrap bootstrap;
        private int readTimeout = DEFAULT_TCP_READ_TIMEOUT;
        
        /**
         * Default constructor. Returns a new builder for a RiakNode with 
         * default values set.
         */
        public Builder()
        {
            
        }
        
        /**
         * Sets the remote address for this RiakNode. 
         * @param remoteAddress Can either be a FQDN or IP address
         * @return this
         */
        public Builder withRemoteAddress(String remoteAddress)
        {
            poolBuilder.withRemoteAddress(remoteAddress);
            return this;
        }
        
       /**
         * Specifies the remote port for this RiakNode.
         * @param p - the protocol
         * @param port - the port
         * @return this
         */
        public Builder withRemotePort(int port)
        {
            poolBuilder.withRemotePort(port);
            return this;
        }
        
        /**
         * Specifies the minimum number for connections to be maintained
         * @param p 
         * @param minConnections
         * @return this
         * @see ConnectionPool.Builder#withMinConnections(int) 
         */
        public Builder withMinConnections(int minConnections)
        {
            poolBuilder.withMinConnections(minConnections);
            return this;
        }
        
        /**
         * Specifies the maximum number of connections allowed.
         * @param p
         * @param maxConnections
         * @return this
         * @see ConnectionPool.Builder#withMaxConnections(int) 
         */
        public Builder withMaxConnections(int maxConnections)
        {
            poolBuilder.withMaxConnections(maxConnections);
            return this;
        }
        
        /**
         * Specifies the idle timeout for connections.
         * @param p
         * @param idleTimeoutInMillis
         * @return this
         * @see ConnectionPool.Builder#withIdleTimeout(int) 
         */
        public Builder withIdleTimeout(int idleTimeoutInMillis)
        {
            poolBuilder.withIdleTimeout(idleTimeoutInMillis);
            return this;
        }
        
        /**
         * Specifies the connection timeout when creating new connections.
         * @param p
         * @param connectionTimeoutInMillis
         * @return this
         * @see ConnectionPool.Builder#withConnectionTimeout(int) 
         */
        public Builder withConnectionTimeout(int connectionTimeoutInMillis)
        {
            poolBuilder.withConnectionTimeout(connectionTimeoutInMillis);
            return this;
        }
        
        //TODO: Now that we have operation timeouts, do we really want to expose the TCP read timeout?
        /**
         * Specifies the TCP read timeout when waiting for a reply from Riak.
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
         * Provides an executor for this node to use for internal maintenance tasks.
         * This same executor will be used for this node's connection pool
         * @param executor
         * @return this
         * @see ConnectionPool.Builder#withExecutor(java.util.concurrent.ScheduledExecutorService) 
         */
        public Builder withExecutor(ScheduledExecutorService executor)
        {
            this.executor = executor;
            return this;
        }
        
        /**
         * Provides a Netty Bootstrap for this node to use. 
         * This same Bootstrap will be used for this node's underlying connection pool(s)
         * @param bootstrap
         * @return this
         * @see ConnectionPool.Builder#withBootstrap(io.netty.bootstrap.Bootstrap) 
         */
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }
        
        /**
         * The Netty {@code ChannelInitializer} to use with the connection pool.
         * @param initializer the initializer
         * @return this
         * @see ConnectionPool.Builder#withChannelInitializer(io.netty.channel.ChannelInitializer) 
         */
        public Builder withChannelInitializer(ChannelInitializer<SocketChannel> initializer)
        {
            poolBuilder.withChannelInitializer(initializer);
            return this;
        }
        
        
        
        /**
         * Builds a RiakNode.
         * If a Netty {@code Bootstrap} and/or a {@code ScheduledExecutorService} has not been provided they
         * will be created. 
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
         * @param builder a configured builder
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
