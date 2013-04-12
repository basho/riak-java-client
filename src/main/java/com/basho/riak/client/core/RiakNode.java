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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    private final EnumMap<Protocol, ConnectionPool> connectionPoolMap =
        new EnumMap<Protocol, ConnectionPool>(Protocol.class);
    private final String remoteAddress;
    private final ScheduledExecutorService executor; 
    private final boolean ownsExecutor;
    private final Bootstrap bootstrap;
    private final boolean ownsBootstrap;
    private final List<NodeStateListener> stateListeners = 
        Collections.synchronizedList(new LinkedList<NodeStateListener>());
    
    private volatile State state;
    private Map<Integer, InProgressOperation> inProgressMap = 
        new ConcurrentHashMap<Integer, InProgressOperation>();
    
    // TODO: Harden to prevent operation from being executed > 1 times?
    // TODO: how many channels on one event loop? 
    private RiakNode(Builder builder) throws UnknownHostException
    {
        this.remoteAddress = builder.remoteAddress;
        
        if (builder.executor == null)
        {
            executor = Executors.newSingleThreadScheduledExecutor();
            ownsExecutor = true;
        }
        else
        {
            this.executor = builder.executor;
            this.ownsExecutor = false;
        }

        if (builder.bootstrap == null)
        {
            bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);
            ownsBootstrap = true;
        }
        else
        {
            this.bootstrap = builder.bootstrap;
            this.ownsBootstrap = false;
        }
        
        for (Map.Entry<Protocol, ConnectionPool.Builder> e : builder.protocolMap.entrySet())
        {
            ConnectionPool cp = e.getValue().withExecutor(executor).withBootstrap(bootstrap).build();
            connectionPoolMap.put(e.getKey(), cp);
        }
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
    
    public synchronized RiakNode start()
    {
        stateCheck(State.CREATED);
        for (Map.Entry<Protocol, ConnectionPool> e : connectionPoolMap.entrySet())
        {
            e.getValue().addStateListener(this);
            e.getValue().start();
        }
        state = State.RUNNING;
        notifyStateListeners();
        return this;
    }
    
    public synchronized void shutdown()
    {
        stateCheck(State.RUNNING, State.HEALTH_CHECKING);
        
        for (Map.Entry<Protocol, ConnectionPool> e : connectionPoolMap.entrySet())
        {
            e.getValue().shutdown();
        }
        // Notifications from the pools change our state. 
    }
    
    public void addStateListener(NodeStateListener listener)
    {
        stateListeners.add(listener);
    }
    
    public void removeStateListener(NodeStateListener listener)
    {
        stateListeners.remove(listener);
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
        
        Protocol protoToUse = chooseProtocol(operation);
        
        if (null == protoToUse)
        {
            throw new IllegalArgumentException("Node does not support required protocol");
        }
        
        operation.setLastNode(this);
        Channel channel = connectionPoolMap.get(protoToUse).getConnection();
        if (channel != null)
        {
            // add callback handler to end of pipeline which will callback to here
            // These remove themselves once they notify the listener
            channel.pipeline().addLast("riakResponseHandler", protoToUse.responseHandler(this));
            inProgressMap.put(channel.id(), new InProgressOperation(protoToUse, operation));
            ChannelFuture writeFuture = channel.write(operation); 
            writeFuture.addListener(this);
            return true;
        }
        else
        {
            return false;
        }
    }
    
    private Protocol chooseProtocol(FutureOperation operation)
    {
        List<Protocol> prefList = operation.getProtocolPreflist();
        Protocol protoToUse = null;
        for (Protocol p : prefList)
        {
            if (connectionPoolMap.keySet().contains(p))
            {
                protoToUse = p;
                break;
            }
        }
        
        return protoToUse;
    }
    
    @Override
    public synchronized void poolStateChanged(ConnectionPool pool, ConnectionPool.State newState)
    {
        switch (newState)
        {
            case HEALTH_CHECKING:
                this.state = State.HEALTH_CHECKING;
                notifyStateListeners();
                logger.info("RiakNode offline, health checking; {}", remoteAddress);
                break;
            case RUNNING:
                this.state = State.RUNNING;
                notifyStateListeners();
                logger.info("RiakNode running; {}", remoteAddress);
                break;
            case SHUTTING_DOWN:
                if (this.state == State.RUNNING ||  this.state == State.HEALTH_CHECKING)
                {
                    this.state = State.SHUTTING_DOWN;
                    notifyStateListeners();
                    logger.info("RiakNode shutting down due to pool shutdown; {}", remoteAddress);
                }
                break;
            case SHUTDOWN:
                connectionPoolMap.remove(pool.getProtocol());
                if (connectionPoolMap.isEmpty())
                {
                    this.state = State.SHUTDOWN;
                    notifyStateListeners();
                    if (ownsExecutor)
                    {
                        executor.shutdown();
                    }
                    if (ownsBootstrap)
                    {
                        bootstrap.shutdown();
                    }
                    logger.info("RiakNode shut down due to pool shutdown; {}", remoteAddress);
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void onSuccess(Channel channel, RiakResponse response)
    {
        InProgressOperation inProgress = inProgressMap.remove(channel.id());
        connectionPoolMap.get(inProgress.getProtocol()).returnConnection(channel);
        inProgress.getOperation().setResponse(response);
    }

    @Override
    public void onException(Channel channel, Throwable t)
    {
        InProgressOperation inProgress = inProgressMap.remove(channel.id());
        // There is an edge case where a write could fail after the message encoding
        // occured in the pipeline. In that case we'll get an exception from the 
        // handler due to it thinking there was a request in flight 
        // but will not have an entry in inProgress
        if (inProgress != null)
        {
            connectionPoolMap.get(inProgress.getProtocol()).returnConnection(channel);
            inProgress.getOperation().setException(t);
        }
    }
    
    // The only Netty future we are listening to is the WriteFuture
    @Override
    public void operationComplete(ChannelFuture future) throws Exception
    {
        // See how the write worked out ...
        if (!future.isSuccess())
        {
            InProgressOperation inProgress = inProgressMap.remove(future.channel().id());
            connectionPoolMap.get(inProgress.getProtocol()).returnConnection(future.channel());
            inProgress.getOperation().setException(future.cause());
        }
    }
    
    private class InProgressOperation
    {
        private final Protocol p;
        private final FutureOperation operation;
        
        public InProgressOperation(Protocol p, FutureOperation operation)
        {
            this.p = p;
            this.operation = operation;
        }
        
        public Protocol getProtocol()
        {
            return p;
        }
        
        public FutureOperation getOperation()
        {
            return operation;
        }
    }
    
    /**
     * Returns the {@code remoteAddress} for this RiakNode
     * @return The IP address or FQDN as a {@code String}
     */
    public String getRemoteAddress()
    {
        return this.remoteAddress;
    }
    
    /**
     * Builder used to construct a RiakNode.
     * 
     * <p>If a protocol is not specified protocol buffers will be used on the default 
     * port.
     * </p>
     * <p>
     * Note that many of these options revolve around constructing the underlying
     * {@link ConnectionPool}s used by this node. 
     * </p>
     * 
     * 
     */
    public static class Builder
    {
        /**
         * The default remote address to be used if not specified: {@value #DEFAULT_REMOTE_ADDRESS}
         * @see #withRemoteAddress(java.lang.String) 
         */
        public final static String DEFAULT_REMOTE_ADDRESS = "127.0.0.1";
        
        private String remoteAddress = DEFAULT_REMOTE_ADDRESS;
        private ScheduledExecutorService executor;
        private boolean ownsExecutor;
        private Bootstrap bootstrap;
        private boolean ownsBootstrap;
        private final EnumMap<Protocol, ConnectionPool.Builder> protocolMap = 
            new EnumMap<Protocol, ConnectionPool.Builder>(Protocol.class);
        
        
        public Builder(Protocol p)
        {
            getPoolBuilder(p);
        }
        
        // Used by the from() method
        private Builder() {}
        
        /**
         * Sets the remote address for this RiakNode. 
         * @param remoteAddress Can either be a FQDN or IP address
         * @return this
         */
        public Builder withRemoteAddress(String remoteAddress)
        {
            this.remoteAddress = remoteAddress;
            return this;
        }
        
        /**
         * Specifies a protocol this node will support using the default port
         * 
         * @param p - the protocol
         * @return this
         * @see {@link Protocol}
         */
        public Builder addProtocol(Protocol p)
        {
            getPoolBuilder(p);
            return this;
        }
        
        /**
         * Specifies a protocol this node will support using the supplied port
         * @param p - the protocol
         * @param port - the port
         * @return this
         * @see {@link Protocol}
         */
        public Builder withPort(Protocol p, int port)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withPort(port);
            return this;
        }
        
        /**
         * Specifies the minimum number for connections to be maintained for the specific protocol
         * @param p 
         * @param minConnections
         * @return this
         * @see {@link ConnectionPool.Builder#withMinConnections(int) 
         */
        public Builder withMinConnections(Protocol p, int minConnections)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withMinConnections(minConnections);
            return this;
        }
        
        /**
         * Specifies the maximum number of connections allowed for the specific protocol
         * @param p
         * @param maxConnections
         * @return this
         * @see {@link ConnectionPool.Builder#withMaxConnections(int) 
         */
        public Builder withMaxConnections(Protocol p, int maxConnections)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withMaxConnections(maxConnections);
            return this;
        }
        
        /**
         * Specifies the idle timeout for the specific protocol
         * @param p
         * @param idleTimeoutInMillis
         * @return this
         * @see {@link ConnectionPool.Builder#withIdleTimeout(int) 
         */
        public Builder withIdleTimeout(Protocol p, int idleTimeoutInMillis)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withIdleTimeout(idleTimeoutInMillis);
            return this;
        }
        
        /**
         * Specifies the connection timeout for the specific protocol
         * @param p
         * @param connectionTimeoutInMillis
         * @return this
         * @see {@link ConnectionPool.Builder#withConnectionTimeout(int) 
         */
        public Builder withConnectionTimeout(Protocol p, int connectionTimeoutInMillis)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withConnectionTimeout(connectionTimeoutInMillis);
            return this;
        }
        
        /**
         * Specifies the read timeout for the specific protocol
         * @param p
         * @param readTimeoutInMillis
         * @return this
         * @see {@link ConnectionPool.Builder#withReadTimeout(int) 
         */
        public Builder withReadTimeout(Protocol p, int readTimeoutInMillis)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withReadTimeout(readTimeoutInMillis);
            return this;
        }
        
        /**
         * Specifies the permit timeout for the specific protocol
         * @param p
         * @param permitTimeoutInMillis
         * @return this
         * @see {@link ConnectionPool.Builder#withPermitTimeout(int) 
         */
        public Builder withPermitTimeout(Protocol p, int permitTimeoutInMillis)
        {
            ConnectionPool.Builder builder = getPoolBuilder(p);
            builder.withReadTimeout(permitTimeoutInMillis);
            return this;
        }
        
        /**
         * Provides an executor for this node to use for internal maintenance tasks.
         * This same executor will be used for this node's connection pool(s)
         * @param executor
         * @return this
         * @see {@link ConnectionPool.Builder#withExecutor(java.util.concurrent.ScheduledExecutorService) 
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
         * @see {@link ConnectionPool.Builder#withBootstrap(io.netty.bootstrap.Bootstrap) 
         */
        public Builder withBootstrap(Bootstrap bootstrap)
        {
            this.bootstrap = bootstrap;
            return this;
        }
        
        private ConnectionPool.Builder getPoolBuilder(Protocol p)
        {
            ConnectionPool.Builder builder = protocolMap.get(p);
            if (builder == null)
            {
                builder = new ConnectionPool.Builder(p);
                protocolMap.put(p, builder);
            }
            return builder;
        }
        
        public static Builder from(Builder b)
        {
            Builder builder = new Builder();
            builder.bootstrap = b.bootstrap;
            builder.executor = b.executor;
            builder.ownsBootstrap = b.ownsBootstrap;
            builder.ownsExecutor = b.ownsExecutor;
            builder.protocolMap.putAll(b.protocolMap);
            builder.remoteAddress = b.remoteAddress;
            return builder;
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
        
        public static List<RiakNode.Builder> createBuilderList(Builder builder, List<String> remoteAddresses)
        {
            List<RiakNode.Builder> builders = new ArrayList<RiakNode.Builder>(remoteAddresses.size());
            for (String remoteAddress : remoteAddresses)
            {
                Builder b = Builder.from(builder);
                b.withRemoteAddress(remoteAddress);
                builders.add(b);
            }
            return builders;
        }
        
    }
    
}
