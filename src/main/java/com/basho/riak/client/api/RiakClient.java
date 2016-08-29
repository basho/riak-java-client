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
package com.basho.riak.client.api;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.util.HostAndPort;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * The client used to perform operations on Riak.
 * <p>
 * The core of the Java client models a Riak cluster:
 * </p>
 * <img src="doc-files/client-image.png">
 * </p>
 * <p>
 * The easiest way to get started with the client API is using one of the static
 * methods provided to instantiate and start the client:
 * </p>
 * <pre class="prettyprint">
 * {@code
 * RiakClient client =
 *     RiakClient.newClient("192.168.1.1","192.168.1.2","192.168.1.3"); } </pre>
 *
 * Note that the Riak Java client uses the Riak Protocol Buffers API exclusively.
 * <p>
 * For more complex configurations you will instantiate one or more {@link com.basho.riak.client.core.RiakNode}s
 * and build a {@link com.basho.riak.client.core.RiakCluster} to supply to the
 * RiakClient constructor.
 * </p>
 * <pre class="prettyprint">
 * {@code
 * RiakNode.Builder builder = new RiakNode.Builder();
 * builder.withMinConnections(10);
 * builder.withMaxConnections(50);
 *
 * List<String> addresses = new LinkedList<String>();
 * addresses.add("192.168.1.1");
 * addresses.add("192.168.1.2");
 * addresses.add("192.168.1.3");
 *
 * List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, addresses);
 * RiakCluster cluster = new RiakCluster.Builder(nodes).build();
 * cluster.start();
 * RiakClient client = new RiakClient(cluster); }</pre>
 * <p>
 * Once you have a client, {@literal RiakCommand}s from the {@literal com.basho.riak.client.api.commands.*}
 * packages are built then executed by the client:
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("default","my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchValue fv = new FetchValue.Builder(loc).build();
 * FetchValue.Response response = client.execute(fv);
 * RiakObject obj = response.getValue(RiakObject.class);}</pre>
 * </p>
 * <p>
 * You can also execute all {@literal RiakCommand}s asynchronously. A
 * {@link RiakFuture} for the operation is immediately returned:
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("default","my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchValue fv = new FetchValue.Builder(loc).build();
 * RiakFuture<FetchValue.Response, Location> future = client.executeAsync(fv);
 * future.await();
 * if (future.isSuccess())
 * {
 *     FetchValue.Response response = future.getNow();
 *     RiakObject obj = response.getValue(RiakObject.class);
 *     ...
 * }
 * else
 * {
 *     Throwable error = future.cause();
 *     ...
 * }}</pre>
 * </p>
 * <p>
 * <h1>RiakCommand subclasses</h1>
 * <h4>Fetching, storing and deleting objects</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.kv.FetchValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.MultiFetch}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.StoreValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.UpdateValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.DeleteValue}</li>
 * </ul>
 * <h4>Listing keys in a namespace</h4>
 * <ul><li>{@link com.basho.riak.client.api.commands.kv.ListKeys}</li></ul>
 * <h4>Secondary index (2i) commands</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.indexes.RawIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.BinIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.IntIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.BigIntIndexQuery}</li>
 * </ul>
 * <h4>Fetching and storing datatypes (CRDTs)</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchCounter}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchSet}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchMap}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateCounter}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateSet}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateMap}</li>
 * </ul>
 * <h4>Querying and modifying buckets</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.buckets.FetchBucketProperties}</li>
 * <li>{@link com.basho.riak.client.api.commands.buckets.StoreBucketProperties}</li>
 * <li>{@link com.basho.riak.client.api.commands.buckets.ListBuckets}</li>
 * </ul>
 * <h4>Search commands</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.search.Search}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.FetchIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.StoreIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.DeleteIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.FetchSchema}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.StoreSchema}</li>
* </ul>
* <h4>Map-Reduce</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.BucketMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.BucketKeyMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.IndexMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.SearchMapReduce}</li>
 * </ul>
 * @author Dave Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0
 */
public class RiakClient
{

	private final RiakCluster cluster;

    /**
	 * Create a new RiakClient to perform operations on the given cluster.
     * <p>
     * The RiakClient provides a user API on top of the client core. Once
     * instantiated, commands are submitted to it for execution on Riak.
     * </p>
     * @param cluster the started RiakCluster to use.
	 */
	public RiakClient(RiakCluster cluster)
	{
		this.cluster = cluster;
	}

    /**
     * Static factory method to create a new client instance.
     * This method produces a client that connects to 127.0.0.1 on the default
     * protocol buffers port (8087).
     *
     * @return a new client instance.
     * @throws UnknownHostException
     */
    public static RiakClient newClient() throws UnknownHostException
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);
        RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        return new RiakClient(cluster);

    }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses on
     * the supplied port.
     * @param remoteAddresses a list of IP addresses or hostnames
     * @param port the (protocol buffers) port to connect to on the supplied hosts.
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
     public static RiakClient newClient(int port, String... remoteAddresses) throws UnknownHostException
     {
         return newClient(port, Arrays.asList(remoteAddresses));
     }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses on
     * the default (protocol buffers) port (8087).
     * @param remoteAddresses a list of IP addresses or hostnames
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(List<String> remoteAddresses) throws UnknownHostException
    {
        return newClient(RiakNode.Builder.DEFAULT_REMOTE_PORT, remoteAddresses);
    }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses on
     * the default (protocol buffers) port (8087).
     * @param remoteAddresses a list of IP addresses or hostnames
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(String... remoteAddresses) throws UnknownHostException
    {
        return newClient(RiakNode.Builder.DEFAULT_REMOTE_PORT, Arrays.asList(remoteAddresses));
    }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses on
     * the supplied port.
     * @param remoteAddresses a list of IP addresses or hostnames
     * @param port the (protocol buffers) port to connect to on the supplied hosts.
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(int port, List<String> remoteAddresses) throws UnknownHostException
    {
        RiakNode.Builder builder = createDefaultNodeBuilder()
                                        .withRemotePort(port);
        return newClient(builder, remoteAddresses);
    }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses.
     * @param addresses one or more addresses to connect to.
     * @return a new RiakClient instance.
     * @throws java.net.UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(InetSocketAddress... addresses) throws UnknownHostException
    {
        final List<String> remoteAddresses = new ArrayList<>(addresses.length);

        for (InetSocketAddress addy : addresses)
        {
            remoteAddresses.add(
                    String.format("%s:%s", addy.getHostName(), addy.getPort())
                );
        }

        return newClient(createDefaultNodeBuilder(), remoteAddresses);
    }

    /**
     * Static factory method to create a new client instance.
     * This method produces a client connected to the supplied addresses and containing the {@link RiakNode}s
     * that will be build by using provided builder.
     * @param addresses one or more addresses to connect to.
     * @return a new RiakClient instance.
     * @throws java.net.UnknownHostException if a supplied hostname cannot be resolved.
     * @since 2.0.3
     * @see com.basho.riak.client.core.RiakCluster.Builder#Builder(RiakNode.Builder, List)
     */
    public static RiakClient newClient(RiakNode.Builder nodeBuilder,  List<String> addresses) throws UnknownHostException
    {
        final RiakCluster cluster = new RiakCluster.Builder(nodeBuilder, addresses).build();
        cluster.start();

        return new RiakClient(cluster);
    }

    /**
     * Static factory method to create a new client instance.
     *
     * @since 2.0.3
     * @see #newClient(RiakNode.Builder, List)
     */
    public static RiakClient newClient(RiakNode.Builder nodeBuilder, String... addresses) throws UnknownHostException
    {
        return newClient(nodeBuilder, Arrays.asList(addresses));
    }

    /**
     * Static factory method to create a new client instance.
     *
     * @since 2.0.6
     */
    public static RiakClient newClient(Collection<HostAndPort> hosts) throws UnknownHostException
    {
        return newClient(hosts, createDefaultNodeBuilder());
    }

    /**
     * Static factory method to create a new client instance.
     *
     * @since 2.0.6
     */
    public static RiakClient newClient(Collection<HostAndPort> hosts, RiakNode.Builder nodeBuilder) throws UnknownHostException
    {
        final RiakCluster cluster = new RiakCluster.Builder(hosts, nodeBuilder).build();
        cluster.start();

        return new RiakClient(cluster);
    }

    /**
     *
     * @since 2.0.3
     */
    public static RiakNode.Builder createDefaultNodeBuilder()
    {
        return new RiakNode.Builder()
                .withMinConnections(10);
    }

	/**
	 * Execute a RiakCommand synchronously.
     * <p>
     * Calling this method causes the client to execute the provided RiakCommand synchronously.
     * It will block until the operation completes then either return the response
     * on success or throw an exception on failure.
	 * </p>
     *
	 * @param command
	 * 	The RiakCommand to execute.
	 * @param <T>
	 * 	The RiakCommand's return type.
     * @param <S> The RiakCommand's query info type.
	 * @return a response from Riak.
	 * @throws ExecutionException if the command fails for any reason.
	 * @throws InterruptedException
	 */
	public <T,S> T execute(RiakCommand<T,S> command) throws ExecutionException, InterruptedException
	{
		return command.execute(cluster);
	}

    /**
     * Execute a RiakCommand synchronously with a specified client timeout.
     * <p>
     * Calling this method causes the client to execute the provided RiakCommand synchronously.
     * It will block until the operation completes or up to the given timeout.
     * It will either return the response on success or throw an
     * exception on failure.
     * Note: Using this timeout is different that setting a timeout on the command
     * itself using the timeout() method of the command's associated builder.
     * The command timeout is a Riak-side timeout value. This timeout is client-side.
     * </p>
     *
     * @param command
     *            The RiakCommand to execute.
     * @param timeout the amount of time to wait before returning an exception
     * @param unit the unit of time.
     * @param <T>
     *            The RiakCommand's return type.
     * @param <S>
     *            The RiakCommand's query info type.
     * @return a response from Riak.
     * @throws ExecutionException
     *             if the command fails for any reason.
     * @throws InterruptedException
     * @throws TimeoutException
     *             if the call to execute the command did not finish within the time limit
     */
    public <T, S> T execute(RiakCommand<T, S> command, long timeout, TimeUnit unit) throws ExecutionException,
    InterruptedException, TimeoutException {
        return command.execute(cluster, timeout, unit);
    }

    /**
     * Execute a RiakCommand asynchronously.
     * <p>
     * Calling this method  causes the client to execute the provided RiakCommand
     * asynchronously. It will immediately return a RiakFuture that contains the
     * running operation.
     * @param <T> RiakCommand's return type.
     * @param <S> The RiakCommand's query info type.
     * @param command The RiakCommand to execute.
     * @return a RiakFuture for the operation.
     * @see RiakFuture
     */
    public <T,S> RiakFuture<T,S> executeAsync(RiakCommand<T,S> command)
    {
        return command.executeAsync(cluster);
    }

	/**
	 * Shut down the client and the underlying RiakCluster.
	 * <p>
     * The underlying client core (RiakCluster) uses a number of threads as
     * does Netty. Calling this method will shut down all those threads cleanly.
     * Failure to do so may prevent your application from exiting.
     * </p>
	 * @return a future that will complete when shutdown
	 */
	public Future<Boolean> shutdown()
	{
		return cluster.shutdown();
	}

    /**
     * Get the RiakCluster being used by this client.
     * <p>
     * Allows for adding/removing nodes, etc.
     * </p>
     * @return The RiakCluster instance being used by this client.
     */
    public RiakCluster getRiakCluster()
    {
        return cluster;
    }
}
