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
import java.net.UnknownHostException;
import java.util.List;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * The client used to perform operations on Riak.
 * 
 * The easiest way to get started with the client is using one of the static 
 * methods provided to instantiate and start the client:
 * <pre>
 * <code>
 * List<String> addresses = new LinkedList<String>();
 * addresses.add("192.168.1.1");
 * addresses.add("192.168.1.2");
 * addresses.add("192.168.1.3");
 * RiakClient client = RiakClient.newClient(addresses);
 * </code>
 * </pre>
 * For more complex configurations, you can instantiate a RiakCluster from the 
 * core packages and supply it to the RiakClient constructor.
 * 
 * Once you have a client, commands from the {@literal com.basho.riak.client.api.commands.*}
 * packages are built then executed by the client:
 * <pre>
 * <code>
 * Namespace ns = new Namespace("default","my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchValue fv = new FetchValue.Builder(loc).build();
 * FetchValue.Response response = client.execute(fv);
 * RiakObject obj = response.getValue(RiakObject.class);
 * </code>
 * </pre>
 * 
 * @author Dave Rusek <drusek at basho dot com>
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
     * <pre>
     * RiakClient client = RiakClient.newClient();
     * Namespace ns = new Namespace("default","my_bucket");
     * Location loc = new Location(ns, "my_key");
     * FetchValue fv = new FetchValue.Builder(loc).build();
     * FetchValue.Response response = client.execute(fv);
     * RiakObject obj = response.getValue(RiakObject.class);
     * client.shutdown();
     * </pre>
     * 
	 * @param cluster the started RiakCluster to use.
	 */
	public RiakClient(RiakCluster cluster)
	{
		this.cluster = cluster;
	}

    /**
     * Static factory method to create a new client instance.
     * This method produces a client that connects to 127.0.0.1.
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
     * the default port.
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
     * the supplied port.
     * @param remoteAddresses a list of IP addresses or hostnames
     * @param port the port to connect to on the supplied hosts.
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(int port, List<String> remoteAddresses) throws UnknownHostException
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withRemotePort(port)
                                        .withMinConnections(10);
        List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, remoteAddresses);
        RiakCluster cluster = new RiakCluster.Builder(nodes).build();
        cluster.start();
        return new RiakClient(cluster);
    }

    /**
     * Static factory method to create a new client instance for a single-node test cluster.
     * This method produces a client connected to a single node on the supplied
     * address and port.
     * @param address the IP address of the node
     * @param port the port to connect to on the supplied host
     * @return a new client instance
     * @throws UnknownHostException if a supplied hostname cannot be resolved.
     */
    public static RiakClient newClient(String address, int port) throws UnknownHostException {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withRemotePort(port)
                                        .withRemoteAddress(address)
                                        .withMinConnections(10);
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();
        return new RiakClient(cluster);
    }
    
	/**
	 * Execute a command against Riak
	 *
	 * @param command
	 * 	The command
	 * @param <T>
	 * 	The command's return type
	 * @return a response from Riak
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public <T,S> T execute(RiakCommand<T,S> command) throws ExecutionException, InterruptedException
	{
		return command.execute(cluster);
	}

    /**
     * Execute a command asynchronously against Riak.
     * @param command The command to execute.
     * @return a future for the operation.
     */
    public <T,S> RiakFuture<T,S> executeAsync(RiakCommand<T,S> command)
    {
        return command.executeAsync(cluster);
    }
    
	/**
	 *  Shutdown the client and the underlying cluster.
	 *
	 *  @return a future that will complete when shutdown
	 */
	public Future<Boolean> shutdown()
	{
		return cluster.shutdown();
	}
}
