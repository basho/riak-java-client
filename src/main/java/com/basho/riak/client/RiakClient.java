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
package com.basho.riak.client;

import com.basho.riak.client.core.RiakCluster;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * The client used to perform operations on Riak.
 * 
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public class RiakClient
{

	private final RiakCluster cluster;

	/**
	 * Create a new RiakClient to perform operations on the given cluster
	 *
	 * @param cluster
	 * 	the cluster to perform operations against
	 */
	public RiakClient(RiakCluster cluster)
	{
		this.cluster = cluster;
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
	public <T> T execute(RiakCommand<T> command) throws ExecutionException, InterruptedException
	{
		return command.execute(cluster);
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
