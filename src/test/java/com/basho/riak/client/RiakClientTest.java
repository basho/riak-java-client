/*
 * Copyright 2014 Kazuhiro Suzuki <kaz at basho dot com>.
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

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

/**
 * 
 * @author Kazuhiro Suzuki <kaz at basho dot com>
 */
public class RiakClientTest {
	private RiakClient client;

	@After
	public void teardown() {
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	public void newClientUsingStringList() throws Exception {
		List<String> addresses = new ArrayList<String>();
		addresses.add("localhost");
		addresses.add("127.0.0.1");
		client = RiakClient.newClient(addresses);

		Field field = client.getClass().getDeclaredField("cluster");
		field.setAccessible(true);
		RiakCluster cluster = (RiakCluster) field.get(client);
		List<RiakNode> nodes = cluster.getNodes();

		for (int i = 0; i < addresses.size(); i++) {
			assertEquals(addresses.get(i), nodes.get(i).getRemoteAddress());
			assertEquals(8087, nodes.get(i).getPort());
		}
	}

	@Test
	public void newClientWithStringArray() throws Exception {
		int port = 8000;
		String[] addresses = new String[]{"localhost", "127.0.0.1"};
		client = RiakClient.newClient(port, addresses);

		Field field = client.getClass().getDeclaredField("cluster");
		field.setAccessible(true);
		RiakCluster cluster = (RiakCluster) field.get(client);
		List<RiakNode> nodes = cluster.getNodes();

		for (int i = 0; i < addresses.length; i++) {
			assertEquals(addresses[i], nodes.get(i).getRemoteAddress());
			assertEquals(port, nodes.get(i).getPort());
		}
	}

	@Test
	public void newClientWithInetSocketAddress() throws Exception {
		InetSocketAddress[] addresses = new InetSocketAddress[] {
				new InetSocketAddress("localhost", 1111),
				new InetSocketAddress("127.0.0.1", 2222) };
		client = RiakClient.newClient(addresses);

		Field field = client.getClass().getDeclaredField("cluster");
		field.setAccessible(true);
		RiakCluster cluster = (RiakCluster) field.get(client);
		List<RiakNode> nodes = cluster.getNodes();

		for (int i = 0; i < addresses.length; i++) {
			assertEquals(addresses[i].getHostString(), nodes.get(i).getRemoteAddress());
			assertEquals(addresses[i].getPort(), nodes.get(i).getPort());
		}
	}
}
