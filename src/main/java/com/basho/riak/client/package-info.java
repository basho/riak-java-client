/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/**
 * Provides the top-level {@link com.basho.riak.client.IRiakClient}
 * and {@link com.basho.riak.client.IRiakObject} classes required to store data
 * in <a href="http://wiki.basho.com">Riak</a>, start here.
 * <p>
 * Riak is a Dynamo-style, distributed key-value store that can be queried via
 * Map/Reduce. Riak exposes both an HTTP REST interface and a Protocol Buffers
 * interface. This library provides an abstract, high-level, transport agnostic
 * interface to Riak.
 * </p>
 * <p>
 * In order to use the client you first need an instance of one of the two
 * legacy, transport specific clients http.
 * {@link com.basho.riak.client.http.RiakClient} or pbc.
 * {@link com.basho.riak.pbc.RiakClient} which is then wrapped as an
 * {@link com.basho.riak.client.IRiakClient}.
 * {@link com.basho.riak.client.RiakFactory} provides convenient methods for
 * this:
 * 
 * <code>
 * <pre>
 * IRiakClient client = RiakFactory.pbcClient(yourPBClient);
 * </pre>
 * </code>
 * 
 * If you are running Riak locally using the default ports simply call
 * <code><pre>
 * IRiakClient client = RiakFactory.httpClient()
 * </pre></code> Or <code><pre>
 * IRiakClient client = RiakFactory.pbcClient()
 * </pre></code>
 * 
 * Configuration of the low-level clients is coming soon to the API.
 * 
 * @see com.basho.riak.client.raw
 * @see com.basho.riak.client.query
 */
package com.basho.riak.client;