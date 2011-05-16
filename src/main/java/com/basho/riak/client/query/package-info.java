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
 * Classes and interfaces for running map/reduce and link walk operations on
 * Riak.
 * 
 * <p>
 * {@link com.basho.riak.client.query.MapReduce} and
 * {@link com.basho.riak.client.query.LinkWalk} are
 * {@link com.basho.riak.client.operations.RiakOperation}s. Use the
 * {@link com.basho.riak.client.IRiakClient} as a factory to create the
 * operation. Both are implemented as fluent builders. Unlike other operations
 * they do not (yet) run with a {@link com.basho.riak.client.cap.Retrier}
 * </p>
 * 
 * <p>
 * Please see the basho wiki on 
 * <a href="http://wiki.basho.com/MapReduce.html">Map/Reduce</a> and 
 * <a href="http://wiki.basho.com/Links.html">Link Walking</a> for more details.
 * </p>
 * 
 * @see com.basho.riak.client.IRiakClient#mapReduce()
 * @see com.basho.riak.client.IRiakClient#mapReduce(String)
 * @see com.basho.riak.client.IRiakClient#walk(com.basho.riak.client.IRiakObject)
 */
package com.basho.riak.client.query;