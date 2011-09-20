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
package com.basho.riak.client;

import java.util.Set;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.FetchBucket;
import com.basho.riak.client.bucket.WriteBucket;
import com.basho.riak.client.cap.ClientId;
import com.basho.riak.client.query.BucketKeyMapReduce;
import com.basho.riak.client.query.BucketMapReduce;
import com.basho.riak.client.query.IndexMapReduce;
import com.basho.riak.client.query.LinkWalk;
import com.basho.riak.client.query.MapReduce;
import com.basho.riak.client.query.SearchMapReduce;
import com.basho.riak.client.query.indexes.FetchIndex;
import com.basho.riak.client.raw.query.indexes.IndexQuery;

/**
 * Primary high-level interface for accessing Riak.
 * <p>
 * Used to create/fetch/update {@link Bucket}s and to 
 * perform Map/Reduce query operations.
 * </p>
 * <p>For example:
 * <code><pre>
 * IRiakClient client = RiakFactory.pbcClient();
 * final byte[] id = client.generateAndSetClientId()
 * Bucket b = client.createBucket("myNewBucket")
 *                      .nVal(3)
 *                      .allowSiblings(true)
 *                  .execute();
 *                  
 * // do things with the bucket
 * </pre></code>
 * 
 * @see Bucket
 * @see MapReduce
 * 
 * @author russell
 * 
 */
public interface IRiakClient {

    /**
     * Set an ID for this client.
     * All requests should include a client id,
     * which can be any 4 bytes that uniquely identify the client,
     * for purposes of tracing object modifications in the vclock.
     * 
     * Note: this is 2 calls to Riak.
     * @param clientId byte[4] that uniquely identify the client
     * @return this
     * @throws RiakException if operation fails
     * @throws IllegalArgumentException if clientId is null or not byte[4]
     */
    IRiakClient setClientId(byte[] clientId) throws RiakException;

    /**
     * Generate, set and return "random" byte[4] id for the client.
     * 
     * Note: this is a call to Riak.
     * @see ClientId
     * @return a byte[4] id for this client.
     * @throws RiakException
     */
    byte[] generateAndSetClientId() throws RiakException;

    /**
     * Retrieve the client id from Riak that this client is using.
     * 
     * Note: this is a call to Riak.
     * @return a byte[4] that Riak uses to identify this client.
     * @throws RiakException
     */
    byte[] getClientId() throws RiakException;

    /**
     * Set view of buckets in Riak
     * @return a Set<String> of buckets (or empty)
     * @throws RiakException 
     */
    Set<String> listBuckets() throws RiakException;

    /**
     * Create a new {@link FetchBucket} operation, and return it.
     * 
     * @param bucketName
     * @return a {@link FetchBucket} configured to return the {@link Bucket} called bucketName
     *  for further configuration and execution.
     * @see FetchBucket
     */
    FetchBucket fetchBucket(String bucketName);

    /**
     * Create a new {@link WriteBucket} operation to update passed bucket.
     * @param bucket the name of the {@link Bucket}.
     * @return a {@link WriteBucket} configured to update the supplied bucket
     *  for further configuration and execution.
     *  @see WriteBucket
     */
    WriteBucket updateBucket(Bucket bucket);

    /**
     * Create a new {@link WriteBucket} operation
     * to create a {@link Bucket} named for the passed String.
     * 
     * @param bucketName the name of the new bucket.
     * @return a {@link WriteBucket} configured to create the new bucket
     *  for further configuration and execution.
     *  @see WriteBucket
     */
    WriteBucket createBucket(String bucketName);

    /**
     * Create a {@link LinkWalk} operation that starts at startObject.
     * 
     * See also <a href="http://wiki.basho.com/Links.html">Link Walking</a> on the basho site.
     * 
     * @param startObject the IRiakObject to start the Link walk from.
     * @return a {@link LinkWalk} operation for further configuration and execution.
     * @see LinkWalk
     */
    LinkWalk walk(final IRiakObject startObject);

    /**
     * Create {@link MapReduce} operation for a set of
     * bucket/key inputs.
     * 
     * See also <a href="http://wiki.basho.com/MapReduce.html">Map Reduce</a> on the basho site.
     * @return a {@link BucketKeyMapReduce} for configuration and execution.
     * @see MapReduce
     * @see BucketKeyMapReduce
     */
    BucketKeyMapReduce mapReduce();
    
    /**
     * Create {@link MapReduce} operation that has the supplied bucket as its input.
     * 
     * @param bucket the String name of the input bucket to the M/R job.
     * @return a {@link BucketMapReduce} for further configuration and execution.
     * @see MapReduce
     * @see BucketMapReduce
     */
    BucketMapReduce mapReduce(String bucket);

    /**
     * Create a {@link MapReduce} operation that uses the supplied Riak Search
     * query as input.
     * 
     * See also 
     * <a href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">
     * Riak Search</a> on the basho wiki for more information.
     * 
     * @param bucket
     *            the input bucket for the search query
     * @param query
     *            the input query for the search
     * @return a {@link SearchMapReduce} operation for further configuration and
     *         execution.
     */
    SearchMapReduce mapReduce(String bucket, String query);

    /**
     * Create a {@link MapReduce} operation that uses the supplied
     * {@link IndexQuery} as input
     * 
     * <p>
     * Note: if you just want to fetch an index see
     * {@link Bucket#fetchIndex(com.basho.riak.client.query.indexes.RiakIndex)}
     * to create a {@link FetchIndex} operation
     * </p>
     * 
     * @param query
     *            the {@link IndexQuery} to use as input
     * @return a {@link MapReduce} operation for further configuration and
     *         execution
     */
    IndexMapReduce mapReduce(IndexQuery query);

    /**
     * Ping Riak, check it is available
     * 
     * @throws RiakException
     *             if Riak does not respond OK
     */
    void ping() throws RiakException;
}
