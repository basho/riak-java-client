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
package com.basho.riak.client.raw;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.raw.query.indexes.IndexQuery;

/**
 * Common interface for low-level clients.
 * 
 * <p>
 * If you have the urge to write a client to Riak, please, please implement this
 * interface so it can drop straight into all the higher level API code
 * </p>
 * 
 * @author russell
 * 
 */
public interface RawClient {

    /**
     * Fetch data from <code>bucket/key</code>
     * 
     * @param bucket
     *            the bucket
     * @param key
     *            the key
     * @return a {@link RiakResponse}
     * @throws IOException
     */
    RiakResponse fetch(String bucket, String key) throws IOException;

    /**
     * Fetch data from the given <code>bukcet/key</code> with read quorum
     * <code>readQuorum</code>
     * 
     * @param bucket
     *            the bucket
     * @param key
     *            the key
     * @param readQuorum
     *            readQuorum, needs to be =< the buckets n_val
     * @return a {@link RiakResponse}
     * @throws IOException
     */
    RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException;

    /**
     * Fetch data from the given <code>bucket/key</code> with
     * <code>fetchMeta</code>
     * 
     * @param bucket
     *            the bucket
     * @param key
     *            the key
     * @param fetchMeta
     *            the extra fetch parameters {@link FetchMeta}
     * @return a {@link RiakResponse}
     * @throws IOException
     */
    RiakResponse fetch(String bucket, String key, FetchMeta fetchMeta) throws IOException;

    /**
     * Store the given {@link IRiakObject} in Riak at the location
     * <code>bucket/key</code>
     * 
     * @param object
     *            the data to store
     * @param storeMeta
     *            meta data for the store operation as a {@link StoreMeta}
     * @return a {@link RiakResponse} if {@link StoreMeta#getReturnBody()} is
     *         true, or null
     * @throws IOException
     */
    RiakResponse store(IRiakObject object, StoreMeta storeMeta) throws IOException;

    /**
     * Store the given {@link IRiakObject} in Riak using the bucket default w/dw
     * and false for returnBody
     * 
     * @param object
     *            the data to store as an {@link IRiakObject}
     * @throws IOException
     */
    void store(IRiakObject object) throws IOException;

    /**
     * Delete the data at <code>bucket/key</code>
     * 
     * @param bucket
     * @param key
     * @throws IOException
     */
    void delete(String bucket, String key) throws IOException;

    /**
     * Delete the data at <code>bucket/key</code> using
     * <code>deleteQuorum</code> as the rw param
     * 
     * @param bucket
     * @param key
     * @param deleteQuorum
     *            an int that is less than or equal to the bucket's n_val
     * @throws IOException
     */
    void delete(String bucket, String key, int deleteQuorum) throws IOException;

    /**
     * Delete the data at <code>bucket/key</code> using the parameters in
     * <code>deleteMeta</code>
     * 
     * @param bucket
     * @param key
     * @param deleteMeta
     *            the {@link DeleteMeta} containing the operation parameters
     * @throws IOException
     */
    void delete(String bucket, String key, DeleteMeta deleteMeta) throws IOException;

    // Bucket
    /**
     * An Unmodifiable {@link Iterator} view of the all the Buckets in Riak
     */
    Set<String> listBuckets() throws IOException;

    /**
     * The set of properties for the given bucket
     * 
     * @param bucketName
     *            the name of the bucket
     * @return a populated {@link BucketProperties} (by populated, as populated
     *         as the underlying API allows)
     * @throws IOException
     */
    BucketProperties fetchBucket(String bucketName) throws IOException;

    /**
     * Update a buckets properties from the {@link BucketProperties} provided.
     * No guarantees that the underlying API is able to set all the properties
     * passed.
     * 
     * @param name
     *            the bucket to be updated
     * @param bucketProperties
     *            the set of properties to be writen
     * @throws IOException
     */
    void updateBucket(String name, BucketProperties bucketProperties) throws IOException;

    /**
     * An unmodifiable {@link Iterator} view of the keys for the bucket named
     * <code>bucketName</code>
     * 
     * May be backed by a stream or a collection. Be careful, expensive.
     * 
     * @param bucketName
     * @return an unmodifiable, iterable view of the keys for tha bucket
     * @throws IOException
     */
    Iterable<String> listKeys(String bucketName) throws IOException;

    // Query
    /**
     * Performs a link walk operation described by the {@link LinkWalkSpec}
     * provided.
     * 
     * The underlying API may not support Link Walking directly but will
     * approximate it at some cost.
     * 
     * @param linkWalkSpec
     * @return a {@link WalkResult}
     * @throws IOException
     */
    WalkResult linkWalk(final LinkWalkSpec linkWalkSpec) throws IOException;

    /**
     * Perform a map/reduce query defined by {@link MapReduceSpec}
     * @param spec the m/r job specification
     * @return A {@link MapReduceResult}
     * @throws IOException
     * @throws MapReduceTimeoutException
     */
    MapReduceResult mapReduce(final MapReduceSpec spec) throws IOException, MapReduceTimeoutException;

    /**
     * If you don't set a client id explicitly at least call this to set one. It
     * generates the 4 byte ID and sets that Id on the client IE you *don't*
     * need to call setClientId() with the result of generate.
     * 
     * @return the generated clientId for the client
     */
    byte[] generateAndSetClientId() throws IOException;

    /**
     * Set a client id, currently must be a 4 bytes exactly
     * @param clientId any 4 bytes
     * @throws IOException
     */
    void setClientId(byte[] clientId) throws IOException;

    /**
     * Ask Riak for the client id for the current connection.
     * @return whatever 4 bytes Riak uses to identify this client
     * @throws IOException
     */
    byte[] getClientId() throws IOException;

    /**
     * Riak connection health check, is Riak reachable. Can be used for
     * diagnostics/monitoring
     * 
     * @throws IOException
     *             if Riak is not reachable or returns anything other than OK
     */
    void ping() throws IOException;

    /**
     * Performs an 2i index query
     * 
     * @param <T>
     * @param indexQuery
     *            the query to perform
     * @return a List of keys (or empty list)
     * @throws IOException
     */
    List<String> fetchIndex(IndexQuery indexQuery) throws IOException;

    /**
     * The raw transport name.
     * @return the {@link Transport} for the client or null if not implemented.
     */
    Transport getTransport();
}
