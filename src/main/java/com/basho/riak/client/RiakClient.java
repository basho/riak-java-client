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

import java.io.IOException;

import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIOException;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.response.WalkResponse;

/**
 * Interface for accessing the Riak document store via HTTP. See
 * {@link JiakClient} to use the JSON interface and {@link RawClient} to use the
 * Raw interface.
 */
public interface RiakClient {

    /**
     * Set the schema describing the structure and per-field permissions for a
     * Riak bucket.
     * 
     * @param bucket
     *            The bucket name.
     * @param bucketInfo
     *            Contains the schema to use for the bucket. Refer to the
     *            documentation for a specific Riak interface (i.e. Jiak, Raw)
     *            for a list of the recognized schema properties and the format
     *            of their values.
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            and query parameters.
     * 
     * @return {@link HttpResponse} containing HTTP response information.
     * 
     * @throws IllegalArgumentException
     *             If the provided schema values cannot be serialized to send to
     *             Riak.
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     */
    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta);

    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo);

    /**
     * Return the schema and keys for a Riak bucket.
     * 
     * @param bucket
     *            The bucket to list.
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            and query parameters.
     * 
     * @return {@link BucketResponse} containing HTTP response information and
     *         the parsed schema and keys
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseException
     *             If the Riak server returns a malformed response.
     */
    public BucketResponse listBucket(String bucket, RequestMeta meta);

    public BucketResponse listBucket(String bucket);

    /**
     * Store a {@link RiakObject}.
     * 
     * @param object
     *            The {@link RiakObject} to store.
     * @param meta
     *            Extra metadata to attach to the request such as w and dw
     *            values for the request, HTTP headers, and other query
     *            parameters. See RequestMeta.writeParams().
     * 
     * @return {@link StoreResponse} containing HTTP response information and a
     *         {@link RiakObject} with any updated information returned by the
     *         server such as the vclock, last modified date, and stored value.
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseException
     *             If the Riak server returns a malformed response.
     */
    public StoreResponse store(RiakObject object, RequestMeta meta);

    public StoreResponse store(RiakObject object);

    /**
     * Fetch metadata for the {@link RiakObject} stored at <code>bucket</code>
     * and <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return {@link FetchResponse} containing HTTP response information and a
     *         {@link RiakObject} containing only metadata and no value.
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseException
     *             If the Riak server returns a malformed response.
     */
    public FetchResponse fetchMeta(String bucket, String key, RequestMeta meta);

    public FetchResponse fetchMeta(String bucket, String key);

    /**
     * Fetch the {@link RiakObject} stored at <code>bucket</code> and
     * <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return {@link FetchResponse} containing HTTP response information and a
     *         {@link RiakObject}.
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseException
     *             If the Riak server returns a malformed response.
     */
    public FetchResponse fetch(String bucket, String key, RequestMeta meta);

    public FetchResponse fetch(String bucket, String key);

    /**
     * Fetch and process the object stored at <code>bucket</code> and
     * <code>key</code> as a stream.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param handler
     *            A {@link StreamHandler} to process the Riak response.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return Result from the handler.process() or true if handler is null.
     * 
     * @throws IOException
     *             If an error occurs during communication with the Riak server.
     * 
     * @see StreamHandler
     */
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException;

    /**
     * Delete the object at <code>bucket</code> and <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the object.
     * @param key
     *            The key of the object
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return {@link HttpResponse} containing HTTP response information.
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     */
    public HttpResponse delete(String bucket, String key, RequestMeta meta);

    public HttpResponse delete(String bucket, String key);

    /**
     * Perform a map/reduce link walking operation and return the objects for
     * which the "accumulate" flag is true.
     * 
     * @param bucket
     *            The bucket of the "starting object"
     * @param key
     *            The key of the "starting object"
     * @param walkSpec
     *            A URL-path (omit beginning /) of the form
     *            <code>bucket,tag-spec,accumulateFlag</code> The
     *            <code>tag-spec "_"</code> matches all tags.
     *            <code>accumulateFlag</code> is either the String "1" or "0".
     * 
     * @return {@link WalkResponse} containing HTTP response information and a
     *         <code>List</code> of <code>Lists</code>, where each sub-list
     *         corresponds to a <code>walkSpec</code> element that had
     *         <code>accumulateFlag</code> equal to 1.
     * 
     * @throws RiakIOException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseException
     *             If the Riak server returns a malformed response.
     * 
     * @see RiakWalkSpec
     */
    public WalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta);

    public WalkResponse walk(String bucket, String key, String walkSpec);

    public WalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec);

}