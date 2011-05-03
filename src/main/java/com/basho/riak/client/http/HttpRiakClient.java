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
package com.basho.riak.client.http;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;

import com.basho.riak.client.http.request.MapReduceBuilder;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.RiakExceptionHandler;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;
import com.basho.riak.client.http.response.StoreResponse;
import com.basho.riak.client.http.response.StreamHandler;
import com.basho.riak.client.http.response.WalkResponse;
import com.basho.riak.client.http.util.ClientUtils;

/**
 * @author russell
 *
 */
public interface HttpRiakClient {

    RiakConfig getConfig();

    /**
     * Set the properties for a Riak bucket.
     * 
     * @param bucket
     *            The bucket name.
     * @param bucketInfo
     *            Contains the schema to use for the bucket. Refer to the Riak
     *            documentation for a list of the recognized properties and the
     *            format of their values.
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            and query parameters.
     * 
     * @return {@link HttpResponse} containing HTTP response information.
     * 
     * @throws IllegalArgumentException
     *             If the provided schema values cannot be serialized to send to
     *             Riak.
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     */
    HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta);

    HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo);

    /**
     * Return the properties for a Riak bucket without listing the keys in it.
     * 
     * @param bucket
     *            The target bucket.
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            and query parameters.
     * 
     * @return {@link BucketResponse} containing HTTP response information and
     *         the parsed schema
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    BucketResponse getBucketSchema(String bucket, RequestMeta meta);

    BucketResponse getBucketSchema(String bucket);

    /**
     * Return the properties and keys for a Riak bucket.
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
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    BucketResponse listBucket(String bucket, RequestMeta meta);

    BucketResponse listBucket(String bucket);

    /**
     * Same as {@link RiakClient#listBucket(String, RequestMeta)}, except
     * streams the response, so the user must remember to call
     * {@link BucketResponse#close()} on the return value.
     */
    BucketResponse streamBucket(String bucket, RequestMeta meta);

    BucketResponse streamBucket(String bucket);

    /**
     * Store a {@link RiakObject}.
     * 
     * @param object
     *            The {@link RiakObject} to store.
     * @param meta
     *            Extra metadata to attach to the request such as w and dw
     *            values for the request, HTTP headers, and other query
     *            parameters. See
     *            {@link RequestMeta#writeParams(Integer, Integer)}.
     * 
     * @return A {@link StoreResponse} containing HTTP response information and
     *         any updated information returned by the server such as the
     *         vclock, last modified date.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    StoreResponse store(RiakObject object, RequestMeta meta);

    StoreResponse store(RiakObject object);

    /**
     * Fetch metadata (e.g. vclock, last modified, vtag) for the
     * {@link RiakObject} stored at <code>bucket</code> and <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            {@link RequestMeta#readParams(int)}.
     * 
     * @return {@link FetchResponse} containing HTTP response information and a
     *         {@link RiakObject} containing only metadata and no value.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    FetchResponse fetchMeta(String bucket, String key, RequestMeta meta);

    FetchResponse fetchMeta(String bucket, String key);

    /**
     * Fetch the {@link RiakObject} (which can include sibling objects) stored
     * at <code>bucket</code> and <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            {@link RequestMeta#readParams(int)}.
     * 
     * @return {@link FetchResponse} containing HTTP response information and a
     *         {@link RiakObject} or sibling objects.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    FetchResponse fetch(String bucket, String key, RequestMeta meta);

    FetchResponse fetch(String bucket, String key);

    /**
     * Similar to fetch(), except the HTTP connection is left open for
     * successful responses, and the Riak response is provided as a stream.
     * The user must remember to call {@link FetchResponse#close()} on the
     * return value.
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
     * @return A streaming {@link FetchResponse} containing HTTP response
     *         information and the response stream. The HTTP connection must be
     *         closed manually by the user by calling
     *         {@link FetchResponse#close()}.
     */
    FetchResponse stream(String bucket, String key, RequestMeta meta);

    FetchResponse stream(String bucket, String key);

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
     * @return Result from calling handler.process() or true if handler is null.
     * 
     * @throws IOException
     *             If an error occurs during communication with the Riak server.
     * 
     * @see StreamHandler
     */
    boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException;

    /**
     * Delete the object at <code>bucket</code> and <code>key</code>.
     * 
     * @param bucket
     *            The bucket containing the object.
     * @param key
     *            The key of the object
     * @param meta
     *            Extra metadata to attach to the request such as w and dw
     *            values for the request, HTTP headers, and other query
     *            parameters. See
     *            {@link RequestMeta#writeParams(Integer, Integer)}.
     * 
     * @return {@link HttpResponse} containing HTTP response information.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     */
    HttpResponse delete(String bucket, String key, RequestMeta meta);

    HttpResponse delete(String bucket, String key);

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
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            or query parameters.
     * 
     * @return {@link WalkResponse} containing HTTP response information and a
     *         <code>List</code> of <code>Lists</code>, where each sub-list
     *         corresponds to a <code>walkSpec</code> element that had
     *         <code>accumulateFlag</code> equal to 1.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     * 
     * @see RiakWalkSpec
     */
    WalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta);

    WalkResponse walk(String bucket, String key, String walkSpec);

    WalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec);

    /**
     * Execute a map reduce job on the Riak server.
     * 
     * @param job
     *            JSON string representing the map reduce job to run, which can
     *            be created using {@link MapReduceBuilder}
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            or query parameters.
     * 
     * @return {@link MapReduceResponse} containing HTTP response information
     *         and the result of the map reduce job
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server does not return a valid JSON array.
     */
    MapReduceResponse mapReduce(String job, RequestMeta meta);

    MapReduceResponse mapReduce(String job);

    /**
     * A convenience method for creating a MapReduceBuilder used for building a
     * map reduce job to submission to this client
     * 
     * @param bucket
     *            The bucket to perform the map reduce job over
     * @return A {@link MapReduceBuilder} to build the map reduce job
     */
    MapReduceBuilder mapReduceOverBucket(String bucket);

    /**
     * Same as {@link RiakClient#mapReduceOverBucket(String)}, except over a set
     * of objects instead of a bucket.
     * 
     * @param objects
     *            A set of objects represented as a map of { bucket : [ list of
     *            keys in bucket ] }
     */
    MapReduceBuilder mapReduceOverObjects(Map<String, Set<String>> objects);

    /**
     * The installed exception handler or null if not installed
     */
    RiakExceptionHandler getExceptionHandler();

    /**
     * If an exception handler is provided, then the Riak client will hand
     * exceptions to the handler rather than throwing them.
     * {@link ClientUtils#throwChecked(Throwable)} can be used to throw
     * undeclared checked exceptions to effectively "convert" RiakClient's
     * unchecked exceptions to checked exceptions.
     */
    void setExceptionHandler(RiakExceptionHandler exceptionHandler);

    /**
     * Return the {@link HttpClient} used to make requests, which can be
     * configured.
     */
    HttpClient getHttpClient();

    /**
     * A 4-byte unique ID for this client. The ID is base 64 encoded and sent to
     * Riak to generating the object vclock on store operations. Refer to the
     * Riak documentation and
     * http://lists.basho.com/pipermail/riak-users_lists.basho.com/2009-
     * November/000153.html for information about the client ID.
     */
    byte[] getClientId();

    void setClientId(String clientId);

}