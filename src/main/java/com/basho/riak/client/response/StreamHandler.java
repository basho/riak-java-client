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
package com.basho.riak.client.response;

import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpResponse;

/**
 * Used with RiakClient.stream() to process the HTTP responses for fetch
 * requests as a stream.
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.response.StreamHandler
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.response.StreamHandler
 */
@Deprecated
public interface StreamHandler {

    /**
     * Process the HTTP response whose value is given as a stream.
     * 
     * @param bucket
     *            The object's bucket
     * @param key
     *            The object's key
     * @param status
     *            The HTTP status code returned for the request
     * @param headers
     *            The HTTP headers returned in the response
     * @param in
     *            InputStream of the object's value (body)
     * @param httpMethod
     *            The original {@link HttpResponse} used to make the request. Its
     *            connection is still open and will be closed by the caller on
     *            return.
     * @return true if the object was processed; false otherwise
     */
    public boolean process(String bucket, String key, int status, Map<String, String> headers, InputStream in,
                           HttpResponse httpMethod);
}
