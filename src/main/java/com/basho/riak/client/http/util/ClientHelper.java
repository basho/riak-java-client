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
package com.basho.riak.client.http.util;

import static com.basho.riak.client.util.CharsetUtils.utf8StringToBytes;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.DefaultHttpResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.RiakExceptionHandler;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;
import com.basho.riak.client.http.response.StreamHandler;
import com.basho.riak.client.util.CharsetUtils;

/**
 * This class performs the actual HTTP requests underlying the operations in
 * RiakClient and returns the resulting HTTP responses. It is up to RiakClient
 * to interpret the responses and translate them into the appropriate format.
 */
public class ClientHelper {

    private RiakConfig config;
    private HttpClient httpClient;
    private String clientId = null;
    private RiakExceptionHandler exceptionHandler = null;

    public ClientHelper(RiakConfig config, String clientId) {
        this.config = config;
        httpClient = ClientUtils.newHttpClient(config);
        setClientId(clientId);
    }

    /** Used for testing -- inject an HttpClient */
    void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * See {@link RiakClient#getClientId()}
     */
    public byte[] getClientId() {
        return Base64.decodeBase64(utf8StringToBytes(clientId));
    }

    public void setClientId(String clientId) {
        if (clientId != null) {
            this.clientId = ClientUtils.encodeClientId(clientId);
        } else {
            this.clientId = ClientUtils.randomClientId();
        }
    }

    /**
     * See
     * {@link RiakClient#setBucketSchema(String, com.basho.riak.client.http.RiakBucketInfo, RequestMeta)}
     */
    public HttpResponse setBucketSchema(String bucket, JSONObject schema, RequestMeta meta) {
        if (schema == null) {
            schema = new JSONObject();
        }
        if (meta == null) {
            meta = new RequestMeta();
        }

        meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);

        HttpPut put = new HttpPut(ClientUtils.makeURI(config, bucket));
        ByteArrayEntity entity = new ByteArrayEntity(utf8StringToBytes(schema.toString()));
        entity.setContentType(Constants.CTYPE_JSON_UTF8);
        put.setEntity(entity);

        return executeMethod(bucket, null, put, meta);
    }

    /**
     * Same as {@link RiakClient#getBucketSchema(String, RequestMeta)}, except
     * only returning the HTTP response.
     */
    public HttpResponse getBucketSchema(String bucket, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_KEYS) == null) {
            meta.setQueryParam(Constants.QP_KEYS, Constants.NO_KEYS);
        }
        return listBucket(bucket, meta, false);
    }

    /**
     * List the buckets in Riak
     * 
     * @return an {@link HttpResponse} whose body should be the result of asking
     *         Riak to list buckets.
     */
    public HttpResponse listBuckets() {
        final RequestMeta  meta = new RequestMeta();
        meta.setQueryParam(Constants.QP_BUCKETS, Constants.LIST_BUCKETS);
        HttpGet get = new HttpGet(config.getUrl());
        return executeMethod(null, null, get, meta);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response, and
     * if streamResponse==true, the response will be streamed back, so the user
     * is responsible for calling {@link BucketResponse#close()}
     */
    public HttpResponse listBucket(String bucket, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_KEYS) == null) {
            if (streamResponse) {
                meta.setQueryParam(Constants.QP_KEYS, Constants.STREAM_KEYS);
            } else {
                meta.setQueryParam(Constants.QP_KEYS, Constants.INCLUDE_KEYS);
            }
        }
        if (meta.getHeader(Constants.HDR_CONTENT_TYPE) == null) {
            meta.setHeader(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        }
        if (meta.getHeader(Constants.HDR_ACCEPT) == null) {
            meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        }

        HttpGet get = new HttpGet(ClientUtils.makeURI(config, bucket));
        return executeMethod(bucket, null, get, meta, streamResponse);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public HttpResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getClientId() == null) {
            meta.setClientId(clientId);
        }
        if (meta.getHeader(Constants.HDR_CONNECTION) == null) {
            meta.setHeader(Constants.HDR_CONNECTION, "keep-alive");
        }

        String bucket = object.getBucket();
        String key = object.getKey();
        String url = ClientUtils.makeURI(config, bucket, key);
        HttpPut put = new HttpPut(url);

        object.writeToHttpMethod(put);
        return executeMethod(bucket, key, put, meta);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public HttpResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        HttpHead head = new HttpHead(ClientUtils.makeURI(config, bucket, key));
        return executeMethod(bucket, key, head, meta);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response and
     * allows the response to be streamed.
     * 
     * @param bucket
     *            Same as {@link RiakClient}
     * @param key
     *            Same as {@link RiakClient}
     * @param meta
     *            Same as {@link RiakClient}
     * @param streamResponse
     *            If true, the connection will NOT be released. Use
     *            HttpResponse.getHttpMethod().getResponseBodyAsStream() to get
     *            the response stream; HttpResponse.getBody() will return null.
     * 
     * @return Same as {@link RiakClient}
     */
    public HttpResponse fetch(String bucket, String key, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        HttpGet get = new HttpGet(ClientUtils.makeURI(config, bucket, key));
        return executeMethod(bucket, key, get, meta, streamResponse);
    }

    public HttpResponse fetch(String bucket, String key, RequestMeta meta) {
        return fetch(bucket, key, meta, false);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        HttpGet get = new HttpGet(ClientUtils.makeURI(config, bucket, key));
        try {
            org.apache.http.HttpResponse response = httpClient.execute(get);
            HttpEntity entity = response.getEntity();

            boolean result = true;
            if (handler != null) {

                result = handler.process(bucket, key, response.getStatusLine().getStatusCode(),
                                         ClientUtils.asHeaderMap(response.getAllHeaders()), entity.getContent(),
                                         response);
            }
            EntityUtils.consume(entity);

            return result;
        } catch (IOException e) {
            get.abort();
            throw e;
        }
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public HttpResponse delete(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        String url = ClientUtils.makeURI(config, bucket, key);
        HttpDelete delete = new HttpDelete(url);
        return executeMethod(bucket, key, delete, meta);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public HttpResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        HttpGet get = new HttpGet(ClientUtils.makeURI(config, bucket, key, walkSpec));
        return executeMethod(bucket, key, get, meta);
    }

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public HttpResponse mapReduce(String job, RequestMeta meta) {
        HttpPost post = new HttpPost(config.getMapReduceUrl());
        try {
            StringEntity entity = new StringEntity(job, Constants.CTYPE_JSON_UTF8, CharsetUtils.UTF_8.name());
            post.setEntity(entity);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("StringEntity should always support UTF-8 charset", e);
        }
        return executeMethod(null, null, post, meta);
    }

    /** @return the installed exception handler or null if not installed */
    public RiakExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Install an exception handler. If an exception handler is provided, then
     * the Riak client will hand exceptions to the handler rather than throwing
     * them.
     */
    public void setExceptionHandler(RiakExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Hands exception <code>e</code> to installed exception handler if there is
     * one or throw it.
     * 
     * @return A 0-status {@link HttpResponse}.
     */
    public HttpResponse toss(RiakIORuntimeException e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
            return new DefaultHttpResponse(null, null, 0, null, null, null, null, null);
        } else
            throw e;
    }

    public HttpResponse toss(RiakResponseRuntimeException e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
            return new DefaultHttpResponse(null, null, 0, null, null, null, null, null);
        } else
            throw e;
    }

    /**
     * Return the {@link HttpClient} used to make requests, which can be
     * configured.
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * @return The config used to construct the HttpClient connecting to Riak.
     */
    public RiakConfig getConfig() {
        return config;
    }

    /**
     * Perform and HTTP request and return the resulting response using the
     * internal HttpClient.
     * 
     * @param bucket
     *            Bucket of the object receiving the request.
     * @param key
     *            Key of the object receiving the request or null if the request
     *            is for a bucket.
     * @param httpMethod
     *            The HTTP request to perform; must not be null.
     * @param meta
     *            Extra HTTP headers to attach to the request. Query parameters
     *            are ignored; they should have already been used to construct
     *            <code>httpMethod</code> and query parameters.
     * @param streamResponse
     *            If true, the connection will NOT be released. Use
     *            HttpResponse.getHttpMethod().getResponseBodyAsStream() to get
     *            the response stream; HttpResponse.getBody() will return null.
     * 
     * @return The HTTP response returned by Riak from executing
     *         <code>httpMethod</code>.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server
     *             (i.e. HttpClient threw an IOException)
     */
    HttpResponse executeMethod(String bucket, String key, HttpRequestBase httpMethod, RequestMeta meta,
                               boolean streamResponse) {

        if (meta != null) {
            Map<String, String> headers = meta.getHeaders();
            for (String header : headers.keySet()) {
                httpMethod.addHeader(header, headers.get(header));
            }

            Map<String, String> queryParams = meta.getQueryParamMap();
            if (!queryParams.isEmpty()) {
                URI originalURI = httpMethod.getURI();
                List<NameValuePair> currentQuery = URLEncodedUtils.parse(originalURI, CharsetUtils.UTF_8.name());
                List<NameValuePair> newQuery = new LinkedList<NameValuePair>(currentQuery);

                for(Map.Entry<String, String> qp : queryParams.entrySet()) {
                    newQuery.add(new BasicNameValuePair(qp.getKey(), qp.getValue()));
                }

                // For this, HC4.1 authors, I hate you
                URI newURI;
                try {
                    newURI = URIUtils.createURI(originalURI.getScheme(),
                                                           originalURI.getHost(),
                                                           originalURI.getPort(),
                                                           originalURI.getPath(),
                                                           URLEncodedUtils.format(newQuery, "UTF-8"), null);
                } catch (URISyntaxException e) {
                    throw new RiakIORuntimeException(e);
                }
                httpMethod.setURI(newURI);
            }
        }
        HttpEntity entity;
        try {
            org.apache.http.HttpResponse response =  httpClient.execute(httpMethod);

            int status = 0;
            if (response.getStatusLine() != null) {
                status = response.getStatusLine().getStatusCode();
            }

            Map<String, String> headers = ClientUtils.asHeaderMap(response.getAllHeaders());
            byte[] body = null;
            InputStream stream = null;
            entity = response.getEntity();

            if (streamResponse) {
                stream = entity.getContent();
            } else {
                if(null != entity) {
                    body = EntityUtils.toByteArray(entity);
                }
            }

            if (!streamResponse) {
                EntityUtils.consume(entity);
            }

            return new DefaultHttpResponse(bucket, key, status, headers, body, stream, response, httpMethod);
        } catch (IOException e) {
            httpMethod.abort();
            return toss(new RiakIORuntimeException(e));
        }
    }

    HttpResponse executeMethod(String bucket, String key, HttpRequestBase httpMethod, RequestMeta meta) {
        return executeMethod(bucket, key, httpMethod, meta, false);
    }
}
