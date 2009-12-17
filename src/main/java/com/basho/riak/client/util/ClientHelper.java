/*
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.  
*/
package com.basho.riak.client.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.DefaultHttpResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIOException;
import com.basho.riak.client.response.StreamHandler;

public class ClientHelper {

    private RiakConfig config;
    private HttpClient httpClient;
    
    public ClientHelper(RiakConfig config) {
        this.config = config;
        this.httpClient = ClientUtils.newHttpClient(config);
    }

    public HttpResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields,
            RequestMeta meta) {
        
        if (meta == null) meta = new RequestMeta();
        if (requiredFields == null) requiredFields = new ArrayList<String>();
        if (writeMask == null) writeMask = new ArrayList<String>(allowedFields);
        if (readMask == null) readMask = new ArrayList<String>(allowedFields);

        final JSONObject schema = new JSONObject();
        final JSONObject schemaReqBody = new JSONObject();

        try {
            schema.put("allowed_fields", allowedFields);
            schema.put("required_fields", requiredFields);
            schema.put("read_mask", readMask);
            schema.put("write_mask", writeMask);
            schemaReqBody.put("schema", schema);
        } catch (JSONException unreached) {
            throw new IllegalStateException("should not happen", unreached);
        }

        meta.put(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        
        PutMethod put = new PutMethod(ClientUtils.makeURI(config, bucket));
        return executeMethod(bucket, null, put, meta);
    }
    public HttpResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields) {
        return setBucketSchema(bucket, allowedFields, writeMask, readMask, requiredFields, null);
    }
    
    public HttpResponse listBucket(String bucket, RequestMeta meta) {
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket));
        get.setRequestHeader(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        get.setRequestHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        return executeMethod(bucket, null, get, meta);
    }
    public HttpResponse listBucket(String bucket) {
        return listBucket(bucket, null);
    }

    public HttpResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        String bucket = object.getBucket();
        String key = object.getKey();
        String url = ClientUtils.makeURI(config, bucket, key, "?" + meta.getQueryParams());
        PutMethod put = new PutMethod(url);
        
        if (object.getEntityStream() != null) {
            if (object.getEntityStreamLength() >= 0) {
                put.setRequestEntity(
                        new InputStreamRequestEntity(
                                object.getEntityStream(),
                                object.getEntityStreamLength(),
                                object.getContentType()));
            } else {
                put.setRequestEntity(
                        new InputStreamRequestEntity(
                                object.getEntityStream(),
                                object.getContentType()));
            }
        } else if (object.getEntity() != null) {
            put.setRequestEntity(
                    new ByteArrayRequestEntity(
                            object.getEntity().getBytes(),
                            object.getContentType()));
        }

        return executeMethod(bucket, key, put, meta);
    }
    public HttpResponse store(RiakObject object) {
        return store(object, null);
    }
    
    public HttpResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        if (meta.getQueryParam(Constants.QP_R) == null) 
            meta.addQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        HeadMethod head = new HeadMethod(ClientUtils.makeURI(config, bucket, key, "?" + meta.getQueryParams()));
        return executeMethod(bucket, key, head, meta);
    }
    public HttpResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    public HttpResponse fetch(String bucket, String key, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        if (meta.getQueryParam(Constants.QP_R) == null) 
            meta.addQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key, "?" + meta.getQueryParams()));
        return executeMethod(bucket, key, get, meta);
    }
    public HttpResponse fetch(String bucket, String key) {
        return fetch(bucket, key, null);
    }
    
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        if (meta == null) meta = new RequestMeta();
        if (meta.getQueryParam(Constants.QP_R) == null) 
            meta.addQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key, "?" + meta.getQueryParams()));
        try {
            int status = httpClient.executeMethod(get);
            if (handler == null)
                return true;
            
            return handler.process(
                    bucket, key, 
                    status, 
                    ClientUtils.asHeaderMap(get.getResponseHeaders()), 
                    get.getResponseBodyAsStream(), 
                    get);
        } finally {
            get.releaseConnection();
        }
    }

    public HttpResponse delete(String bucket, String key, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        String url = ClientUtils.makeURI(config, bucket, key, "?" + meta.getQueryParams());
        DeleteMethod delete = new DeleteMethod(url);
        return executeMethod(bucket, key, delete, meta);
    }
    public HttpResponse delete(String bucket, String key) {
        return delete(bucket, key, null);
    }

    public HttpResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key, walkSpec));
        return executeMethod(bucket, key, get, meta);
    }
    public HttpResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }
    public HttpResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
    
    protected RiakConfig getConfig() {
        return this.config;
    }

    protected HttpResponse executeMethod(String bucket, String key, HttpMethod httpMethod, RequestMeta meta) {

        if (meta != null) {
            Map<String, String> headers = meta.getHttpHeaders();
            for (String header : headers.keySet())  {
                httpMethod.setRequestHeader(header, headers.get(header));
            }
        }

        try {
            httpClient.executeMethod(httpMethod);
            return DefaultHttpResponse.fromHttpMethod(bucket, key, httpMethod);
        } catch (IOException e) {
            throw new RiakIOException(e);
        } finally {
            httpMethod.releaseConnection();
        }
    }
}
