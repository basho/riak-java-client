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

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.RiakExceptionHandler;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;
import com.basho.riak.client.http.response.StreamHandler;
import com.basho.riak.client.http.util.ClientHelper;
import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.util.CharsetUtils;

public class TestClientHelper {

    final RiakConfig config = new RiakConfig("http://127.0.0.1:8098/riak");
    final String bucket = "bucket";
    final String key = "key";
    final String walkSpec = "walkSpec";
    final String mrJob = "mrJob";
    final String clientId = "test";
    final RequestMeta meta = new RequestMeta();

    @Mock HttpClient mockHttpClient;
    @Mock JSONObject schema;
    @Mock RiakObject object;
    @Mock org.apache.http.HttpResponse mockHttpResponse;
    @Mock HttpEntity mockHttpEntity;
    ClientHelper impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);

        when(object.getBucket()).thenReturn(bucket);
        when(object.getKey()).thenReturn(key);

        impl = new ClientHelper(config, clientId);
        impl.setHttpClient(mockHttpClient);
    }
    
    @Test public void client_helper_uses_passed_in_client_id() throws UnsupportedEncodingException {
        assertEquals(clientId, CharsetUtils.asUTF8String(impl.getClientId()));
    }

    @Test public void client_helper_generates_client_id_if_null() {
        impl = new ClientHelper(config, null);
        assertNotNull(impl.getClientId());
        assertEquals(4, impl.getClientId().length);
    }

    @Test public void fetch_defaults_to_not_streaming() throws IOException {
        stubResponse(true);
        impl = spy(impl);
        impl.fetch(bucket, key, meta);
        verify(impl).fetch(bucket, key, meta, false);
    }

    @Test public void setBucketSchema_PUTs_to_bucket_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket));
        stubResponse(false);
        impl.setBucketSchema(bucket, schema, meta);
        verify(mockHttpClient).execute(any(HttpPut.class));
    }

    @Test public void getBucketSchema_GETs_bucket_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket));
        stubResponse(false);
        impl.getBucketSchema(bucket, meta);
        verify(mockHttpClient).execute(any(HttpGet.class));
    }

    @Test public void getBucketSchema_adds_no_keys_qp() throws IOException {
        RequestMeta meta = spy(new RequestMeta());
        stubResponse(true);
        impl = spy(impl);
        impl.getBucketSchema(bucket, meta);
        verify(meta).setQueryParam(Constants.QP_KEYS, Constants.NO_KEYS);
        verify(impl).executeMethod(eq(bucket), anyString(), any(HttpGet.class), same(meta), eq(false));
    }

    @Test public void listBucket_GETs_bucket_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket));
        stubResponse(false);
        impl.listBucket(bucket, meta, false);
        verify(mockHttpClient).execute(any(HttpGet.class));
    }

    @Test public void listBuckets_GET_adds_qp() throws IOException {
        impl = spy(impl);
        stubResponse(true);
        impl.listBuckets(false);
        ArgumentCaptor<RequestMeta> metaCaptor = ArgumentCaptor.forClass(RequestMeta.class);
        verify(impl).executeMethod(eq((String) null), eq((String) null), any(HttpGet.class), metaCaptor.capture(), eq(false));

        RequestMeta capturedMeta = metaCaptor.getValue();
        assertEquals(capturedMeta.getQueryParam(Constants.QP_BUCKETS), Constants.LIST_BUCKETS);
    }
    
    @Test public void listBucket_adds_keys_qp_when_streaming_response() throws IOException {
        RequestMeta meta = spy(new RequestMeta());
        impl = spy(impl);
        stubResponse(true);
        impl.listBucket(bucket, meta, true);
        verify(meta).setQueryParam(Constants.QP_KEYS, Constants.STREAM_KEYS);
        verify(impl).executeMethod(eq(bucket), anyString(), any(HttpGet.class), same(meta), eq(true));
    }
    
    @Test public void listBucket_add_keys_equals_true_qp_if_not_streaming_response() throws IOException {
        RequestMeta meta = spy(new RequestMeta());
        impl = spy(impl);
        stubResponse(true);
        impl.listBucket(bucket, meta, false);
        verify(meta).setQueryParam(eq(Constants.QP_KEYS), eq(Constants.INCLUDE_KEYS));
        verify(impl).executeMethod(eq(bucket), anyString(), any(HttpGet.class), same(meta), eq(false));
    }
    
    @Test public void store_PUTs_object_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        stubResponse(false);
        impl.store(object, meta);
        verify(mockHttpClient).execute(any(HttpPut.class));
    }
    
    @Test public void store_sets_client_id() throws IOException {
        stubResponse(true);
        impl.store(object, meta);
        assertEquals(ClientUtils.encodeClientId(clientId), meta.getClientId());
    }
    
    @Test public void store_doesnt_overwrite_client_id() throws IOException {
        stubResponse(true);
        meta.setClientId("clientId");
        impl.store(object, meta);
        assertEquals("clientId", meta.getClientId());
    }

    @Test public void store_sets_connection_header() throws IOException {
        stubResponse(true);
        impl.store(object, meta);
        assertEquals("keep-alive", meta.getHeader(Constants.HDR_CONNECTION));
    }

    @Test public void store_doesnt_overwrite_connection_header() throws IOException {
        stubResponse(true);
        meta.setHeader(Constants.HDR_CONNECTION, "connection");
        impl.store(object, meta);
        assertEquals("connection", meta.getHeader(Constants.HDR_CONNECTION));
    }

    @Test public void fetchMeta_HEADs_object_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        stubResponse(false);
        impl.fetchMeta(bucket, key, meta);
        verify(mockHttpClient).execute(any(HttpHead.class));
    }

    @Test public void fetch_GETs_object_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        stubResponse(false);
        impl.fetch(bucket, key, meta);
        verify(mockHttpClient).execute(any(HttpGet.class));
    }
    
    @Test public void stream_GETs_object_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        stubResponse(false);
        impl.stream(bucket, key, mock(StreamHandler.class), meta);
        verify(mockHttpClient).execute(any(HttpGet.class));
    }
    
    @Test public void delete_DELETEs_object_URL() throws IOException {
        stubResponse(false);
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.delete(bucket, key, meta);
        verify(mockHttpClient).execute(any(HttpDelete.class));
    }
    
    @Test public void walk_GETs_object_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key + "/" + walkSpec));
        stubResponse(false);
        impl.walk(bucket, key, walkSpec, meta);
        verify(mockHttpClient).execute(any(HttpGet.class));
    }
    
    @Test public void mapreduce_POSTs_to_mapred_URL() throws IOException {
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenAnswer(pathVerifier("/mapred"));
        stubResponse(false);
        impl.mapReduce(mrJob, meta);
        verify(mockHttpClient).execute(any(HttpPost.class));
    }

    @Test public void all_methods_add_query_params() throws IOException {
        stubResponse(true);
        impl.setBucketSchema(bucket, schema, meta);
        impl.listBucket(bucket, meta, false);
        impl.store(object, meta);
        impl.fetchMeta(bucket, key, meta);
        impl.fetch(bucket, key, meta);
        impl.delete(bucket, key, meta);
        impl.walk(bucket, key, walkSpec, meta);
        impl.mapReduce(mrJob, meta);
    }

    @Test public void execute_method_adds_headers() throws IOException {
        stubResponse(true);
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);
        meta.setHeader("p", "v");

        impl.executeMethod(null, null, mockHttpRequestBase, meta);

        verify(mockHttpClient).execute(mockHttpRequestBase);
        verify(mockHttpRequestBase).addHeader("p", "v");
    }

    @Test public void execute_method_adds_query_params() throws IOException, URISyntaxException {
        stubResponse(true);
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);
        when(mockHttpRequestBase.getURI()).thenReturn(new URI("http://127.0.0.1:8098/riak"));
        meta.setQueryParam("p", "v");

        impl.executeMethod(null, null, mockHttpRequestBase, meta);

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);

        verify(mockHttpClient).execute(mockHttpRequestBase);
        verify(mockHttpRequestBase).setURI(uriCaptor.capture());

        assertTrue(uriCaptor.getValue().getQuery().contains("p=v"));
    }

    @Test public void execute_method_without_stream_response_closes_connection() throws IOException {
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);
        
        InputStream is = new ByteArrayInputStream(CharsetUtils.utf8StringToBytes("a horse a horse my kingdom for a horse"));
        stubResponse(true);
        is = spy(is);
        when(mockHttpEntity.getContent()).thenReturn(is);
        impl.executeMethod(null, null, mockHttpRequestBase, meta);
        verify(is).close(); // close releases the connection, we have to trust hc authors on that
    }

    @Test public void execute_method_with_stream_response_returns_null_body() throws IOException {
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);
        stubResponse(true);
        HttpResponse r = impl.executeMethod(null, null, mockHttpRequestBase, meta, true);
        
        assertNull(r.getBody());
    }

    @Test public void execute_method_with_stream_response_doesnt_consume_stream_or_close_connection() throws IOException {
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);
        InputStream is = new ByteArrayInputStream(CharsetUtils.utf8StringToBytes("a horse a horse my kingdom for a horse"));
        stubResponse(true);
        is = spy(is);
        when(mockHttpEntity.getContent()).thenReturn(is);
        impl.executeMethod(null, null, mockHttpRequestBase, meta, true);
        
        verify(is, never()).close();
    }

    @Test public void execute_method_defers_exceptions_to_toss() throws IOException {
        HttpRequestBase mockHttpRequestBase = mock(HttpRequestBase.class);

        impl = spy(impl);
        doReturn(null).when(impl).toss(any(RiakIORuntimeException.class));
        when(mockHttpClient.execute(any(HttpRequestBase.class))).thenThrow(new IOException());

        impl.executeMethod(null, null, mockHttpRequestBase, meta);
        impl.executeMethod(null, null, mockHttpRequestBase, meta);
        
        verify(impl, times(2)).toss(any(RiakIORuntimeException.class));
    }

    @Test(expected=RiakIORuntimeException.class) public void toss_throws_io_exception_if_no_exception_handler_installed() {
        impl.toss(new RiakIORuntimeException());
    }
    @Test(expected=RiakResponseRuntimeException.class) public void toss_throws_response_exception_if_no_exception_handler_installed() {
        impl.toss(new RiakResponseRuntimeException(null));
    }

    @Test public void toss_doesnt_throw_if_exception_handler_installed() {
        impl.setExceptionHandler(mock(RiakExceptionHandler.class));
        impl.toss(new RiakIORuntimeException());
        impl.toss(new RiakResponseRuntimeException(null));
    }

    @Test public void store_will_use_post_when_no_key_is_provided() throws ClientProtocolException, IOException {
        when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockHttpResponse);
        RiakObject objectWithNoKey = new RiakObject(bucket, null);
        impl.store(objectWithNoKey, meta);
        ArgumentCaptor<HttpRequestBase> httpRequestBaseCaptor = ArgumentCaptor.forClass(HttpRequestBase.class);
        verify(mockHttpClient).execute(httpRequestBaseCaptor.capture());

        HttpRequestBase requestObject = httpRequestBaseCaptor.getValue();
        assertEquals("Store without key needs to be POST for Riak to understand.", "POST", requestObject.getMethod());
    }

    @Test public void store_will_return_key_generated_by_server_when_no_key_is_provided()
            throws ClientProtocolException, IOException {
        String generatedKey = "abc123";
        Header locationHeader = new BasicHeader("Location", "/riak/" + bucket + "/" + generatedKey);
        when(mockHttpResponse.getFirstHeader("Location")).thenReturn(locationHeader);
        when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockHttpResponse);
        RiakObject objectWithNoKey = new RiakObject(bucket, null);
        HttpResponse storeResponse = impl.store(objectWithNoKey, meta);

        assertEquals("Key generated by server should be assigned to object in response.", generatedKey,
                     storeResponse.getKey());
    }

    private Answer<org.apache.http.HttpResponse> pathVerifier(final String pathSuffix) {
        return new Answer<org.apache.http.HttpResponse>() {
            public org.apache.http.HttpResponse answer(InvocationOnMock invocation) throws Throwable {
                String path = ((HttpRequestBase) invocation.getArguments()[0]).getURI().getPath();
                assertTrue("URL path should end with " + pathSuffix + " but was '" + path + "'", path.endsWith(pathSuffix) || path.endsWith(pathSuffix + "?"));
                return mockHttpResponse;
            }
        };
    }

    private void stubResponse(boolean stubRequest) throws IOException {
        if(stubRequest) {
            when(mockHttpClient.execute(any(HttpRequestBase.class))).thenReturn(mockHttpResponse);
        }
        when(mockHttpResponse.getStatusLine()).thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    }
}
