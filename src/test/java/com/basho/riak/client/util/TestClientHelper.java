package com.basho.riak.client.util;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StreamHandler;

public class TestClientHelper {

    final RiakConfig config = new RiakConfig();
    final String bucket = "bucket";
    final String key = "key";
    final String walkSpec = "walkSpec";
    final RequestMeta meta = new RequestMeta();

    @Mock HttpClient mockHttpClient;
    @Mock JSONObject schema;
    @Mock RiakObject object;
    ClientHelper impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);

        when(object.getBucket()).thenReturn(bucket);
        when(object.getKey()).thenReturn(key);

        impl = new ClientHelper(config);
        impl.setHttpClient(mockHttpClient);
    }
    
    @Test public void fetch_defaults_to_not_streaming() {
        impl = spy(impl);
        impl.fetch(bucket, key, meta);
        verify(impl).fetch(bucket, key, meta, false);
    }

    @Test public void setBucketSchema_PUTs_to_bucket_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket));
        impl.setBucketSchema(bucket, schema, meta);
        verify(mockHttpClient).executeMethod(any(PutMethod.class));
    }

    @Test public void listBucket_GETs_bucket_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket));
        impl.listBucket(bucket, meta);
        verify(mockHttpClient).executeMethod(any(GetMethod.class));
    }
    
    @Test public void store_PUTs_object_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.store(object, meta);
        verify(mockHttpClient).executeMethod(any(PutMethod.class));
    }
    
    @Test public void fetchMeta_HEADs_object_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.fetchMeta(bucket, key, meta);
        verify(mockHttpClient).executeMethod(any(HeadMethod.class));
    }

    @Test public void fetchMeta_adds_default_R_value() {
        
    }

    @Test public void fetch_GETs_object_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.fetch(bucket, key, meta);
        verify(mockHttpClient).executeMethod(any(GetMethod.class));
    }
    
    @Test public void fetch_adds_default_R_value() {
        impl.fetch(bucket, key, meta);
        assertEquals(Integer.toString(Constants.DEFAULT_R), meta.getQueryParam(Constants.QP_R));
    }
    
    @Test public void stream_GETs_object_URL() throws IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.stream(bucket, key, mock(StreamHandler.class), meta);
        verify(mockHttpClient).executeMethod(any(GetMethod.class));
    }

    @Test public void stream_adds_default_R_value() throws IOException {
        impl.stream(bucket, key, mock(StreamHandler.class), meta);
        assertEquals(Integer.toString(Constants.DEFAULT_R), meta.getQueryParam(Constants.QP_R));
    }
    
    @Test public void delete_DELETEs_object_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key));
        impl.delete(bucket, key, meta);
        verify(mockHttpClient).executeMethod(any(DeleteMethod.class));
    }
    
    @Test public void walk_GETs_object_URL() throws HttpException, IOException {
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenAnswer(pathVerifier("/" + bucket + "/" + key + "/" + walkSpec));
        impl.walk(bucket, key, walkSpec, meta);
        verify(mockHttpClient).executeMethod(any(GetMethod.class));
    }

    @Test public void all_methods_add_query_params() throws HttpException, IOException {

        impl.setBucketSchema(bucket, schema, meta);
        impl.listBucket(bucket, meta);
        impl.store(object, meta);
        impl.fetchMeta(bucket, key, meta);
        impl.fetch(bucket, key, meta);
        impl.delete(bucket, key, meta);
        impl.walk(bucket, key, walkSpec, meta);
    }

    @Test public void execute_method_adds_headers() throws HttpException, IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 
        meta.setHeader("p", "v");
        
        impl.executeMethod(null, null, mockHttpMethod, meta);
        
        verify(mockHttpClient).executeMethod(mockHttpMethod);
        verify(mockHttpMethod).setRequestHeader("p", "v");
    }

    @Test public void execute_method_adds_query_params() throws HttpException, IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 
        meta.setQueryParam("p", "v");
        
        impl.executeMethod(null, null, mockHttpMethod, meta);
        
        verify(mockHttpClient).executeMethod(mockHttpMethod);
        verify(mockHttpMethod).setQueryString(contains("p=v"));
    }

    @Test public void execute_method_without_stream_response_closes_connection() throws IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 
        
        impl.executeMethod(null, null, mockHttpMethod, meta);
        
        verify(mockHttpMethod).releaseConnection();
    }

    @Test public void execute_method_with_stream_response_returns_null_body() throws IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 
        
        HttpResponse r = impl.executeMethod(null, null, mockHttpMethod, meta, true);
        
        assertNull(r.getBody());
    }

    @Test public void execute_method_with_stream_response_doesnt_consume_stream_or_close_connection() throws IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 
        
        impl.executeMethod(null, null, mockHttpMethod, meta, true);
        
        verify(mockHttpMethod, never()).getResponseBody();
        verify(mockHttpMethod, never()).getResponseBodyAsString();
        verify(mockHttpMethod, never()).releaseConnection();
    }

    @Test public void execute_method_defers_exceptions_to_toss() throws HttpException, IOException {
        HttpMethod mockHttpMethod = mock(HttpMethod.class); 

        impl = spy(impl);
        doReturn(null).when(impl).toss(any(RiakIORuntimeException.class));
        when(mockHttpClient.executeMethod(any(HttpMethod.class))).thenThrow(new IOException()).thenThrow(new HttpException());

        impl.executeMethod(null, null, mockHttpMethod, meta);
        impl.executeMethod(null, null, mockHttpMethod, meta);
        
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

    private Answer<Integer> pathVerifier(final String pathSuffix) {
        return new Answer<Integer>() {
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                String path = ((HttpMethod) invocation.getArguments()[0]).getURI().getPath();
                assertTrue("URL path should end with " + pathSuffix + " but was '" + path + "'", path.endsWith(pathSuffix) || path.endsWith(pathSuffix + "?"));
                return 0;
            }
        };
    }
}
