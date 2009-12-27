package com.basho.riak.client.jiak;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.util.ClientHelper;
import com.basho.riak.client.util.Constants;

public class TestJiakClient {

    final String bucket = "bucket";
    final String key = "key";
    final String walkSpec = "walkSpec";

    @Mock ClientHelper mockHelper;
    @Mock RiakBucketInfo bucketInfo;
    @Mock StreamHandler handler;
    @Mock RequestMeta meta;
    @Mock RiakObject object;
    @Mock RiakWalkSpec riakWalkSpec;
    @Mock HttpResponse mockHttpResponse;
    JiakClient impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(new HashMap<String, String>());
        impl = new JiakClient(mockHelper);
    }
    
    @Test public void all_interface_methods_defer_to_helper() throws IOException {

        impl.setBucketSchema(bucket, bucketInfo, meta);
        verify(mockHelper).setBucketSchema(eq(bucket), any(JSONObject.class), same(meta));

        impl.listBucket(bucket, meta);
        verify(mockHelper).listBucket(bucket, meta);

        impl.store(object, meta);
        verify(mockHelper).store(object, meta);

        impl.fetchMeta(bucket, key, meta);
        verify(mockHelper).fetch(bucket, key, meta); // Jiak should use fetch, since it doesn't return proper metadata in HEAD
        reset(mockHelper);

        impl.fetch(bucket, key, meta);
        verify(mockHelper).fetch(bucket, key, meta);

        impl.stream(bucket, key, handler, meta);
        verify(mockHelper).stream(bucket, key, handler, meta);

        impl.delete(bucket, key, meta);
        verify(mockHelper).delete(bucket, key, meta);

        impl.walk(bucket, key, walkSpec, meta);
        verify(mockHelper).walk(bucket, key, walkSpec, meta);
    }
    
    @Test public void convenience_methods_defer_to_main_methods_with_null_meta() {
        impl = spy(impl);
        
        impl.setBucketSchema(bucket, bucketInfo);
        verify(impl).setBucketSchema(bucket, bucketInfo, null);
        
        impl.listBucket(bucket);
        verify(impl).listBucket(bucket, null);
        
        impl.store(object);
        verify(impl).store(object, null);
        
        impl.fetchMeta(bucket, key);
        verify(impl).fetchMeta(bucket, key, null);
        reset(impl); // need to reset since fetchMeta defers to fetch.
        
        impl.fetch(bucket, key);
        verify(impl).fetch(bucket, key, null);
        
        impl.delete(bucket, key);
        verify(impl).delete(bucket, key, null);

        when(riakWalkSpec.toString()).thenReturn(walkSpec);
        impl.walk(bucket, key, walkSpec);
        impl.walk(bucket, key, riakWalkSpec);
        verify(impl, times(2)).walk(bucket, key, walkSpec, null);
    }
    
    @Test public void methods_defer_to_helper_on_exception() {
        // return invalid json body to cause methods to throw
        final String body = "invalid json";
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        when(mockHttpResponse.getBody()).thenReturn(body);

        when(mockHelper.listBucket(bucket, meta)).thenReturn(mockHttpResponse);
        impl.listBucket(bucket, meta);
        verify(mockHelper).toss(any(RiakResponseException.class));
        reset(mockHelper);
        
        when(mockHelper.store(object, meta)).thenReturn(mockHttpResponse);
        impl.store(object, meta);
        verify(mockHelper).toss(any(RiakResponseException.class));
        reset(mockHelper);

        when(mockHelper.fetch(bucket, key, meta)).thenReturn(mockHttpResponse);
        impl.fetchMeta(bucket, key, meta);
        verify(mockHelper).toss(any(RiakResponseException.class));
        reset(mockHelper);
        
        when(mockHelper.fetch(bucket, key, meta)).thenReturn(mockHttpResponse);
        impl.fetch(bucket, key, meta);
        verify(mockHelper).toss(any(RiakResponseException.class));
        reset(mockHelper);
        
        when(mockHelper.walk(bucket, key, walkSpec, meta)).thenReturn(mockHttpResponse);
        impl.walk(bucket, key, walkSpec, meta);
        verify(mockHelper).toss(any(RiakResponseException.class));
        reset(mockHelper);
    }

    @Test public void setBucketSchema_puts_schema_in_schema_field() {
        final JSONObject mockJSONObject = mock(JSONObject.class);
        
        when(mockHelper.setBucketSchema(eq(bucket), any(JSONObject.class), same(meta))).thenAnswer(new Answer<HttpResponse>() {
            public HttpResponse answer(InvocationOnMock invocation) throws Throwable {
                assertSame(mockJSONObject,
                           ((JSONObject) invocation.getArguments()[1])      // second argument is "schema"
                               .getJSONObject(Constants.JIAK_FL_SCHEMA));
                return null;
            } 
        });
        when(bucketInfo.getSchema()).thenReturn(mockJSONObject);
        
        impl.setBucketSchema(bucket, bucketInfo, meta);
        
        // since we need to peer into the internals of the JSONObject created by setBucketSchema(), 
        // verification is done in the Answer impl above.
    }

    @Test public void store_adds_return_body_query_parameter() {
        impl.store(object, meta);
        verify(meta).setQueryParam(Constants.QP_RETURN_BODY, "true");
    }
}
