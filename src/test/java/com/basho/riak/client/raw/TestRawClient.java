package com.basho.riak.client.raw;

import static org.mockito.Mockito.*;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.util.ClientHelper;

public class TestRawClient {

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
    RawClient impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(new HashMap<String, String>());
        impl = new RawClient(mockHelper);
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
        
        impl.fetch(bucket, key);
        verify(impl).fetch(bucket, key, null);
        
        impl.delete(bucket, key);
        verify(impl).delete(bucket, key, null);

        when(riakWalkSpec.toString()).thenReturn(walkSpec);
        impl.walk(bucket, key, walkSpec);
        impl.walk(bucket, key, riakWalkSpec);
        verify(impl, times(2)).walk(bucket, key, walkSpec, null);
    }}
