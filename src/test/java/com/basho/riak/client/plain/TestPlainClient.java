package com.basho.riak.client.plain;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.WalkResponse;

public class TestPlainClient {
    
    final String bucket = "bucket";
    final String key = "key";
    final String walkSpec = "walkSpec";

    @Mock RiakBucketInfo bucketInfo;
    @Mock RiakClient mockRiakClient;
    @Mock RiakObject object;
    @Mock RequestMeta meta;
    PlainClient impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        impl = new PlainClient(mockRiakClient);
    }

    @Test public void all_methods_defer_to_impl() throws IOException {
    }
    
    @Test public void methods_translate_runtime_to_checked_exceptions() {
        
    }

    @Test public void setBucketSchema_throws_except_for_204(){
        HttpResponse mockResponse = mock(HttpResponse.class);

        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockRiakClient.setBucketSchema(bucket, bucketInfo, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.setBucketSchema(bucket, bucketInfo, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 204 }, i, threw));
        }
    }

    @Test public void listBucket_throws_except_for_200() {
        BucketResponse mockResponse = mock(BucketResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockRiakClient.listBucket(bucket, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.listBucket(bucket, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200 }, i, threw));
        }
    }

    @Test public void store_throws_except_for_200_and_204() {
        StoreResponse mockResponse = mock(StoreResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockRiakClient.store(object, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.store(object, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200, 204 }, i, threw));
        }
    }

    @Test public void fetchMeta_throws_except_for_200_304_or_404() {
        FetchResponse mockResponse = mock(FetchResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockResponse.hasObject()).thenReturn(true);
            when(mockRiakClient.fetchMeta(bucket, key, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.fetchMeta(bucket, key, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200, 304, 404 }, i, threw));
        }
    }

    @Test public void fetchMeta_returns_null_on_404() {
        
    }
    
    @Test public void fetchMeta_throws_if_metadata_not_returned() {
        
    }

    @Test public void fetch_throws_except_for_200_304_or_404() {
        FetchResponse mockResponse = mock(FetchResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockResponse.hasObject()).thenReturn(true);
            when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.fetch(bucket, key, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200, 304, 404 }, i, threw));
        }
    }
    
    @Test public void fetch_returns_null_on_404() {
        
    }

    @Test public void fetch_throws_if_object_not_returned() {
        
    }

    @Test public void fetchAll_throws_except_for_200_304_or_404() {
        FetchResponse mockResponse = mock(FetchResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockResponse.hasObject()).thenReturn(true);
            when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.fetchAll(bucket, key, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200, 304, 404 }, i, threw));
        }
    }
    
    @Test public void fetchAll_returns_null_on_404() {
        
    }

    @Test public void fetchAll_returns_siblings_if_exists() {
        
    }

    @Test public void fetchAll_returns_object_if_no_siblings() {
        
    }

    @Test public void fetchAll_throws_if_object_not_returned() {
        
    }

    @Test public void delete_throws_except_for_204_and_404() {
        HttpResponse mockResponse = mock(HttpResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockRiakClient.delete(bucket, key, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.delete(bucket, key, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 204, 404 }, i, threw));
        }
    }

    @Test public void walk_throws_except_for_200_and_404() {
        WalkResponse mockResponse = mock(WalkResponse.class);
        for (int i = 200; i < 600; i++) {
            when(mockResponse.getStatusCode()).thenReturn(i);
            when(mockResponse.hasSteps()).thenReturn(true);
            when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenReturn(mockResponse);
            boolean threw = true;
            try {
                impl.walk(bucket, key, walkSpec, meta);
                threw = false;
            } catch (RiakIOException e) {
            } catch (RiakResponseException e) {
            }
            assertTrue("Wrong behavior for status " + i, throwsForAllStatusesExcept(new int[] { 200, 404 }, i, threw));
        }
    }

    @Test public void walk_returns_null_on_404() {
        
    }

    @Test public void walk_throws_if_steps_not_returned() {
        
    }

    private boolean throwsForAllStatusesExcept(int[] okStatus, int status, boolean threw) {
        boolean ok = false;
        for (int s : okStatus) {
            if (status == s)
                ok = true;
        }
        return (ok && !threw) || (!ok && threw);
    }
}
