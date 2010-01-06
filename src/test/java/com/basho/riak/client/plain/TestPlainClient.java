package com.basho.riak.client.plain;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.response.WalkResponse;

public class TestPlainClient {
    
    final String bucket = "bucket";
    final String key = "key";
    final String walkSpec = "walkSpec";

    @Mock RiakBucketInfo bucketInfo;
    @Mock RiakClient mockRiakClient;
    @Mock RiakObject object;
    @Mock RequestMeta meta;
    @Mock StreamHandler handler;
    PlainClient impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        impl = new PlainClient(mockRiakClient);
    }

    @Test public void methods_defer_to_impl() throws RiakIOException, RiakResponseException, IOException {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockRiakClient.setBucketSchema(bucket, bucketInfo, meta)).thenReturn(mockHttpResponse);
        when(mockHttpResponse.getStatusCode()).thenReturn(204);
        impl.setBucketSchema(bucket, bucketInfo, meta);
        verify(mockRiakClient).setBucketSchema(bucket, bucketInfo, meta);

        final BucketResponse mockBucketResponse = mock(BucketResponse.class);
        when(mockRiakClient.listBucket(bucket, meta)).thenReturn(mockBucketResponse);
        when(mockBucketResponse.getStatusCode()).thenReturn(200);
        impl.listBucket(bucket, meta);
        verify(mockRiakClient).listBucket(bucket, meta);

        final StoreResponse mockStoreResponse = mock(StoreResponse.class);
        when(mockRiakClient.store(object, meta)).thenReturn(mockStoreResponse);
        when(mockStoreResponse.getStatusCode()).thenReturn(200);
        impl.store(object, meta);
        verify(mockRiakClient).store(object, meta);

        final FetchResponse mockFetchResponse = mock(FetchResponse.class);
        when(mockRiakClient.fetchMeta(bucket, key, meta)).thenReturn(mockFetchResponse);
        when(mockFetchResponse.getStatusCode()).thenReturn(200);
        when(mockFetchResponse.hasObject()).thenReturn(true);
        impl.fetchMeta(bucket, key, meta);
        verify(mockRiakClient).fetchMeta(bucket, key, meta);

        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockFetchResponse);
        when(mockFetchResponse.getStatusCode()).thenReturn(200);
        when(mockFetchResponse.hasObject()).thenReturn(true);
        impl.fetch(bucket, key, meta);
        verify(mockRiakClient).fetch(bucket, key, meta);

        reset(mockRiakClient);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockFetchResponse);
        when(mockFetchResponse.getStatusCode()).thenReturn(200);
        when(mockFetchResponse.hasObject()).thenReturn(true);
        impl.fetchAll(bucket, key, meta);
        verify(mockRiakClient).fetch(bucket, key, meta);

        impl.stream(bucket, key, handler, meta);
        verify(mockRiakClient).stream(bucket, key, handler, meta);

        when(mockRiakClient.delete(bucket, key, meta)).thenReturn(mockHttpResponse);
        when(mockHttpResponse.getStatusCode()).thenReturn(204);
        impl.delete(bucket, key, meta);
        verify(mockRiakClient).delete(bucket, key, meta);

        final WalkResponse mockWalkResponse = mock(WalkResponse.class);
        when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenReturn(mockWalkResponse);
        when(mockWalkResponse.getStatusCode()).thenReturn(200);
        when(mockWalkResponse.hasSteps()).thenReturn(true);
        impl.walk(bucket, key, walkSpec, meta);
        verify(mockRiakClient).walk(bucket, key, walkSpec, meta);
    }
    
    // the below can be summarized as methods_translate_io_runtime_exception_to_checked()
    @Test(expected=RiakIOException.class) public void setBucketSchema_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.setBucketSchema(bucket, bucketInfo, meta)).thenThrow(new RiakIORuntimeException());
        impl.setBucketSchema(bucket, bucketInfo, meta);
    }
    @Test(expected=RiakIOException.class) public void listBucket_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.listBucket(bucket, meta)).thenThrow(new RiakIORuntimeException());
        impl.listBucket(bucket, meta);
    }
    @Test(expected=RiakIOException.class) public void store_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.store(object, meta)).thenThrow(new RiakIORuntimeException());
        impl.store(object, meta);
    }
    @Test(expected=RiakIOException.class) public void fetchMeta_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetchMeta(bucket, key, meta)).thenThrow(new RiakIORuntimeException());
        impl.fetchMeta(bucket, key, meta);
    }
    @Test(expected=RiakIOException.class) public void fetch_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetch(bucket, key, meta)).thenThrow(new RiakIORuntimeException());
        impl.fetch(bucket, key, meta);
    }
    @Test(expected=RiakIOException.class) public void fetchAll_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetch(bucket, key, meta)).thenThrow(new RiakIORuntimeException());
        impl.fetchAll(bucket, key, meta);
    }
    @Test(expected=RiakIOException.class) public void delete_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.delete(bucket, key, meta)).thenThrow(new RiakIORuntimeException());
        impl.delete(bucket, key, meta);
    }
    @Test(expected=RiakIOException.class) public void walk_translates_io_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenThrow(new RiakIORuntimeException());
        impl.walk(bucket, key, walkSpec, meta);
    }

    // the below can be summarized as methods_translate_response_runtime_exception_to_checked()
    // note, setBucketSchema & delete do not throw RiakResponseRuntimeExceptions
    @Test(expected=RiakResponseException.class) public void listBucket_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.listBucket(bucket, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.listBucket(bucket, meta);
    }
    @Test(expected=RiakResponseException.class) public void store_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.store(object, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.store(object, meta);
    }
    @Test(expected=RiakResponseException.class) public void fetchMeta_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetchMeta(bucket, key, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.fetchMeta(bucket, key, meta);
    }
    @Test(expected=RiakResponseException.class) public void fetch_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetch(bucket, key, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.fetch(bucket, key, meta);
    }
    @Test(expected=RiakResponseException.class) public void fetchAll_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.fetch(bucket, key, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.fetchAll(bucket, key, meta);
    }
    @Test(expected=RiakResponseException.class) public void walk_translates_response_runtime_exception_to_checked() throws RiakIOException, RiakResponseException {
        when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenThrow(new RiakResponseRuntimeException(null));
        impl.walk(bucket, key, walkSpec, meta);
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

    @Test public void fetchMeta_returns_null_on_404() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(404);
        when(mockRiakClient.fetchMeta(bucket, key, meta)).thenReturn(mockResponse);

        assertNull(impl.fetchMeta(bucket, key, meta));
    }
    
    @Test(expected=RiakResponseException.class) public void fetchMeta_throws_if_metadata_not_returned() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasObject()).thenReturn(false);
        when(mockRiakClient.fetchMeta(bucket, key, meta)).thenReturn(mockResponse);

        impl.fetchMeta(bucket, key, meta);
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
    
    @Test public void fetch_returns_null_on_404() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(404);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        assertNull(impl.fetch(bucket, key, meta));
    }

    @Test(expected=RiakResponseException.class) public void fetch_throws_if_object_not_returned() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasObject()).thenReturn(false);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        impl.fetch(bucket, key, meta);
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
    
    @Test public void fetchAll_returns_null_on_404() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(404);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        assertNull(impl.fetchAll(bucket, key, meta));
    }

    @Test public void fetchAll_returns_siblings_if_exists() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        final List<JiakObject> siblings = new ArrayList<JiakObject>();

        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasSiblings()).thenReturn(true);
        when(mockResponse.getSiblings()).thenAnswer(new Answer<List<? extends RiakObject>>() {
            public List<? extends RiakObject> answer(InvocationOnMock invocation) throws Throwable {
                return siblings; 
            }
        });
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        assertSame(siblings, impl.fetchAll(bucket, key, meta));
    }

    @Test public void fetchAll_returns_object_if_no_siblings() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasObject()).thenReturn(true);
        when(mockResponse.hasSiblings()).thenReturn(false);
        when(mockResponse.getObject()).thenReturn(object);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        Collection<? extends RiakObject> siblings = impl.fetchAll(bucket, key, meta);
        
        assertEquals(1, siblings.size());
        assertSame(object, siblings.iterator().next());
    }

    @Test(expected=RiakResponseException.class) public void fetchAll_throws_if_object_not_returned() throws RiakIOException, RiakResponseException {
        final FetchResponse mockResponse = mock(FetchResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasObject()).thenReturn(false);
        when(mockRiakClient.fetch(bucket, key, meta)).thenReturn(mockResponse);

        impl.fetchAll(bucket, key, meta);
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

    @Test public void walk_returns_null_on_404() throws RiakIOException, RiakResponseException {
        final WalkResponse mockResponse = mock(WalkResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(404);
        when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenReturn(mockResponse);

        assertNull(impl.walk(bucket, key, walkSpec, meta));
    }

    @Test(expected=RiakResponseException.class) public void walk_throws_if_steps_not_returned() throws RiakIOException, RiakResponseException {
        final WalkResponse mockResponse = mock(WalkResponse.class);
        
        when(mockResponse.getStatusCode()).thenReturn(200);
        when(mockResponse.hasSteps()).thenReturn(false);
        when(mockRiakClient.walk(bucket, key, walkSpec, meta)).thenReturn(mockResponse);
        
        impl.walk(bucket, key, walkSpec, meta);
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
