package com.basho.riak.client.util;

import org.apache.commons.httpclient.HttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.RiakConfig;

public class TestClientHelper {

    @Mock HttpClient mockHttpClient;
    RiakConfig config = new RiakConfig();

    ClientHelper impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        
        impl = new ClientHelper(config);
        impl.setHttpClient(mockHttpClient);
    }
    
    @Test public void convenience_methods_defer_to_main_methods_with_null_meta() {
    }

    @Test public void setBucketSchema_PUTs_to_bucket_URL() {
        
    }

    @Test public void listBucket_GETs_bucket_URL() {
        
    }
    
    @Test public void store_PUTs_object_URL() {
        
    }
    
    @Test public void fetchMeta_HEADs_object_URL() {
        
    }

    @Test public void fetchMeta_adds_default_R_value() {
        
    }

    @Test public void fetch_GETs_object_URL() {
        
    }
    
    @Test public void fetch_adds_default_R_value() {
        
    }
    
    @Test public void stream_GETs_object_URL() {
        
    }

    @Test public void stream_adds_default_R_value() {
        
    }
    
    @Test public void delete_DELETEs_object_URL() {
        
    }
    
    @Test public void walk_GETs_object_URL() {
        
    }

    @Test public void all_methods_add_query_params() {
        
    }

    @Test public void execute_method_adds_headers() {
        
    }

    @Test public void execute_method_with_stream_response_doesnt_consume_stream_or_close_connection() {
        
    }

    @Test public void execute_method_defers_exceptions_to_toss() {
        
    }

    @Test public void toss_doesnt_throw_if_exception_handler_installed() {
        
    }
}
