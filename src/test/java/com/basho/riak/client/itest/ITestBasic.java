package com.basho.riak.client.itest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakFetchResponse;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakStoreResponse;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RawFetchResponse;
import com.basho.riak.client.raw.RawObject;
import com.basho.riak.client.raw.RawStoreResponse;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.HttpResponse;

public class ITestBasic {

    private static String RAW_URL = "http://localhost:8098/raw";
    private static String RAW_VALUE = "value";
    private static String RAW_BUCKET = "raw_itest";
    private static String RAW_KEY = "raw_key";

    private static String JIAK_URL = "http://localhost:8098/raw";
    private static String JIAK_VALUE = "{\"v\":1}";
    private static String JIAK_BUCKET = "jiak_itest";
    private static String JIAK_KEY = "jiak_key";

    private static RequestMeta DELETE_ALL_REPLICAS = RequestMeta.writeParams(null, 3); 
    
    @Test public void store_and_fetch_an_object_from_raw() {
        RawClient c = new RawClient(RAW_URL);
        
        HttpResponse deleteresp = c.delete(RAW_BUCKET, RAW_KEY, DELETE_ALL_REPLICAS);
        assertTrue(deleteresp.isSuccess());

        RawFetchResponse fetchresp = c.fetch(RAW_BUCKET, RAW_KEY); 
        assertEquals(404, fetchresp.getStatusCode());
        
        RawObject o = new RawObject(RAW_BUCKET, RAW_KEY, RAW_VALUE);
        RawStoreResponse storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());
        
        fetchresp = c.fetch(RAW_BUCKET, RAW_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(RAW_VALUE, fetchresp.getObject().getValue());
    }

    @Test public void store_and_fetch_an_object_from_jiak() throws JSONException {
        JiakClient c = new JiakClient(JIAK_URL);
        
        HttpResponse deleteresp = c.delete(JIAK_BUCKET, JIAK_KEY, DELETE_ALL_REPLICAS);
        assertTrue(deleteresp.isSuccess());

        JiakFetchResponse fetchresp = c.fetch(JIAK_BUCKET, JIAK_KEY); 
        assertEquals(404, fetchresp.getStatusCode());
        
        JiakObject o = new JiakObject(JIAK_BUCKET, JIAK_KEY, new JSONObject(JIAK_VALUE));
        JiakStoreResponse storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());
        
        fetchresp = c.fetch(JIAK_BUCKET, JIAK_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(JIAK_VALUE, fetchresp.getObject().getValue());
    }
}
