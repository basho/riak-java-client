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
package com.basho.riak.client.http.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.AllTests;
import com.basho.riak.client.RiakTestProperties;
import com.basho.riak.client.http.BinIndex;
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.http.IntIndex;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakIndex;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.IndexResponse;
import com.basho.riak.client.raw.http.HTTPClientAdapter;

/**
 * @author russell
 * 
 */
public class ITestSecondaryIndexes {

    private static final String TEST_BIN = "test_bin";
    private static final String TEST_INT = "test_int";
    private static final String EMAIL_BIN = "email_bin";

    private RiakClient client;
    private String bucket;

    @Before public void setUp() {
        Assume.assumeTrue(RiakTestProperties.is2iEnabled());
        client = new RiakClient(Hosts.RIAK_URL);
        bucket = getClass().getName();
    }

    @After public void tearDown() throws Exception {
        if (RiakTestProperties.is2iEnabled()) {
            AllTests.emptyBucket(bucket, new HTTPClientAdapter(client));
        }
    }

    @Test public void storeRetrieveQuery() {
        final String key1 = "k";
        final String key2 = "k2";
        final String email = "xavier@domain.com";
        final String email2 = "alfonso@domain.com";
        final String binValue1 = "binVal1";
        final String binValue2 = "binVal2";
        final int intValue1 = 23;
        final int intValue2 = 49;

        RiakObject o = new RiakObject(bucket, key1);
        o.addIndex(TEST_BIN, binValue1).addIndex(TEST_BIN, binValue2).addIndex(EMAIL_BIN, email).addIndex(TEST_INT,
                                                                                                          intValue1);

        RiakObject o2 = new RiakObject(bucket, key2);
        o2.addIndex(EMAIL_BIN, email2).addIndex(TEST_INT, intValue2);

        client.store(o);
        client.store(o2);

        FetchResponse fr = client.fetch(bucket, key1);

        assertTrue(fr.isSuccess());
        assertTrue(fr.hasObject());
        assertFalse(fr.hasSiblings());

        RiakObject retrieved = fr.getObject();

        @SuppressWarnings("rawtypes") List<RiakIndex> indexes = retrieved.getIndexes();

        assertEquals(4, indexes.size());

        assertTrue(indexes.contains(new IntIndex(TEST_INT, intValue1)));
        assertTrue(indexes.contains(new BinIndex(TEST_BIN, binValue1)));
        assertTrue(indexes.contains(new BinIndex(TEST_BIN, binValue2)));
        assertTrue(indexes.contains(new BinIndex(EMAIL_BIN, email)));
        
        IndexResponse ir = client.index(bucket, TEST_INT, intValue1);
        assertTrue(ir.isSuccess());
        List<String> keys = ir.getKeys();
        assertEquals(1, keys.size());
        assertEquals(key1, keys.get(0));
        
        ir = client.index(bucket, TEST_INT, intValue1-10, intValue2+10);
        assertTrue(ir.isSuccess());
        keys = ir.getKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));

        ir = client.index(bucket, TEST_BIN, binValue2);
        assertTrue(ir.isSuccess());
        keys = ir.getKeys();
        assertEquals(1, keys.size());
        assertEquals(key1, keys.get(0));
        
        ir = client.index(bucket, EMAIL_BIN, "a", "z");
        assertTrue(ir.isSuccess());
        keys = ir.getKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));

        ir = client.index(bucket, EMAIL_BIN, "zolanda@domain.com");
        assertTrue(ir.isSuccess());
        keys = ir.getKeys();
        assertEquals(0, keys.size());
    }
}
