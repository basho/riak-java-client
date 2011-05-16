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
package com.basho.riak.client.itest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 * 
 */
public abstract class ITestClientBasic {

    protected IRiakClient client;

    @Before public void setUp() throws RiakException {
        this.client = getClient();
    }

    /**
     * @return
     */
    protected abstract IRiakClient getClient() throws RiakException;

    @Test public void fetchBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());
    }

    @Test public void updateBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());

        b = client.updateBucket(b).nVal(4).allowSiblings(true).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(4), b.getNVal());
        assertTrue(b.getAllowSiblings());
    }

    @Test public void createBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.createBucket(bucketName).nVal(1).allowSiblings(true).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(1), b.getNVal());
        assertTrue(b.getAllowSiblings());
    }

    @Test public void clientIds() throws Exception {
        final byte[] clientId = CharsetUtils.utf8StringToBytes("abcd");

        client.setClientId(clientId.clone());
        assertArrayEquals(clientId, client.getClientId());

        byte[] newId = client.generateAndSetClientId();

        assertArrayEquals(newId, client.getClientId());
    }

    @Test public void listBuckets() throws RiakException {
        final String bucket1 = UUID.randomUUID().toString();
        final String bucket2 = UUID.randomUUID().toString();

        Bucket b1 = client.createBucket(bucket1).execute();
        Bucket b2 = client.createBucket(bucket2).execute();

        b1.store("key", "value").execute();
        b2.store("key", "value").execute();

        Set<String> buckets = client.listBuckets();

        assertTrue("Expected bucket 1 to be present", buckets.contains(bucket1));
        assertTrue("Expected bucket 2 to be present", buckets.contains(bucket2));
    }
}
