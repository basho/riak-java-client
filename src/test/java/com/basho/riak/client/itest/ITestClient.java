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

import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;

import com.basho.riak.newapi.RiakClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakFactory;
import com.basho.riak.newapi.bucket.Bucket;

/**
 * @author russell
 * 
 */
public class ITestClient {

    @Test public void fetchBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();
        RiakClient c = RiakFactory.pbcClient();

        Bucket b = c.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());
    }
    
    @Test public void updateBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();
        RiakClient c = RiakFactory.pbcClient();

        Bucket b = c.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());
        
        b = c.updateBucket(b).nVal(4).allowSiblings(true).execute();
        
        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(4), b.getNVal());
        assertTrue(b.getAllowSiblings());
    }
    
    
    @Test public void createBucket() throws RiakException {
        final String bucketName = UUID.randomUUID().toString();
        RiakClient c = RiakFactory.pbcClient();

        Bucket b = c.createBucket(bucketName).nVal(1).allowSiblings(true).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(1), b.getNVal());
        assertTrue(b.getAllowSiblings());
    }

    @Test public void clientIds() throws Exception {
        final byte[] clientId = "abcd".getBytes("UTF-8");
        RiakClient c = RiakFactory.pbcClient();
        c.setClientId(clientId.clone());
        assertArrayEquals(clientId, c.getClientId());
        
        byte[] newId = c.generateAndSetClientId();
        
        assertArrayEquals(newId, c.getClientId());
    }
}
